(ns rotary.client
  "Amazon DynamoDB client functions."
  (:use [clojure.algo.generic.functor :only (fmap)])
  (:require [clojure.string :as str])
  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.services.dynamodb.AmazonDynamoDBClient
           [com.amazonaws.services.dynamodb.model
            AttributeValue
            Condition
            CreateTableRequest
            UpdateTableRequest
            DescribeTableRequest
            DescribeTableResult
            DeleteTableRequest
            DeleteItemRequest
            GetItemRequest
            GetItemResult
            Key
            KeySchema
            KeySchemaElement
            ProvisionedThroughput
            ProvisionedThroughputDescription
            PutItemRequest
            ResourceNotFoundException
            ScanRequest
            QueryRequest]))

(defn- db-client*
  "Get a AmazonDynamoDBClient instance for the supplied credentials."
  [cred]
  (AmazonDynamoDBClient.
   (BasicAWSCredentials. (:access-key cred) (:secret-key cred))))

(def db-client
  (memoize db-client*))

(defprotocol AsMap
  (as-map [x]))

(defn- key-schema-element
  "Create a KeySchemaElement object."
  [{key-name :name, key-type :type}]
  (doto (KeySchemaElement.)
    (.setAttributeName (str key-name))
    (.setAttributeType (str/upper-case (name key-type)))))

(defn- key-schema
  "Create a KeySchema object."
  [hash-key & [range-key]]
  (let [schema (KeySchema. (key-schema-element hash-key))]
    (when range-key
      (.setRangeKeyElement schema (key-schema-element range-key)))
    schema))

(defn- provisioned-throughput
  "Created a ProvisionedThroughput object."
  [{read-units :read, write-units :write}]
  (doto (ProvisionedThroughput.)
    (.setReadCapacityUnits (long read-units))
    (.setWriteCapacityUnits (long write-units))))

(defn create-table
  "Create a table in DynamoDB with the given map of properties."
  [cred {:keys [name hash-key range-key throughput]}]
  (.createTable
   (db-client cred)
   (doto (CreateTableRequest.)
     (.setTableName (str name))
     (.setKeySchema (key-schema hash-key range-key))
     (.setProvisionedThroughput
      (provisioned-throughput throughput)))))

(defn update-table
  "Update a table in DynamoDB with the given name."
  [cred {:keys [name throughput]}]
  (.updateTable
   (db-client cred)
   (doto (UpdateTableRequest.)
     (.setTableName (str name))
     (.setProvisionedThroughput
      (provisioned-throughput throughput)))))

(extend-protocol AsMap
  KeySchemaElement
  (as-map [element]
    {:name (.getAttributeName element)
     :type (-> (.getAttributeType element)
               (str/lower-case)
               (keyword))})
  KeySchema
  (as-map [schema]
    (merge
     (if-let [e (.getHashKeyElement schema)]  {:hash-key  (as-map e)} {})
     (if-let [e (.getRangeKeyElement schema)] {:range-key (as-map e)} {})))
  ProvisionedThroughputDescription
  (as-map [throughput]
    {:read  (.getReadCapacityUnits throughput)
     :write (.getWriteCapacityUnits throughput)
     :last-decrease (.getLastDecreaseDateTime throughput)
     :last-increase (.getLastIncreaseDateTime throughput)})
  DescribeTableResult
  (as-map [result]
    (let [table (.getTable result)]
      {:name          (.getTableName table)
       :creation-date (.getCreationDateTime table)
       :item-count    (.getItemCount table)
       :key-schema    (as-map (.getKeySchema table))
       :throughput    (as-map (.getProvisionedThroughput table))
       :status        (-> (.getTableStatus table)
                          (str/lower-case)
                          (keyword))})))

(defn describe-table
  "Returns a map describing the table in DynamoDB with the given name, or nil
  if the table does not exist."
  [cred name]
  (try
    (as-map
     (.describeTable
      (db-client cred)
      (doto (DescribeTableRequest.)
        (.setTableName name))))
    (catch ResourceNotFoundException _
      nil)))

(defn ensure-table
  "Creates the table if it does not already exist, updates the provisioned
  throughput if it does."
  [cred {:keys [name hash-key range-key throughput] :as properties}]
  (if-let [table (describe-table cred name)]
    (if (not= throughput (-> table :throughput (select-keys [:read :write])))
      (update-table cred properties))
    (create-table cred properties)))

(defn delete-table
  "Delete a table in DynamoDB with the given name."
  [cred name]
  (.deleteTable
   (db-client cred)
   (DeleteTableRequest. name)))

(defn list-tables
  "Return a list of tables in DynamoDB."
  [cred]
  (-> (db-client cred)
      .listTables
      .getTableNames
      seq))

(defn- to-attr-value
  "Convert a value into an AttributeValue object."
  [value]
  (cond
   (string? value)
   (doto (AttributeValue.) (.setS value))
   (number? value)
   (doto (AttributeValue.) (.setN (str value)))))

(defn- get-value
  "Get the value of an AttributeValue object."
  [attr-value]
  (or (.getS attr-value)
      (.getN attr-value)
      (.getNS attr-value)
      (.getSS attr-value)))

(defn- item-map
  "Turn a item in DynamoDB into a Clojure map."
  [item]
  (if item
    (fmap get-value (into {} item))))

(extend-protocol AsMap
  GetItemResult
  (as-map [result]
    (item-map (.getItem result))))

(defn put-item
  "Add an item (a Clojure map) to a DynamoDB table."
  [cred table item]
  (.putItem
   (db-client cred)
   (doto (PutItemRequest.)
     (.setTableName table)
     (.setItem (fmap to-attr-value item)))))

(defn- item-key
  "Create a Key object from a value."
  [hash-key]
  (Key. (to-attr-value hash-key)))

(defn get-item
  "Retrieve an item from a DynamoDB table by its hash key."
  [cred table hash-key]
  (as-map
   (.getItem
    (db-client cred)
    (doto (GetItemRequest.)
      (.setTableName table)
      (.setKey (item-key hash-key))))))

(defn delete-item
  "Delete an item from a DynamoDB table by its hash key."
  [cred table hash-key]
  (.deleteItem
   (db-client cred)
   (DeleteItemRequest. table (item-key hash-key))))

(defn scan
  "Return the items in a DynamoDB table."
  [cred table]
  (map item-map
       (.getItems
        (.scan
         (db-client cred)
         (ScanRequest. table)))))

(defn- set-range-condition
  "Add the range key condition to a QueryRequest object"
  [query-request operator & [range-key range-end]]
  (let [attribute-list (map (fn [arg] (to-attr-value arg)) (remove nil? [range-key range-end]))]
    (.setRangeKeyCondition query-request
                           (doto (Condition.)
                             (.withComparisonOperator operator)
                             (.withAttributeValueList attribute-list)))))

(defn- resolve-operator
  "Maps Clojure operators to DynamoDB operators"
  [operator]
  (let [operator-map {`> "GT" `>= "GE" `< "LT" `<= "LE" `= "EQ"}
        resolved-operator (get operator-map operator)]
    (or resolved-operator operator)))

(defn- query-request
  "Create a QueryRequest object."
  [table hash-key range-clause {:keys [order limit count consistent]}]
  (let [qr (QueryRequest. table (to-attr-value hash-key))
        [operator range-key range-end] range-clause]
    (when operator
      (set-range-condition qr (resolve-operator operator) range-key range-end))
    (when order
      (.setScanIndexForward qr (not= order :desc)))
    (when limit
      (.setLimit qr (int limit)))
    (when count
      (.setCount qr count))
    (when consistent
      (.setConsistentRead qr consistent))
    qr))

(defn query
  "Return the items in a DynamoDB table matching the supplied hash key.
  Can specify a range clause if the table has a range-key ie. `(>= 234)
  Takes the following options:
    :order - may be :asc or :desc (defaults to :asc)
    :limit - should be a positive integer
    :count - return a count if logical true
    :consistent - return a consistent read if logical true"
  [cred table hash-key & [range-clause options]]
  (map item-map
       (.getItems
        (.query
         (db-client cred)
         (query-request table hash-key range-clause options)))))
