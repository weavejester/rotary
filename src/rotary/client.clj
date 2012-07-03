(ns rotary.client
  "Amazon DynamoDB client functions."
  (:use [clojure.algo.generic.functor :only (fmap)])
  (:require [clojure.string :as str])
  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.services.dynamodb.AmazonDynamoDBClient
           [com.amazonaws.services.dynamodb.model
            AttributeValue
            AttributeValueUpdate
            Condition
            CreateTableRequest
            DeleteItemRequest
            DeleteItemResult
            DeleteTableRequest
            DescribeTableRequest
            DescribeTableResult
            ExpectedAttributeValue
            GetItemRequest
            GetItemResult
            Key
            KeySchema
            KeySchemaElement
            ProvisionedThroughput
            ProvisionedThroughputDescription
            PutItemRequest
            PutItemResult
            QueryRequest
            ResourceNotFoundException
            ScanRequest
            UpdateItemRequest
            UpdateTableRequest]))

(defn- db-client*
  "Get a AmazonDynamoDBClient instance for the supplied credentials."
  [cred]
  (let [aws-creds (BasicAWSCredentials. (:access-key cred) (:secret-key cred))
        client (AmazonDynamoDBClient. aws-creds)]
    (when-let [endpoint (:endpoint cred)]
      (.setEndpoint client endpoint))
    client))

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
      {:name             (.getTableName table)
       :creation-date    (.getCreationDateTime table)
       :item-count       (.getItemCount table)
       :table-size-bytes (.getTableSizeBytes table)
       :key-schema       (as-map (.getKeySchema table))
       :throughput       (as-map (.getProvisionedThroughput table))
       :status           (-> (.getTableStatus table)
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

(defn- normalize-operator [operator]
  "Maps Clojure operators to DynamoDB operators"
  (let [operator-map {:> "GT" :>= "GE" :< "LT" :<= "LE" := "EQ"}
        op (->> operator name str/upper-case)]
    (operator-map (keyword op) op)))

(defn- to-attr-value
  "Convert a value into an AttributeValue object."
  [value]
  (cond
   (string? value)
   (doto (AttributeValue.) (.setS value))
   (number? value)
   (doto (AttributeValue.) (.setN (str value)))
   (coll? value)
   (cond
    (string? (first value))
    (doto (AttributeValue.) (.setSS value))
    (number? (first value))
    (doto (AttributeValue.) (.setNS (map str value))))))

(defn- to-attr-value-update
  "Convert an action and a value into an AttributeValueUpdate object."
  [value-clause]
  (let [[action value] value-clause]
    (AttributeValueUpdate. (to-attr-value value) (normalize-operator action))))

(defn- get-value
  "Get the value of an AttributeValue object."
  [attr-value]
  (or (.getS attr-value)
      (.getN attr-value)
      (if-let [v (.getNS attr-value)] (into #{} v))
      (if-let [v (.getSS attr-value)] (into #{} v))))

(defn- to-expected-value
  "Convert a value to an ExpectedValue object. Handles ::exists
  and ::not-exists specially."
  [value]
  (cond (= value ::exists) (ExpectedAttributeValue. true)
        (= value ::not-exists) (ExpectedAttributeValue. false)
        :else (if-let [av (to-attr-value value)] (ExpectedAttributeValue. av))))

(defn- to-expected-values
  [values]
  (and (seq values)
       (fmap to-expected-value values)))

(defn- item-map
  "Turn a item in DynamoDB into a Clojure map."
  [item]
  (if item
    (fmap get-value (into {} item))))

(extend-protocol AsMap
  GetItemResult
  (as-map [result]
    (with-meta
      (item-map (or (.getItem result) {}))
      {:consumed-capacity-units (.getConsumedCapacityUnits result)}))

  PutItemResult
  (as-map [result]
    (with-meta
      (item-map (or (.getAttributes result) {}))
      {:consumed-capacity-units (.getConsumedCapacityUnits result)}))

  DeleteItemResult
  (as-map [result]
    (with-meta
      (item-map (or (.getAttributes result) {}))
      {:consumed-capacity-units (.getConsumedCapacityUnits result)})))

(defn put-item
  "Add an item (a Clojure map) to a DynamoDB table.
  Takes the following options:
    :expected - a map from attribute name to:
       :rotary.client/exists - checks if the attribute exists
       :rotary.client/not-exists - checks if the attribute doesn't exist
       anything else - checks if the attribute is equal to it
    :return-values - specify what to return:
       \"NONE\", \"ALL_OLD\"

  The metadata of the return value contains:
    :consumed-capacity-units - the consumed capacity units"
  [cred table item & {:keys [expected return-values]}]
  (as-map
   (.putItem
    (db-client cred)
    (doto (PutItemRequest.)
      (.setTableName table)
      (.setItem (fmap to-attr-value item))
      (.setExpected (to-expected-values expected))
      (.setReturnValues (or return-values "NONE"))))))

(defn- item-key
  "Create a Key object from a value."
  ([key]
     (cond
      (nil? key) nil
      (vector? key) (item-key (first key) (second key))
      :else (item-key key nil)))
  ([hash-key range-key]
     (Key. (to-attr-value hash-key)
           (to-attr-value range-key))))

(defn- decode-key [k]
  (let [hash (.getHashKeyElement k)
        range (.getRangeKeyElement k)]
    (if range
      [(get-value hash) (get-value range)]
      (get-value hash))))

(defn update-item
  "Update an item (a Clojure map) in a DynamoDB table.

  The key can be: hash-key, [hash-key], or [hash-key range-key]

  Update map is a map from attribute name to [action value], where
  action is one of :add, :put, or :delete.

  Takes the following options:
    :expected - a map from attribute name to:
       :rotary.client/exists - checks if the attribute exists
       :rotary.client/not-exists - checks if the attribute doesn't exist
       anything else - checks if the attribute is equal to it
    :return-values - specify what to return:
       \"NONE\", \"ALL_OLD\", \"UPDATED_OLD\", \"ALL_NEW\", \"UPDATED_NEW\""
  [cred table key update-map & {:keys [expected return-values]}]
  (letfn [(to-attr-value-update-map [x] [(first x) (to-attr-value-update (last x))])]
    (let [attribute-update-map (into {} (map to-attr-value-update-map update-map))]
      (.updateItem
       (db-client cred)
       (doto (UpdateItemRequest.)
         (.setTableName table)
         (.setKey (item-key key))
         (.setAttributeUpdates attribute-update-map)
         (.setExpected (to-expected-values expected))
         (.setReturnValues return-values))))))

(defn get-item
  "Retrieve an item (a Clojure map) from a DynamoDB table by its key.

  The key can be: hash-key, [hash-key], or [hash-key range-key]

  Options can be:
    :consistent - consistent read
    :attributes-to-get - a list of attribute names to return

  The metadata of the return value contains:
    :consumed-capacity-units - the consumed capacity units"
  [cred table key & {:keys [consistent attributes-to-get] :or {consistent false}}]
  (as-map
   (.getItem
    (db-client cred)
    (doto (GetItemRequest.)
      (.setTableName table)
      (.setKey (item-key key))
      (.setConsistentRead consistent)
      (.setAttributesToGet attributes-to-get)))))

(defn delete-item
  "Delete an item from a DynamoDB table specified by its key, if the
  coditions are met. Takes the following options:
    :expected - a map from attribute name to:
       :rotary.client/exists - checks if the attribute exists
       :rotary.client/not-exists - checks if the attribute doesn't exist
       anything else - checks if the attribute is equal to it
    :return-values - specify what to return:
       \"NONE\", \"ALL_OLD\", \"UPDATED_OLD\", \"ALL_NEW\", \"UPDATED_NEW\"

  The metadata of the return value contains:
    :consumed-capacity-units - the consumed capacity units"
  [cred table key & {:keys [expected return-values]}]
  (as-map
   (.deleteItem
    (db-client cred)
    (doto (DeleteItemRequest. table (item-key key))
      (.setExpected (to-expected-values expected))
      (.setReturnValues return-values)))))

(defn- build-condition
  [operator & values]
  (let [attribute-list (map to-attr-value (remove nil? values))]
    (doto (Condition.)
      (.withComparisonOperator (normalize-operator operator))
      (.withAttributeValueList attribute-list))))

(defn- query-request
  "Create a QueryRequest object."
  [table hash-key range-clause {:keys [order limit count consistent attributes-to-get exclusive-start-key]}]
  (let [qr (QueryRequest. table (to-attr-value hash-key))
        [operator range-key range-end] range-clause]
    (when operator
      (.setRangeKeyCondition qr (build-condition operator range-key range-end)))
    (when order
      (.setScanIndexForward qr (not= order :desc)))
    (when limit
      (.setLimit qr (int limit)))
    (when count
      (.setCount qr count))
    (when consistent
      (.setConsistentRead qr consistent))
    (when attributes-to-get
      (.setAttributesToGet qr attributes-to-get))
    (when exclusive-start-key
      (.setExclusiveStartKey qr (item-key exclusive-start-key)))
    qr))

(defn query
  "Return the items in a DynamoDB table matching the supplied hash key.
  Can specify a range clause if the table has a range-key ie. `(>= 234)
  Takes the following options:
    :order - may be :asc or :desc (defaults to :asc)
    :limit - should be a positive integer
    :count - return a count if logical true
    :consistent - return a consistent read if logical true
    :attributes-to-get - a list of attribute names
    :exclusive-start-key - primary key of the item from which to
         continue an earlier query

  The metadata of the return value contains:
    :count - the number of items in the response
    :consumed-capacity-units - the consumed capacity units
    :last-evaluated-key - the primary key of the item where the query
         operation stopped, or nil if the query is fully completed. It
         can be used to continue the operation by supplying it as a
         value to :exclusive-start-key"
  [cred table hash-key range-clause & {:keys [order limit count consistent attributes-to-get exclusive-start-key] :as options}]
  (let [query-result (.query
                      (db-client cred)
                      (query-request table hash-key range-clause options))
        item-count   (.getCount query-result)]
    (with-meta
      (map item-map (or (.getItems query-result) {}))
      (merge {:count item-count
              :consumed-capacity-units (.getConsumedCapacityUnits query-result)}
             (if-let [k (.getLastEvaluatedKey query-result)]
               {:last-evaluated-key (decode-key k)})))))

(defn- to-scan-filter
  [scan-filter]
  (when scan-filter
    (fmap #(apply build-condition %) scan-filter)))

(defn scan
  "Return the items in a DynamoDB table.

  The scan-filter is a map from attribute name to condition in the
  form [op param1 param2 ...].

  Takes the following options:
    :limit - should be a positive integer
    :count - return a count if logical true
    :attributes-to-get - a list of attribute names
    :exclusive-start-key - primary key of the item from which to
         continue an earlier query

  The metadata of the return value contains:
    :count - the number of items in the response
    :scanned-count - number of items in the complete scan before any
         filters are applied
    :consumed-capacity-units - the consumed capacity units
    :last-evaluated-key - the primary key of the item where the scan
         operation stopped, or nil if the scan is fully completed. It
         can be used to continue the operation by supplying it as a
         value to :exclusive-start-key"
  [cred table scan-filter & {:keys [limit count attributes-to-get exclusive-start-key]}]
  (let [result (.scan
                (db-client cred)
                (doto (ScanRequest. table)
                  (.setScanFilter (to-scan-filter scan-filter))
                  (.setLimit limit)
                  (.setCount count)
                  (.setAttributesToGet attributes-to-get)
                  (.setExclusiveStartKey (item-key exclusive-start-key))))]
    (with-meta
      (map item-map (or (.getItems result) {}))
      (merge {:count (.getCount result)
              :scanned-count (.getScannedCount result)
              :consumed-capacity-units (.getConsumedCapacityUnits result)}
             (if-let [k (.getLastEvaluatedKey result)]
               {:last-evaluated-key (decode-key k)})))))
