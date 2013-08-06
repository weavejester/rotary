(ns rotary.client
  "Amazon DynamoDB client functions."
  (:use [clojure.algo.generic.functor :only (fmap)]
        [clojure.core.incubator :only (-?>>)])
  (:require [clojure.string :as str])
  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
           [com.amazonaws.services.dynamodbv2.model
            AttributeValue
            AttributeDefinition
            BatchGetItemRequest
            BatchGetItemResult
            BatchWriteItemRequest
            BatchWriteItemResult
            Condition
            CreateTableRequest
            UpdateTableRequest
            DescribeTableRequest
            DescribeTableResult
            DeleteTableRequest
            DeleteItemRequest
            DeleteRequest
            GetItemRequest
            GetItemResult
            KeySchemaElement
            KeysAndAttributes
            LocalSecondaryIndex
            Projection
            ProvisionedThroughput
            ProvisionedThroughputDescription
            PutItemRequest
            PutRequest
            ResourceNotFoundException
            ScanRequest
            QueryRequest
            WriteRequest]))

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

(defn- to-long [x] (Long. x))

(defn- get-value
  "Get the value of an AttributeValue object."
  [attr-value]
  (or (.getS attr-value)
      (-?>> (.getN attr-value) to-long)
      (-?>> (.getNS attr-value) (map to-long) (into #{}))
      (-?>> (.getSS attr-value) (into #{}))))

(defn- key-schema-element
  "Create a KeySchemaElement object."
  [key-name key-type]
  (doto (KeySchemaElement.)
    (.setAttributeName (str key-name))
    (.setKeyType (str/upper-case (name key-type)))))

(defn- key-schema
  "Create a KeySchema object."
  [hash-key & [range-key]]
  (let [schema [(key-schema-element (:name hash-key) :hash)]]
    (if-not (nil? range-key)
      (conj schema (key-schema-element (:name range-key) :range))
      schema)))

(defn- provisioned-throughput
  "Created a ProvisionedThroughput object."
  [{read-units :read, write-units :write}]
  (doto (ProvisionedThroughput.)
    (.setReadCapacityUnits (long read-units))
    (.setWriteCapacityUnits (long write-units))))

(defn- create-projection
  "Creates a Projection Object"
  [projection & [included-attrs]]
  (let [pr (Projection.)]
    (.setProjectionType pr (str/upper-case (name projection)))
    (when included-attrs
      (.setNonKeyAttributes pr included-attrs))
    pr))

(defn- local-index
  "Creates a LocalSecondaryIndex Object"
  [hash-key {:keys [name range-key projection included-attrs] :or {projection :all}}]
  (doto (LocalSecondaryIndex.)
    (.setIndexName name)
    (.setKeySchema
      (key-schema hash-key range-key))
    (.setProjection
      (create-projection projection included-attrs))))

(defn- local-indexes
  "Creates a vector of LocalSecondaryIndexes"
  [hash-key indexes]
  (map (partial local-index hash-key) indexes))

(defn- attribute-definition
  "Creates an AttributeDefinition Object"
  [{key-name :name key-type :type}]
  (doto (AttributeDefinition.)
    (.setAttributeName key-name)
    (.setAttributeType (str/upper-case (name key-type)))))

(defn- attribute-definitions
  "Creates a vector of AttributeDefinition Objects"
  [defs]
  (map attribute-definition defs))

(defn create-table
  "Create a table in DynamoDB with the given map of properties. The properties
  available are:
    :name - the name of the table (required)
    :hash-key - a map that defines the hash key name and type (required)
    :range-key - a map that defines the range key name and type (optional)
    :throughput - a map that defines the read and write throughput (required)
    :indexes - a vector of maps that defines local secondary indexes on a table (optional)

  The hash-key and range-key definitions are maps with the following keys:
    :name - the name of the key
    :type - the type of the key (:s, :n, :ss, :ns)

  Where :s is a string type, :n is a number type, and :ss and :ns are sets of
  strings and number respectively. 

  The throughput is a map with two keys:
    :read - the provisioned number of reads per second
    :write - the provisioned number of writes per second
  
  The indexes vector is a vector of maps with two keys and two further optional ones
    :name - the name of the Local Secondary Index (required)
    :range-key - a map that defines the range key name and type (required)
    :projection - keyword that defines the projection may be:
    :all, :keys_only, :include (optional - default is :keys-only)
    :included-attrs - a vector of attribute names when :projection is :include (optional)"
  [cred {:keys [name hash-key range-key throughput indexes]}]
  (.createTable
    (db-client cred)
    (let [defined-attrs (->> (conj [] hash-key range-key)
                             (concat (map #(:range-key %) indexes))
                             (remove nil?))]
      (doto (CreateTableRequest.)
        (.setTableName (str name))
        (.setKeySchema (key-schema hash-key range-key))
        (.setAttributeDefinitions (attribute-definitions defined-attrs))
        (.setProvisionedThroughput
          (provisioned-throughput throughput))
        (.setLocalSecondaryIndexes
          (local-indexes hash-key indexes))))))

(defn update-table
  "Update a table in DynamoDB with the given name. Only the throughput may be
  updated. The throughput values can be increased by no more than a factor of
  two over the current values (e.g. if your read throughput was 20, you could
  only set it from 1 to 40). See create-table."
  [cred {:keys [name throughput]}]
  (.updateTable
   (db-client cred)
   (doto (UpdateTableRequest.)
     (.setTableName (str name))
     (.setProvisionedThroughput
      (provisioned-throughput throughput)))))

(extend-protocol AsMap
  java.util.ArrayList
  (as-map [alist]
    (into [] (map as-map alist)))
  java.util.HashMap
  (as-map [hmap]
    (fmap as-map (into {} hmap)))
  AttributeValue
  (as-map [aval]
    (get-value aval))
  KeySchemaElement
  (as-map [element]
    {:name (.getAttributeName element)})
  ProvisionedThroughputDescription
  (as-map [throughput]
    {:read (.getReadCapacityUnits throughput)
     :write (.getWriteCapacityUnits throughput)
     :last-decrease (.getLastDecreaseDateTime throughput)
     :last-increase (.getLastIncreaseDateTime throughput)})
  DescribeTableResult
  (as-map [result]
    (let [table (.getTable result)]
      {:name (.getTableName table)
       :creation-date (.getCreationDateTime table)
       :item-count (.getItemCount table)
       :key-schema (as-map (.getKeySchema table))
       :throughput (as-map (.getProvisionedThroughput table))
       :status (-> (.getTableStatus table)
                          (str/lower-case)
                          (keyword))}))
  BatchWriteItemResult
  (as-map [result]
    {:unprocessed-items (into {} (.getUnprocessedItems result))})
  KeysAndAttributes
  (as-map [result]
    (merge
      (if-let [a (.getAttributesToGet result)] {:attrs (into [] a)} {})
      (if-let [c (.getConsistentRead result)] {:consistent c} nil)
      (if-let [k (.getKeys result)] {:keys (fmap as-map (into [] k))} {})))
  BatchGetItemResult
  (as-map [result]
    {:responses (fmap #(as-map %) (into {} (.getResponses result)))
     :unprocessed-keys (into {} (.getUnprocessedKeys result))}))

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
  "Creates the table if it does not already exist."
  [cred {name :name :as properties}]
  (if-not (describe-table cred name)
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

(defn- set-of [f s]
  (and (set? s) (every? f s)))

(defn- to-attr-value
  "Convert a value into an AttributeValue object."
  [value]
  (cond
   (string? value) (doto (AttributeValue.) (.setS value))
   (number? value) (doto (AttributeValue.) (.setN (str value)))
   (set-of string? value) (doto (AttributeValue.) (.setSS value))
   (set-of number? value) (doto (AttributeValue.) (.setNS (map str value)))
   (set? value) (throw (Exception. "Set must be all numbers or all strings"))
   :else (throw (Exception. (str "Unknown value type: " (type value))))))

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
     (.setItem
      (into {}
            (for [[k v] item]
              [(name k) (to-attr-value v)]))))))

(defn- item-key
  "Create a new Key Map"
  [key]
  (into {} (map (fn [[k v]] {(name k) (to-attr-value v)}) key)))

(defn get-item
  "Retrieve an item from a DynamoDB table by its hash key.
  hash-key should be specified as a map of {:attr-name value}."
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

(extend-protocol AsMap
  nil
  (as-map [_] nil))

(defn- batch-item-keys [request-or-keys]
  (for [key (:keys request-or-keys request-or-keys)]
    (item-key {(:key-name request-or-keys) key})))

(defn- keys-and-attrs [{:keys [attrs consistent] :as request}]
  (let [kaa (KeysAndAttributes.)]
    (.setKeys kaa (batch-item-keys request))
    (when attrs
      (.setAttributesToGet kaa attrs))
    (when consistent
      (.setConsistentRead kaa consistent))
    kaa))

(defn- batch-request-items [requests]
  (into {}
    (for [[k v] requests]
      [(name k) (keys-and-attrs v)])))

(defn batch-get-item
  "Retrieve a batch of items in a single request. DynamoDB limits
  apply - 100 items and 1MB total size limit. Requested items
  which were elided by Amazon are available in the returned map
  key :unprocessed-keys.

  Examples:
  (batch-get-item cred
    {:users {:keys [\"alice\" \"bob\"]
     :key-name \"names\"}
     :posts {:key-name \"id\"
     :keys [1 2 3]
     :attrs [\"timestamp\" \"subject\"]
     :consistent true}})"
  [cred requests]
  (as-map
    (.batchGetItem
      (db-client cred)
      (doto (BatchGetItemRequest.)
        (.setRequestItems (batch-request-items requests))))))

(defn- delete-request [item]
  (doto (DeleteRequest.)
    (.setKey (item-key item))))

(defn- put-request [item]
  (doto (PutRequest.)
    (.setItem
      (into {}
        (for [[id field] item]
          {(name id) (to-attr-value field)})))))

(defn- write-request [verb item]
  (let [wr (WriteRequest.)]
    (case verb
      :delete (.setDeleteRequest wr (delete-request item))
      :put (.setPutRequest wr (put-request item)))
    wr))

(defn batch-write-item
  "Execute a batch of Puts and/or Deletes in a single request.
  DynamoDB limits apply - 25 items max. No transaction
  guarantees are provided, nor conditional puts.
  Example:
  (batch-write-item cred
    [:put :users {:user-id 1 :username \"sally\"}]
    [:put :users {:user-id 2 :username \"jane\"}]
    [:delete :users {:hash-key 3}])"
  [cred & requests]
  (as-map
    (.batchWriteItem
      (db-client cred)
      (doto (BatchWriteItemRequest.)
        (.setRequestItems
         (fmap
          (fn [reqs]
            (reduce
             #(conj %1 (write-request (first %2) (last %2)))
             []
             (partition 3 (flatten reqs))))
          (group-by #(name (second %)) requests)))))))

(defn- result-map [results]
  {:items (map item-map (.getItems results))
   :count (.getCount results)
   :last-key (as-map (.getLastEvaluatedKey results))})

(defn- scan-request
  "Create a ScanRequest object."
  [table {:keys [limit count after]}]
  (let [sr (ScanRequest. table)]
    (when limit
      (.setLimit sr (int limit)))
    (when count
      (.setCount sr count))
    (when after
      (.setExclusiveStartKey sr (item-key after)))
    sr))

(defn scan
  "Return the items in a DynamoDB table. Takes the following options:
    :limit - the maximum number of items to return
    :after - only return results after this key

  The items are returned as a map with the following keys:
    :items - the list of items returned
    :count - the count of items matching the query
    :last-key - the last evaluated key (useful for paging) "
  [cred table & [options]]
  (result-map
   (.scan
    (db-client cred)
    (scan-request table options))))

(defn- set-hash-condition
  "Create a map of specifying the hash-key condition for query"
  [hash-key]
  (fmap #(doto (Condition.)
           (.setComparisonOperator "EQ")
           (.setAttributeValueList [(to-attr-value %)]))
        hash-key))

(defn- set-range-condition
  "Add the range key condition to a QueryRequest object"
  [range-key operator & [range-value range-end]]
  (let [attribute-list (->> [range-value range-end] (remove nil?) (map to-attr-value))]
    {range-key (doto (Condition.)
                 (.setComparisonOperator operator)
                 (.setAttributeValueList attribute-list))}))

(defn- normalize-operator
  "Maps Clojure operators to DynamoDB operators"
  [operator]
  (let [operator-map {:> "GT" :>= "GE" :< "LT" :<= "LE" := "EQ"}
        op (->> operator name str/upper-case)]
    (operator-map (keyword op) op)))

(defn- query-request
  "Create a QueryRequest object."
  [table hash-key range-clause {:keys [order limit after count consistent attrs index]}]
  (let [qr (QueryRequest.)
        hash-clause (set-hash-condition hash-key)
        [range-key operator range-value range-end] range-clause
        query-conditions (if (nil? operator) 
                           hash-clause
                           (merge hash-clause (set-range-condition range-key
                                                                   (normalize-operator operator)
                                                                   range-value
                                                                   range-end)))]
    (.setTableName qr table)
    (.setKeyConditions qr query-conditions)
    (when attrs
      (.setAttributesToGet qr (map name attrs)))
    (when order
      (.setScanIndexForward qr (not= order :desc)))
    (when limit
      (.setLimit qr (int limit)))
    (when count
      (.setCount qr count))
    (when consistent
      (.setConsistentRead qr consistent))
    (when after
      (.setExclusiveStartKey qr (item-key after)))
    (when index
      (.setIndexName qr index))
    qr))

(defn- extract-range [[range options]]
  (if (and (map? range) (nil? options))
    [nil range]
    [range options]))

(defn query
  "Return the items in a DynamoDB table matching the supplied hash key,
  defined in the form {\"hash-attr\" hash-value}.
  Can specify a range clause if the table has a range-key ie. `(\"range-attr\" >= 234)
  Takes the following options:
    :order - may be :asc or :desc (defaults to :asc)
    :attrs - limit the values returned to the following attribute names
    :limit - the maximum number of items to return
    :after - only return results after this key
    :consistent - return a consistent read if logical true
    :index - the secondary index to query

  The items are returned as a map with the following keys:
    :items - the list of items returned
    :count - the count of items matching the query
    :last-key - the last evaluated key (useful for paging)"
  [cred table hash-key & range-and-options]
  (let [[range options] (extract-range range-and-options)]
    (result-map
     (.query
      (db-client cred)
      (query-request table hash-key range options)))))
