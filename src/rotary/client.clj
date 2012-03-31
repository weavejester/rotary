(ns rotary.client
  "Amazon DynamoDB client functions."
  (:use [clojure.algo.generic.functor :only (fmap)])
  (:require [clojure.string :as str])
  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.services.dynamodb.AmazonDynamoDBClient
           [com.amazonaws.services.dynamodb.model
            AttributeValue
            CreateTableRequest
            UpdateTableRequest
            DeleteTableRequest
            DeleteItemRequest
            GetItemRequest
            Key
            KeySchema
            KeySchemaElement
            ProvisionedThroughput
            PutItemRequest
            ScanRequest]))

(defn- db-client
  "Get a AmazonDynamoDBClient instance for the supplied credentials."
  [cred]
  (AmazonDynamoDBClient.
   (BasicAWSCredentials. (:access-key cred) (:secret-key cred))))

(defn- key-schema-element
  "Create a KeySchemaElement object."
  [[key-name type]]
  (doto (KeySchemaElement.)
    (.setAttributeName (str key-name))
    (.setAttributeType (str/upper-case (name type)))))

(defn- provisioned-throughput
  "Created a ProvisionedThroughput object."
  [{read-units :read, write-units :write}]
  (doto (ProvisionedThroughput.)
    (.setReadCapacityUnits (long read-units))
    (.setWriteCapacityUnits (long write-units))))

(defn create-table
  "Create a table in DynamoDB with the given name and properties."
  [cred name & {:keys [hash-key throughput]}]
  (.createTable
   (db-client cred)
   (doto (CreateTableRequest.)
     (.setTableName (str name))
     (.setKeySchema
      (KeySchema. (key-schema-element hash-key)))
     (.setProvisionedThroughput
      (provisioned-throughput throughput)))))

(defn update-table
  "Update a table in DynamoDB with the given name."
  [cred name & {:keys [throughput]}]
  (.updateTable
   (db-client cred)
   (doto (UpdateTableRequest.)
     (.setTableName (str name))
     (.setProvisionedThroughput
      (provisioned-throughput throughput)))))

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
   (doto (AttributeValue.) (.setN value))))

(defn- get-value
  "Get the value of an AttributeValue object."
  [attr-value]
  (or (.getS attr-value)
      (.getN attr-value)
      (.getNS attr-value)
      (.getSS attr-value)))

(defn- to-map
  "Turn a item in DynamoDB into a Clojure map."
  [item]
  (if item
    (fmap get-value (into {} item))))

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
  (to-map
   (.getItem
    (.getItem
     (db-client cred)
     (doto (GetItemRequest.)
       (.setTableName table)
       (.setKey (item-key hash-key)))))))

(defn delete-item
  "Delete an item from a DynamoDB table by its hash key."
  [cred table hash-key]
  (.deleteItem
   (db-client cred)
   (DeleteItemRequest. table (item-key hash-key))))

(defn scan
  "Return the items in a DynamoDB table."
  [cred table]
  (map to-map
       (.getItems
        (.scan
         (db-client cred)
         (ScanRequest. table)))))
