(ns rotary.client
  "Amazon DynamoDB client functions."
  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.services.dynamodb.AmazonDynamoDBClient
           [com.amazonaws.services.dynamodb.model
            AttributeValue
            CreateTableRequest
            KeySchema
            KeySchemaElement
            ProvisionedThroughput
            PutItemRequest]))

(defn- db-client
  "Get a AmazonDynamoDBClient instance for the supplied credentials."
  [cred]
  (AmazonDynamoDBClient.
   (BasicAWSCredentials. (:access-key cred) (:secret-key cred))))

(defn- key-schema-element
  "Create a KeySchemaElement object."
  [name type]
  (doto (KeySchemaElement.)
    (.setAttributeName (str name))
    (.setAttributeType (str type))))

(defn- provisioned-throughput
  "Created a ProvisionedThroughput object."
  [read-units write-units]
  (doto (ProvisionedThroughput.)
    (.setReadCapacityUnits (long read-units))
    (.setWriteCapacityUnits (long write-units))))

(defn create-table
  "Create a table in DynamoDB with the given name and hash-key."
  [cred name hash-key]
  (.createTable
   (db-client cred)
   (doto (CreateTableRequest.)
     (.setTableName (str name))
     (.setKeySchema
      (KeySchema. (key-schema-element name "S")))
     (.setProvisionedThroughput
      (provisioned-throughput 10 10)))))

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

(defn- to-attr-map
  "Convert a map's values into AttributeValue objects."
  [value-map]
  (into {} (for [[k v] value-map] [k (to-attr-value v)])))

(defn put-item
  "Add an item (a Clojure map) to a DynamoDB table."
  [cred table item]
  (.putItem
   (db-client cred)
   (doto (PutItemRequest.)
     (.setTableName table)
     (.setItem (to-attr-map item)))))
