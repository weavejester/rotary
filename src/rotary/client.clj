(ns rotary.client
  "Amazon DynamoDB client functions."
  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.services.dynamodb.AmazonDynamoDBClient
           [com.amazonaws.services.dynamodb.model
            CreateTableRequest
            KeySchema
            KeySchemaElement
            ProvisionedThroughput]))

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
