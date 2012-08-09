(ns unit.test.rotary.client
  (:use [rotary.client])
  (:use [midje
         sweet
         [util :only [testable-privates]]])
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

(def cred {:access-key "AWS_ACCESS_KEY_ID" 
           :secret-key "AWS_SECRET_ACCESS_KEY"})

(testable-privates rotary.client
  db-client* key-schema-element key-schema provisioned-throughput to-attr-value
  get-value item-map item-key set-range-condition normalize-operator
  query-request)

;; db-client*
(let [client (db-client* cred)]
  (facts "We can create a well-formed DynamoDB client."
    client => #(instance? AmazonDynamoDBClient %)))
;;TODO check endpoint, is there a way to get at the config?

;; key-schema-element
(facts "We can create well-formed KeySchemaElement's"
  (let [kse (key-schema-element {:name "ksname" :type :s})]
    kse => #(instance? KeySchemaElement %)
    (.getAttributeName kse) => "ksname"
    (.getAttributeType kse) => "S")
  (let [kse (key-schema-element {:name "ksname" :type :n})]
    kse => #(instance? KeySchemaElement %)
    (.getAttributeName kse) => "ksname"
    (.getAttributeType kse) => "N")
  )

(future-fact "only S and N are valid element types"
  (key-schema-element {:name "ksname" :type :x}) =throws=> Exception
  (key-schema-element {:name "ksname" :type :number}) =throws=> Exception
  (key-schema-element {:name "ksname" :type :string}) =throws=> Exception)

;; key-schema
(facts "We can create well-formed KeySchema's"
  (let [ks (key-schema {:name "hashkey" :type :n})]
    ks => #(instance? KeySchema %)
    (.. ks getHashKeyElement getAttributeName) => "hashkey"
    (.. ks getHashKeyElement getAttributeType) => "N")
  (let [ks (key-schema {:name "hashkey" :type :n} {:name "rangekey" :type :s})]
    ks => #(instance? KeySchema %)
    (.. ks getHashKeyElement getAttributeName) => "hashkey"
    (.. ks getHashKeyElement getAttributeType) => "N"
    (.. ks getRangeKeyElement getAttributeName) => "rangekey"
    (.. ks getRangeKeyElement getAttributeType) => "S"))

;; provisioned-throughput
(facts "We can create well-formed ProvisionedThroughput's"
  (let [pt (provisioned-throughput {:read 1 :write 1})]
    pt => #(instance? ProvisionedThroughput %)
    (.getReadCapacityUnits pt) => 1
    (.getWriteCapacityUnits pt) => 1)
  (let [pt (provisioned-throughput {:read 5 :write 5})]
    pt => #(instance? ProvisionedThroughput %)
    (.getReadCapacityUnits pt) => 5
    (.getWriteCapacityUnits pt) => 5)
  (let [pt (provisioned-throughput {:read 10 :write 2})]
    pt => #(instance? ProvisionedThroughput %)
    (.getReadCapacityUnits pt) => 10
    (.getWriteCapacityUnits pt) => 2))

;; to-attr-value
(tabular
 (fact "We can create well-formed AttributeValue's from strings"
       (.getS (to-attr-value ?s)) => ?s) 
 ?s
 "", "string", " ", ":", "!@#$", "\n", "\t\r\n", "\uffff")

;; TODO crazy DynamoDB numbers
(tabular
 (fact "We can create well-formed AttributeValue's from numbers"
       (.getN (to-attr-value ?n)) => (str ?n)) 
 ?n
 0
 1 1M 1.0M 1.00M 1.0 1.00 1.23 1.234
 -1 -1M -1.0M -1.00M -1.0 -1.00 -1.23 -1.234
 )
;; TODO to-attr-value for "sets" of strings, numbers?

;; get-value
(tabular
 (facts "we can get-value of an AttributeValue"
               (get-value (doto (AttributeValue.) ?set-form)) => ?res) 
 ?set-form ?res
 (.setS "string") "string"
 (.setN "1") "1"
 (.setSS ["foo" "bar"]) ["foo" "bar"]
 (.setNS ["1" "1"]) ["1" "1"])

(let [allowed-values 
      ["", "string", " ", ":", "!@#$", "\n", "\t\r\n", "\uffff"
       0
       1 1M 1.0M 1.00M 1.0 1.00 1.23 1.234
       -1 -1M -1.0M -1.00M -1.0 -1.00 -1.23 -1.234]]
  (future-fact "(comp get-value to-attr-value) is the identity"
    (map (comp get-value to-attr-value) allowed-values) => allowed-values))

;; TODO returns an ArrayList, which is seqable.  sufficient?

;; item-map
(let [item (.getItem (doto (GetItemResult.)
                       (.setItem (zipmap ["x" "y"]
                                         (map to-attr-value ["ex" 42])))))]
  (fact "we can convert an Item into a map"
        (item-map item) => {"x" "ex", "y", "42"}))

;; item-key
(tabular
 (fact "We can create a well-formed Key."
   (let [k (item-key ?val)]
     k => #(instance? Key %)
     (get-value (.getHashKeyElement k)) => (str ?val)))
 ?val
 "", "string", " ", ":", "!@#$", "\n", "\t\r\n", "\uffff"
 0
 1 1M 1.0M 1.00M 1.0 1.00 1.23 1.234
 -1 -1M -1.0M -1.00M -1.0 -1.00 -1.23 -1.234
 )
;;TODO hash+range key?

;;TODO set-range-condition
;; (against-background
;;  [(around :facts
;;     (let [qreq (QueryRequest. "tablename" (to-attr-value "hashkey"))]
;;       ?form))]
;;  (tabular
;;   (fact "we can set a range condition on a query request"
;;     (apply set-range-condition qreq ?range)
;;     => FOO)
;;    ?range
;;    [`< 3])
;;  )

;;; TODO test:
;; query-request
;; normalize-operator
;; (map normalize-operator '[ns/< < > <= >= =])
;; ;=> ("LT" "LT" "GT" "LE" "GE" "EQ")

;;; TODO test as-map for:
;; KeySchemaElement KeySchema ProvisionedThroughputDescription
;; DescribeTableResult GetItemResult

