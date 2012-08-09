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
  (key-schema-element {:name "ksname" :type :x}) => (throws Exception)
  (key-schema-element {:name "ksname" :type :number}) => (throws Exception)
  (key-schema-element {:name "ksname" :type :string}) => (throws Exception))

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

;; ;;set-range-condition
;; ;;TODO should we test this?  All the conversion logic is in query-request.
;; (against-background
;;  [(around :facts
;;     (let [qreq (QueryRequest. "tablename" (to-attr-value "hashkey"))]
;;       ?form))]
;;  (tabular
;;   (fact "we can set a range condition on a query request"
;;         (let [qreq (apply set-range-condition qreq ?range)
;;               rkc (.getRangeKeyCondition qreq)
;;               cop (.getComparisonOperator rkc)
;;               avlist (.getAttributeValueList rkc)
;;               vlist (map get-value avlist)]
;;           [qreq
;;            rkc
;;            cop
;;            vlist])
;;         => (just [#(instance? QueryRequest %)
;;                   #(instance? Condition %)
;;                   ?cop
;;                   ?vlist]))
;;    ?range ?cop ?vlist
;;    [`< 42] "LT" ["42"]
;;    ['any-ns/< 42] "LT" ["42"]
;;    [`> 42] "GT" ["42"]
;;    [`<= 42] "LE" ["42"]
;;    [`>= 42] "GE" ["42"]
;;    [`= 42] "EQ" ["42"]

;;    [`< 42 99] "LT" ["42" "99"]
;;    [`< "aardvark" "zebra"] "LT" ["aardvark" "zebra"]
;;    )
;;  )

;; normalize-operator
(fact "we can convert comparison operators to strings DynamoDB understands"
  (map normalize-operator '[ns/< < > <= >= =]) 
  => [ "LT" "LT" "GT" "LE" "GE" "EQ"])

(fact "the ns doesn't matter when normalizing operators"
      (map normalize-operator ['< `< 'any-ns/< 'at-all/<])
      => ["LT" "LT" "LT" "LT"])

;; query-request
(tabular
 (fact "we can create a QueryRequest, and it has the fields we expect"
       (let [qreq (query-request ?table ?hash-key ?range-clause ?kwargs)
             [count? hkval limit
              rkcond scan-forward?
              tname is-count? is-scan-forward?]
                (map #(% qreq)
                     (eval (vec (map (fn [m] `(memfn ~m))
                                  '[getCount getHashKeyValue getLimit
                                    getRangeKeyCondition getScanIndexForward
                                    getTableName isCount isScanIndexForward]))))]
         [tname (get-value hkval) 
          (.getComparisonOperator rkcond)
          (map get-value (.getAttributeValueList rkcond))
          limit count? is-count? scan-forward? is-scan-forward?])
       => (just ?retval))

 ?table ?hash-key ?range-clause ?kwargs, ?retval

 "AnotherTable" 22 `(> 13392) {},
 ["AnotherTable" "22" "GT" ["13392"] nil? nil? nil? nil? nil?]

 "AnotherTable" 22 `(> 13392) {:limit 100},
 ["AnotherTable" "22" "GT" ["13392"] 100 falsey falsey falsey falsey]

 "AnotherTable" 22 `(> 13392) {:count true},
 ["AnotherTable" "22" "GT" ["13392"] falsey TRUTHY TRUTHY falsey falsey]

 "AnotherTable" 22 `(> 13392) {:count false},
 ["AnotherTable" "22" "GT" ["13392"] falsey falsey falsey falsey falsey]

 "AnotherTable" 22 `(> 13392) {:order :asc},
 ["AnotherTable" "22" "GT" ["13392"] falsey falsey falsey TRUTHY TRUTHY]

 "AnotherTable" 22 `(> 13392) {:order :desc},
 ["AnotherTable" "22" "GT" ["13392"] falsey falsey falsey falsey falsey]

 ;; different range clauses
 "AnotherTable" 22 `(< 13392) {},
 ["AnotherTable" "22" "LT" ["13392"] nil? nil? nil? nil? nil?]

 "AnotherTable" 22 `(<= 13392) {},
 ["AnotherTable" "22" "LE" ["13392"] nil? nil? nil? nil? nil?]

 "AnotherTable" 22 `(>= 13392) {},
 ["AnotherTable" "22" "GE" ["13392"] nil? nil? nil? nil? nil?]

 "AnotherTable" 22 `(= 13392) {},
 ["AnotherTable" "22" "EQ" ["13392"] nil? nil? nil? nil? nil?]
 )
;;TODO add test for :consistent arg
;;TODO tests that should break things, but don't (:limit 0, etc)
;;TODO between, not=, in ?

(tabular
 (fact "as-map and key-schema-element are inverses for KeySchemaElement's"
       (as-map (key-schema-element ?map)) => ?map)
 ?map
 {:name "name" :type :s}
 {:name "name" :type :n}

 {:name "" :type :s}  ;;TODO uh-oh.
 {:name "a" :type :s}
 {:name "A" :type :s}
 )

(fact "as-map can't handle empty KeySchemaElement's"
      (as-map (KeySchemaElement.)) => (throws java.lang.NullPointerException))


(tabular
 (facts "as-map and key-schema are approximate inverses for KeySchema's"
        (as-map (key-schema ?hk-map)) => {:hash-key ?hk-map})
 ?hk-map
 {:name "name" :type :s}
 {:name "name" :type :n}

 {:name "" :type :s}  ;;TODO uh-oh.
 {:name "a" :type :s}
 {:name "A" :type :s}
 )

(tabular
 (facts "as-map and key-schema are approximate inverses for KeySchema's"
        (as-map (key-schema ?hk-map ?rk-map))
        => {:hash-key ?hk-map :range-key ?rk-map})
 ?hk-map ?rk-map
 {:name "hk" :type :s}
 {:name "hk" :type :n}

 {:name "" :type :s} {:name "" :type :s} ;;TODO uh-oh.

 {:name "hk" :type :n} {:name "rk" :type :n}
 {:name "hk" :type :n} {:name "rk" :type :s}
 {:name "hk" :type :s} {:name "rk" :type :n}
 {:name "hk" :type :s} {:name "rk" :type :s}
 )

;;; TODO test as-map for:
;; ProvisionedThroughputDescription
;; DescribeTableResult GetItemResult

(fact "as-map isn't the identity on maps"
      (as-map {}) => (throws java.lang.IllegalArgumentException))

