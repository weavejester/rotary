(ns integration.test.rotary.client
  (:use [rotary.client])
  (:use [midje.sweet])
  (:import [com.amazonaws.services.dynamodb.model
            PutItemResult]))

(let [access-key (System/getenv "AWS_ACCESS_KEY_ID")
      secret-key (System/getenv "AWS_SECRET_ACCESS_KEY")]
  (assert (and access-key secret-key))
  (def cred {:access-key access-key :secret-key secret-key}))

(def tname_hash "dydb_test_hash")
(def tname_hash_range_str "dydb_test_hash_range_str")
(def tname_hash_range_num "dydb_test_hash_range_num")

(defmacro timeout [& {:keys [timeout-ms timeout-val poll-bindings done? wait return]}]
  `(try
     (let [~'timeout-ms ~timeout-ms
           f# (future (loop []
                       (let ~poll-bindings
                         (if ~done? ~return
                             (do ~wait (recur))))))]
      (.get f# ~'timeout-ms java.util.concurrent.TimeUnit/MILLISECONDS)) 
    (catch RuntimeException e# ~timeout-val)))

(defn create-table-timeout [cred tablespec]
  (let [tname (:name tablespec)]
    (let [new-tab (create-table cred tablespec)]
      (timeout
       :timeout-ms 60000
       :timeout-val nil
       :poll-bindings [table-on-aws (describe-table cred tname)]
       :done? (= :active (:status table-on-aws))
       :wait (Thread/sleep (quot timeout-ms 10))
       :return table-on-aws))))

(defn create-tables [cred & tablespecs]
  (doall (pmap #(create-table-timeout cred %) tablespecs)))

(against-background
 [
  (before :contents
    (do
      (create-tables cred
        {:name tname_hash
         :hash-key {:name "id" :type :n}
         :throughput {:read 1 :write 1}}
        {:name tname_hash_range_str
         :hash-key {:name "id" :type :n}
         :range-key {:name "name" :type :s}
         :throughput {:read 1 :write 1}}
        {:name tname_hash_range_num
         :hash-key {:name "id" :type :n}
         :range-key {:name "num" :type :n}
         :throughput {:read 1 :write 1}})
      (println "Created tables, waiting for them to be active (max 60s before failing)")))
    (after :contents
           (do
             (delete-table cred tname_hash)
             (delete-table cred tname_hash_range_str)
             (delete-table cred tname_hash_range_num)
             (println "Finished tests.")))]
 

 (fact "we have indeed created tables with the right params"
   (describe-table cred tname_hash) =>
   (contains {:name tname_hash
              ;;NOTE :item-count may be off
              :item-count 0
              :key-schema {:hash-key {:name "id", :type :n}}
              :throughput {:read 1, :write 1, :last-decrease nil, :last-increase nil}
              :status :active})
   (describe-table cred tname_hash_range_str) =>
   (contains {:name tname_hash_range_str  
              :item-count 0
              :key-schema {:hash-key {:name "id", :type :n}
                           :range-key {:name "name" :type :s}}
              :throughput {:read 1, :write 1, :last-decrease nil, :last-increase nil}
              :status :active})
   (describe-table cred tname_hash_range_num) =>
   (contains {:name tname_hash_range_num
              :item-count 0
              :key-schema {:hash-key {:name "id", :type :n}
                           :range-key {:name "num" :type :n}}
              :throughput {:read 1, :write 1, :last-decrease nil, :last-increase nil}
              :status :active})
   )

 ;; put-item
 (fact "we can put things into a hash table"
   (put-item cred tname_hash {"id" 1, "name" "White Rabbit", "gender" "?"})
   => #(instance? PutItemResult %)
   (get-item cred tname_hash 1)
   => {"id" "1" , "name" "White Rabbit", "gender" "?"})
 
 (fact "we can overwrite things in a hash table"
   (put-item cred tname_hash {"id" 1, "name" "Alice", "gender" "f"})
   => #(instance? PutItemResult %) 
   (get-item cred tname_hash 1)
   => {"id" "1" , "name" "Alice", "gender" "f"})
 
 ;;TODO 2-arg get-item for hash+range tables

 ;; populate data
 
 (tabular
  (fact "we can put things into a hash + str range table"
        (put-item cred tname_hash_range_str ?item) => #(instance? PutItemResult %))
  ?item
  {"id" 1, "name" "Bob", "type" "nickname"}
  {"id" 1, "name" "Bobby", "type" "nickname"}
  {"id" 1, "name" "Rob", "type" "nickname"}
  {"id" 1, "name" "Robert", "type" "given_name"}
  {"id" 2, "name" "Alice", "type" "given_name"}
  )

 (tabular
  (fact "we can put things into a hash + num range table"
        (put-item cred tname_hash_range_num ?item) => #(instance? PutItemResult %))
  ?item
  {"id" 1, "num" 5.0e0, "en" "five"}
  {"id" 1, "num" 1.0e1, "en" "ten"}
  {"id" 1, "num" 2.0e2, "en" "two hundred"}
  {"id" 1, "num" 1.0e3, "en" "thousand"}
  )
 
 ;; query
 (fact "we can query a hash_range table for all items with a certain hash key"
   (query cred tname_hash_range_str 1)
   => [
       {"id" "1", "name" "Bob", "type" "nickname"}
       {"id" "1", "name" "Bobby", "type" "nickname"}
       {"id" "1", "name" "Rob", "type" "nickname"}
       {"id" "1", "name" "Robert", "type" "given_name"}
       ]
   )

 (facts "we can query a hash_range table for all items with a certain hash key,
         with order and limit options"
   (query cred tname_hash_range_str 1 nil {:order :asc})
   => [
       {"id" "1", "name" "Bob", "type" "nickname"}
       {"id" "1", "name" "Bobby", "type" "nickname"}
       {"id" "1", "name" "Rob", "type" "nickname"}
       {"id" "1", "name" "Robert", "type" "given_name"}
       ]
   (query cred tname_hash_range_str 1 nil {:order :desc})
   => [
       {"id" "1", "name" "Robert", "type" "given_name"}
       {"id" "1", "name" "Rob", "type" "nickname"}
       {"id" "1", "name" "Bobby", "type" "nickname"}
       {"id" "1", "name" "Bob", "type" "nickname"}
       ]
   (query cred tname_hash_range_str 1 nil {:limit 1})
   => [
       {"id" "1", "name" "Bob", "type" "nickname"}
       ]
   (query cred tname_hash_range_str 1 nil {:limit 1 :order :desc})
   => [
       {"id" "1", "name" "Robert", "type" "given_name"}
       ]
   )

 (future-fact "we can just get a count of results that would be returned"
   (query cred tname_hash_range_str 1 nil {:count true})
   => 4
   )
 
 (facts "we can query a hash_range table for all items with a certain hash key
        with a range clause"
   (query cred tname_hash_range_str 1 `(< "Rob"))
   => [
       {"id" "1", "name" "Bob", "type" "nickname"}
       {"id" "1", "name" "Bobby", "type" "nickname"}
       ]
   (query cred tname_hash_range_str 1 `(<= "Rob"))
   => [
       {"id" "1", "name" "Bob", "type" "nickname"}
       {"id" "1", "name" "Bobby", "type" "nickname"}
       {"id" "1", "name" "Rob", "type" "nickname"}
       ]
   (query cred tname_hash_range_str 1 `(> "Bobby"))
   => [
       {"id" "1", "name" "Rob", "type" "nickname"}
       {"id" "1", "name" "Robert", "type" "given_name"}
       ]
   (query cred tname_hash_range_str 1 `(>= "Bobby"))
   => [
       {"id" "1", "name" "Bobby", "type" "nickname"}
       {"id" "1", "name" "Rob", "type" "nickname"}
       {"id" "1", "name" "Robert", "type" "given_name"}
       ]
   (query cred tname_hash_range_str 1 `(= "Bob"))
   => [
       {"id" "1", "name" "Bob", "type" "nickname"}
       ]
   (query cred tname_hash_range_str 1 `(= "Bobb"))
   => []
   )

 (facts "we can query a hash_range table for all items with a certain hash key
        with a range clause, order, and limit"
   (query cred tname_hash_range_str 1 `(<= "Rob"))
   => [
       {"id" "1", "name" "Bob", "type" "nickname"}
       {"id" "1", "name" "Bobby", "type" "nickname"}
       {"id" "1", "name" "Rob", "type" "nickname"}
       ]
   (query cred tname_hash_range_str 1 `(<= "Rob") {:order :desc})
   => [
       {"id" "1", "name" "Rob", "type" "nickname"}
       {"id" "1", "name" "Bobby", "type" "nickname"}
       {"id" "1", "name" "Bob", "type" "nickname"}
       ]
   (query cred tname_hash_range_str 1 `(<= "Rob") {:limit 2})
   => [
       {"id" "1", "name" "Bob", "type" "nickname"}
       {"id" "1", "name" "Bobby", "type" "nickname"}
       ]
   (query cred tname_hash_range_str 1 `(<= "Rob") {:order :desc :limit 2})
   => [
       {"id" "1", "name" "Rob", "type" "nickname"}
       {"id" "1", "name" "Bobby", "type" "nickname"}
       ]
   )

 (facts "we can query a hash_range table, and range clauses for numeric
         attributes work"
   (query cred tname_hash_range_num 1 nil)
   => [
       {"id" "1", "num" "5", "en" "five"}
       {"id" "1", "num" "10", "en" "ten"}
       {"id" "1", "num" "200", "en" "two hundred"}
       {"id" "1", "num" "1000", "en" "thousand"}
       ]
   (query cred tname_hash_range_num 1 `(< 10))
   => [
       {"id" "1", "num" "5", "en" "five"}
       ]
   (query cred tname_hash_range_num 1 `(<= 10))
   => [
       {"id" "1", "num" "5", "en" "five"}
       {"id" "1", "num" "10", "en" "ten"}
       ]
   (query cred tname_hash_range_num 1 `(> 200))
   => [
       {"id" "1", "num" "1000", "en" "thousand"}
       ]
   (query cred tname_hash_range_num 1 `(>= 200))
   => [
       {"id" "1", "num" "200", "en" "two hundred"}
       {"id" "1", "num" "1000", "en" "thousand"}
       ]
   (query cred tname_hash_range_num 1 `(= 200))
   => [
       {"id" "1", "num" "200", "en" "two hundred"}
       ]
   (query cred tname_hash_range_num 1 `(< 0))
   => []
   )

 (fact "we can get all items with a table scan"
   (scan cred tname_hash_range_str)
   => (just [
             {"id" "1", "name" "Bob", "type" "nickname"}
             {"id" "1", "name" "Bobby", "type" "nickname"}
             {"id" "1", "name" "Rob", "type" "nickname"}
             {"id" "1", "name" "Robert", "type" "given_name"}
             {"id" "2", "name" "Alice", "type" "given_name"}
             ]
            :in-any-order))

 ) ; against-background

;;TODO update-table ensure-table list-tables
;;TODO delete-item
;;TODO range queries, query :order :limit :count

;;TODO check if dynamodb really enforces set semantics on attributes,
;;or if that's just (misleading) terminology

