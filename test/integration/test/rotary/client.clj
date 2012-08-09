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
(def tname_hash_range "dydb_test_hash_range")

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
        {:name tname_hash_range
         :hash-key {:name "id" :type :n}
         :range-key {:name "name" :type :s}
         :throughput {:read 1 :write 1}})
      (println "Created tables, waiting for them to be active (max 60s before failing)")))
    (after :contents
           (do
             (delete-table cred tname_hash)
             (delete-table cred tname_hash_range)
             (println "Finished tests.")))]
 

 (facts "we have indeed created tables with the right params"
   (describe-table cred tname_hash) =>
   (contains {:name tname_hash
              ;;NOTE :item-count may be off
              :item-count 0
              :key-schema {:hash-key {:name "id", :type :n}}
              :throughput {:read 1, :write 1, :last-decrease nil, :last-increase nil}
              :status :active})
   (describe-table cred tname_hash_range) =>
   (contains {:name tname_hash_range
              :item-count 0
              :key-schema {:hash-key {:name "id", :type :n}
                           :range-key {:name "name" :type :s}}
              :throughput {:read 1, :write 1, :last-decrease nil, :last-increase nil}
              :status :active}))

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
 
 (fact "we can put things into a hash+range table"
   (put-item cred tname_hash_range {"id" 1, "name" "Bob", "type" "nickname"})
   => #(instance? PutItemResult %)
   (put-item cred tname_hash_range {"id" 1, "name" "Robert", "type" "given_name"})
   => #(instance? PutItemResult %)
   (put-item cred tname_hash_range {"id" 2, "name" "Alice", "type" "given_name"})
   => #(instance? PutItemResult %)
   )
 
 (fact "we can query a hash_range table for all items with a certain hash key"
   (query cred tname_hash_range 1)
   => [{"id" "1", "name" "Bob", "type" "nickname"} {"id" "1", "name" "Robert", "type" "given_name"}])

 (fact "we can get all items with a table scan"
   (scan cred tname_hash_range)
   => (just [{"id" "1", "name" "Bob", "type" "nickname"}
             {"id" "1", "name" "Robert", "type" "given_name"}
             {"id" "2", "name" "Alice", "type" "given_name"}]
            :in-any-order))
)

;;TODO update-table ensure-table list-tables
;;TODO delete-item
;;TODO range queries, query :order :limit :count

;;TODO check if dynamodb really enforces set semantics on attributes,
;;or if that's just (misleading) terminology

