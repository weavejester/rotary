(ns rotary.test.client
  (:use [rotary.client])
  (:use [clojure.test]))

(def cred {:access-key "",
           :secret-key ""})

(def table "TableName")
(def id "id-field")
(def attr "text")

(deftest test-batch-simple 
  (let [_        (batch-write-item cred :put {table [{id "1" attr "foobar"} ]})
        read     (batch-get-item cred table ["1"])
        with-get (get-item cred table "1")]
    (is (= "foobar" 
           (-> (get-in read [:responses table :items]) 
             first 
             (get attr)) 
           (with-get attr)) "batch-write-item :put failed to store item.")
    (batch-write-item cred :delete {table [{:hash-key "1"}]})
    (is (= nil (get-item cred table "1")) "batch-write-item :delete failed to delete item.")))