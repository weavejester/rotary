(ns rotary.test.client
  (:use [rotary.client])
  (:use [clojure.test]))

(def cred {:access-key (get (System/getenv) "AMAZON_SECRET_ID")
           :secret-key (get (System/getenv) "AMAZON_SECRET_ACCESS_KEY")})

(def table "rotary-dev-test")
(def id "test-id")
(def attr "test-attr")
(def other-attr "second-attr")

(ensure-table cred {:name table
                    :hash-key {:name id :type :s}
                    :throughput {:write 1 :read 1}})

(deftest test-batch-simple
  (batch-write-item cred
    [:delete table {id "1"}]
    [:delete table {id "2"}]
    [:delete table {id "3"}]
    [:delete table {id "4"}])
  
  (batch-write-item cred
    [:put table {id "1" attr "foo"}]
    [:put table {id "2" attr "bar"}]
    [:put table {id "3" attr "baz"}]
    [:put table {id "4" attr "foobar"}])

  (let [result (batch-get-item cred {
                 table {
                   :key-name "test-id"
                   :keys ["1" "2" "3" "4"]
                   :consistent true}})
        consis (batch-get-item cred {
                 table {
                   :key-name "test-id"
                   :consistent true
                   :keys ["1" "2" "3" "4"]}})
        attrs  (batch-get-item cred {
                 table {
                   :key-name "test-id"
                   :consistent true
                   :attrs [attr]
                   :keys ["1" "2" "3" "4"]}})
        items  (get-in result [:responses table])
        item-1 (get-item cred table {id "1"})
        item-2 (get-item cred table {id "2"})
        item-3 (get-item cred table {id "3"})
        item-4 (get-item cred table {id "4"})]

    (is (= "foo" (item-1 attr)) "batch-write-item :put failed")
    (is (= "bar" (item-2 attr)) "batch-write-item :put failed")
    (is (= "baz" (item-3 attr)) "batch-write-item :put failed")
    (is (= "foobar" (item-4 attr)) "batch-write-item :put failed")

    (is (= true (some #(= (% attr) "foo") items)))
    (is (= true (some #(= (% attr) "bar") (get-in consis [:responses table]))))
    (is (= true (some #(= (% attr) "baz") (get-in attrs [:responses table]))))
    (is (= true (some #(= (% attr) "foobar") items)))

    (batch-write-item cred 
      [:delete table {id "1"}]
      [:delete table {id "2"}]
      [:delete table {id "3"}]
      [:delete table {id "4"}])
    
    (is (= nil (get-item cred table {id "1"})) "batch-write-item :delete failed")
    (is (= nil (get-item cred table {id "2"})) "batch-write-item :delete failed")
    (is (= nil (get-item cred table {id "3"})) "batch-write-item :delete failed")
    (is (= nil (get-item cred table {id "4"})) "batch-write-item :delete failed")))