(ns rotary.test.client
  (:use [rotary.client])
  (:use [clojure.test])
  (:import [com.amazonaws.services.dynamodb.model ConditionalCheckFailedException]))

(def cred {:access-key (get (System/getenv) "AMAZON_SECRET_ID")
           :secret-key (get (System/getenv) "AMAZON_SECRET_ACCESS_KEY")})

(def table "rotary-dev-test")
(def id "test-id")
(def attr "test-attr")

(ensure-table cred {:name "rotary-dev-test" 
                    :hash-key {:name "test-id" :type :s}
                    :throughput {:write 1 :read 1}})

(deftest test-batch-simple
  (batch-write-item cred
    [:delete table {:hash-key "1"}]
    [:delete table {:hash-key "2"}]
    [:delete table {:hash-key "3"}]
    [:delete table {:hash-key "4"}])
  
  (batch-write-item cred
    [:put table {id "1" attr "foo"}]
    [:put table {id "2" attr "bar"}]
    [:put table {id "3" attr "baz"}]
    [:put table {id "4" attr "foobar"}])

  (let [consis (batch-get-item cred {
                 table {
                   :consistent true
                   :keys ["1" "2" "3" "4"]}})
        attrs  (batch-get-item cred {
                 table {
                   :consistent true
                   :attrs [attr]
                   :keys ["1" "2" "3" "4"]}})
        item-1 (get-item cred table "1")
        item-2 (get-item cred table "2")
        item-3 (get-item cred table "3")
        item-4 (get-item cred table "4")]

    (is (= "foo" (item-1 attr)) "batch-write-item :put failed")
    (is (= "bar" (item-2 attr)) "batch-write-item :put failed")
    (is (= "baz" (item-3 attr)) "batch-write-item :put failed")
    (is (= "foobar" (item-4 attr)) "batch-write-item :put failed")

    (is (= true (some #(= (% attr) "bar") (get-in consis [:responses table :items]))))
    (is (= true (some #(= (% attr) "baz") (get-in attrs [:responses table :items]))))
                               
    (batch-write-item cred 
      [:delete table {:hash-key "1"}]
      [:delete table {:hash-key "2"}]
      [:delete table {:hash-key "3"}]
      [:delete table {:hash-key "4"}])
    
    (is (= nil (get-item cred table "1")) "batch-write-item :delete failed")
    (is (= nil (get-item cred table "2")) "batch-write-item :delete failed")
    (is (= nil (get-item cred table "3")) "batch-write-item :delete failed")
    (is (= nil (get-item cred table "4")) "batch-write-item :delete failed")))

(deftest conditional-put
  (batch-write-item cred
                    [:delete table {:hash-key "42"}]
                    [:delete table {:hash-key "9"}]
                    [:delete table {:hash-key "6"}]
                    [:delete table {:hash-key "23"}])
  (batch-write-item cred
                    [:put table {id "42" attr "foo"}]
                    [:put table {id "6" attr "foobar"}]
                    [:put table {id "9" attr "foobaz"}])

  ;; Should update item 42 to have attr bar
  (put-item cred table {id "42" attr "bar"} :expected {attr "foo"})
  (is (= "bar" ((get-item cred table "42") attr)))
  
  ;; Should fail to update item 6
  (is (thrown? ConditionalCheckFailedException (put-item cred table {id "6" attr "baz"} :expected {id false})))
  (is (not (= "baz" ((get-item cred table "6") attr))))  

  ;; Should upate item 9 to have attr baz
  (put-item cred table {id "9" attr "baz"} :expected {attr "foobaz"})
  (is (= "baz" ((get-item cred table "9") attr)))

  ;; Should add item 23
  (put-item cred table {id "23" attr "bar"} :expected {id false})
  (is (not (= nil (get-item cred table "23")))))