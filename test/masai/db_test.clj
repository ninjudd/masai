(ns masai.db-test
  (:refer-clojure :exclude [get count sync])
  (:use clojure.test useful.debug)
  (:require [masai.tokyo :as tokyo]
            [masai.tokyo-sorted :as tokyo-btree]
            [masai.memcached :as memcached]
            [masai.redis :as redis]
            [masai.db :as db]
            [masai.cursor :as cursor]
            [retro.core :as retro]))

(deftest tests
  (doseq [db [(redis/make)
              (tokyo/make {:path "/tmp/masai-test-tokyo-db" :create true :prepop true})
              (tokyo-btree/make {:path "/tmp/masai-test-sorted-tokyo-db" :create true :prepop true})]]
    (db/open db)
    (db/truncate! db)

    (testing "add! doesn't overwrite existing record"
      (is (= nil (db/fetch db "foo")))
      (is (= true (db/add! db "foo" (.getBytes "bar"))))
      (is (= "bar" (String. (db/fetch db "foo"))))
      (is (= false (db/add! db "foo" (.getBytes "baz"))))
      (is (= "bar" (String. (db/fetch db "foo")))))

    (testing "put! overwrites existing record"
      (is (= true (db/put! db "foo" (.getBytes "baz"))))
      (is (= "baz" (String. (db/fetch db "foo")))))

    (testing "append! to existing record"
      (is (= true (db/append! db "foo" (.getBytes "bar"))))
      (is (= "bazbar" (String. (db/fetch db "foo"))))
      (is (= true (db/append! db "foo" (.getBytes "!"))))
      (is (= "bazbar!" (String. (db/fetch db "foo")))))

    (testing "append! to nonexistent record"
      (is (= true (db/append! db "baz" (.getBytes "1234"))))
      (is (= "1234" (String. (db/fetch db "baz")))))

    (testing "delete! record returns true on success"
      (is (= true (db/delete! db "foo")))
      (is (= nil (db/fetch db "foo")))
      (is (= true (db/delete! db "baz")))
      (is (= nil (db/fetch db "baz"))))

    (testing "delete! nonexistent records returns false"
      (is (= false (db/delete! db "foo")))
      (is (= false (db/delete! db "bar"))))

    (testing "len returns -1 for nonexistent records"
      (is (= nil (db/fetch db "foo")))
      (is (= -1 (db/len db "foo"))))

    (testing "len returns the length for existing records"
      (is (= true (db/put! db "foo" (.getBytes ""))))
      (is (= "" (String. (db/fetch db "foo"))))
      (is (= 0 (db/len db "foo")))
      (is (= true (db/put! db "bar" (.getBytes "12345"))))
      (is (= 5 (db/len db "bar")))
      (is (= true (db/append! db "bar" (.getBytes "6789"))))
      (is (= 9 (db/len db "bar")))
      (is (= true (db/add! db "baz" (.getBytes ".........."))))
      (is (= 10 (db/len db "baz"))))

    (testing "exists? returns true if record exists"
      (is (= true (db/add! db "bazr" (.getBytes ""))))
      (is (= true (db/exists? db "bazr"))))

    (testing "exists? returns false if record is non-existent"
      (is (= nil (db/fetch db "baze")))
      (is (= false (db/exists? db "baze"))))

    (testing "a closed db appears empty"
      (db/close db)
      (is (= nil (db/fetch db "bar")))
      (is (= nil (db/fetch db "baz")))
      (is (= -1 (db/len db "baz")))
      (is (= false (db/exists? db "baz"))))

    (testing "can reopen a closed db"
      (db/open db)
      (is (not= nil (db/fetch db "bar")))
      (is (not= nil (db/fetch db "baz")))
      (is (= "123456789" (String. (db/fetch db "bar")))))

    (testing "truncate deletes all records"
      (is (= true (db/truncate! db)))
      (is (= nil (db/fetch db "foo")))
      (is (= nil (db/fetch db "bar")))
      (is (= nil (db/fetch db "baz"))))

    (db/close db)))

(deftest transactions
  (doseq [db [(tokyo/make {:path "/tmp/masai-test-tokyo-db" :create true :prepop true})
              (tokyo-btree/make {:path "/tmp/masai-test-sorted-tokyo-db" :create true :prepop true})]]
    (retro/txn-begin! db)
    (retro/txn-begin! db) ; this will block forever if we actually open two transactions
    (retro/txn-commit! db)
    (retro/txn-commit! db)
    (is (= true true)))) ; if we got here, things are A-OK

(deftest sorted-db
  (let [db (tokyo-btree/make {:path "/tmp/masai-test-sorted-tokyo-db2" :create true :prepop true})]
    (db/open db)
    (db/truncate! db)
    (let [xs ["bar" "baz" "bam" "cat" "foo"]
          sorted-xs (sort xs)
          bytes (.getBytes "")]
      (doseq [x xs]
        (db/add! db x bytes))

      (testing "fetch-subseq works as in core"
        (are [ks kvs] (= ks (map first kvs))
             '("baz" "cat" "foo")       (db/fetch-subseq db > "bar")
             '("baz" "cat")             (db/fetch-subseq db > "bar" < "foo")
             '("bam" "bar" "baz" "cat") (db/fetch-subseq db < "foo")))

      (testing "fetch-rsubseq works as in core"
        (are [ks kvs] (= ks (map first kvs))
              '("foo" "cat" "baz")       (db/fetch-rsubseq db > "bar")
              '("cat" "baz")             (db/fetch-rsubseq db > "bar" < "foo")
              '("cat" "baz" "bar" "bam") (db/fetch-rsubseq db < "foo")))

      (testing "cursors"
        (is (nil?
             (reduce (fn [cursor x]
                       (is (= x (String. (cursor/key cursor) "UTF-8")))
                       (is (= (seq bytes) (seq (cursor/val cursor))))
                       (cursor/next cursor))
                     (db/cursor db :first)
                     sorted-xs)))
        (let [v (.getBytes "test")
              c (db/cursor db :first)
              c (cursor/next c)
              _ (is (not (nil? c)))
              c (cursor/put c v)
              _ (is (= (seq v) (seq (cursor/val c))))
              c (cursor/prev c)
              _ (is (= (first sorted-xs) (String. (cursor/key c) "UTF-8")))
              c (cursor/delete c)
              _ (is (= (seq v) (seq (cursor/val c))))])))))