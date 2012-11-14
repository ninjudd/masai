(ns flatland.masai.db-test
  (:refer-clojure :exclude [get count sync])
  (:use clojure.test flatland.useful.debug)
  (:require [flatland.masai.tokyo :as tokyo]
            [flatland.masai.tokyo-sorted :as tokyo-btree]
            [flatland.masai.memcached :as memcached]
            [flatland.masai.redis :as redis]
            [flatland.masai.db :as db]
            [flatland.masai.cursor :as cursor]
            [flatland.retro.core :as retro]))

(def b (memfn getBytes))

(deftest tests
  (doseq [db [;(redis/make) TODO uncomment this test once it starts redis automatically
              (tokyo/make {:path "/tmp/masai-test-tokyo-db" :create true :prepop true})
              (tokyo-btree/make {:path "/tmp/masai-test-sorted-tokyo-db" :create true :prepop true})]]
    (db/open db)
    (db/truncate! db)

    (testing "add! doesn't overwrite existing record"
      (is (= nil (db/fetch db (.getBytes "foo"))))
      (is (= true (db/add! db (.getBytes "foo") (.getBytes "bar"))))
      (is (= "bar" (String. (db/fetch db (.getBytes "foo")))))
      (is (= false (db/add! db (.getBytes "foo") (.getBytes "baz"))))
      (is (= "bar" (String. (db/fetch db (.getBytes "foo"))))))

    (testing "put! overwrites existing record"
      (is (= true (db/put! db (.getBytes "foo") (.getBytes "baz"))))
      (is (= "baz" (String. (db/fetch db (.getBytes "foo"))))))

    (testing "append! to existing record"
      (is (= true (db/append! db (.getBytes "foo") (.getBytes "bar"))))
      (is (= "bazbar" (String. (db/fetch db (.getBytes "foo")))))
      (is (= true (db/append! db (.getBytes "foo") (.getBytes "!"))))
      (is (= "bazbar!" (String. (db/fetch db (.getBytes "foo"))))))

    (testing "append! to nonexistent record"
      (is (= true (db/append! db (.getBytes "baz") (.getBytes "1234"))))
      (is (= "1234" (String. (db/fetch db (.getBytes "baz"))))))

    (testing "delete! record returns true on success"
      (is (= true (db/delete! db (.getBytes "foo"))))
      (is (= nil (db/fetch db (.getBytes "foo"))))
      (is (= true (db/delete! db (.getBytes "baz"))))
      (is (= nil (db/fetch db (.getBytes "baz")))))

    (testing "delete! nonexistent records returns false"
      (is (= false (db/delete! db (.getBytes "foo"))))
      (is (= false (db/delete! db (.getBytes "bar")))))

    (testing "len returns -1 for nonexistent records"
      (is (= nil (db/fetch db (.getBytes "foo"))))
      (is (= -1 (db/len db (.getBytes "foo")))))

    (testing "len returns the length for existing records"
      (is (= true (db/put! db (.getBytes "foo") (.getBytes ""))))
      (is (= "" (String. (db/fetch db (.getBytes "foo")))))
      (is (= 0 (db/len db (.getBytes "foo"))))
      (is (= true (db/put! db (.getBytes "bar") (.getBytes "12345"))))
      (is (= 5 (db/len db (.getBytes "bar"))))
      (is (= true (db/append! db (.getBytes "bar") (.getBytes "6789"))))
      (is (= 9 (db/len db (.getBytes "bar"))))
      (is (= true (db/add! db (.getBytes "baz") (.getBytes ".........."))))
      (is (= 10 (db/len db (.getBytes "baz")))))

    (testing "exists? returns true if record exists"
      (is (= true (db/add! db (.getBytes "bazr") (.getBytes ""))))
      (is (= true (db/exists? db (.getBytes "bazr")))))

    (testing "exists? returns false if record is non-existent"
      (is (= nil (db/fetch db (.getBytes "baze"))))
      (is (= false (db/exists? db (.getBytes "baze")))))

    (testing "a closed db appears empty"
      (db/close db)
      (is (= nil (db/fetch db (.getBytes "bar"))))
      (is (= nil (db/fetch db (.getBytes "baz"))))
      (is (= -1 (db/len db (.getBytes "baz"))))
      (is (= false (db/exists? db (.getBytes "baz")))))

    (testing "can reopen a closed db"
      (db/open db)
      (is (not= nil (db/fetch db (.getBytes "bar"))))
      (is (not= nil (db/fetch db (.getBytes "baz"))))
      (is (= "123456789" (String. (db/fetch db (.getBytes "bar"))))))

    (testing "truncate deletes all records"
      (is (= true (db/truncate! db)))
      (is (= nil (db/fetch db (.getBytes "foo"))))
      (is (= nil (db/fetch db (.getBytes "bar"))))
      (is (= nil (db/fetch db (.getBytes "baz")))))

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
          sorted-xs (sort xs)]
      (doseq [x xs]
        (db/add! db (.getBytes x) (.getBytes "")))

      (testing "fetch-subseq works as in core"
        (are [ks kvs] (= ks (map #(String. (first %)) kvs))
             ["baz" "cat" "foo"]       (db/fetch-subseq db > (.getBytes "bar"))
             ["baz" "cat"]             (db/fetch-subseq db > (.getBytes "bar") < (.getBytes "foo"))
             ["bam" "bar" "baz" "cat"] (db/fetch-subseq db < (.getBytes "foo"))))

      (testing "fetch-rsubseq works as in core"
        (are [ks kvs] (= ks (map #(String. (first %)) kvs))
             ["foo" "cat" "baz"]       (db/fetch-rsubseq db > (.getBytes "bar"))
             ["cat" "baz"]             (db/fetch-rsubseq db > (.getBytes "bar") < (.getBytes "foo"))
             ["cat" "baz" "bar" "bam"] (db/fetch-rsubseq db < (.getBytes "foo"))))

      (testing "fetch-key-subseq works as in core"
        (are [ks keys] (= ks (map #(String. %) keys))
             ["baz" "cat" "foo"]       (db/fetch-key-subseq db > (.getBytes "bar"))
             ["baz" "cat"]             (db/fetch-key-subseq db > (.getBytes "bar") < (.getBytes "foo"))
             ["bam" "bar" "baz" "cat"] (db/fetch-key-subseq db < (.getBytes "foo"))))

      (testing "fetch-key-rsubseq works as in core"
        (are [ks kvs] (= ks (map #(String. %) kvs))
             ["foo" "cat" "baz"]       (db/fetch-key-rsubseq db > (.getBytes "bar"))
             ["cat" "baz"]             (db/fetch-key-rsubseq db > (.getBytes "bar") < (.getBytes "foo"))
             ["cat" "baz" "bar" "bam"] (db/fetch-key-rsubseq db < (.getBytes "foo"))))

      (testing "cursors"
        (is (nil?
             (reduce (fn [cursor x]
                       (is (= x  (String. (cursor/key cursor))))
                       (is (= "" (String. (cursor/val cursor))))
                       (cursor/next cursor))
                     (db/cursor db :first)
                     sorted-xs)))
        (let [test (.getBytes "test")
              append (.getBytes "append")
              c (db/cursor db :first)
              c (cursor/next c)
              _ (is (not (nil? c)))
              c (cursor/put c test)
              _ (is (= (seq test) (seq (cursor/val c))))
              c (cursor/prev c)
              _ (is (= (first sorted-xs) (String. (cursor/key c))))
              c (cursor/delete c)
              _ (is (= (seq test) (seq (cursor/val c))))
              c (cursor/append c append)
              _ (is (= (concat test append) (seq (cursor/val c))))])))))
