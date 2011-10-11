(ns masai.db-test
  (:refer-clojure :exclude [get count sync])
  (:use clojure.test)
  (:require [masai.tokyo :as tokyo]
            [masai.tokyo-sorted :as tokyo-btree]
            [masai.memcached :as memcached]
            [masai.redis :as redis]
            [masai.db :as db]))

(deftest tests
  (doseq [db [(redis/make)
              (tokyo/make {:path "/tmp/masai-test-tokyo-db" :create true :prepop true})
              (tokyo-btree/make {:path "/tmp/masai-test-sorted-tokyo-db" :create true :prepop true})]]
    (db/open db)
    (db/truncate! db)

    (testing "add! doesn't overwrite existing record"
      (is (= nil (db/get db "foo")))
      (is (= true (db/add! db "foo" (.getBytes "bar"))))
      (is (= "bar" (String. (db/get db "foo"))))
      (is (= false (db/add! db "foo" (.getBytes "baz"))))
      (is (= "bar" (String. (db/get db "foo")))))

    (testing "put! overwrites existing record"
      (is (= true (db/put! db "foo" (.getBytes "baz"))))
      (is (= "baz" (String. (db/get db "foo")))))

    (testing "append! to existing record"
      (is (= true (db/append! db "foo" (.getBytes "bar"))))
      (is (= "bazbar" (String. (db/get db "foo"))))
      (is (= true (db/append! db "foo" (.getBytes "!"))))
      (is (= "bazbar!" (String. (db/get db "foo")))))

    (testing "append! to nonexistent record"
      (is (= true (db/append! db "baz" (.getBytes "1234"))))
      (is (= "1234" (String. (db/get db "baz")))))

    (testing "delete! record returns true on success"
      (is (= true (db/delete! db "foo")))
      (is (= nil (db/get db "foo")))
      (is (= true (db/delete! db "baz")))
      (is (= nil (db/get db "baz"))))

    (testing "delete! nonexistent records returns false"
      (is (= false (db/delete! db "foo")))
      (is (= false (db/delete! db "bar"))))

    (testing "len returns -1 for nonexistent records"
      (is (= nil (db/get db "foo")))
      (is (= -1 (db/len db "foo"))))

    (testing "len returns the length for existing records"
      (is (= true (db/put! db "foo" (.getBytes ""))))
      (is (= "" (String. (db/get db "foo"))))
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
      (is (= nil (db/get db "baze")))
      (is (= false (db/exists? db "baze"))))

    (testing "a closed db appears empty"
      (db/close db)
      (is (= nil (db/get db "bar")))
      (is (= nil (db/get db "baz")))
      (is (= -1 (db/len db "baz")))
      (is (= false (db/exists? db "baz"))))

    (testing "can reopen a closed db"
      (db/open db)
      (is (not= nil (db/get db "bar")))
      (is (not= nil (db/get db "baz")))
      (is (= "123456789" (String. (db/get db "bar")))))

    (testing "truncate deletes all records"
      (is (= true (db/truncate! db)))
      (is (= nil (db/get db "foo")))
      (is (= nil (db/get db "bar")))
      (is (= nil (db/get db "baz"))))

    (db/close db)))

(defn keys-equal? [s test]
  (= test (map first s)))

(deftest sorted-db
  (let [db (tokyo-btree/make {:path "/tmp/masai-test-sorted-tokyo-db2" :create true :prepop true})]
    (db/open db)
    (db/truncate! db)
    (let [bytes (.getBytes "")]
      (doseq [x ["bar" "baz" "bam" "cat" "foo"]]
        (db/add! db x bytes))

      (testing "subseq works as in core"
        (is (keys-equal? (db/subseq db > "bar") '("baz" "cat" "foo")))
        (is (keys-equal? (db/subseq db > "bar" < "foo") '("baz" "cat")))
        (is (keys-equal? (db/subseq db < "foo") '("bam" "bar" "baz" "cat"))))

      (testing "rsubseq works as in core"
        (is (keys-equal? (db/rsubseq db > "bar") '("foo" "cat" "baz")))
        (is (keys-equal? (db/rsubseq db > "bar" < "foo") '("cat" "baz")))
        (is (keys-equal? (db/rsubseq db < "foo") '("cat" "baz" "bar" "bam")))))))