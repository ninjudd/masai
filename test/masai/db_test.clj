(ns masai.db-test
  (:refer-clojure :exclude [get count sync])
  (:use clojure.test masai.db)
  (:require [masai.tokyo :as tokyo]
            [masai.memcached :as memcached]
            [masai.redis :as redis]))

(deftest tokyo-database
  (let [db (tokyo/make {:path "/tmp/masai-test-tokyo-db" :create true :prepop true :mlock true})]
    (open db)
    (truncate! db)

    (testing "add! doesn't overwrite existing record"
      (is (= nil (get db "foo")))
      (is (= true (add! db "foo" (.getBytes "bar"))))
      (is (= "bar" (String. (get db "foo"))))
      (is (= false (add! db "foo" (.getBytes "baz"))))
      (is (= "bar" (String. (get db "foo")))))

    (testing "put! overwrites existing record"
      (is (= true (put! db "foo" (.getBytes "baz"))))
      (is (= "baz" (String. (get db "foo")))))

    (testing "append! to existing record"
      (is (= true (append! db "foo" (.getBytes "bar"))))
      (is (= "bazbar" (String. (get db "foo"))))
      (is (= true (append! db "foo" (.getBytes "!"))))
      (is (= "bazbar!" (String. (get db "foo")))))

    (testing "append! to nonexistent record"
      (is (= true (append! db "baz" (.getBytes "1234"))))
      (is (= "1234" (String. (get db "baz")))))

    (testing "delete! record returns true on success"
      (is (= true (delete! db "foo")))
      (is (= nil (get db "foo")))
      (is (= true (delete! db "baz")))
      (is (= nil (get db "baz"))))

    (testing "delete! nonexistent records returns false"
      (is (= false (delete! db "foo")))
      (is (= false (delete! db "bar"))))

    (testing "len returns -1 for nonexistent records"
      (is (= nil (get db "foo")))
      (is (= -1 (len db "foo"))))

    (testing "len returns the length for existing records"
      (is (= true (put! db "foo" (.getBytes ""))))
      (is (= "" (String. (get db "foo"))))
      (is (= 0 (len db "foo")))
      (is (= true (put! db "bar" (.getBytes "12345"))))
      (is (= 5 (len db "bar")))
      (is (= true (append! db "bar" (.getBytes "6789"))))
      (is (= 9 (len db "bar")))
      (is (= true (add! db "baz" (.getBytes ".........."))))
      (is (= 10 (len db "baz"))))

    (testing "exists? returns true if record exists"
      (is (= true (add! db "bazr" "")))
      (is (= true (exists? db "bazr"))))
    
    (testing "exists? returns false if record is non-existent"
      (is (= nil (get db "baze")))
      (is (= false (exists? db "baze"))))

    (testing "a closed db appears empty"
      (close db)
      (is (= nil (get db "bar")))
      (is (= nil (get db "baz"))))

    (testing "can reopen a closed db"
      (open db)
      (is (not= nil (get db "bar")))
      (is (not= nil (get db "baz")))
      (is (= "123456789" (String. (get db "bar")))))

    (testing "truncate deletes all records"
      (is (= true (truncate! db)))
      (is (= nil (get db "foo")))
      (is (= nil (get db "bar")))
      (is (= nil (get db "baz"))))

    (close db)))

(deftest memcache-database
  (let [db (memcached/make)]
    (truncate! db)

    (testing "add! doesn't overwrite existing record"
      (is (= nil (get db "foo")))
      (is (= true (add! db "foo" "bar")))
      (is (= "bar" (get db "foo")))
      (is (= false (add! db "foo" "baz")))
      (is (= "bar" (get db "foo"))))

    (testing "put! overwrites existing record"
      (is (= true (put! db "foo" "baz")))
      (is (= "baz" (get db "foo"))))

    (testing "append! to existing record"
      (is (= true (append! db "foo" "bar")))
      (is (= "bazbar" (get db "foo")))
      (is (= true (append! db "foo" "!")))
      (is (= "bazbar!" (get db "foo"))))

    (testing "append! to nonexistent record"
      (is (= true (append! db "baz" "1234")))
      (is (= "1234" (get db "baz"))))

    (testing "delete! record returns true on success"
      (is (= true (delete! db "foo")))
      (is (= nil (get db "foo")))
      (is (= true (delete! db "baz")))
      (is (= nil (get db "baz"))))

    (testing "delete! nonexistent records returns false"
      (is (= false (delete! db "foo")))
      (is (= false (delete! db "bar"))))

    (testing "len returns -1 for nonexistent records"
      (is (= nil (get db "foo")))
      (is (= -1 (len db "foo"))))

    (testing "exists? returns true if record exists"
      (is (= true (add! db "bazr" "")))
      (is (= true (exists? db "bazr"))))
    
    (testing "exists? returns false if record is non-existent"
      (is (= nil (get db "baze")))
      (is (= false (exists? db "baze"))))
    
    (testing "len returns the length for existing records"
      (is (= true (put! db "foo" "")))
      (is (= "" (get db "foo")))
      (is (= 0 (len db "foo")))
      (is (= true (put! db "bar" "12345")))
      (is (= 5 (len db "bar")))
      (is (= true (append! db "bar" "6789")))
      (is (= 9 (len db "bar")))
      (is (= true (add! db "baz" "..........")))
      (is (= 10 (len db "baz"))))

    (testing "truncate deletes all records"
      (is (= true (truncate! db)))
      (is (= nil (get db "foo")))
      (is (= nil (get db "bar")))
      (is (= nil (get db "baz"))))
    
    (close db)))

(deftest redis-database
  (let [db (redis/make)]
    (truncate! db)

    (testing "add! doesn't overwrite existing record"
      (is (= nil (get db "foo")))
      (is (= true (add! db "foo" "bar")))
      (is (= "bar" (get db "foo")))
      (is (= false (add! db "foo" "baz")))
      (is (= "bar" (get db "foo"))))

    (testing "put! overwrites existing record"
      (is (= true (put! db "foo" "baz")))
      (is (= "baz" (get db "foo"))))

    (testing "append! to existing record"
      (is (= true (append! db "foo" "bar")))
      (is (= "bazbar" (get db "foo")))
      (is (= true (append! db "foo" "!")))
      (is (= "bazbar!" (get db "foo"))))

    (testing "append! to nonexistent record"
      (is (= true (append! db "baz" "1234")))
      (is (= "1234" (get db "baz"))))

    (testing "delete! record returns true on success"
      (is (= true (delete! db "foo")))
      (is (= nil (get db "foo")))
      (is (= true (delete! db "baz")))
      (is (= nil (get db "baz"))))

    (testing "delete! nonexistent records returns false"
      (is (= false (delete! db "foo")))
      (is (= false (delete! db "bar"))))

    (testing "len returns -1 for nonexistent records"
      (is (= nil (get db "foo")))
      (is (= -1 (len db "foo"))))

    (testing "len returns the length for existing records"
      (is (= true (put! db "foo" "")))
      (is (= "" (get db "foo")))
      (is (= 0 (len db "foo")))
      (is (= true (put! db "bar" "12345")))
      (is (= 5 (len db "bar")))
      (is (= true (append! db "bar" "6789")))
      (is (= 9 (len db "bar")))
      (is (= true (add! db "baz" "..........")))
      (is (= 10 (len db "baz"))))

    (testing "exists? returns true if record exists"
      (is (= true (add! db "bazr" "")))
      (is (= true (exists? db "bazr"))))

    (testing "exists? returns false if record is non-existent"
      (is (= nil (get db "baze")))
      (is (= false (exists? db "baze"))))

    (testing "truncate deletes all records"
      (is (= true (truncate! db)))
      (is (= nil (get db "foo")))
      (is (= nil (get db "bar")))
      (is (= nil (get db "baz"))))
    
    (close db)))