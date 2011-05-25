(ns masai.cereal_test
  (:use clojure.test masai.cereal cereal.java cereal.format)
  (:require [masai [redis :as r] [tokyo :as t] [db :as db]]))

(deftest cereal
  (doseq [db [(r/make)
              (t/make {:path "/tmp/masai-test-tokyo-db" :create true :prepop true})]]
    (with-db db (make)
      (truncate!)

      (testing "add! encodes records"
        (is (add! "foo" "bar"))
        (is (= "bar" (decode *format* (db/get *db* "foo")))))
      (testing "get decodes records"
        (is (= "bar" (get "foo")))))))