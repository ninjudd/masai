;; ## Cereal is tasty
;; This namespace wraps masai's DB protocol functions. The wrapper uses cereal
;; to cerealize... serialize values.

(ns masai.cereal
  (:refer-clojure :exclude [get])
  (:require [masai.db :as db]
            [cereal.format :as c]))

(def ^{:dynamic true} *db* nil)
(def ^{:dynamic true} *format* nil)

(defn- encode [val] (c/encode *format* val))
(defn- decode [val] (c/decode *format* val))

;; ## DB Wrapper

(defn open
  "Open the database."
  [] (db/open *db*))

(defn close
  "Close the database."
  [] (db/close *db*))

(defn sync!
  "Sync the database to the disk."
  [] (db/sync! *db*))

(defn optimize!
  "Optimize the database."
  [] (db/optimize! *db*))

(defn get
  "Get a record from the database."
  [key] (decode (db/get *db* key)))

(defn len
  "Get the length of a record from the database. Returns -1 if record
   is non-existent."
  [key] (db/len *db* key))

(defn exists?
  "Check to see if a record exists in the database."
  [key] (db/exists? *db* key))

(defn key-seq
  "Get a sequence of all of the keys in the database."
  [] (db/key-seq *db*))

(defn add!
  "Add a record to the database only if it doesn't already exist."
  [key val] (db/add! *db* key (encode val)))

(defn put!
  "Put a record into the database, overwriting it if it already exists."
  [key val] (db/put! *db* key (encode val)))

(defn append!
  "Append data to a record in the database."
  [key val] (db/append! *db* key (encode val)))

(defn inc!
  "Increment a record in the database. Handles negative values of
   i by decrementing."
  [key i] (db/inc! *db* key i))

(defn delete!
  "Delete a record from the database."
  [key] (db/delete! *db* key))

(defn truncate!
  "Flush all records from the database."
  [] (db/truncate! *db*))

;; ## EphemeralDB Wrapper

(defn add-expiry!
  "Add a record to the database with an expiration time. Doesn't add if
   record already exists."
  [key val exp] (db/add-expiry! *db* key (encode val) exp))

(defn put-expiry!
  "Puts a record into the database with an expiration time. Overwrites the
   record if it already exists."
  [key val exp] (db/put-expiry! *db* key (encode val) exp))

;; ## Utilities

(defmacro with-db
  "Run the body with *db* bound to db and *format* bound to *format*
   Always closes *db*."
  [db format & body]
  `(binding [*db* ~db
             *format* ~format]
     (try (open)
          ~@body
          (finally (close)))))