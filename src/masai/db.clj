(ns masai.db)

;; Instead of having separate, incompatible libraries to interface with
;; different key-value stores, Masai opts to define a common and simple
;; interface to implement for all of them. DB defines the basic things
;; that you need to work with key-value stores, and little more. It
;; isn't meant to be a comprehensive interface to all of them, but just
;; a common interface.

(defprotocol DB
  "Key-value database."
  (open [db]
    "Open the database.")
  (close [db]
    "Close the database.")
  (unique-id [db]
    "An identifier for this database, unique within the current jvm. Two DBs with the
     same backing storage should have the same unique-id. For example, the jdbc URI or a filepath.")
  (sync! [db]
    "Sync the database to the disk.")
  (optimize! [db]
    "Optimize the database.")
  (fetch [db key]
    "Get a record from the database.")
  (len [db key]
    "Get the length of a record from the database. Returns -1 if record is non-existent.")
  (exists? [db key]
    "Check to see if a record exists in the database.")
  (key-seq [db]
    "Get a sequence of all of the keys in the database.")
  (add! [db key val]
    "Add a record to the database only if it doesn't already exist.")
  (put! [db key val]
    "Put a record into the database, overwriting it if it already exists.")
  (append! [db key val]
    "Append data to a record in the database.")
  (inc! [db key i]
    "Increment a record in the database. Handles negative values of i by decrementing.")
  (delete! [db key]
    "Delete a record from the database.")
  (truncate! [db]
    "Flush all records from the database."))

;; Some key-value stores have expiring `add` and `put` methods. Those
;; databases should implement EphemeralDB.

(defprotocol EphemeralDB
  (add-expiry! [db key val exp]
    "Add a record to the database with an expiration time. Doesn't add if record already exists.")
  (put-expiry! [db key val exp]
    "Puts a record into the database with an expiration time. Overwrites the record if it already
    exists."))

(defprotocol SequentialDB
  (fetch-seq [db key]
    "Return a forward seq of all key/val pairs in the database starting at key, or the first key in
    the database if key is nil.")
  (fetch-rseq [db key]
    "Return a revere seq of all key/val pairs in the database starting at key, or the last key in
    the database if key is nil.")
  (cursor [db key]
    "Return a Cursor on this db, starting at key. The key may be an actual string key, or one of
    the special keywords :first or :last"))

(defprotocol SortedDB
  (fetch-subseq [db test key] [db start-test start end-test end]
    "Like clojure.core/subseq, return a forward seq of all key/val pairs between start and end.")
  (fetch-rsubseq [db test key] [db start-test start end-test end]
    "Like clojure.core/rsubseq, return a reverse seq of all key/val pairs between start and end."))

(defn- include [test key]
  (fn [[k]] (test (compare k key) 0)))

(defn- subseq* "A helper for creating a subseq or rsubseq using fetch-seq and fetch-rseq."
  ([fetch-seq bounded? db test key]
     (seq (let [include? (include test key)]
            (if (bounded? test)
              (drop-while (complement include?) (fetch-seq db key))
              (take-while include?              (fetch-seq db nil))))))
  ([fetch-seq db start-test start-key end-test end-key]
     (seq (->> (fetch-seq db start-key)
               (drop-while (complement (include start-test start-key)))
               (take-while (include end-test end-key))))))

(extend-type Object
  SortedDB
  (fetch-subseq
    ([db test key]
       (subseq* fetch-seq #{>= >} db test key))
    ([db start-test start end-test end]
       (subseq* fetch-seq db start-test start end-test end)))
  (fetch-rsubseq
    ([db test key]
       (subseq* fetch-rseq #{<= <} db test key))
    ([db start-test start end-test end]
       (subseq* fetch-rseq db end-test end start-test start))))
