(ns masai.db
  (:refer-clojure :exclude [get]))

(defprotocol DB "Key-value database."
  (open       [db])
  (close      [db])
  (sync!      [db])
  (optimize!  [db])
  (get        [db key])
  (len        [db key])
  (exists?    [db key])
  (key-seq    [db])
  (add!       [db key val])
  (put!       [db key val])
  (append!    [db key val])
  (inc!       [db key i])
  (delete!    [db key])
  (truncate!  [db]))

(defprotocol EphemeralDB
  (add-expiry! [db key val exp])
  (put-expiry! [db key val exp]))