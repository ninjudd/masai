(ns masai.db
  (:refer-clojure :exclude [get]))

(defprotocol DB "Key-value database."
  (open [db])
  (close      [db] "Close the database.")
  (sync!      [db] "Sync the database to the disk.")
  (optimize!  [db] "Optimize the database.")
  (get        [db key] "Get a record from the database.")
  (len        [db key] "Get the length of a record from the database.
                        Returns -1 if record is non-existent.")
  (exists?    [db key] "Check to see if a record exists in the database.")
  (key-seq    [db] "Get a sequence of all of the keys in the database.")
  (add!       [db key val] "Add a record to the database only if it doesn't
                            already exist.")
  (put!       [db key val] "Put a record into the database, overwriting it if
                            it already exists.")
  (append!    [db key val] "Append data to a record in the database.")
  (inc!       [db key i] "Increment a record in the database. Handles negative
                          values of i by decrementing.")
  (delete!    [db key] "Delete a record from the database.")
  (truncate!  [db] "Flush all records from the database."))

(defprotocol EphemeralDB
  (add-expiry! [db key val exp] "Add a record to the database with an expiration
                                 time. Doesn't add if record already exists.")
  (put-expiry! [db key val exp] "Puts a record into the database with an expiration
                                 time. Overwrites the record if it already exists."))