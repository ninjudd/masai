(ns masai.cursor
  (:refer-clojure :exclude [next key val]))

(defprotocol Cursor
  "A mutable object for iterating through a sequential database. Rather than pointing 'between'
  items like a java.util.Iterator, the cursor points directly at items. The current item's key or
  value can be inspected, and the cursor can move in either direction, as well as jumping to a
  different location entirely. Again, note that the cursor is not persistent: (next cursor) returns
  a Cursor object which may (or may not) be identical? to the previous cursor, and in either case
  the previous cursor should not be reused.

  In general when operations fail (because the cursor is in an illegal position), nil is
  returned. Otherwise, a cursor is returned. Only if nil is returned (indicating a failed
  operation) is it safe to reuse the previous cursor.

  No mutation of the database is supported by this protocol: to mutate you must use a MutableCursor
  instead."
  (next [cursor]
    "Advance the cursor by one entry; leave in place if no more entries exist.")
  (prev [cursor]
    "Retreat the cursor by one entry; leave in place if no more entries exist.")
  (key [cursor]
    "Get the key of the cursor's current entry.")
  (val [cursor]
    "Get the value of the cursor's current entry.")
  (jump [cursor key]
    "Move the cursor to the first entry whose key is >= key."))

(defprotocol MutableCursor
  "Protocol functions for mutating a database through a Cursor. When an operation fails, an
  exception is thrown; otherwise a cursor is returned."
  (put [cursor value]
    "Replace the current record's value.")
  (append [cursor value]
    "Append to the current record's value.")
  (delete [cursor]
    "Delete the current record."))

(extend nil
  Cursor
  (into {} (for [m [:next :prev :key :val :jump]]
             [m (constantly nil)]))

  MutableCursor
  (into {} (for [m [:put :delete :append]]
             [m (fn [& args] (throw (IllegalStateException. "Invalid cursor: nil")))])))
