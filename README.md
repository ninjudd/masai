Masai is key-value database for Clojure with pluggable backends. Currently TokyoCabinet, Memcache and Redis are supported, but alternative backend implementations are welcome. SQL, BerkeleyDB JE, and Lucene implementations are planned.

# Usage

    (use '(masai db tokyo))

    (with-open [db (make :path "foo" :create true)]
      (open db)
      (put! db "a" "123")
      (get db "a"))
    ; "123"
