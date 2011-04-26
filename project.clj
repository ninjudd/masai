(defproject masai "0.5.1-SNAPSHOT"
  :description "Key-value database for Clojure with pluggable backends."
  :repositories {"spymemcached" "http://bleu.west.spy.net/~dustin/m2repo/"}
  :dependencies [[clojure "1.2.0"]
                 [tokyocabinet "1.24.1-SNAPSHOT"]
                 [clojure-useful "0.3.3"]
                 [retro "0.5.0"]
                 [spy/memcached "2.4rc1"]
                 [redis.clients/jedis "1.5.2"]])
