(defproject masai "0.5.1-SNAPSHOT"
  :description "Key-value database for Clojure with pluggable backends."
  :dependencies [[clojure "1.2.0"]
                 [tokyocabinet "1.24.1-SNAPSHOT"]
                 [clojure-useful "0.3.3"]
                 [retro "0.5.0"]
                 [spy/memcached "2.4rc1"]
                 [org.clojars.raynes/jedis "2.0.0-SNAPSHOT"]])