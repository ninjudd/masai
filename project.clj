(defproject masai "0.5.2-SNAPSHOT"
  :description "Key-value database for Clojure with pluggable backends."
  :dependencies [[clojure "1.2.0"]
                 [useful "0.4.0"]
                 [retro "0.5.0"]]
  :dev-dependencies [[tokyocabinet "1.24.1-SNAPSHOT"]
                     [spy/memcached "2.4rc1"]
                     [org.clojars.raynes/jedis "2.0.0-SNAPSHOT"]
                     [clojure-contrib "1.2.0"]
                     [marginalia "0.5.1"]]
  :tasks [marginalia.tasks])