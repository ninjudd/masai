(defproject masai "0.7.0-alpha8"
  :description "Key-value database for Clojure with pluggable backends."
  :dependencies [[clojure "1.3.0"]
                 [useful "0.7.5-alpha3"]
                 [retro "0.6.0-alpha3"]]
  :dev-dependencies [[tokyocabinet "1.24.2"]
                     [spy/memcached "2.4rc1"]
                     [org.clojars.raynes/jedis "2.0.0-SNAPSHOT"]])
