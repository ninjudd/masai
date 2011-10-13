(defproject masai "0.7.0-alpha1"
  :description "Key-value database for Clojure with pluggable backends."
  :dependencies [[clojure "1.2.0"]
                 [useful "0.7.1"]
                 [retro "0.6.0-alpha2"]]
  :dev-dependencies [[tokyocabinet "1.24.1-SNAPSHOT" :ext true]
                     [spy/memcached "2.4rc1"]
                     [org.clojars.raynes/jedis "2.0.0-SNAPSHOT"]])
