(defproject org.flatland/masai "0.7.1"
  :url "https://github.com/flatland/masai"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :description "Key-value database for Clojure with pluggable backends."
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [useful "0.8.4"]
                 [retro "0.7.0"]]
  :profiles {:1.5 {:dependencies [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]}
             :1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :dev {:dependencies [[tokyocabinet "1.24.4"]
                                  [spy/memcached "2.4rc1"]
                                  [org.clojars.raynes/jedis "2.0.0-SNAPSHOT"]]}}
  :aliases {"testall" ["with-profile" "dev,default:dev,1.3,default:dev,1.5,default" "test"]}
  :repositories {"sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}})
