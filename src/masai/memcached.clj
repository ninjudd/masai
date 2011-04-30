(ns masai.memcached
  (:use [useful :only [into-map]])
  (:require masai.db)
  (:import net.spy.memcached.MemcachedClient
           [java.net InetSocketAddress InetAddress]))

(deftype DB [#^MemcachedClient mdb key-format]
  masai.db/EphemeralDB
  
  (add-expiry! [db key val exp] (.get (.add mdb (key-format key) exp val)))
  (put-expiry! [db key val exp] (.get (.set mdb (key-format key) exp val)))
  
  masai.db/DB
  
  (close [db] (.shutdown mdb))
  (get [db key] (.get mdb (key-format key)))
  (add! [db key val] (masai.db/add-expiry! db key val 0))
  (put! [db key val] (masai.db/put-expiry! db key val 0))
  
  (append!
   [db key val]
   (let [fkey (key-format key)]
     (if-let [appended (when-let [cas (.gets mdb key)]
                         (.get (.append mdb (.getCas cas) fkey val)))]
       appended
       (masai.db/add! db key val))))

  (len
   [db key]
   (if-let [record (masai.db/get db key)]
     (count (str record))
     -1))
  
  (inc!
   [db key i]
   (if (> 0 i)
     (.decr mdb (key-format key) (Math/abs i))
     (.incr mdb (key-format key) i)))
  
  (delete! [db key] (.get (.delete mdb (key-format key))))
  (truncate! [db] (.get (.flush mdb))))

(defn make [& opts]
  (let [{:keys [key-format addresses]
         :or {key-format identity addresses {"localhost" 11211}}}
        (into-map opts)]
    (DB.
     (MemcachedClient.
      (for [[addr port] addresses]
        (InetSocketAddress. (InetAddress/getByName addr) port)))
     key-format)))