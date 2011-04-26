(ns masai.memcached
  (:require masai.db)
  (:import net.spy.memcached.MemcachedClient
           [java.net InetSocketAddress InetAddress]))

(deftype DB [#^MemcachedClient mdb key-format]
  masai.db/EphemeralDB
  
  (add-expiry! [db key val exp] (.add mdb (key-format key) exp val))
  (put-expiry! [db key val exp] (.set mdb (key-format key) exp val))
  
  masai.db/DB
  
  (close [db] (.shutdown mdb))
  (get [db key] (.get mdb (key-format key)))
  (add! [db key val] (masai.db/add-expiry! db key val 0))
  (put! [db key val] (masai.db/put-expiry! db key val 0))
  
  (append!
   [db key val]
   (let [fkey (key-format key)]
     (.append mdb (.getCas (.gets mdb fkey)) fkey val)))
  
  (inc!
   [db key i]
   (if (> 0 i)
     (.decr mdb (key-format key) (Math/abs i))
     (.incr mdb (key-format key) i)))
  
  (delete! [db key] (.delete mdb (key-format key))))

(defn make [& {:keys [key-format addresses]
               :or {key-format identity addresses {"localhost" 11211}}}]
  (DB.
   (MemcachedClient.
    (for [[addr port] addresses]
      (InetSocketAddress. (InetAddress/getByName addr) port)))
   key-format))