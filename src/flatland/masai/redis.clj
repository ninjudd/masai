(ns flatland.masai.redis
  (:use [flatland.useful.map :only [into-map]]
        [clojure.stacktrace :only [root-cause]])
  (:require flatland.masai.db)
  (:import redis.clients.jedis.BinaryJedis))

(defn i-to-b
  "If input is 0, returns false. Otherwise, true."
  [i] (not= i 0))

;; When there isn't an active connection, Jedis throws errors when methods
;; are called. We don't want this happening in masai so we will check to
;; make sure there is an active connection ourselves before excuting read
;; methods.
(defmacro if-connected
  "Same as if with a predicate that checks for a redis connection."
  [db & body]
  `(if (.isConnected ~db) ~@body))

(deftype DB [^BinaryJedis rdb opts]
  flatland.masai.db/EphemeralDB

  (add-expiry! [db key val exp]
    (flatland.masai.db/add! db key val)
    (.expire rdb key exp))

  (put-expiry! [db key val exp]
    (.setex rdb key exp (bytes val)))

  flatland.masai.db/DB

  (open [db]
    (when-let [pass (:password opts)]
      (.auth rdb pass))
    ;; We have to call `disconnect` before we call connect because
    ;; Jedis's `quit` method leaves the socket in a bad state and
    ;; disconnect cleans up.
    (.disconnect rdb)
    (.connect rdb))

  (close [db]
    (.quit rdb)
    (.disconnect rdb))

  (unique-id [db]
    (let [client (.getClient rdb)]
      [(.getHost client) (.getPort client)]))

  (sync! [db]
    (.save rdb))

  (fetch [db key]
    (if-connected rdb
      (.get rdb key)))

  (len [db key]
    (if-connected rdb
      (if-let [record (flatland.masai.db/fetch db key)]
        (count record)
        -1)
      -1))

  (exists? [db key]
    (if-connected rdb
      (.exists rdb key)
      false))

  (key-seq [db]
    (set (.keys rdb (.getBytes "*"))))

  (add!    [db key val] (i-to-b (.setnx  rdb key (bytes val))))
  (put!    [db key val] (i-to-b (.set    rdb key (bytes val))))
  (append! [db key val] (i-to-b (.append rdb key (bytes val))))

  (inc! [db key i]
    (if (> 0 i)
      (.decrBy rdb key (long (Math/abs ^Integer i)))
      (.incrBy rdb key (long i))))

  (delete! [db key]
    (i-to-b (.del rdb (into-array [key]))))

  (truncate! [db]
    (= "OK" (.flushDB rdb))))

(defn make
  "Create an instance of DB with redis as the backend."
  [& opts]
  (let [{:keys [host port timeout]
         :or {host "localhost" port 6379}
         :as opts} (into-map opts)]
    (DB.
     (if timeout
       (BinaryJedis. host port timeout)
       (BinaryJedis. host port))
     opts)))
