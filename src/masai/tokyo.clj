(ns masai.tokyo
  (:use [useful.map :only [into-map]]
        [useful.utils :only [thread-local]])
  (:require masai.db retro.core
            [masai.tokyo-common :as tokyo])
  (:import [tokyocabinet HDB]))

(def compress
  {:deflate HDB/TDEFLATE
   :bzip    HDB/TBZIP
   :tcbs    HDB/TTCBS})

(defn- tflags
  "Check for T flags in opts and returns the corresponding tokyo flag."
  [opts]
  (bit-or
   (if (:large opts) HDB/TLARGE 0)
   (or (compress (:compress opts)) 0)))

(defn- oflags
  "Check for o flags in opts and return the corresponding tokyo flag"
  [opts]
  (reduce bit-or 0
    (list (if (:readonly opts) HDB/OREADER HDB/OWRITER)
          (if (:create   opts) HDB/OCREAT  0)
          (if (:truncate opts) HDB/OTRUNC  0)
          (if (:tsync    opts) HDB/OTSYNC  0)
          (if (:nolock   opts) HDB/ONOLCK  0)
          (if (:noblock  opts) HDB/OLCKNB  0)
          (if (:prepop   opts) HDB/OPREPOP 0)
          (if (:mlock    opts) HDB/OMLOCK  0))))

(defmacro check
  "Run a form and if it returns a false value, check for e codes. Throw
   an IOException if no e codes are present."
  [form]
  `(or ~form
       (let [code# (.ecode ~'hdb)]
         (case code#
           ~HDB/EKEEP  false
           ~HDB/ENOREC false
           (throw (java.io.IOException.
                   (format "Tokyocabinet error %d: %s" code# (.errmsg ~'hdb)) ))))))

(defn- key-seq*
  "Get a truly lazy sequence of the keys in the database." [^HDB hdb]
  (lazy-seq
   (if-let [key (.iternext2 hdb)]
     (cons key (key-seq* hdb))
     nil)))

(defrecord DB [^HDB hdb opts key-format]
  Object
  (toString [this]
    (pr-str this))

  masai.db/DB
  (open [this]
    (let [path (:path opts)
          bnum (or (:bnum opts)  0)
          apow (or (:apow opts) -1)
          fpow (or (:fpow opts) -1)]
      (.mkdirs (.getParentFile (java.io.File. ^String path)))
      (check (.tune hdb bnum apow fpow (tflags opts)))
      (when-let [rcnum (:cache opts)]
        (check (.setcache hdb rcnum)))
      (when-let [xmsiz (:xmsiz opts)]
        (check (.setxmsiz hdb xmsiz)))
      (check (.open hdb path (oflags opts)))))
  (close [this]
    (.close hdb))
  (sync! [this]
    (.sync  hdb))
  (optimize! [this]
    (.optimize hdb))
  (unique-id [this]
    (.path hdb))

  (fetch [this key]
    (.get  hdb ^bytes (key-format key)))
  (len [this key]
    (.vsiz hdb ^bytes (key-format key)))
  (exists? [this key]
    (not (= -1 (masai.db/len this key))))
  (key-seq [this]
    (.iterinit hdb)
    (key-seq* hdb))

  (add! [this key val]
    (check (.putkeep hdb ^bytes (key-format key) (bytes val))))
  (put! [this key val]
    (check (.put hdb ^bytes (key-format key) (bytes val))))
  (append! [this key val]
    (check (.putcat  hdb ^bytes (key-format key) (bytes val))))
  (inc! [this key i]
    (.addint hdb ^bytes (key-format key) ^Integer i))

  (delete! [db key]
    (check (.out hdb ^bytes (key-format key))))
  (truncate! [db]
    (check (.vanish hdb))))

(extend DB
  retro.core/Transactional
  (tokyo/transaction-impl DB hdb HDB))

(defn make
  "Create an instance of DB with Tokyo Cabinet Hash as the backend."
  [& opts]
  (let [{:keys [key-format]
         :or {key-format (fn [^String s] (.getBytes s))}
         :as opts}
        (into-map opts)]
    (DB. (HDB.) opts key-format)))
