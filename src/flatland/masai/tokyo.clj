(ns flatland.masai.tokyo
  (:use [flatland.useful.map :only [into-map]]
        [flatland.useful.utils :only [thread-local]])
  (:require flatland.masai.db flatland.retro.core
            [flatland.masai.tokyo-common :as tokyo])
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
   (if-let [key (.iternext hdb)]
     (cons key (key-seq* hdb))
     nil)))

;; tokyocabinet makes it an error to open an open db, or close a closed one.
;; we'd prefer that it be a no-op, so we just ignore the request.
(def ^:private open-paths (atom #{}))

(defrecord DB [^HDB hdb opts]
  Object
  (toString [this]
    (pr-str this))

  flatland.masai.db/DB
  (open [this]
    (let [path (:path opts)]
      (when-not (@open-paths path)
        (let [bnum (or (:bnum opts)  0)
              apow (or (:apow opts) -1)
              fpow (or (:fpow opts) -1)]
          (.mkdirs (.getParentFile (java.io.File. ^String path)))
          (check (.tune hdb bnum apow fpow (tflags opts)))
          (when-let [rcnum (:cache opts)]
            (check (.setcache hdb rcnum)))
          (when-let [xmsiz (:xmsiz opts)]
            (check (.setxmsiz hdb xmsiz)))
          (check (.open hdb path (oflags opts)))
          (swap! open-paths conj path)))))
  (close [this]
    (let [path (:path opts)]
      (when (@open-paths path)
        (.close hdb)
        (swap! open-paths disj path))))
  (sync! [this]
    (.sync  hdb))
  (optimize! [this]
    (.optimize hdb))
  (unique-id [this]
    (:path opts))

  (fetch [this key]
    (.get  hdb ^bytes key))
  (len [this key]
    (.vsiz hdb ^bytes key))
  (exists? [this key]
    (not (= -1 (flatland.masai.db/len this key))))
  (key-seq [this]
    (.iterinit hdb)
    (key-seq* hdb))

  (add! [this key val]
    (check (.putkeep hdb ^bytes key (bytes val))))
  (put! [this key val]
    (check (.put hdb ^bytes key (bytes val))))
  (append! [this key val]
    (check (.putcat  hdb ^bytes key (bytes val))))
  (inc! [this key i]
    (.addint hdb ^bytes key ^Integer i))

  (delete! [db key]
    (check (.out hdb ^bytes key)))
  (truncate! [db]
    (check (.vanish hdb))))

(extend DB
  flatland.retro.core/Transactional
  (tokyo/transaction-impl DB hdb HDB))

(defn make
  "Create an instance of DB with Tokyo Cabinet Hash as the backend."
  [& opts]
  (DB. (HDB.) (into-map opts)))
