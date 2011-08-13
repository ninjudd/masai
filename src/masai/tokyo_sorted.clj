(ns masai.tokyo-sorted
  (:use [useful.map :only [into-map]])
  (:require masai.db retro.core)
  (:import [tokyocabinet BDB]))

(def compress
  {:deflate BDB/TDEFLATE
   :bzip    BDB/TBZIP
   :tcbs    BDB/TTCBS})

(defn- tflags
  "Check for T flags in opts and returns the corresponding tokyo flag."
  [opts]
  (bit-or
   (if (:large opts) BDB/TLARGE 0)
   (or (compress (:compress opts)) 0)))

(defn- oflags
  "Check for o flags in opts and return the corresponding tokyo flag"
  [opts]
  (reduce bit-or 0
    (list (if (:readonly opts) BDB/OREADER BDB/OWRITER)
          (if (:create   opts) BDB/OCREAT  0)
          (if (:truncate opts) BDB/OTRUNC  0)
          (if (:tsync    opts) BDB/OTSYNC  0)
          (if (:nolock   opts) BDB/ONOLCK  0)
          (if (:noblock  opts) BDB/OLCKNB  0)
          (if (:prepop   opts) BDB/OPREPOP 0)
          (if (:mlock    opts) BDB/OMLOCK  0))))

(defmacro check
  "Run a form and if it returns a false value, check for e codes. Throw
   an IOException if no e codes are present."
  [form]
  `(or ~form
       (case (.ecode ~'hdb)
         ~BDB/EKEEP  false
         ~BDB/ENOREC false
         (throw (java.io.IOException. (.errmsg ~'hdb) )))))

(defn- key-seq*
  "Get a truly lazy sequence of the keys in the database." [^BDB hdb]
  (lazy-seq
   (if-let [key (.iternext2 hdb)]
     (cons key (key-seq* hdb))
     nil)))

(deftype DB [^BDB hdb opts key-format]
  masai.db/DB

  (open [db]
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

  (close     [db] (.close hdb))
  (sync!     [db] (.sync  hdb))
  (optimize! [db] (.optimize hdb))

  (get [db key] (.get  hdb ^"[B" (key-format key)))
  (len [db key] (.vsiz hdb ^"[B" (key-format key)))
  (exists? [db key] (not (= -1 (masai.db/len db key))))

  (key-seq [db]
    (.iterinit hdb)
    (key-seq* hdb))

  (add!    [db key val] (check (.putkeep hdb ^"[B" (key-format key) (bytes val))))
  (put!    [db key val] (check (.put     hdb ^"[B" (key-format key) (bytes val))))
  (append! [db key val] (check (.putcat  hdb ^"[B" (key-format key) (bytes val))))
  (inc!    [db key i]   (.addint hdb ^"[B" (key-format key) ^Integer i))

  (delete!   [db key] (check (.out    hdb ^"[B" (key-format key))))
  (truncate! [db]     (check (.vanish hdb)))

  retro.core/Transactional

  (txn-begin    [db] (.tranbegin  hdb))
  (txn-commit   [db] (.trancommit hdb))
  (txn-rollback [db] (.tranabort  hdb)))

(defn make
  "Create an instance of DB with Tokyo Cabinet B-Tree as the backend."
  [& opts]
  (let [{:keys [key-format]
         :or {key-format (fn [^String s] (bytes (.getBytes (str s))))}
         :as opts}
        (into-map opts)]
    (DB. (BDB.) opts key-format)))