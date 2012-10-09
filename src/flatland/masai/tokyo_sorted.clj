(ns flatland.masai.tokyo-sorted
  (:use [useful.map :only [into-map]]
        [useful.seq :only [lazy-loop]]
        [useful.experimental :only [order-let-if]])
  (:require flatland.masai.db retro.core flatland.masai.cursor
            [flatland.masai.tokyo-common :as tokyo])
  (:import [tokyocabinet BDB BDBCUR]))

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
          (if (:noblock  opts) BDB/OLCKNB  0))))

(defmacro check
  "Run a form and if it returns a false value, check for e codes. Throw
   an IOException if no e codes are present."
  [form]
  `(or ~form
       (case (.ecode ~'bdb)
         ~BDB/EKEEP  false
         ~BDB/ENOREC false
         (throw (java.io.IOException. (.errmsg ~'bdb) )))))

(defn- cursor-seq
  "Return a lazy cursor sequence by initializing the cursor by calling first and advances the cursor
  with next."
  [bdb first next]
  (let [cursor (BDBCUR. bdb)]
    (lazy-loop [more? (first cursor)]
      (when more?
        (cons [(.key2 cursor) (.val cursor)]
              (lazy-recur (next cursor)))))))

(defmacro curfn
  "Like memfn, except type hinted for BDBCUR."
  [method & args]
  `(fn [^BDBCUR cur#]
     (. cur# ~method ~@args)))

;; tokyocabinet makes it an error to open an open db, or close a closed one.
;; we'd prefer that it be a no-op, so we just ignore the request.
(def ^:private open-paths (atom #{}))

(defrecord DB [^BDB bdb opts key-format]
  flatland.masai.db/DB
  (open [db]
    (let [path  (:path opts)]
      (when-not (@open-paths path)
        (let [bnum  (or (:bnum opts)  0)
              apow  (or (:apow opts) -1)
              fpow  (or (:fpow opts) -1)
              lmemb (or (:lmemb opts) 0)
              nmemb (or (:nmemb opts) 0)]
          (.mkdirs (.getParentFile (java.io.File. ^String path)))
          (check (.tune bdb lmemb nmemb bnum apow fpow (tflags opts)))
          (when-let [[lcnum rcnum] (:cache opts)]
            (check (.setcache bdb lcnum rcnum)))
          (when-let [xmsiz (:xmsiz opts)]
            (check (.setxmsiz bdb xmsiz)))
          (check (.open bdb path (oflags opts)))
          (swap! open-paths conj path)))))
  (close [db]
    (let [path (:path opts)]
      (when (@open-paths path)
        (.close bdb)
        (swap! open-paths disj path))))
  (sync! [db]
    (.sync  bdb))
  (optimize! [db]
    (.optimize bdb))
  (unique-id [db]
    (:path opts))

  (fetch [db key]
    (.get  bdb ^bytes (key-format key)))
  (len [db key]
    (.vsiz bdb ^bytes (key-format key)))
  (exists? [db key]
    (not (= -1 (flatland.masai.db/len db key))))
  (key-seq [db]
    (.iterinit bdb)
    (lazy-loop []
      (when-let [key (.iternext2 bdb)]
        (cons key (lazy-recur)))))

  (add! [db key val]
    (check (.putkeep bdb ^bytes (key-format key) (bytes val))))
  (put! [db key val]
    (check (.put bdb ^bytes (key-format key) (bytes val))))
  (append! [db key val]
    (check (.putcat bdb ^bytes (key-format key) (bytes val))))
  (inc! [db key i]
    (.addint bdb ^bytes (key-format key) ^Integer i))

  (delete! [db key]
    (check (.out bdb ^bytes (key-format key))))
  (truncate! [db]
    (check (.vanish bdb)))

  flatland.masai.db/SequentialDB
  (cursor [db key]
    (-> (BDBCUR. bdb)
        (flatland.masai.cursor/jump (key-format key)))))

(extend DB
  retro.core/Transactional
  (tokyo/transaction-impl DB bdb BDB))

(extend-type BDBCUR
  flatland.masai.cursor/Cursor
  (next [this]
    (when (.next this)
      this))
  (prev [this]
    (when (.prev this)
      this))
  (key [this]
    (.key this))
  (val [this]
    (.val this))
  (jump [this k]
    (when (cond ;; this should be (case), but getting weird transient bugs from that
           (or (nil? k) (= :first k))
           (.first this)

           (= :last k)
           (.last this)

           :else
           (.jump this ^bytes k))
      this))

  flatland.masai.cursor/MutableCursor
  (put [this value]
    (if (.put this ^bytes value BDBCUR/CPBEFORE)
      this
      (throw (IllegalStateException. "No record to overwrite"))))
  (append [this value]
    (if (.put this ^bytes value BDBCUR/CPCAT)
      this
      (throw (IllegalStateException. "No record to append to"))))
  (delete [this]
    (if (.out this)
      this
      (throw (IllegalStateException. "No record to delete")))))

(defn make
  "Create an instance of DB with Tokyo Cabinet B-Tree as the backend."
  [& opts]
  (let [{:keys [key-format]
         :or {key-format (fn [^String s] (bytes (.getBytes (str s))))}
         :as opts}
        (into-map opts)]
    (DB. (BDB.) opts (fn [k]
                       (if (or (nil? k) (keyword? k))
                         k
                         (key-format k))))))
