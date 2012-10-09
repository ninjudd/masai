(ns flatland.masai.tokyo-common
  (:use [useful.utils :only [thread-local]])
  (:require [flatland.masai.db :as db]
            [retro.core :as retro]))

;; I hate this so much. But we need per-thread mutable state, without the safety of binding
(def transaction-depths (thread-local (atom {})))

(defmacro maybe-transact
  "Update the transaction-depth for db by calling f on its current value; if the
   new value equals the value passed in, then also perform action."
  [db f value action]
  `(let [id# (db/unique-id ~db)]
     (when (= ~value (get (swap! @transaction-depths update-in [id#] ~f)
                     id#))
       ~action)))

(defmacro transaction-impl
  "Generate an implementation (suitable for use with extend) for a tokyo layer
   with the given class and tokyo-db fieldname and type-hinted appropriately."
  [class db-field db-type]
  (let [db (with-meta (gensym 'db) {:tag class})
        impl (with-meta `(. ~db ~db-field) {:tag db-type})]
    `{:txn-begin! (fn [~db]
                    (maybe-transact ~db (fnil inc 0) 1 (.tranbegin ~impl)))
      :txn-commit! (fn [~db]
                     (maybe-transact ~db dec 0 (.trancommit ~impl)))
      :txn-rollback! (fn [~db]
                       ;; not really a maybe here, but using the macro is simpler than doing it by hand
                       (maybe-transact ~db (constantly 0) 0 (.tranabort ~impl)))}))