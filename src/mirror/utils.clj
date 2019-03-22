(ns mirror.utils
  "Utils to manipulate copy-state map"
  (:require
   [honeysql.core :as hsql]))

(defn map-kv [f m]
  (reduce-kv (fn [m k v]
               (apply assoc m (f k v))) {} m))

(defn map-k [f m]
  (map-kv (fn [k v] [(f k) v]) m))

(defn map-v [f m]
  (map-kv (fn [k v] [k (f v)]) m))

(defn referenced-columns->fk-columns
  "Rename referenced columns in copy-state to fk-columns
   by looking up fk-relationships of a specific table
   e.g.
   for :cluster_items

   {:table :cluster_items,
    :pk-columns '(:cluster_items_id),
    :fk-relationships
    {:documents
     {:document_id :item_id},
    :clusters
     {:cluster_id :cluster_id}}}

    {:documents {:document_id {821 822}}
     :clusters {:cluster_id {45,66}}}

    ->

    {:documents {:item_id {821 822}}
     :clusters {:cluster_id {45,66}}}

   this updated map could be used to query in :cluster_items "
  [copy-state fk-relationships]
  (reduce-kv
   (fn [acc table v]
     (assoc acc table (reduce-kv (fn [m column column-vals]
                                   (let [fk-column (column (table fk-relationships))]
                                     (assoc m fk-column column-vals))) {} v)))
   {}
   copy-state))

(defn pick-from-ids
  "Pick up from ids from copy-state
   e.g.
   {:matters {:matter_id {107 108}}
    :documents {:document_id {1395 3251,1396 3252}}}

   ->

   {:matter_id [107]
    :document_id [1395 1396]} "
  [copy-state]
  (reduce-kv
   (fn [acc table [column column-vals]]
     (assoc acc column  (keys column-vals)))
   {}
   copy-state))
