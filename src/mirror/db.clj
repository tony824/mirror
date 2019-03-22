(ns mirror.db
  ""
  (:require
   [clojure.java.jdbc :as j]
   [clojure.tools.logging :as logging]
   [clojure.spec.alpha :as s]
   [honeysql.core :as hsql]))

(defn build-query
  "{:user_id [1 2], :document_id [1395 1396], :result_id [1 2]}"
  [table m]
  (let [where (reduce-kv  (fn [acc k v] (conj acc [:in k v])) [:and ] m)
        query (hsql/build {:select  [:*]
                           :from [table]
                           :where where})]
    (hsql/format query :parameterizer :none)))

(defn create-temporary-table!
  "CREATE TEMPORARY TABLE IF NOT EXISTS temp_worksheets
   ON COMMIT DROP AS
   SELECT * FROM worksheets WHERE (matter_id) in ((6),(9))"
  [db table sql]
  (let [query (str "CREATE TEMPORARY TABLE IF NOT EXISTS " (name table)
                   " ON COMMIT DROP AS "
                   sql)]
    (j/execute! db query {:return-keys [:*]})))

(defn- where-impl
  [left-table right-table columns]
  (let [filter-fn (fn [left-table right-table column]
                    [:= (keyword (str (name left-table) "." (name column)))
                        (keyword (str (name right-table) "." (name column)))])]
    (->> columns
         (map (partial filter-fn left-table right-table))
         (into [:and]))))


(defn- build-select-impl
  [table column]
  (let [sequence (hsql/call :pg_get_serial_sequence
                             (name table)
                             (name column))
        new-pk (hsql/call :nextval sequence)]
    [column
     [(hsql/call :coalesce new-pk column) (keyword (str "next_" (name column)))]]))

(defn- build-select
  [table pk-columns fk-columns]
  (let [pk-selects (mapcat (partial build-select-impl table)pk-columns)])
  (reduce (fn [acc fk-column]
            (conj acc (keyword (str "vals." (name column)))
                      (keyword (str "vals.next_" (name column)))))
          pk-selects fk-columns))

;; update pk-columns ,for each pk-column use  nextval
;; update fk-columns , for each fk-column , get the mapping from copy-state
;; for a table with no auto increment pk , no fk , no need to update , insert into select directly
;;{:matters {:matter_id {6,155,9,178}} :documents {:document_id  {1 2 3 4 5 6}}}
(defn- from-to-mapping-impl
  " SELECT worksheet_id,
           COALESCE (nextval (pg_get_serial_sequence('worksheets' ,'worksheet_id')),  worksheet_id) as next_worksheet_id
           vals.matter_id,
           vals.next_matter_id
    FROM  worksheets t
    JOIN (VALUES(6,155),(9,178)) as vals(matter_id,next_matter_id) on t.matter_id=vals.matter_id
    WHERE (t.matter_id) in ((6),(9))"
  [table pk-columns fk-columns-vals]
  (let [fk-columns (keys fk-columns-vals)]
    {:select (build-select table pk-columns fk-columns)
     :from [[table :t]]
     :join [[{:values
              (map
               (fn [columns-values-map]
                 (mapcat first (vals columns-values-map)))
               fk-columns)}
             (apply hsql/call :vals fk-columns)]
            (where-impl :t :vals fk-columns)]
     :where (reduce-kv  (fn [acc k v] (conj acc [:in k v])) [:and ] fk-columns-vals)}))

(defn- rename-column
  ([table column]
   (rename-column table column nil))
  ([table column prefix]
   (keyword (str (name table) "." prefix (name column)))))

;; be carefule with set and where
;; remove columns whose values are null in set (pk-columns and fk-columns in set)
;; ensure columns in where  could be found in copy-state (fk-columns in where)

(defn- update-temp-table-impl
  "UPDATE temp_worksheets as tmp
   SET tmp.worksheet_id = ftm.next_worksheet_id,tmp.matter_id = ftm.next_matter_id
   FROM from_to_mapping as  pfm
   WHERE tmp.worksheet_id = ftm.worksheet_id and tmp.matter_id = ftm.matter_id
   RETURNING tmp.*"
  [temp-table pk-column columns]
  (let [from (rename-column :tmp)
        to (rename-column :ftm "next_")
        set-clause (zipmap from to)]
    {:update    [temp-table :tmp]
     :set       set-clause
     :from      [[:from_to_mapping :ftm]]
     :where     (where-impl :tmp :ftm columns)
     :returning [:tmp.*]}))

(defn copy-from-temp-table!
  "WITH from_to_mapping   AS (from-to-mapping-impl)
        update_temp_table AS (update-temp-table-impl)
        copying           AS (INSERT INTO worksheets
		               SELECT * from update_temp_table)
   SELECT * from from_to_mapping "
  [db table temp-table pk-column columns]
  (let [query {:with [[:from_to_mapping (from-to-mapping-impl pk-column columns)
                       :update_temp_table (update-temp-table-impl temp-table pk-column columns)
                       :copying {:insert-into
                                 [(keyword table)
                                  {:select [:*]
                                   :from   [:update_temp_table]}]}]]
               :select [:*]
               :from   [:from_to_mapping]}
        q (hsql/format (hsql/build query) :parameterizer :none)]
    (println q)
    #_(->> query
           (hsql/build)
           (hsql/format)
           (j/execute! db))))
