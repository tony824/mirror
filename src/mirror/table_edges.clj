(ns mirror.table-edges
  "Functions dealoing with table-edges"
  (:require
   [clojure.java.jdbc :as j]
   [clojure.spec.alpha :as s]
   [honeysql.core :as hsql]
   [honeysql.types :as hsqlt]
   [loom.graph :as graph]
   [loom.alg-generic :as graph-algs]
   [mirror.canonical-keys :refer [db->clojure]]
   [mirror.postgresql]
   [mirror.utils :as  utils]))

;; -----------------------------------------------------------------------------
;; Generate Table Edges
;; -----------------------------------------------------------------------------

(def  pg-constraint-type
  {:foreign-key "f"
   :check "c"
   :primary-key "p"
   :unique "u"
   :trigger "t"
   :exclusion "x"})

(def  pg-class-relkind
  {:ordinary "r"
   :index "i"
   :sequence "S"
   :view "v"
   :composite "c"
   :toast "t"
   :foreign "f"})

(def  pg-namespace-type
  {:public "public"
   :catalog "pg_catalog"
   :information "information_schema"})

(def project-child-tables-black-list
  "Set of table-names we do not want included when finding the `matters` table
  dependencies while copying a matter.

  We chose to ignore these tables because they are either deprecated, specific
  to deloitte, not in use, related but not specific to a project or just not
  needed while copying a project"
  #{:metadata_values
    :document_counts
    :field_counts
    :document_storage_penalties
    :document_view_history
    :model_validation_details
    :online_validation_data
    :pending_approvals
    :shared_matters
    :downloads
    :uploads
    :matter_document_counts
    ; deprecated tables
    :__extract_standoff_migration_deprecated_r50
    :field_instance_options_deprecated_r53
    :matters_provision_sets_deprecated_r54
    ; ignored because permissions and roles need to be copied separatley
    :deleted_project_roles_permissions
    :archived_project_roles_permissions
    :archived_project_user_roles})

(defn- quote-ident
  "Return the given string suitably quoted to be used as an identifier in an SQL
  statement string

  Arguments:
     s - string.  table name or column name"
  [s]
  (hsql/call :quote_ident s))


(defn- get-table-oid
  "Returns a table's identifier

  Arguments:
     table-name - string.  Name of table to get identifier"
  [table-name]
  (->> table-name
       quote-ident
       (hsql/call :regclass)))


(defn- pk-columns-impl
  "Returns an HSQL map which is used to get primary key of a table.
  Supports composite keys e,g. online_model_metadata '(:field_id,:user_id,:batch_id)

  Arguments:
     table-name - string.  Name of table to get primary key"
  [table-name]
  {:select [[(quote-ident :a.attname) :column_name]]
   :from   [[:pg_constraint :r]]
   :join   [[:pg_attribute :a] [:and
                                [:= :a.attnum (hsql/call :any :r.conkey)]
                                [:= :a.attrelid :r.conrelid]]]
   :where  [:and
            [:= :r.contype (:primary-key pg-constraint-type)]
            [:= :r.conrelid (get-table-oid table-name)]]})


(defn- fk-relationships-impl
  "Returns an HSQL map which is used to get an array of arrays.
   Each subarray is a list like (fk-table,fk-table-column,fk-column)
   e,g. '(:users :user_id :creator_user_id) for 'documents_metadata' table

  Arguments:
     table-name - string.  Name of table to get foreign key relationship"
  [table-name]
  {:select [[(hsqlt/array [(quote-ident :c.relname)
                           (quote-ident :fa.attname)
                           (quote-ident :a.attname)])
             :column_name]]
   :from   [[:pg_constraint :r]]
   :join   [[:pg_attribute :a] [:and
                                [:= :a.attnum (hsql/call :any :r.conkey)]
                                [:= :a.attrelid :r.conrelid]]
            [:pg_attribute :fa] [:and
                                 [:= :fa.attnum (hsql/call :any :r.confkey)]
                                 [:= :fa.attrelid :r.confrelid]]
            [:pg_class :c] [:= :c.oid :r.confrelid]]
   :where  [:and
            [:= :r.contype (:foreign-key pg-constraint-type)]
            [:= :r.conrelid (get-table-oid table-name)]
            [:=
             (hsql/call :array_position :r.confkey :fa.attnum)
             (hsql/call :array_position :r.conkey :a.attnum)]]})


(defn- transform-row
  "Transform table and child-table from string to keyword
   Transform pk-columns from a list of string to a list of keyword
   Transform fk-relationships from a list of list to a list of map

  e.g.
  {:table :cluster_items
   :child-table nil
   :pk-columns '(:cluster_items_id)
   :fk-relationships {:documents {:document_id :item_id}
                      :clusters {:cluster_id :cluster_id}}}

  Arguments
    row - a row from the database
  Returns database records"
  [row]
  (letfn [(keywordize
            [s]
            (map keyword s))
          (update-fk-relationships
            [fk-relationships]
            (->> fk-relationships
                 (map keywordize)
                 (group-by first)
                 (utils/map-kv (fn [k v]
                                 [k (apply hash-map (mapcat rest v))]))))]
    (-> row
        (update :table keyword)
        (update :child-table keyword)
        (update :pk-columns keywordize)
        (update :fk-relationships update-fk-relationships))))

(defn resolve-all-table-edges
  "Get all oridnary tables in database,
   find child_table for each table with fk if fk exists,
   build pk-columns and and fk-relationships for talbe (parent)

  Arguments:
     db          map? database connection"
  [db]
  (let [query {:select [[(quote-ident :pc.relname) :table]
                        [(quote-ident :pfc.relname) :child_table]
                        [(hsql/call :array [(pk-columns-impl :pc.relname)])
                         :pk_columns]
                        [(hsql/call :array [(fk-relationships-impl :pc.relname)])
                         :fk_relationships]]
               :from   [[:pg_class :pc]]
               :join   [[:pg_namespace :pn] [:= :pn.oid :pc.relnamespace]]
               :left-join [[:pg_constraint :pr]  [:= :pc.oid :pr.confrelid]
                           [:pg_class :pfc]  [:= :pfc.oid :pr.conrelid]]
               :where  [:and
                        [:= :pc.relkind
                         (:ordinary pg-class-relkind)]
                        [:= :pn.nspname
                         (:public pg-namespace-type)]]}]
    (->> query
         (hsql/build)
         (hsql/format)
         (#(j/query db % {:identifiers db->clojure
                          :row-fn transform-row})))))

;; -----------------------------------------------------------------------------
;; Sort Table Edges
;; -----------------------------------------------------------------------------


(defn graph-from-table-edges
  "Takes a set of fk edges with {table, child-table} keys and turns them into
   a graph.

   Arguments:
   * table-edges: The fk edges, from resolve-all-table-edges or similar.
   * blacklist: A list of table names whose incident edges should not be used."
  [table-edges blacklist]
  (let [blacklisted? (set blacklist)
        ; Some entries in table-edges have nil child-tables;
        ; we don't care about these entries.
        ; We also don't want to include any blacklisted edges.
        table-edges (filter (fn [{parent :table child :child-table}]
                              (and child
                                   (not (blacklisted? parent))
                                   (not (blacklisted? child))))
                         table-edges)

        vertices (set (concat (map :table       table-edges)
                              (map :child-table table-edges)))
        edges    (keep (fn [{p :table c :child-table}]
                        ; Remove loops
                        (when (not= p c) [p c]))
                       table-edges)]
    (apply graph/digraph (concat vertices edges))))

(defn ordered-tables-to-copy
  "Returns a list of tables to be copied, sorted so that tables later in the
   list may (but will not always) have dependencies on tables earlier in the
   list.

   Returns nil if no ordering is possible (i.e. if there are cycles).

   Arguments:
   * table-edges: The fk relationships, from resolve-all-table-edges or
                  similar.
   * starting-table: The table which should be the ancestor of all other tables
                     in the order.
   * blacklist: A list of table names which should not be included in results,
                nor should their children (unless their children are also
                children of a non-blacklisted table)."
  [table-edges starting-table blacklist]
  (let [g (graph-from-table-edges table-edges blacklist)]
    (graph-algs/topsort-component (graph/successors g) starting-table)))


(defn decorate-ordered-tables-seq
 "Takes a list of tables ordered by their dependencies and decorates
 it by adding primary and foreign keys informations preserving the
 dependencies order.

 :child-table key is removed from all table-edges because its not specifically
 needed during the copy operation.

 Returns a list of table to be copied with their
 primary and foreign keys informations order by thiere dependencies."
 [fk-relationships ordered-tables-seq]
 (let [fk-map (zipmap (map :table fk-relationships)
                      (map #(select-keys % [:table
                                            :pk-columns
                                            :fk-relationships])
                           fk-relationships))]
   (map fk-map ordered-tables-seq)))


(defn ordered-table-edges
  "Extract tables copy dependencies, tables primary and tables foreign
  keys informations from the DB for a given table.

  Arguments:
    db             - database connection
    starting-table - keyword. The table which should be the ancestor of all other
                     tables in the order.
    blacklist   -   optional , A set of keywords.  Each keyword is a table name.  Add
                     tables to this set when you do not want them included in
                     the table-edges returned by this function"
  ([db starting-table]
   (ordered-table-edges db starting-table {}))
  ([db
    starting-table
    blacklist]
   (let [table-edges (resolve-all-table-edges db)
         ordered-tables-seq (ordered-tables-to-copy
                              table-edges
                              starting-table
                              blacklist)]
     (if ordered-tables-seq
       (decorate-ordered-tables-seq table-edges ordered-tables-seq)
       (throw (Exception. "DB schema contain cycles"))))))

(defn get-fk-columns
  [fk-relationships]
  (->> fk-relationships
       vals
       (mapcat vals)))
