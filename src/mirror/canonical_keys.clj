(ns mirror.canonical-keys
  "Functions for canonicalizing keywords that are sent to or returned form de.data.db interfaces
  with the hope of someday eliminating mixed use of keywords that differ only by whether they have
  hyphens or underscores.  It would be nice to have every function using clojure style hyphenated
  keywords, and to see keywords with underscores only (and only if necessary) in SQL bits.
  Even then, you can pass some of the functions in this namespace to jdbc interfaces to do the
  translations for you.")

(defn db->clojure
  "Translates strings, keywords, and collections of same
  (keys only in the case of maps), from database names which typically
  have underscores, to clojure names which are typically hyphenated.

  Non-map collections will emerge as vectors.
  Things we don't know how to translate will be returned as-is.

  The goal is never to have keywords with underscores in them 
  propagating up out of the database interface into higher layers of clojure code.

  Note that you can specify this to clojure.java.jdbc/query with the 
  :identifiers option.

  See also: clojure->db, kira.utils.core/->kebab-keys, camel-snake-kebab.core/->kebab-case."
  [in]
  (cond 
    (string? in) (-> (clojure.string/lower-case in) 
                     (#(clojure.string/replace % \_ \-)))
    (keyword? in) (keyword (db->clojure (name in)))
    (map? in) (into {} (for [[k v] in] [(db->clojure k) v]))
    (coll? in) (mapv db->clojure in)
    ;; Ignoring things we don't understand, so you can have 
    ;; heterogeneous collections and we'll only transform
    ;; the things we know how to do.
    :else in))

(defn clojure->db
  "Translates strings, keywords, and collections of same
  (keys only in the case of maps), from hyphenated token values to values with underscores
  so that the tokens in question may be used as postgresql database table and field names.
  This reverses the action of the db->clojure function.

  Non-map collections will emerge as vectors.
  Things we don't know how to translate will be returned as-is.

  Note that you can specify this to clojure.java.jdbc/{insert!,update!} and similar functions with the 
  :entities option.

  See also: db->clojure."
  [in]
  (cond 
    (string? in) (clojure.string/replace in \- \_)
    (keyword? in) (keyword (clojure->db (name in)))
    (map? in) (into {} (for [[k v] in] [(clojure->db k) v]))
    (coll? in) (mapv clojure->db in)
    :else in))


