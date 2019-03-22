(ns mirror.postgresql
  (:require
    [cheshire.core :as json]
    [clojure.java.jdbc :as j])
  (:import
   [org.postgresql.util PGobject]
   [java.sql Array]))

(deftype Json [v])
(deftype Jsonb [v])

(defn as-json [v] (Json. v))
(defn as-jsonb [v] (Jsonb. v))

(defn value->json-pgobject [value]
  (doto (PGobject.)
    (.setType "json")
    (.setValue (json/generate-string value))))

(defn value->jsonb-pgobject [value]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (json/generate-string value))))

(extend-protocol j/ISQLValue
  Json
  (sql-value [value] (value->json-pgobject (.v value)))
  Jsonb
  (sql-value [value] (value->jsonb-pgobject (.v value))))

(extend-protocol j/IResultSetReadColumn
  Array
  (result-set-read-column [pgobj metadata idx]
    (seq (.getArray pgobj)))

  PGobject
  (result-set-read-column [pgobj metadata idx]
    (let [type  (.getType pgobj)
          value (.getValue pgobj)]
      (case type
        ("json" "jsonb") (json/parse-string value true)
        value))))
