(defproject mirror "0.1.0-SNAPSHOT"
  :description "A library for deep copying postgres table records"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[aysylu/loom "1.0.2"]
                 [cheshire "5.8.1"]
                 [honeysql "0.9.4"]
                 [org.clojure/clojure "1.10.0"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [org.clojure/tools.logging "0.4.1"]
                 [org.postgresql/postgresql "42.2.5"]]
  :repl-options {:init-ns mirror.core})
