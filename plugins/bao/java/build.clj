(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'org.trex/trexsql)
(def version "0.1.0")
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def uber-file (format "target/trexsql-%s-standalone.jar" version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/compile-clj {:basis basis
                  :src-dirs ["src"]
                  :class-dir class-dir
                  :ns-compile '[trexsql.api trexsql.core trexsql.servlet]})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis basis
           :main 'trexsql.core
           ;; Exclude SLF4J implementation to avoid conflicts with WebAPI's logback
           :exclude ["org/slf4j/impl/.*"
                     "org/slf4j/simple/.*"
                     "simplelogger.properties"]}))
