(ns trexsql.webapi.search-provider
  (:require [trexsql.core :as core]
            [trexsql.vocab :as vocab]
            [clojure.tools.logging :as log])
  (:import [org.ohdsi.vocabulary Concept SearchProvider SearchProviderConfig]
           [org.springframework.core.env Environment])
  (:gen-class
   :name org.trex.webapi.TrexSQLSearchProvider
   :implements [org.ohdsi.vocabulary.SearchProvider]
   :init init-state
   :state state
   :constructors {[org.springframework.core.env.Environment] []}))

(defn -init-state [^Environment env]
  [[] {:env env
       :cache-path (.getProperty env "trexsql.cache-path" "./data/cache")}])

(defn -supports [this vocabulary-version-key]
  (boolean (some? @core/current-database)))
(defn -getPriority [this] 1)

(defn- row->concept [^java.util.HashMap row]
  (let [c (Concept.)]
    (set! (.conceptId c) (some-> (.get row "concept_id") long))
    (set! (.conceptName c) (.get row "concept_name"))
    (set! (.domainId c) (.get row "domain_id"))
    (set! (.vocabularyId c) (.get row "vocabulary_id"))
    (set! (.conceptClassId c) (.get row "concept_class_id"))
    (set! (.standardConcept c) (.get row "standard_concept"))
    (set! (.conceptCode c) (.get row "concept_code"))
    (set! (.invalidReason c) (.get row "invalid_reason"))
    c))

(defn- map-results-to-concepts [results]
  (java.util.ArrayList. ^java.util.Collection (mapv row->concept results)))

(defn -executeSearch [this ^SearchProviderConfig config ^String query ^String rows]
  (let [{:keys [cache-path]} (.state this)
        source-key (.getSourceKey config)
        cache-file (java.io.File. (str cache-path "/" source-key ".db"))]
    (if-not (.exists cache-file)
      (do (log/warn (str "TrexSQL cache not available for source: " source-key))
          (java.util.ArrayList.))
      (let [max-rows (try (min (Integer/parseInt (or rows "1000")) 10000)
                          (catch Exception _ 1000))
            db (core/get-database)
            options {:database-code source-key
                     :max-rows max-rows
                     :cache-path cache-path}
            results (vocab/search-vocab db query options)]
        (map-results-to-concepts results)))))
