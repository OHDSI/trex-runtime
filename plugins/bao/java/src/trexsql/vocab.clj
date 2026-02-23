(ns trexsql.vocab
  "Vocabulary search functionality using TrexSQL Full-Text Search (FTS).
   Provides search-vocab for concept lookup with FTS and ILIKE fallback."
  (:require [trexsql.db :as db]
            [trexsql.util :as util]
            [trexsql.errors :as errors]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [honey.sql :as sql])
  (:import [java.util Map List ArrayList HashMap]))

(defn validate-search-options
  "Validate VocabSearchConfig. Returns nil if valid, error message if invalid."
  [options]
  (cond
    (nil? options)
    "Search options cannot be nil"

    (str/blank? (:database-code options))
    "Missing required option: database-code"

    (not (re-matches #"^[a-zA-Z0-9_-]+$" (str (:database-code options))))
    (str "Invalid database-code: " (:database-code options)
         ". Must contain only alphanumeric, underscore, or hyphen characters.")

    (and (:schema-name options)
         (not (re-matches #"^[a-zA-Z0-9_]+$" (str (:schema-name options)))))
    (str "Invalid schema-name: " (:schema-name options)
         ". Must contain only alphanumeric or underscore characters.")

    (and (:max-rows options)
         (or (not (integer? (:max-rows options)))
             (< (:max-rows options) 1)
             (> (:max-rows options) 10000)))
    (str "Invalid max-rows: " (:max-rows options)
         ". Must be a positive integer between 1 and 10000.")

    :else nil))

(defn- get-schema-from-cache
  "Get the schema name from the cache by querying for the concept table."
  [db database-code]
  (try
    (let [query "SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_catalog = ? AND table_name = 'concept' LIMIT 1"
          results (db/query-with-params db query [database-code])]
      (log/debug (format "Schema detection for %s: found %d results" database-code (count results)))
      (when (seq results)
        (let [schema (.get ^HashMap (first results) "table_schema")]
          (log/info (format "Detected schema '%s' for database %s" schema database-code))
          schema)))
    (catch Exception e
      (log/warn (format "Failed to get schema from cache for %s: %s" database-code (.getMessage e)) e)
      nil)))

(defn java-map->search-options
  "Convert Java Map to search options map with keyword keys.
   Applies defaults for optional fields.
   Auto-detects schema by querying information_schema if not provided."
  ([^Map m]
   (java-map->search-options nil m))
  ([db ^Map m]
   (when m
     (let [clj-map (util/java-map->clj-map m)
           database-code (:database-code clj-map)
           provided-schema (:schema-name clj-map)
           detected-schema (when (and db (not provided-schema))
                             (get-schema-from-cache db database-code))
           schema-name (or provided-schema detected-schema)]
       {:database-code database-code
        :schema-name schema-name
        :max-rows (or (:max-rows clj-map) 1000)}))))

(defn has-fts-index?
  "Check if an FTS index exists for the concept table."
  [db database-code schema-name]
  (try
    (db/validate-identifier! database-code "database-code")
    (db/validate-identifier! schema-name "schema-name")
    (let [fts-fn-name (format "fts_main_%s_%s_concept" database-code schema-name)
          check-sql "SELECT * FROM duckdb_functions() WHERE function_name = ? LIMIT 1"
          results (db/query-with-params db check-sql [fts-fn-name])]
      (pos? (count results)))
    (catch Exception _
      false)))

(defn build-fts-search-sql
  "Build FTS search query using match_bm25 function."
  [database-code schema-name max-rows]
  (db/validate-identifier! database-code "database-code")
  (db/validate-identifier! schema-name "schema-name")
  (let [escaped-db (db/escape-identifier database-code "database-code")
        escaped-schema (db/escape-identifier schema-name "schema-name")
        table-ref (format "%s.%s.%s" escaped-db escaped-schema (db/escape-identifier "concept" "table"))
        fts-fn (format "fts_main_%s_%s_concept" database-code schema-name)]
    (format "SELECT c.concept_id, c.concept_name, c.domain_id,
       c.vocabulary_id, c.concept_class_id, c.standard_concept,
       c.concept_code, fts.score
FROM %s c
JOIN (SELECT concept_id, %s.match_bm25(concept_id, ?, fields := 'concept_name') AS score
      FROM %s) AS fts ON c.concept_id = fts.concept_id
WHERE fts.score IS NOT NULL
ORDER BY fts.score DESC
LIMIT %d"
            table-ref fts-fn table-ref max-rows)))

(defn build-fallback-search-sql
  "Build fallback ILIKE search query for when FTS is not available."
  [database-code schema-name max-rows]
  (db/validate-identifier! database-code "database-code")
  (db/validate-identifier! schema-name "schema-name")
  (let [escaped-db (db/escape-identifier database-code "database-code")
        escaped-schema (db/escape-identifier schema-name "schema-name")
        table-ref (format "%s.%s.%s" escaped-db escaped-schema (db/escape-identifier "concept" "table"))]
    (format "SELECT concept_id, concept_name, domain_id,
       vocabulary_id, concept_class_id, standard_concept,
       concept_code
FROM %s
WHERE concept_name ILIKE ?
   OR concept_code ILIKE ?
   OR vocabulary_id ILIKE ?
LIMIT %d"
            table-ref max-rows)))

(defn execute-fts-search
  "Execute FTS search and return results.
   Returns vector of result maps or nil on failure."
  [db database-code schema-name search-term max-rows]
  (try
    (util/load-fts-extension! db)
    (let [sql (build-fts-search-sql database-code schema-name max-rows)]
      (db/query-with-params db sql [search-term]))
    (catch Exception e
      (log/debug "FTS search failed, trying fallback" e)
      nil)))

(defn execute-fallback-search
  "Execute fallback ILIKE search.
   Returns vector of result maps."
  [db database-code schema-name search-term max-rows]
  (let [sql (build-fallback-search-sql database-code schema-name max-rows)
        pattern (str "%" search-term "%")]
    (db/query-with-params db sql [pattern pattern pattern])))

(defn search-vocab
  "Search vocabulary concepts using FTS with ILIKE fallback.
   Returns vector of concept maps.
   Auto-attaches cache database if it exists but isn't attached."
  [db search-term options]
  (when (str/blank? search-term)
    (throw (errors/validation-error "Search term cannot be empty" {:field "search-term"})))
  (when-let [error (validate-search-options options)]
    (throw (errors/validation-error error {:field "options"})))

  (let [{:keys [database-code schema-name max-rows cache-path]} options
        cache-base (or cache-path "/data/cache")
        cache-file (str cache-base "/" database-code ".db")
        is-attached (db/is-attached? db database-code)
        file-exists (.exists (java.io.File. cache-file))]
    (log/debug (format "Auto-attach check for %s: cache-path=%s, cache-file=%s, is-attached=%s, file-exists=%s"
                       database-code cache-path cache-file is-attached file-exists))
    ;; Auto-attach cache if exists but not attached
    (when-not is-attached
      (when file-exists
        (log/info (format "Auto-attaching cache for %s from %s" database-code cache-file))
        (db/attach-cache-file! db database-code cache-base)))

    ;; Detect schema AFTER attaching cache, if not provided
    (let [detected-schema (when (not schema-name)
                            (get-schema-from-cache db database-code))
          final-schema (or schema-name detected-schema database-code)]
      (log/debug (format "Using schema '%s' for search (provided=%s, detected=%s, database-code=%s)"
                         final-schema schema-name detected-schema database-code))
      (or (execute-fts-search db database-code final-schema search-term max-rows)
          (execute-fallback-search db database-code final-schema search-term max-rows)))))

(defn result-row->concept-map
  "Convert a single result row to concept HashMap."
  [row]
  (doto (HashMap.)
    (.put "concept_id" (.get ^HashMap row "concept_id"))
    (.put "concept_name" (.get ^HashMap row "concept_name"))
    (.put "domain_id" (.get ^HashMap row "domain_id"))
    (.put "vocabulary_id" (.get ^HashMap row "vocabulary_id"))
    (.put "concept_class_id" (.get ^HashMap row "concept_class_id"))
    (.put "standard_concept" (.get ^HashMap row "standard_concept"))
    (.put "concept_code" (.get ^HashMap row "concept_code"))))

(defn results->concept-list
  "Convert search results to ArrayList<HashMap> for Java API compatibility."
  [results]
  (let [al (ArrayList.)]
    (doseq [row results]
      (.add al (result-row->concept-map row)))
    al))
