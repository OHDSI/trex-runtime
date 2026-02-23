(ns trexsql.datamart
  "Datamart creation functionality for caching source database schemas in TrexSQL.
   Supports PostgreSQL, BigQuery, and JDBC sources (SQL Server, Oracle, MySQL, MariaDB)
   with filtering, FTS indexing, and progress reporting."
  (:require [trexsql.db :as db]
            [trexsql.util :as util]
            [trexsql.errors :as errors]
            [trexsql.batch :as batch]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [honey.sql :as sql])
  (:import [java.util Map List ArrayList HashMap]
           [java.io File]))

(defrecord SourceCredentials
  [dialect host port database-name user password])

(defrecord DatamartConfig
  [database-code schema-name target-schema-name source-credentials table-filter patient-filter timestamp-filter fts-tables cache-path parallel-copy])

(defrecord TableResult
  [table-name rows-copied indexes-created])

(defrecord TableError
  [table-name error phase])

(defrecord CacheResult
  [success? database-code schema-name tables-copied tables-failed fts-indexes-created duration-ms error])

(def valid-dialects #{"postgres" "postgresql" "bigquery" "sql server" "oracle" "mysql" "mariadb"})

;; JDBC-only dialects require batch transfer instead of TrexSQL scanner extensions
(def jdbc-only-dialects #{"sql server" "oracle" "mysql" "mariadb"})

(defn- valid-database-code?
  "Check if database-code is valid for filesystem naming."
  [code]
  (and (string? code)
       (seq code)
       (re-matches #"^[a-zA-Z0-9_-]+$" code)))

(defn validate-credentials
  "Validate SourceCredentials. Returns nil if valid, error message if invalid."
  [creds]
  (cond
    (nil? creds)
    "Missing required config: source-credentials"

    (not (contains? valid-dialects (:dialect creds)))
    (str "Unsupported dialect: " (:dialect creds) ". Must be 'postgres', 'postgresql', or 'bigquery'")

    (or (= "postgres" (:dialect creds)) (= "postgresql" (:dialect creds)))
    (cond
      (str/blank? (:host creds)) "Missing required PostgreSQL config: host"
      (nil? (:port creds)) "Missing required PostgreSQL config: port"
      (str/blank? (:database-name creds)) "Missing required PostgreSQL config: database-name"
      (str/blank? (:user creds)) "Missing required PostgreSQL config: user"
      (str/blank? (:password creds)) "Missing required PostgreSQL config: password"
      :else nil)

    (= "bigquery" (:dialect creds))
    (cond
      (str/blank? (:host creds)) "Missing required BigQuery config: host (project ID)"
      (str/blank? (:database-name creds)) "Missing required BigQuery config: database-name (dataset)"
      :else nil)

    :else nil))

(defn validate-config
  "Validate DatamartConfig. Returns nil if valid, error message if invalid."
  [config]
  (cond
    (nil? config)
    "Config is nil"

    (not (valid-database-code? (:database-code config)))
    (str "Invalid database-code: " (:database-code config)
         ". Must be non-empty and contain only alphanumeric, underscore, or hyphen characters.")

    (str/blank? (:schema-name config))
    "Missing required config: schema-name"

    :else
    (validate-credentials (:source-credentials config))))

(defn java-map->source-credentials
  "Convert Java Map to SourceCredentials record."
  [^Map m]
  (let [clj-map (util/java-map->clj-map m)]
    (map->SourceCredentials clj-map)))

(defn java-map->datamart-config
  "Convert Java Map to DatamartConfig record.
   Applies defaults for optional fields."
  [^Map m]
  (let [clj-map (util/java-map->clj-map m)
        source-creds (when-let [creds (:source-credentials clj-map)]
                       (if (map? creds)
                         (map->SourceCredentials creds)
                         creds))]
    (map->DatamartConfig
     {:database-code (:database-code clj-map)
      :schema-name (:schema-name clj-map)
      :target-schema-name (or (:target-schema-name clj-map)
                              (:schema-name clj-map))
      :source-credentials source-creds
      :table-filter (:table-filter clj-map)
      :patient-filter (when-let [pf (:patient-filter clj-map)]
                        (vec pf))
      :timestamp-filter (:timestamp-filter clj-map)
      :fts-tables (or (:fts-tables clj-map) ["concept"])
      :cache-path (or (:cache-path clj-map) "./data/cache")
      :parallel-copy (boolean (:parallel-copy clj-map))})))

(defn table-result->java-map
  "Convert TableResult record to Java HashMap."
  [^TableResult tr]
  (doto (HashMap.)
    (.put "table-name" (:table-name tr))
    (.put "rows-copied" (:rows-copied tr))
    (.put "indexes-created" (:indexes-created tr))))

(defn table-error->java-map
  "Convert TableError record to Java HashMap."
  [^TableError te]
  (doto (HashMap.)
    (.put "table-name" (:table-name te))
    (.put "error" (:error te))
    (.put "phase" (:phase te))))

(defn tables-to-arraylist
  "Convert list of TableResult or TableError records to ArrayList<HashMap>."
  [records converter-fn]
  (let [al (ArrayList.)]
    (doseq [r records]
      (.add al (converter-fn r)))
    al))

(defn result->java-map
  "Convert CacheResult record to Java HashMap for Spring Batch compatibility."
  [^CacheResult result]
  (doto (HashMap.)
    (.put "success" (boolean (:success? result)))
    (.put "database-code" (:database-code result))
    (.put "schema-name" (:schema-name result))
    (.put "tables-copied" (tables-to-arraylist (:tables-copied result) table-result->java-map))
    (.put "tables-failed" (tables-to-arraylist (:tables-failed result) table-error->java-map))
    (.put "fts-indexes-created" (ArrayList. ^java.util.Collection (or (:fts-indexes-created result) [])))
    (.put "duration-ms" (:duration-ms result))
    (.put "error" (:error result))))
(defn get-source-tables
  "Get list of table names from source database schema."
  [db source-alias schema-name]
  (db/validate-identifier! source-alias "source-alias")
  (db/validate-identifier! schema-name "schema-name")
  (let [[query-sql & params] (sql/format
                               {:select [:table_name]
                                :from [:information_schema.tables]
                                :where [:and
                                        [:= :table_schema schema-name]
                                        [:= :table_catalog source-alias]]})
        results (db/query-with-params db query-sql (vec params))]
    (mapv #(.get ^HashMap % "table_name") results)))

(defn get-table-columns
  "Get list of column names for a table in the source database."
  [db source-alias schema-name table-name]
  (db/validate-identifier! source-alias "source-alias")
  (db/validate-identifier! schema-name "schema-name")
  (db/validate-identifier! table-name "table-name")
  (let [[query-sql & params] (sql/format
                               {:select [:column_name]
                                :from [:information_schema.columns]
                                :where [:and
                                        [:= :table_schema schema-name]
                                        [:= :table_name table-name]
                                        [:= :table_catalog source-alias]]})
        results (db/query-with-params db query-sql (vec params))]
    (mapv #(.get ^HashMap % "column_name") results)))
(defn apply-table-filter
  "Filter table list based on table-filter config.
   If table-filter is nil, returns all tables.
   Otherwise, returns only tables present in table-filter keys."
  [tables table-filter]
  (if (nil? table-filter)
    tables
    (filter #(contains? table-filter %) tables)))

(defn build-select-clause
  "Build SELECT clause based on column filter.
   If columns is nil or [\"*\"], returns \"*\".
   Otherwise, returns comma-separated escaped column names."
  [columns]
  (if (or (nil? columns)
          (empty? columns)
          (= ["*"] columns)
          (= "*" (first columns)))
    "*"
    (do
      (doseq [col columns]
        (db/validate-identifier! col "column-name"))
      (str/join ", " (map #(db/escape-identifier % "column") columns)))))

(defn- validate-patient-ids
  "Validate that all patient IDs are numeric. Returns nil if valid, error message if invalid."
  [patient-filter]
  (when patient-filter
    (let [invalid (seq (remove #(or (integer? %)
                                    (and (string? %) (re-matches #"^\d+$" %)))
                               patient-filter))]
      (when invalid
        (str "Invalid patient IDs (must be numeric): " (pr-str (take 5 invalid)))))))

(defn- validate-timestamp-filter
  "Validate timestamp format (ISO 8601). Returns nil if valid, error message if invalid."
  [timestamp-filter]
  (when timestamp-filter
    (when-not (re-matches #"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?$" (str timestamp-filter))
      (str "Invalid timestamp format: " timestamp-filter ". Expected ISO 8601 (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"))))

(defn build-where-clause
  "Build WHERE clause for patient and timestamp filters.
   Validates inputs to prevent SQL injection.
   Returns nil if no filters, otherwise returns WHERE clause string.
   Throws validation-error if patient IDs are not numeric or timestamp format is invalid."
  [patient-filter timestamp-filter]
  (when-let [error (validate-patient-ids patient-filter)]
    (throw (errors/validation-error error {:field :patient-filter})))
  (when-let [error (validate-timestamp-filter timestamp-filter)]
    (throw (errors/validation-error error {:field :timestamp-filter})))
  (let [clauses (cond-> []
                  patient-filter
                  (conj (str "person_id IN ("
                             (str/join ", " (map #(if (integer? %) % (Long/parseLong (str %)))
                                                 patient-filter))
                             ")"))

                  timestamp-filter
                  (conj (str "observation_date >= '" timestamp-filter "'")))]
    (when (seq clauses)
      (str " WHERE " (str/join " AND " clauses)))))
(defn copy-table
  "Copy a single table from source to cache.
   Returns TableResult on success, TableError on failure."
  [db source-alias cache-alias schema-name target-schema table-name config]
  (try
    (db/validate-identifier! source-alias "source-alias")
    (db/validate-identifier! cache-alias "cache-alias")
    (db/validate-identifier! schema-name "schema-name")
    (db/validate-identifier! target-schema "target-schema")
    (db/validate-identifier! table-name "table-name")

    (let [{:keys [table-filter patient-filter timestamp-filter]} config
          columns (get table-filter table-name)
          select-clause (build-select-clause columns)
          where-clause (build-where-clause patient-filter timestamp-filter)
          source-table (format "%s.%s.%s"
                               (db/escape-identifier source-alias "source-alias")
                               (db/escape-identifier schema-name "schema-name")
                               (db/escape-identifier table-name "table-name"))
          target-table (format "%s.%s.%s"
                               (db/escape-identifier cache-alias "cache-alias")
                               (db/escape-identifier target-schema "target-schema")
                               (db/escape-identifier table-name "table-name"))
          create-sql (format "CREATE OR REPLACE TABLE %s AS SELECT %s FROM %s WHERE false"
                             target-table
                             select-clause
                             source-table)
          insert-sql (format "INSERT INTO %s SELECT %s FROM %s%s"
                             target-table
                             select-clause
                             source-table
                             (or where-clause ""))]
      (db/execute! db create-sql)
      (db/execute! db insert-sql)
      ;; Get row count from target table (changes() not available in all TrexSQL contexts)
      (let [count-sql (format "SELECT COUNT(*) as cnt FROM %s" target-table)
            count-result (db/query db count-sql)
            row-count (or (some-> count-result first (.get "cnt")) 0)]
        (->TableResult table-name row-count 0)))
    (catch Exception e
      (->TableError table-name (.getMessage e) "copy"))))

(defn- copy-tables-sequential
  "Copy tables sequentially with progress logging."
  [db source-alias cache-alias schema-name target-schema tables-to-copy config]
  (let [total-count (count tables-to-copy)]
    (loop [remaining tables-to-copy
           idx 1
           copied []
           failed []]
      (if (empty? remaining)
        {:tables-copied copied :tables-failed failed}
        (let [table-name (first remaining)
              _ (log/info (format "Copying table %d of %d: %s" idx total-count table-name))
              result (copy-table db source-alias cache-alias schema-name target-schema table-name config)]
          (if (instance? TableResult result)
            (recur (rest remaining) (inc idx) (conj copied result) failed)
            (recur (rest remaining) (inc idx) copied (conj failed result))))))))

(defn- copy-tables-parallel
  "Copy tables in parallel using pmap."
  [db source-alias cache-alias schema-name target-schema tables-to-copy config]
  (let [total-count (count tables-to-copy)
        _ (log/info (format "Copying %d tables in parallel" total-count))
        results (doall
                 (pmap (fn [table-name]
                         (log/debug (format "Parallel copy: %s" table-name))
                         (copy-table db source-alias cache-alias schema-name target-schema table-name config))
                       tables-to-copy))
        {copied true failed false} (group-by #(instance? TableResult %) results)]
    (log/info (format "Parallel copy complete: %d succeeded, %d failed"
                      (count copied) (count failed)))
    {:tables-copied (vec copied) :tables-failed (vec failed)}))

(defn copy-schema
  "Copy all tables from source schema to cache.
   Supports parallel copying when :parallel-copy is true in config.
   Returns map with :tables-copied and :tables-failed vectors."
  [db source-alias cache-alias config]
  (let [{:keys [schema-name target-schema-name table-filter parallel-copy]} config
        target-schema (or target-schema-name schema-name)
        _ (db/validate-identifier! cache-alias "cache-alias")
        _ (db/validate-identifier! target-schema "target-schema")
        all-tables (get-source-tables db source-alias schema-name)
        tables-to-copy (apply-table-filter all-tables table-filter)
        create-schema-sql (format "CREATE SCHEMA IF NOT EXISTS %s.%s"
                                  (db/escape-identifier cache-alias "cache-alias")
                                  (db/escape-identifier target-schema "target-schema"))]
    (try
      (db/execute! db create-schema-sql)
      (catch Exception e
        (when-not (re-find #"(?i)already exists" (.getMessage e))
          (log/warn (format "Failed to create schema %s.%s: %s"
                           cache-alias target-schema (.getMessage e))))))
    (if parallel-copy
      (copy-tables-parallel db source-alias cache-alias schema-name target-schema tables-to-copy config)
      (copy-tables-sequential db source-alias cache-alias schema-name target-schema tables-to-copy config))))

(defn get-document-identifier
  "Get document identifier column name for a table.
   Prioritizes columns matching {table}_id pattern, then columns ending with _id, then integer columns."
  [db cache-alias schema-name table-name]
  (try
    (let [primary-id (str table-name "_id")
          [query-sql & params] (sql/format
                                 {:select [:column_name]
                                  :from [:information_schema.columns]
                                  :where [:and
                                          [:= :table_catalog cache-alias]
                                          [:= :table_schema schema-name]
                                          [:= :table_name table-name]
                                          [:or
                                           [:like :column_name "%_id"]
                                           [:in :data_type ["INTEGER" "BIGINT"]]]]
                                  :order-by [[[:raw (format "CASE WHEN column_name = '%s' THEN 0 WHEN column_name LIKE '%%_id' THEN 1 ELSE 2 END" primary-id)]]
                                             :column_name]
                                  :limit 1})
          results (db/query-with-params db query-sql (vec params))]
      (when (seq results)
        (.get ^HashMap (first results) "column_name")))
    (catch Exception e
      (log/warn (format "Failed to get document identifier for %s: %s" table-name (.getMessage e)))
      nil)))

(defn get-text-columns
  "Get text/varchar columns from a table for FTS indexing."
  [db cache-alias schema-name table-name]
  (try
    (let [[query-sql & params] (sql/format
                                 {:select [:column_name]
                                  :from [:information_schema.columns]
                                  :where [:and
                                          [:= :table_catalog cache-alias]
                                          [:= :table_schema schema-name]
                                          [:= :table_name table-name]
                                          [:in :data_type ["VARCHAR" "TEXT"]]]})
          results (db/query-with-params db query-sql (vec params))]
      (mapv #(.get ^HashMap % "column_name") results))
    (catch Exception e
      (log/warn (format "Failed to get text columns for %s: %s" table-name (.getMessage e)))
      [])))

(defn create-fts-index
  "Create FTS index on a table. Returns table name on success, nil on failure.
   Dynamically discovers document ID and text columns from information_schema."
  [db cache-alias schema-name table-name]
  (try
    (db/validate-identifier! cache-alias "cache-alias")
    (db/validate-identifier! schema-name "schema-name")
    (db/validate-identifier! table-name "table-name")

    (log/info (format "Creating FTS index on %s" table-name))
    (let [qualified-table (format "\"%s\".\"%s\".\"%s\"" cache-alias schema-name table-name)
          id-column (get-document-identifier db cache-alias schema-name table-name)
          text-columns (get-text-columns db cache-alias schema-name table-name)]

      (cond
        (not id-column)
        (do
          (log/warn (format "No document identifier found for %s, skipping FTS index" table-name))
          nil)

        (empty? text-columns)
        (do
          (log/warn (format "No text columns found for %s, skipping FTS index" table-name))
          nil)

        :else
        (do
          (doseq [col (cons id-column text-columns)]
            (db/validate-identifier! col "column-name"))
          (let [fts-sql (format "PRAGMA create_fts_index(%s, %s, %s, stemmer='english', stopwords='english', strip_accents=1, lower=1, overwrite=1)"
                                qualified-table
                                id-column
                                (str/join ", " text-columns))]
            (log/debug (format "FTS SQL: %s" fts-sql))
            (db/execute! db fts-sql)
            (log/info (format "FTS index created successfully on %s" table-name))
            table-name))))
    (catch Exception e
      (log/warn (format "Failed to create FTS index on %s: %s" table-name (.getMessage e)))
      nil)))

(defn create-fts-indexes
  "Create FTS indexes on configured tables.
   Returns vector of table names with successful FTS index creation."
  [db cache-alias schema-name fts-tables copied-tables]
  (when (util/load-fts-extension! db)
    (let [copied-table-names (set (map :table-name copied-tables))
          tables-to-index (filter copied-table-names fts-tables)]
      (filterv some? (map #(create-fts-index db cache-alias schema-name %) tables-to-index)))))

(defn attach-source!
  "Attach source database based on dialect.
   Returns source alias on success."
  [db database-code credentials]
  (case (:dialect credentials)
    ("postgres" "postgresql") (db/attach-source-postgres! db database-code credentials)
    "bigquery" (db/attach-source-bigquery! db database-code credentials)
    (throw (errors/config-error
            (str "Unsupported dialect: " (:dialect credentials))
            :dialect))))

(defn create-datamart
  "Create a datamart cache by copying data from a source database.
   Returns CacheResult record with operation results."
  [db config]
  (let [start-time (System/currentTimeMillis)
        {:keys [database-code schema-name target-schema-name source-credentials
                fts-tables cache-path]} config
        target-schema (or target-schema-name schema-name)]

    (if-let [error (validate-config config)]
      (->CacheResult false database-code schema-name [] [] [] 0 error)

      (try
        (log/info (format "Attaching cache file for %s" database-code))
        (db/attach-cache-file! db database-code (or cache-path "./data/cache"))

        (log/info (format "Connecting to source database (%s)" (:dialect source-credentials)))
        (let [source-alias (attach-source! db database-code source-credentials)]

          (log/info (format "Copying schema %s" schema-name))
          (let [{:keys [tables-copied tables-failed]} (copy-schema db source-alias database-code config)

                fts-created (create-fts-indexes db database-code target-schema (or fts-tables ["concept"]) tables-copied)

                _ (try
                    (db/detach-database! db source-alias)
                    (catch Exception e
                      (log/warn (format "Failed to detach source database %s: %s"
                                        source-alias (.getMessage e)))))

                duration-ms (- (System/currentTimeMillis) start-time)]

            (->CacheResult
             (empty? tables-failed)  ; success if no failures
             database-code
             schema-name
             tables-copied
             tables-failed
             fts-created
             duration-ms
             nil)))

        (catch Exception e
          (let [duration-ms (- (System/currentTimeMillis) start-time)
                error-msg (if (re-find #"(?i)connect" (.getMessage e))
                            (str "Failed to connect to source database: " (.getMessage e))
                            (.getMessage e))]
            (->CacheResult false database-code schema-name [] [] [] duration-ms error-msg)))))))

(defn jdbc-dialect?
  "Check if dialect requires JDBC batch transfer (not native TrexSQL extension)."
  [dialect]
  (contains? jdbc-only-dialects (str/lower-case (or dialect ""))))

(defn- convert-config-for-jdbc
  "Convert DatamartConfig to JDBC batch config format."
  [config]
  (let [{:keys [database-code schema-name source-credentials table-filter
                patient-filter timestamp-filter cache-path]} config
        {:keys [dialect jdbc-url user password]} source-credentials]
    {:database-code database-code
     :schema-name schema-name
     :source-credentials {:jdbc-url jdbc-url
                          :user user
                          :password password
                          :dialect dialect}
     :table-filter (when table-filter (keys table-filter))
     :column-filter table-filter
     :patient-filter patient-filter
     :timestamp-filter timestamp-filter
     :cache-path (or cache-path "./data/cache")
     :batch-size (or (:batch-size config) 10000)}))

(defn create-cache
  "Unified cache creation - routes automatically based on dialect.
   For PostgreSQL/BigQuery: uses native TrexSQL scanner extensions.
   For SQL Server/Oracle/MySQL/MariaDB: uses JDBC batch transfer.

   Returns CacheResult record with operation results.
   progress-fn is called with progress events during JDBC transfer."
  ([db config]
   (create-cache db config nil))
  ([db config progress-fn]
   (let [dialect (get-in config [:source-credentials :dialect])]
     (if (jdbc-dialect? dialect)
       (let [jdbc-config (convert-config-for-jdbc config)
             result (batch/create-cache-jdbc db jdbc-config progress-fn)]
         (->CacheResult
          (:success? result)
          (:database-code result)
          (:schema-name result)
          (mapv (fn [t] (->TableResult (:table-name t) (:rows-copied t) 0)) (:tables-copied result))
          (mapv (fn [t] (->TableError (:table-name t) (:error t) (or (:phase t) "copy"))) (:tables-failed result))
          []
          (:duration-ms result)
          (:error result)))
       (create-datamart db config)))))

(defn is-attached?
  "Check if a database is attached. Delegates to db/is-attached?"
  [db database-code]
  (db/is-attached? db database-code))

(defn detach-database!
  "Detach a database. Delegates to db/detach-database!"
  [db database-code]
  (db/detach-database! db database-code))
