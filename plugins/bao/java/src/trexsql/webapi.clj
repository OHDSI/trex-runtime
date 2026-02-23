(ns trexsql.webapi
  "WebAPI integration handlers with Reitit routing."
  (:require [trexsql.datamart :as datamart]
            [trexsql.jobs :as jobs]
            [trexsql.vocab :as vocab]
            [trexsql.circe :as circe]
            [trexsql.db :as db]
            [trexsql.proxy :as proxy]
            [trexsql.config :as config]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [reitit.ring :as ring])
  (:import [java.util HashMap ArrayList Map]
           [java.io File]))

(defonce ^:private source-repository (atom nil))
(defonce ^:private global-config (atom {:cache-path "./data/cache"}))

(defn set-source-repository! [repo]
  (reset! source-repository repo))

(defn set-config! [config]
  (when config
    (swap! global-config merge config)))

(defn get-cache-path-from-config []
  (:cache-path @global-config))

(defn get-source-repository []
  @source-repository)

(defn- find-source-by-key [source-key]
  (when-let [repo @source-repository]
    (try
      (.findBySourceKey repo source-key)
      (catch Exception e
        nil))))

(defn- get-daimon-type
  "Get DaimonType enum value by name at runtime."
  [type-name]
  (let [daimon-class (Class/forName "org.ohdsi.webapi.source.SourceDaimon$DaimonType")]
    (Enum/valueOf daimon-class type-name)))

(defn- get-cdm-schema
  "Get CDM schema from source daimons."
  [source]
  (when source
    (.getTableQualifier source (get-daimon-type "CDM"))))

(defn- get-results-schema
  "Get Results schema from source daimons, falls back to CDM schema."
  [source]
  (when source
    (or (.getTableQualifierOrNull source (get-daimon-type "Results"))
        (get-cdm-schema source))))

(defn- parse-jdbc-url
  "Parse PostgreSQL JDBC URL to extract connection components.
   Format: jdbc:postgresql://host:port/database?user=xxx&password=xxx"
  [jdbc-url]
  (when jdbc-url
    (try
      (let [url-part (str/replace jdbc-url #"^jdbc:postgresql://" "")
            [host-port-db params-str] (str/split url-part #"\?" 2)
            [host-port db] (str/split host-port-db #"/" 2)
            [host port-str] (str/split host-port #":" 2)
            port (when port-str (try (Integer/parseInt port-str) (catch Exception _ 5432)))
            params (when params-str
                     (into {}
                       (for [param (str/split params-str #"&")]
                         (let [[k v] (str/split param #"=" 2)]
                           [(keyword k) v]))))]
        {:host host
         :port (or port 5432)
         :database-name db
         :user (:user params)
         :password (:password params)})
      (catch Exception _ nil))))

(defn- source->credentials [source]
  (when source
    (let [dialect (.getSourceDialect source)
          conn-str (.getSourceConnection source)
          explicit-user (.getUsername source)
          explicit-pass (.getPassword source)]
      (if (or (= "postgres" dialect) (= "postgresql" dialect))
        (let [parsed (parse-jdbc-url conn-str)]
          (merge parsed
                 {:dialect dialect
                  :connection-string conn-str
                  :user (or explicit-user (:user parsed))
                  :password (or explicit-pass (:password parsed))}))
        {:dialect dialect
         :connection-string conn-str
         :user explicit-user
         :password explicit-pass
         :jdbc-url (when (datamart/jdbc-dialect? dialect) conn-str)}))))

(defn response
  ([status body]
   {:status status :body body})
  ([status body headers]
   {:status status :body body :headers headers}))

(defn ok [body]
  (response 200 body))

(defn created [body]
  (response 201 body))

(defn no-content []
  (response 204 nil))

(defn bad-request [message]
  (response 400 {:error "BAD_REQUEST" :message message}))

(defn not-found [message]
  (response 404 {:error "NOT_FOUND" :message message}))

(defn conflict [message]
  (response 409 {:error "CONFLICT" :message message}))

(defn internal-error [message]
  (response 500 {:error "INTERNAL_ERROR" :message message}))

(defn service-unavailable [message]
  (response 503 {:error "SERVICE_UNAVAILABLE" :message message}))

(defn- parse-json [s]
  (when (and s (not (str/blank? s)))
    (try
      (json/read-str s :key-fn keyword)
      (catch Exception e
        nil))))

(defn- java-map->clj [m]
  (cond
    (instance? Map m)
    (into {} (for [[k v] m]
               [(keyword k) (java-map->clj v)]))

    (instance? java.util.List m)
    (vec (map java-map->clj m))

    :else m))

(defn- sanitize-database-code
  "Convert a source key (e.g., UUID) to a valid SQL identifier.
   Replaces hyphens with underscores and ensures it starts with a letter/underscore."
  [source-key]
  (when source-key
    (let [sanitized (str/replace source-key "-" "_")]
      ;; If it starts with a digit, prefix with underscore
      (if (re-matches #"^\d.*" sanitized)
        (str "_" sanitized)
        sanitized))))

(defn- validate-database-code
  "Validate database-code for safe use. Returns nil if valid, error message if invalid."
  [database-code]
  (cond
    (nil? database-code)
    "database-code is required"

    (not (string? database-code))
    "database-code must be a string"

    (str/blank? database-code)
    "database-code cannot be empty"

    (not (re-matches #"^[a-zA-Z_][a-zA-Z0-9_]*$" database-code))
    (str "Invalid database-code: " database-code
         ". Must start with letter/underscore and contain only alphanumeric/underscore characters.")

    (> (count database-code) 128)
    "database-code is too long (max 128 characters)"

    :else nil))

(defn- build-credentials-map [credentials]
  (let [m (doto (HashMap.)
            (.put "dialect" (:dialect credentials))
            (.put "connection-string" (:connection-string credentials))
            (.put "user" (:user credentials))
            (.put "password" (:password credentials)))]
    (when (:host credentials) (.put m "host" (:host credentials)))
    (when (:port credentials) (.put m "port" (:port credentials)))
    (when (:database-name credentials) (.put m "database-name" (:database-name credentials)))
    (when (:jdbc-url credentials) (.put m "jdbc-url" (:jdbc-url credentials)))
    m))

(defn- build-datamart-config [database-code schema-name cache-path credentials request]
  (let [config (doto (HashMap.)
                 (.put "database-code" database-code)
                 (.put "schema-name" schema-name)
                 (.put "cache-path" cache-path)
                 (.put "source-credentials" (build-credentials-map credentials))
                 (.put "fts-tables" (ArrayList. (or (:ftsTables request) ["concept"]))))]
    (when-let [v (:targetSchemaName request)] (.put config "target-schema-name" v))
    (when-let [v (:tables request)] (.put config "tables" (ArrayList. v)))
    (when-let [v (:patientFilter request)] (.put config "patient-filter" (ArrayList. v)))
    (when-let [v (:timestampFilter request)] (.put config "timestamp-filter" v))
    config))

(defn- build-circe-options [cdm-schema result-schema cohort-id target-table generate-stats]
  (doto (HashMap.)
    (.put "cdm-schema" cdm-schema)
    (.put "result-schema" result-schema)
    (.put "cohort-id" cohort-id)
    (.put "target-table" (or target-table "cohort"))
    (.put "generate-stats" (boolean generate-stats))))

(defn- get-schema-from-cache [db database-code]
  "Get the schema name from the cache by querying for the concept table."
  (try
    (let [query "SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_catalog = ? AND table_name = 'concept' LIMIT 1"
          results (db/query-with-params db query [database-code])]
      (when (seq results)
        (.get ^HashMap (first results) "table_schema")))
    (catch Exception e
      (log/warn (format "Failed to get schema from cache for %s: %s" database-code (.getMessage e)))
      nil)))

(defn- build-vocab-options [db database-code max-rows]
  (let [schema-name (or (get-schema-from-cache db database-code) database-code)]
    (doto (HashMap.)
      (.put "database-code" database-code)
      (.put "schema-name" schema-name)
      (.put "max-rows" max-rows))))

(defn- get-cache-path [cache-base-path database-code]
  (str cache-base-path "/" database-code ".db"))

(defn- handle-create-cache [db source-key body params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [request (parse-json body)]
        (if-not request
          (bad-request "Invalid JSON body")
          (if (str/blank? (:schemaName request))
            (bad-request "schemaName is required")
            (let [database-code (or (:databaseCode request) (sanitize-database-code source-key))]
              (if-let [validation-error (validate-database-code database-code)]
                (bad-request validation-error)
                (try
                  (let [cache-path (or (:cachePath params) (get-cache-path-from-config))
                        credentials (source->credentials source)
                        config-map (build-datamart-config database-code (:schemaName request)
                                                          cache-path credentials request)
                        validated-config (datamart/java-map->datamart-config config-map)]
                    (when-let [error (datamart/validate-config validated-config)]
                      (throw (IllegalArgumentException. error)))
                    (let [result (datamart/create-cache db validated-config nil)
                          java-result (datamart/result->java-map result)]
                      (ok (java-map->clj java-result))))
                  (catch IllegalArgumentException e
                    (bad-request (.getMessage e)))
                  (catch Exception e
                    (internal-error (.getMessage e))))))))))))

(defn- handle-get-cache-status [db source-key params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [database-code (or (:databaseCode params) (sanitize-database-code source-key))]
        (if-let [validation-error (validate-database-code database-code)]
          (bad-request validation-error)
          (let [cache-path (or (:cachePath params) (get-cache-path-from-config))
                cache-file (File. (get-cache-path cache-path database-code))
                exists? (.exists cache-file)
                attached? (try (datamart/is-attached? db database-code) (catch Exception _ false))
                job-status (try (jobs/get-job-status db database-code) (catch Exception _ nil))]
            (ok {:sourceKey source-key
                 :cacheExists exists?
                 :cacheAttached attached?
                 :cacheFilePath (when exists? (.getAbsolutePath cache-file))
                 :cacheSizeBytes (when exists? (.length cache-file))
                 :lastModified (when exists? (.lastModified cache-file))
                 :activeJob (when job-status
                              {:jobId (:job-execution-id job-status)
                               :status (:status job-status)
                               :startTime (str (:start-time job-status))
                               :endTime (when (:end-time job-status) (str (:end-time job-status)))
                               :progress {:totalTables (:total-tables job-status)
                                          :completedTables (:completed-tables job-status)
                                          :currentTable (:current-table job-status)
                                          :processedRows (:processed-rows job-status)}
                               :error (:error-message job-status)})})))))))

(defn- handle-list-cache-jobs [db params]
  (try
    (let [status (:status params)
          jobs (if status
                 (jobs/list-jobs db :status status)
                 (jobs/list-jobs db))]
      (ok {:jobs (mapv (fn [job]
                         {:databaseCode (:database-code job)
                          :sourceKey (:source-key job)
                          :status (:status job)
                          :startTime (str (:start-time job))
                          :endTime (when (:end-time job) (str (:end-time job)))
                          :totalTables (:total-tables job)
                          :completedTables (:completed-tables job)
                          :error (:error-message job)})
                       jobs)
           :count (count jobs)}))
    (catch Exception e
      (internal-error (.getMessage e)))))

(defn- handle-cancel-cache-job [db source-key params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [database-code (or (:databaseCode params) (sanitize-database-code source-key))
            job-status (try (jobs/get-job-status db database-code) (catch Exception _ nil))]
        (cond
          (nil? job-status)
          (not-found (str "No cache job found for: " database-code))

          (not= "RUNNING" (:status job-status))
          (conflict (str "Job is not running. Current status: " (:status job-status)))

          :else
          (do
            (jobs/update-local-status! db database-code "CANCELED")
            (ok {:databaseCode database-code
                 :status "CANCELED"
                 :message "Job cancellation requested"})))))))

(defn- handle-delete-cache [db source-key params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [database-code (or (:databaseCode params) (sanitize-database-code source-key))
            cache-path (or (:cachePath params) (get-cache-path-from-config))
            cache-file (File. (get-cache-path cache-path database-code))]
        (if-not (.exists cache-file)
          (not-found (str "Cache not found for source: " source-key))
          (do
            (when (try (datamart/is-attached? db database-code) (catch Exception _ false))
              (try (datamart/detach-database! db database-code) (catch Exception _)))
            (if (.delete cache-file)
              (no-content)
              (internal-error "Failed to delete cache file"))))))))

(defn- handle-execute-circe [db source-key body params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [request (parse-json body)]
        (if-not request
          (bad-request "Invalid JSON body")
          (let [{:keys [circeJson cdmSchema resultSchema cohortId targetTable generateStats]} request]
            (cond
              (str/blank? circeJson) (bad-request "circeJson is required")
              (str/blank? cdmSchema) (bad-request "cdmSchema is required")
              (str/blank? resultSchema) (bad-request "resultSchema is required")
              (nil? cohortId) (bad-request "cohortId is required")
              :else
              (try
                (let [options-map (build-circe-options cdmSchema resultSchema cohortId targetTable generateStats)
                      clj-options (circe/java-map->circe-options options-map)
                      result (circe/execute-circe db circeJson clj-options)
                      java-result (circe/circe-result->java-map result)]
                  (ok (java-map->clj java-result)))
                (catch Exception e
                  (internal-error (.getMessage e)))))))))))

(defn- handle-render-circe [db source-key body params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [request (parse-json body)]
        (if-not request
          (bad-request "Invalid JSON body")
          (let [{:keys [circeJson cdmSchema resultSchema cohortId targetTable generateStats]} request]
            (cond
              (str/blank? circeJson) (bad-request "circeJson is required")
              (str/blank? cdmSchema) (bad-request "cdmSchema is required")
              (str/blank? resultSchema) (bad-request "resultSchema is required")
              (nil? cohortId) (bad-request "cohortId is required")
              :else
              (try
                (let [options-map (build-circe-options cdmSchema resultSchema cohortId targetTable generateStats)
                      clj-options (circe/java-map->circe-options options-map)
                      sql (circe/render-circe-to-sql db circeJson clj-options)]
                  (ok {:success true :sql sql}))
                (catch Exception e
                  (internal-error (.getMessage e)))))))))))

(defn- handle-search-vocab [db source-key params]
  (let [source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [query (:query params)
            max-rows (try (Integer/parseInt (or (:maxRows params) "1000")) (catch Exception _ 1000))
            database-code (or (:databaseCode params) (sanitize-database-code source-key))]
        (if (str/blank? query)
          (bad-request "query parameter is required")
          (try
            (let [options-map (build-vocab-options db database-code max-rows)
                  clj-options (vocab/java-map->search-options options-map)
                  results (vocab/search-vocab db query clj-options)
                  clj-results (mapv java-map->clj results)]
              (ok {:results clj-results :count (count clj-results)}))
            (catch Exception e
              (internal-error (.getMessage e)))))))))

(defn handle-request [db method path body headers params]
  (let [segments (vec (remove str/blank? (str/split (or path "") #"/")))
        source-key (first segments)
        resource (second segments)
        action (nth segments 2 nil)]
    (cond
      (str/blank? source-key)
      (bad-request "Source key is required in path")

      (and (= source-key "cache") (= resource "jobs"))
      (case method
        "GET" (handle-list-cache-jobs db params)
        (not-found (str "Unknown cache jobs endpoint: " method " /cache/jobs")))

      (= resource "cache")
      (case [method action]
        ["POST" nil] (handle-create-cache db source-key body params)
        ["GET" "status"] (handle-get-cache-status db source-key params)
        ["DELETE" nil] (handle-delete-cache db source-key params)
        ["DELETE" "job"] (handle-cancel-cache-job db source-key params)
        (not-found (str "Unknown cache endpoint: " method " /" (str/join "/" segments))))

      (= resource "circe")
      (case [method action]
        ["POST" "execute"] (handle-execute-circe db source-key body params)
        ["POST" "render"] (handle-render-circe db source-key body params)
        (not-found (str "Unknown circe endpoint: " method " /" (str/join "/" segments))))

      (= resource "vocab")
      (case [method action]
        ["GET" "search"] (handle-search-vocab db source-key params)
        (not-found (str "Unknown vocab endpoint: " method " /" (str/join "/" segments))))

      :else
      (not-found (str "Unknown resource: " resource)))))

(defn- clj->java [x]
  (cond
    (map? x)
    (let [m (HashMap.)]
      (doseq [[k v] x]
        (.put m (if (keyword? k) (name k) (str k)) (clj->java v)))
      m)

    (sequential? x)
    (let [l (ArrayList.)]
      (doseq [item x]
        (.add l (clj->java item)))
      l)

    (keyword? x)
    (name x)

    :else x))

(defn response->java-map [resp]
  (clj->java resp))

;; HTTP handlers

(defn- list-cache-jobs-handler
  [{:keys [db query-params]}]
  (handle-list-cache-jobs db query-params))

(defn- create-cache-handler
  [{:keys [db path-params body-params query-params]}]
  (let [source-key (:source-key path-params)
        source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (if (str/blank? (:schemaName body-params))
        (bad-request "schemaName is required")
        (let [database-code (or (:databaseCode body-params) (sanitize-database-code source-key))]
          (if-let [validation-error (validate-database-code database-code)]
            (bad-request validation-error)
            (try
              (let [cache-path (or (:cachePath query-params) (get-cache-path-from-config))
                    credentials (source->credentials source)
                    config-map (build-datamart-config database-code (:schemaName body-params)
                                                      cache-path credentials body-params)
                    validated-config (datamart/java-map->datamart-config config-map)]
                (when-let [error (datamart/validate-config validated-config)]
                  (throw (IllegalArgumentException. error)))
                (let [result (datamart/create-cache db validated-config nil)
                      java-result (datamart/result->java-map result)]
                  (ok (java-map->clj java-result))))
              (catch IllegalArgumentException e
                (bad-request (.getMessage e)))
              (catch Exception e
                (internal-error (.getMessage e))))))))))

(defn- get-cache-status-handler
  [{:keys [db path-params query-params]}]
  (let [source-key (:source-key path-params)]
    (handle-get-cache-status db source-key query-params)))

(defn- delete-cache-handler
  [{:keys [db path-params query-params]}]
  (let [source-key (:source-key path-params)]
    (handle-delete-cache db source-key query-params)))

(defn- cancel-cache-job-handler
  [{:keys [db path-params query-params]}]
  (let [source-key (:source-key path-params)]
    (handle-cancel-cache-job db source-key query-params)))

(defn- execute-circe-handler
  [{:keys [db path-params body-params trex-config]}]
  (let [source-key (:source-key path-params)
        source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [{:keys [circeJson cdmSchema resultSchema cohortId targetTable generateStats]} body-params
            cache-path (or (:cache-path trex-config) (get-cache-path-from-config))]
        (cond
          (str/blank? circeJson) (bad-request "circeJson is required")
          (str/blank? cdmSchema) (bad-request "cdmSchema is required")
          (str/blank? resultSchema) (bad-request "resultSchema is required")
          (nil? cohortId) (bad-request "cohortId is required")
          :else
          (try
            (when-not (db/is-attached? db source-key)
              (log/info (format "Attaching cache for %s from %s" source-key cache-path))
              (db/attach-cache-file! db source-key cache-path))
            ;; Qualify schema names with source-key database alias
            (let [qualified-cdm (str source-key "." cdmSchema)
                  qualified-results (str source-key "." resultSchema)
                  qualified-target (str qualified-results "." (or targetTable "cohort"))
                  options-map (build-circe-options qualified-cdm qualified-results cohortId qualified-target generateStats)
                  clj-options (circe/java-map->circe-options options-map)
                  result (circe/execute-circe db circeJson clj-options)
                  java-result (circe/circe-result->java-map result)]
              (ok (java-map->clj java-result)))
            (catch Exception e
              (internal-error (.getMessage e)))))))))

(defn- render-circe-handler
  [{:keys [db path-params body-params]}]
  (let [source-key (:source-key path-params)
        source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [{:keys [circeJson cdmSchema resultSchema cohortId targetTable generateStats]} body-params]
        (cond
          (str/blank? circeJson) (bad-request "circeJson is required")
          (str/blank? cdmSchema) (bad-request "cdmSchema is required")
          (str/blank? resultSchema) (bad-request "resultSchema is required")
          (nil? cohortId) (bad-request "cohortId is required")
          :else
          (try
            (let [options-map (build-circe-options cdmSchema resultSchema cohortId targetTable generateStats)
                  clj-options (circe/java-map->circe-options options-map)
                  sql (circe/render-circe-to-sql db circeJson clj-options)]
              (ok {:success true :sql sql}))
            (catch Exception e
              (internal-error (.getMessage e)))))))))

(defn- search-vocab-handler
  [{:keys [db path-params query-params]}]
  (let [source-key (:source-key path-params)]
    (handle-search-vocab db source-key query-params)))

(defn- count-patients-handler
  [{:keys [db path-params body-params trex-config]}]
  (let [source-key (:source-key path-params)
        source (find-source-by-key source-key)]
    (if-not source
      (not-found (str "Source not found: " source-key))
      (let [{:keys [expression]} body-params
            expression-str (if (string? expression)
                             expression
                             (json/write-str expression))
            cdm-schema (get-cdm-schema source)
            cache-path (or (:cache-path trex-config) (get-cache-path-from-config))
            start-time (System/currentTimeMillis)]
        (cond
          (or (nil? expression) (and (string? expression) (str/blank? expression)))
          (bad-request "expression is required")

          (str/blank? cdm-schema)
          (bad-request "CDM schema not configured for this source")

          :else
          (try
            (when-not (db/is-attached? db source-key)
              (log/info (format "Attaching cache for %s from %s" source-key cache-path))
              (db/attach-cache-file! db source-key cache-path))
            (let [qualified-cdm (str source-key "." cdm-schema)
                  qualified-results (str source-key "." cdm-schema)
                  temp-table "temp_cohort_count"
                  qualified-target (str qualified-results "." temp-table)
                  cohort-id 999999
                  options-map (build-circe-options qualified-cdm qualified-results cohort-id qualified-target false)
                  clj-options (circe/java-map->circe-options options-map)]
              (try
                (db/execute! db (format "DROP TABLE IF EXISTS %s" qualified-target))
                (db/execute! db (format "CREATE TABLE %s (cohort_definition_id INT, subject_id BIGINT, cohort_start_date DATE, cohort_end_date DATE)" qualified-target))
                (circe/execute-circe db expression-str clj-options)
                (let [cohort-sql (format "SELECT COUNT(DISTINCT subject_id) as cnt FROM %s WHERE cohort_definition_id = %d"
                                         qualified-target cohort-id)
                      total-sql (format "SELECT COUNT(DISTINCT person_id) as cnt FROM %s.person" qualified-cdm)
                      cohort-result (db/query db cohort-sql)
                      total-result (db/query db total-sql)
                      cohort-count (or (some-> cohort-result first (.get "cnt")) 0)
                      total-count (or (some-> total-result first (.get "cnt")) 0)
                      exec-time (- (System/currentTimeMillis) start-time)]
                  (ok {:cohortPatientCount cohort-count
                       :totalPatientCount total-count
                       :executionTimeMs exec-time}))
                (finally
                  (try
                    (db/execute! db (format "DROP TABLE IF EXISTS %s" qualified-target))
                    (catch Exception _)))))
            (catch Exception e
              (log/error e "Failed to count patients")
              (internal-error (.getMessage e)))))))))

;; Router

(def routes
  "WebAPI routes."
  [["/cache/jobs" {:get {:handler list-cache-jobs-handler}}]
   ["/:source-key"
    ["/cache" {:post {:handler create-cache-handler}
               :delete {:handler delete-cache-handler}}]
    ["/cache/status" {:get {:handler get-cache-status-handler}}]
    ["/cache/count" {:post {:handler count-patients-handler}}]
    ["/cache/job" {:delete {:handler cancel-cache-job-handler}}]
    ["/circe/execute" {:post {:handler execute-circe-handler}}]
    ["/circe/render" {:post {:handler render-circe-handler}}]
    ["/vocab/search" {:get {:handler search-vocab-handler}}]]])

(defn- create-proxy-handler [target-url]
  (fn [request]
    (proxy/proxy-request request target-url)))

(defn- build-proxy-routes [routes-map]
  (vec
    (mapcat
      (fn [[path-prefix target-url]]
        (let [handler {:handler (create-proxy-handler target-url)
                       :no-doc true}]
          [[(str "/" path-prefix) handler]
           [(str "/" path-prefix "/*path") handler]]))
      routes-map)))

(defn create-router
  "Create Reitit Ring router for WebAPI endpoints."
  []
  (let [proxy-routes (build-proxy-routes (config/get-proxy-routes))
        all-routes (vec (concat proxy-routes routes))]
    (ring/ring-handler
      (ring/router all-routes {:conflicts nil})
      (ring/create-default-handler
        {:not-found (constantly (not-found "Endpoint not found"))
         :method-not-allowed (constantly (response 405 {:error "METHOD_NOT_ALLOWED"
                                                         :message "Method not allowed"}))}))))
