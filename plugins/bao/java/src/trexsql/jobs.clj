(ns trexsql.jobs
  "Job tracking for JDBC batch cache operations.
   Dual-write to local _cache_jobs.db and Spring Batch tables."
  (:require [trexsql.db :as db]
            [trexsql.errors :as errors]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [honey.sql :as sql]
            [honey.sql.helpers :as h])
  (:import [java.sql Connection DriverManager PreparedStatement ResultSet SQLException]
           [java.util Properties HashMap]
           [java.time Instant LocalDateTime ZoneId]
           [java.io File]))

(def ^:private jobs-db-name "_cache_jobs")

(defn- jobs-table
  "Get qualified jobs table name as keyword."
  [jobs-db]
  (keyword (str jobs-db ".cache_generation_info")))

(def ^:private jobs-db-initialized (atom {}))

(defn get-jobs-db
  "Attach local jobs database. Creates if needed.
   Memoizes initialization per connection to avoid repeated DDL execution."
  [trexsql-db]
  (let [cache-path (or (get-in trexsql-db [:config :cache-path]) "./data/cache")
        cache-dir (File. cache-path)
        jobs-file (File. cache-dir (str jobs-db-name ".db"))
        file-path (.getAbsolutePath jobs-file)
        conn-hash (System/identityHashCode (:handle trexsql-db))]
    (when (not= conn-hash (get @jobs-db-initialized file-path))
      (when-not (.exists cache-dir)
        (.mkdirs cache-dir))
      (let [escaped-path (str/replace file-path "'" "''")
            attach-sql (format "ATTACH IF NOT EXISTS '%s' AS %s" escaped-path jobs-db-name)]
        (db/execute! trexsql-db attach-sql)
        (db/execute! trexsql-db (str "
          CREATE TABLE IF NOT EXISTS " jobs-db-name ".cache_generation_info (
            database_code     VARCHAR PRIMARY KEY,
            job_execution_id  BIGINT,
            source_key        VARCHAR NOT NULL,
            status            VARCHAR NOT NULL,
            start_time        TIMESTAMP,
            end_time          TIMESTAMP,
            total_tables      INTEGER,
            completed_tables  INTEGER,
            current_table     VARCHAR,
            processed_rows    BIGINT,
            tables_copied     JSON,
            tables_failed     JSON,
            error_message     VARCHAR,
            config            JSON,
            retry_count       INTEGER DEFAULT 0,
            last_error        VARCHAR
          )"))
        (swap! jobs-db-initialized assoc file-path conn-hash)))
    jobs-db-name))

(defn create-local-job!
  "Insert job record. Returns database_code."
  [trexsql-db database-code {:keys [job-execution-id source-key status config total-tables]}]
  (let [jobs-db (get-jobs-db trexsql-db)
        now (LocalDateTime/now)
        config-json (when config (json/write-str config))]
    (db/execute-with-params! trexsql-db
      (str "INSERT OR REPLACE INTO " jobs-db ".cache_generation_info "
           "(database_code, job_execution_id, source_key, status, start_time, "
           "total_tables, completed_tables, processed_rows, tables_copied, "
           "tables_failed, config, retry_count) "
           "VALUES (?, ?, ?, ?, ?, ?, 0, 0, '[]', '[]', ?, 0)")
      [database-code
       job-execution-id
       (or source-key database-code)
       (or status "RUNNING")
       now
       (or total-tables 0)
       config-json])
    database-code))

(defn update-local-progress!
  "Update job progress fields."
  [trexsql-db database-code {:keys [completed-tables current-table processed-rows
                                     tables-copied tables-failed]}]
  (let [jobs-db (get-jobs-db trexsql-db)
        set-map (cond-> {}
                  (some? completed-tables) (assoc :completed_tables completed-tables)
                  (some? current-table) (assoc :current_table (str current-table))
                  (some? processed-rows) (assoc :processed_rows processed-rows)
                  (some? tables-copied) (assoc :tables_copied (json/write-str tables-copied))
                  (some? tables-failed) (assoc :tables_failed (json/write-str tables-failed)))]
    (when (seq set-map)
      (let [[update-sql & params] (sql/format
                                    {:update (jobs-table jobs-db)
                                     :set set-map
                                     :where [:= :database_code database-code]})]
        (db/execute-with-params! trexsql-db update-sql (vec params))))))

(defn update-local-status!
  "Update job status. Sets end_time for terminal statuses."
  [trexsql-db database-code status & [error-msg]]
  (let [jobs-db (get-jobs-db trexsql-db)
        now (LocalDateTime/now)
        terminal? (contains? #{"COMPLETE" "ERROR" "CANCELED" "FAILED"} status)
        set-map (cond-> {:status status}
                  terminal? (assoc :end_time now)
                  error-msg (assoc :error_message error-msg))
        [update-sql & params] (sql/format
                                {:update (jobs-table jobs-db)
                                 :set set-map
                                 :where [:= :database_code database-code]})]
    (db/execute-with-params! trexsql-db update-sql (vec params))))

(defn- parse-json-field [value]
  (when value
    (let [s (str value)]
      (when-not (or (str/blank? s) (= s "null") (= s "NULL"))
        (try
          (json/read-str s :key-fn keyword)
          (catch Exception _ nil))))))

(defn get-job-status
  "Get job status. Returns nil if not found."
  [trexsql-db database-code]
  (let [jobs-db (get-jobs-db trexsql-db)
        [query-sql & params] (sql/format
                               {:select [:*]
                                :from [(jobs-table jobs-db)]
                                :where [:= :database_code database-code]})
        results (db/query-with-params trexsql-db query-sql (vec params))]
    (when (seq results)
      (let [row (first results)]
        {:database-code (.get ^HashMap row "database_code")
         :job-execution-id (.get ^HashMap row "job_execution_id")
         :source-key (.get ^HashMap row "source_key")
         :status (.get ^HashMap row "status")
         :start-time (.get ^HashMap row "start_time")
         :end-time (.get ^HashMap row "end_time")
         :total-tables (.get ^HashMap row "total_tables")
         :completed-tables (.get ^HashMap row "completed_tables")
         :current-table (.get ^HashMap row "current_table")
         :processed-rows (.get ^HashMap row "processed_rows")
         :tables-copied (parse-json-field (.get ^HashMap row "tables_copied"))
         :tables-failed (parse-json-field (.get ^HashMap row "tables_failed"))
         :error-message (.get ^HashMap row "error_message")
         :config (parse-json-field (.get ^HashMap row "config"))
         :retry-count (.get ^HashMap row "retry_count")
         :last-error (.get ^HashMap row "last_error")}))))

(defn list-jobs
  "List jobs, optionally filtered by status."
  [trexsql-db & {:keys [status]}]
  (let [jobs-db (get-jobs-db trexsql-db)
        base-query {:select [:database_code :source_key :status :start_time :end_time
                             :total_tables :completed_tables :error_message]
                    :from [(jobs-table jobs-db)]
                    :order-by [[:start_time :desc]]}
        query (if status
                (assoc base-query :where [:= :status status])
                base-query)
        [query-sql & params] (sql/format query)
        results (if (seq params)
                  (db/query-with-params trexsql-db query-sql (vec params))
                  (db/query trexsql-db query-sql))]
    (mapv (fn [^HashMap row]
            {:database-code (.get row "database_code")
             :source-key (.get row "source_key")
             :status (.get row "status")
             :start-time (.get row "start_time")
             :end-time (.get row "end_time")
             :total-tables (.get row "total_tables")
             :completed-tables (.get row "completed_tables")
             :error-message (.get row "error_message")})
          results)))

(defn update-retry-status!
  "Update retry count and last error."
  [trexsql-db database-code retry-count last-error]
  (let [jobs-db (get-jobs-db trexsql-db)
        set-map (cond-> {:retry_count retry-count}
                  last-error (assoc :last_error (str last-error)))
        [update-sql & params] (sql/format
                                {:update (jobs-table jobs-db)
                                 :set set-map
                                 :where [:= :database_code database-code]})]
    (db/execute-with-params! trexsql-db update-sql (vec params))))

(defn store-resume-config!
  "Store config for resume capability."
  [trexsql-db database-code config]
  (let [jobs-db (get-jobs-db trexsql-db)
        config-json (json/write-str config)
        [update-sql & params] (sql/format
                                {:update (jobs-table jobs-db)
                                 :set {:config config-json}
                                 :where [:= :database_code database-code]})]
    (db/execute-with-params! trexsql-db update-sql (vec params))))

(defn get-resume-config
  "Get stored config for resume."
  [trexsql-db database-code]
  (when-let [job-status (get-job-status trexsql-db database-code)]
    (:config job-status)))

(def ^:private terminal-statuses #{"COMPLETED" "FAILED" "STOPPED" "ABANDONED"})

(defn terminal-status? [status]
  (contains? terminal-statuses status))

(defn- hash-job-params [params]
  (str (hash (pr-str params))))

(defn- execute-on-datasource!
  "Execute parameterized SQL on external datasource. Returns row count."
  [datasource sql params]
  (with-open [conn (.getConnection datasource)
              stmt (.prepareStatement conn sql)]
    (doseq [[idx param] (map-indexed vector params)]
      (.setObject stmt (inc idx) param))
    (.executeUpdate stmt)))

(defn- query-on-datasource
  "Execute parameterized query on external datasource. Returns first column of first row."
  [datasource sql params]
  (with-open [conn (.getConnection datasource)
              stmt (.prepareStatement conn sql)]
    (doseq [[idx param] (map-indexed vector params)]
      (.setObject stmt (inc idx) param))
    (with-open [rs (.executeQuery stmt)]
      (when (.next rs)
        (.getObject rs 1)))))

(defn- get-next-sequence-value [webapi-ds sequence-name]
  (try
    (let [[query-sql & params] (sql/format {:select [[[:nextval sequence-name]]]})]
      (query-on-datasource webapi-ds query-sql params))
    (catch SQLException e
      (log/warn (format "Failed to get sequence value for %s: %s" sequence-name (.getMessage e)))
      nil)))

(defn write-spring-batch-job!
  "Insert into Spring Batch tables. Returns job-execution-id or nil."
  [webapi-ds job-name job-params]
  (when webapi-ds
    (try
      (let [instance-id (get-next-sequence-value webapi-ds "BATCH_JOB_SEQ")
            exec-id (get-next-sequence-value webapi-ds "BATCH_JOB_EXECUTION_SEQ")
            job-key (hash-job-params job-params)
            now (java.sql.Timestamp. (System/currentTimeMillis))]
        (when (and instance-id exec-id)
          (let [[insert-sql & params] (sql/format
                                        {:insert-into :BATCH_JOB_INSTANCE
                                         :columns [:job_instance_id :job_name :job_key :version]
                                         :values [[instance-id job-name job-key 0]]})]
            (execute-on-datasource! webapi-ds insert-sql params))
          (let [[insert-sql & params] (sql/format
                                        {:insert-into :BATCH_JOB_EXECUTION
                                         :columns [:job_execution_id :job_instance_id :start_time
                                                   :status :version :create_time]
                                         :values [[exec-id instance-id now "STARTED" 0 now]]})]
            (execute-on-datasource! webapi-ds insert-sql params))
          exec-id))
      (catch SQLException e
        (log/warn (format "Failed to write Spring Batch job: %s" (.getMessage e)))
        nil))))

(defn update-spring-batch-status!
  "Update Spring Batch job status."
  [webapi-ds exec-id status]
  (when (and webapi-ds exec-id)
    (try
      (let [now (java.sql.Timestamp. (System/currentTimeMillis))
            terminal? (terminal-status? status)
            set-map (cond-> {:status status}
                      terminal? (assoc :end_time now))
            [update-sql & params] (sql/format
                                    {:update :BATCH_JOB_EXECUTION
                                     :set set-map
                                     :where [:= :job_execution_id exec-id]})]
        (execute-on-datasource! webapi-ds update-sql params))
      (catch SQLException e
        (log/warn (format "Failed to update Spring Batch status: %s" (.getMessage e)))))))

(defn get-webapi-datasource
  "Get WebAPI datasource from config. Returns nil if not configured."
  [trexsql-db]
  (get-in trexsql-db [:config :webapi-datasource]))
