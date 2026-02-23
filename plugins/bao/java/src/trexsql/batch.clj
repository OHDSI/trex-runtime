(ns trexsql.batch
  "JDBC batch transfer for cache creation.
   Implements streaming data transfer from JDBC sources to TrexSQL cache files
   with progress reporting and resume capability."
  (:require [trexsql.db :as db]
            [trexsql.native :as native]
            [trexsql.jobs :as jobs]
            [trexsql.jdbc-types :as jdbc-types]
            [trexsql.errors :as errors]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [honey.sql :as sql])
  (:import [java.sql Connection DriverManager PreparedStatement ResultSet ResultSetMetaData SQLException]
           [java.util Properties HashMap]
           [com.sun.jna Pointer]
           [java.io File]
           [com.zaxxer.hikari HikariConfig HikariDataSource]))

(def default-batch-size 10000)
(def default-fetch-size 2000)
(def default-progress-interval 10000)

(def min-batch-size 100)
(def max-batch-size 100000)

(def ^:private default-pool-size 5)
(def ^:private default-connection-timeout 30000)
(def ^:private default-idle-timeout 600000)
(def ^:private default-max-lifetime 1800000)

(defn create-connection-pool
  "Create a HikariCP connection pool for JDBC source.
   Returns a HikariDataSource that should be closed when done.

   Options:
   - :jdbc-url - JDBC connection URL (required)
   - :user - database username
   - :password - database password
   - :pool-size - max connections (default 5)
   - :pool-name - name for the pool (default 'batch-source-pool')
   - :connection-timeout - timeout in ms (default 30000)
   - :idle-timeout - idle connection timeout in ms (default 600000)
   - :max-lifetime - max connection lifetime in ms (default 1800000)
   - :read-only - set connections to read-only (default true for source DBs)"
  [{:keys [jdbc-url user password pool-size pool-name
           connection-timeout idle-timeout max-lifetime read-only]
    :or {pool-size default-pool-size
         pool-name "batch-source-pool"
         connection-timeout default-connection-timeout
         idle-timeout default-idle-timeout
         max-lifetime default-max-lifetime
         read-only true}}]
  (let [config (doto (HikariConfig.)
                 (.setJdbcUrl jdbc-url)
                 (.setMaximumPoolSize pool-size)
                 (.setPoolName pool-name)
                 (.setConnectionTimeout connection-timeout)
                 (.setIdleTimeout idle-timeout)
                 (.setMaxLifetime max-lifetime)
                 (.setAutoCommit false))]
    (when (and read-only (not (re-find #"(?i)duckdb" (or jdbc-url ""))))
      (.setReadOnly config true))
    (when user (.setUsername config user))
    (when password (.setPassword config password))
    (log/debug (format "Creating connection pool '%s' with %d connections to %s"
                       pool-name pool-size jdbc-url))
    (HikariDataSource. config)))

(defn close-connection-pool!
  "Close a connection pool and release all connections."
  [^HikariDataSource pool]
  (when pool
    (log/debug (format "Closing connection pool '%s'" (.getPoolName pool)))
    (.close pool)))

(defmacro with-connection-pool
  "Execute body with a connection pool. Pool is automatically closed when done.
   Binds pool-sym to the HikariDataSource."
  [[pool-sym pool-config] & body]
  `(let [~pool-sym (create-connection-pool ~pool-config)]
     (try
       ~@body
       (finally
         (close-connection-pool! ~pool-sym)))))

(defmacro with-pooled-connection
  "Get a connection from pool, execute body, return connection to pool.
   Binds conn-sym to the Connection."
  [[conn-sym pool] & body]
  `(with-open [~conn-sym (.getConnection ~pool)]
     ~@body))

(defn validate-batch-config
  "Validate batch configuration. Returns error message or nil if valid."
  [{:keys [batch-size] :as config}]
  (cond
    (and batch-size (not (<= min-batch-size batch-size max-batch-size)))
    (format "batch-size must be between %d and %d" min-batch-size max-batch-size)

    (str/blank? (:schema-name config))
    "schema-name is required"

    :else nil))

(defn build-select-query
  "Build SELECT query with optional filters using HoneySQL.
   Returns [sql-string & params] for parameterized execution."
  [{:keys [schema table columns patient-filter timestamp-filter column-filter]}]
  (let [select-cols (if-let [cols (get column-filter table)]
                      (mapv keyword cols)
                      (or columns [:*]))
        base-query {:select select-cols
                    :from [[(keyword (str schema "." table))]]}
        with-patient (if patient-filter
                       (assoc base-query :where [:in :person_id patient-filter])
                       base-query)
        with-timestamp (if timestamp-filter
                         (if (:where with-patient)
                           (update with-patient :where #(vector :and % [:>= :observation_date timestamp-filter]))
                           (assoc with-patient :where [:>= :observation_date timestamp-filter]))
                         with-patient)]
    (sql/format with-timestamp {:quoted true})))

(defn build-create-table-ddl
  "Generate CREATE TABLE DDL from column metadata using HoneySQL.
   Returns SQL string."
  [table-name columns]
  (let [col-defs (for [{:keys [name trexsql-type nullable? precision scale]} columns]
                   (let [type-str (if (and (= trexsql-type "DECIMAL") precision (pos? precision))
                                    (format "DECIMAL(%d,%d)" precision (or scale 0))
                                    trexsql-type)
                         null-str (if nullable? "" " NOT NULL")]
                     (format "\"%s\" %s%s" name type-str null-str)))]
    (format "CREATE TABLE IF NOT EXISTS \"%s\" (%s)"
            table-name
            (str/join ", " col-defs))))

(defn retry-with-backoff
  "Retry a function with exponential backoff for transient errors.
   Options: :max-retries (default 3), :initial-delay-ms (default 1000), :max-delay-ms (default 30000)"
  [f {:keys [max-retries initial-delay-ms max-delay-ms on-retry]
      :or {max-retries 3 initial-delay-ms 1000 max-delay-ms 30000}}]
  (loop [attempt 1
         delay initial-delay-ms]
    (let [result (try
                   {:success (f)}
                   (catch java.sql.SQLTransientException e
                     {:error e :retryable true})
                   (catch java.net.SocketTimeoutException e
                     {:error e :retryable true})
                   (catch java.sql.SQLRecoverableException e
                     {:error e :retryable true})
                   (catch Exception e
                     ;; Check for connection-related errors
                     (if (re-find #"(?i)connection|timeout|network|reset" (.getMessage e))
                       {:error e :retryable true}
                       {:error e :retryable false})))]
      (cond
        (:success result) (:success result)

        (and (:retryable result) (< attempt max-retries))
        (do
          (when on-retry
            (on-retry attempt (:error result)))
          (log/warn (format "Retrying after transient error (attempt %d/%d): %s"
                            attempt max-retries (.getMessage (:error result))))
          (Thread/sleep delay)
          (recur (inc attempt) (min (* delay 2) max-delay-ms)))

        :else
        (throw (:error result))))))

(defn create-table-schema!
  "Create table in TrexSQL cache from JDBC column metadata."
  [trexsql-db cache-alias table-name columns]
  (let [full-table-name (format "%s.%s" cache-alias table-name)
        ddl (build-create-table-ddl full-table-name columns)]
    (log/debug (format "Creating table: %s" full-table-name))
    (db/execute! trexsql-db ddl)))

(defn copy-rows-batched!
  "Copy rows from ResultSet to TrexSQL using native Appender API.
   Returns total rows copied.
   progress-fn is called with {:phase :row-progress :table :rows-processed :estimated-rows}"
  [^Pointer db-handle schema table-name ^ResultSet rs columns config progress-fn]
  (let [batch-size (or (:batch-size config) default-batch-size)
        progress-interval (or (:progress-interval config) default-progress-interval)
        estimated-rows (:estimated-rows config)
        appender (native/appender-create db-handle schema table-name)]
    (try
      (loop [total 0]
        (if (.next rs)
          (do
            (doseq [[idx col] (map-indexed vector columns)]
              (let [value (jdbc-types/read-typed-value rs (inc idx) (:trexsql-type col))]
                (jdbc-types/append-typed-value! appender value (:trexsql-type col))))
            (native/appender-end-row! appender)
            (when (zero? (mod (inc total) batch-size))
              (native/appender-flush! appender))
            (when (and progress-fn (zero? (mod (inc total) progress-interval)))
              (progress-fn {:phase :row-progress
                            :table table-name
                            :rows-processed (inc total)
                            :estimated-rows estimated-rows}))
            (recur (inc total)))
          (do
            (native/appender-flush! appender)
            total)))
      (finally
        (native/appender-close! appender)))))

(declare copy-table-jdbc-impl)

(defmacro with-trexsql-transaction
  "Execute body within a TrexSQL transaction.
   Commits on success, rolls back on exception.

   Usage:
   (with-trexsql-transaction [trexsql-db]
     (do-work ...))"
  [[db-sym] & body]
  `(let [handle# (db/get-raw-handle ~db-sym)]
     (native/execute! handle# "BEGIN TRANSACTION")
     (try
       (let [result# (do ~@body)]
         (native/execute! handle# "COMMIT")
         result#)
       (catch Exception e#
         (try
           (native/execute! handle# "ROLLBACK")
           (catch Exception rollback-err#
             (log/warn (format "Rollback failed: %s" (.getMessage rollback-err#)))))
         (throw e#)))))

(defn copy-table-with-transaction
  "Copy a table within a TrexSQL transaction.
   Creates table and copies all rows atomically.
   On failure, rolls back to leave cache in consistent state."
  [trexsql-db source-conn cache-alias table-name config progress-fn]
  (with-trexsql-transaction [trexsql-db]
    (copy-table-jdbc-impl trexsql-db source-conn cache-alias table-name config progress-fn)))

(defn copy-table-jdbc-impl
  "Copy a single table from JDBC source to TrexSQL cache.
   Uses cursor-based streaming with configurable fetch size.
   Returns {:success? true :rows-copied N :duration-ms M} or {:success? false :error E}"
  [trexsql-db source-conn cache-alias table-name config progress-fn]
  (let [start-time (System/currentTimeMillis)
        {:keys [schema-name fetch-size column-filter patient-filter timestamp-filter]} config
        fetch-size (or fetch-size default-fetch-size)]
    (try
      (let [[query-sql & params] (build-select-query
                                   {:schema schema-name
                                    :table table-name
                                    :column-filter column-filter
                                    :patient-filter patient-filter
                                    :timestamp-filter timestamp-filter})]
        (log/debug (format "Executing query for table %s: %s" table-name query-sql))

        (with-open [stmt (doto (.prepareStatement source-conn query-sql
                                 ResultSet/TYPE_FORWARD_ONLY
                                 ResultSet/CONCUR_READ_ONLY)
                           (.setFetchSize fetch-size))]
          (doseq [[idx param] (map-indexed vector params)]
            (.setObject stmt (inc idx) param))

          (with-open [rs (.executeQuery stmt)]
            (let [columns (jdbc-types/get-column-info (.getMetaData rs))
                  db-handle (db/get-raw-handle trexsql-db)]
              (create-table-schema! trexsql-db cache-alias table-name columns)
              (let [rows-copied (copy-rows-batched! db-handle cache-alias table-name rs columns config progress-fn)
                    duration-ms (- (System/currentTimeMillis) start-time)]
                {:success? true
                 :table-name table-name
                 :rows-copied rows-copied
                 :duration-ms duration-ms})))))
      (catch Exception e
        (let [duration-ms (- (System/currentTimeMillis) start-time)]
          (log/error e (format "Failed to copy table %s" table-name))
          {:success? false
           :table-name table-name
           :error (.getMessage e)
           :phase :data
           :duration-ms duration-ms})))))

(defn copy-table-jdbc
  "Copy a single table from JDBC source to TrexSQL cache with transaction support.
   Uses cursor-based streaming with configurable fetch size.
   Wraps copy operation in a TrexSQL transaction for atomicity.
   Returns {:success? true :rows-copied N :duration-ms M} or {:success? false :error E}"
  [trexsql-db source-conn cache-alias table-name config progress-fn]
  (let [use-transactions? (get config :use-transactions true)]
    (if use-transactions?
      (copy-table-with-transaction trexsql-db source-conn cache-alias table-name config progress-fn)
      (copy-table-jdbc-impl trexsql-db source-conn cache-alias table-name config progress-fn))))

(defn get-source-tables
  "Get list of tables in source schema via JDBC metadata."
  [^Connection source-conn schema-name]
  (with-open [rs (.getTables (.getMetaData source-conn) nil schema-name "%" (into-array String ["TABLE"]))]
    (loop [tables []]
      (if (.next rs)
        (recur (conj tables (.getString rs "TABLE_NAME")))
        tables))))

(defn get-completed-tables
  "Get list of tables already copied to cache file."
  [trexsql-db cache-alias]
  (try
    (let [[query-sql & params] (sql/format
                                 {:select [:table_name]
                                  :from [:information_schema.tables]
                                  :where [:= :table_catalog cache-alias]})
          results (db/query-with-params trexsql-db query-sql (vec params))]
      (set (map #(.get ^HashMap % "table_name") results)))
    (catch Exception e
      (log/debug (format "Could not get completed tables for %s: %s" cache-alias (.getMessage e)))
      #{})))

(defn get-tables-to-copy
  "Get tables that need to be copied (not yet in cache).
   If table-filter provided, only include those tables."
  [source-conn trexsql-db cache-alias schema-name table-filter]
  (let [source-tables (get-source-tables source-conn schema-name)
        completed-tables (get-completed-tables trexsql-db cache-alias)
        tables-needed (if table-filter
                        (filter (set table-filter) source-tables)
                        source-tables)]
    (remove completed-tables tables-needed)))

(defn drop-partial-table!
  "Drop a table if it exists (for re-copying partial tables)."
  [trexsql-db cache-alias table-name]
  (try
    (db/execute! trexsql-db (format "DROP TABLE IF EXISTS %s.%s" cache-alias table-name))
    (catch Exception e
      (log/warn (format "Failed to drop partial table %s.%s: %s" cache-alias table-name (.getMessage e))))))

(defn check-cancellation
  "Check if job has been cancelled. Returns true if cancelled."
  [trexsql-db database-code]
  (when-let [job-status (jobs/get-job-status trexsql-db database-code)]
    (= "CANCELED" (:status job-status))))

(declare create-cache-jdbc-with-connection)

(defn create-cache-jdbc
  "Main orchestration function for JDBC batch cache creation.
   Handles job tracking, progress reporting, and resume capability.
   Uses HikariCP connection pooling for efficient source database access.

   config keys:
   - :source-credentials - JDBC connection info {:jdbc-url :user :password :pool-size}
   - :schema-name - source schema to copy
   - :database-code - unique cache identifier
   - :batch-size - rows per batch (default 10000)
   - :table-filter - optional list of table names to copy
   - :column-filter - optional map of table -> columns
   - :patient-filter - optional list of patient IDs
   - :timestamp-filter - optional minimum timestamp
   - :cache-path - cache directory (default ./data/cache)
   - :resume? - if true, skip completed tables
   - :use-transactions - wrap table copies in transactions (default true)
   - :use-pooling - use connection pooling (default true)

   progress-fn is called with progress events:
   - {:phase :job-start :total-tables N}
   - {:phase :table-start :table T :table-index I :total-tables N}
   - {:phase :row-progress :table T :rows-processed R :estimated-rows E}
   - {:phase :table-complete :table T :rows-copied R :duration-ms M}
   - {:phase :table-failed :table T :error E}
   - {:phase :job-complete :tables-copied [...] :tables-failed [...] :duration-ms M}"
  [trexsql-db config progress-fn]
  (let [start-time (System/currentTimeMillis)
        {:keys [source-credentials schema-name database-code cache-path
                table-filter resume? use-pooling]
         :or {use-pooling true}} config
        cache-path (or cache-path "./data/cache")
        webapi-ds (jobs/get-webapi-datasource trexsql-db)]

    (when-let [error (validate-batch-config config)]
      (throw (errors/config-error error :batch-config)))

    (let [{:keys [jdbc-url user password pool-size]} source-credentials
          pool-config {:jdbc-url jdbc-url
                       :user user
                       :password password
                       :pool-size (or pool-size default-pool-size)
                       :pool-name (str "batch-" database-code)
                       :read-only true}
          run-with-connection (fn [get-conn-fn]
                                (db/attach-cache-file! trexsql-db database-code cache-path)
                                (create-cache-jdbc-with-connection trexsql-db config progress-fn
                                  get-conn-fn start-time database-code schema-name cache-path
                                  table-filter webapi-ds))]
      (if use-pooling
        ;; Use connection pooling
        (with-connection-pool [pool pool-config]
          (run-with-connection #(.getConnection pool)))
        ;; Direct connection (for testing or simple cases)
        (run-with-connection #(doto (DriverManager/getConnection jdbc-url user password)
                               (.setReadOnly true)
                               (.setAutoCommit false)))))))

(defn- create-cache-jdbc-with-connection
  "Internal implementation that uses provided connection factory."
  [trexsql-db config progress-fn get-conn-fn start-time database-code schema-name
   cache-path table-filter webapi-ds]
  (with-open [source-conn (get-conn-fn)]
    (let [tables-to-copy (get-tables-to-copy source-conn trexsql-db database-code schema-name table-filter)
          total-tables (count tables-to-copy)
          exec-id (jobs/write-spring-batch-job! webapi-ds "cacheGeneration"
                    {:database-code database-code :schema-name schema-name})
          _ (jobs/create-local-job! trexsql-db database-code
              {:job-execution-id exec-id
               :source-key database-code
               :status "RUNNING"
               :total-tables total-tables
               :config config})]

      (when progress-fn
        (progress-fn {:phase :job-start :total-tables total-tables}))

      (try
        (loop [remaining tables-to-copy
               idx 1
               tables-copied []
               tables-failed []
               total-rows-processed 0]
          (if (empty? remaining)
            (let [duration-ms (- (System/currentTimeMillis) start-time)
                  success? (empty? tables-failed)]
              (jobs/update-spring-batch-status! webapi-ds exec-id
                (if success? "COMPLETED" "FAILED"))
              (jobs/update-local-status! trexsql-db database-code
                (if success? "COMPLETE" "FAILED"))
              (jobs/update-local-progress! trexsql-db database-code
                {:completed-tables (count tables-copied)
                 :tables-copied tables-copied
                 :tables-failed tables-failed})
              (when progress-fn
                (progress-fn {:phase :job-complete
                              :tables-copied tables-copied
                              :tables-failed tables-failed
                              :duration-ms duration-ms}))
              {:success? success?
               :database-code database-code
               :schema-name schema-name
               :tables-copied tables-copied
               :tables-failed tables-failed
               :duration-ms duration-ms})

            (if (check-cancellation trexsql-db database-code)
              (let [duration-ms (- (System/currentTimeMillis) start-time)]
                (jobs/update-spring-batch-status! webapi-ds exec-id "STOPPED")
                (jobs/update-local-status! trexsql-db database-code "CANCELED")
                {:success? false
                 :database-code database-code
                 :schema-name schema-name
                 :tables-copied tables-copied
                 :tables-failed tables-failed
                 :duration-ms duration-ms
                 :error "Job cancelled by user"})

              (let [table-name (first remaining)
                    _ (when progress-fn
                        (progress-fn {:phase :table-start
                                      :table table-name
                                      :table-index idx
                                      :total-tables total-tables}))
                    _ (jobs/update-local-progress! trexsql-db database-code
                        {:current-table table-name})
                    result (retry-with-backoff
                             #(copy-table-jdbc trexsql-db source-conn database-code table-name config progress-fn)
                             {:max-retries 3
                              :on-retry (fn [attempt err]
                                          (jobs/update-retry-status! trexsql-db database-code
                                            attempt (.getMessage err)))})]
                (if (:success? result)
                  (let [new-total (+ total-rows-processed (or (:rows-copied result) 0))]
                    (when progress-fn
                      (progress-fn {:phase :table-complete
                                    :table table-name
                                    :rows-copied (:rows-copied result)
                                    :duration-ms (:duration-ms result)}))
                    (jobs/update-local-progress! trexsql-db database-code
                      {:completed-tables idx
                       :processed-rows new-total})
                    (recur (rest remaining)
                           (inc idx)
                           (conj tables-copied result)
                           tables-failed
                           new-total))
                  (do
                    (when progress-fn
                      (progress-fn {:phase :table-failed
                                    :table table-name
                                    :error (:error result)}))
                    (recur (rest remaining)
                           (inc idx)
                           tables-copied
                           (conj tables-failed result)
                           total-rows-processed)))))))

        (catch Exception e
          (let [duration-ms (- (System/currentTimeMillis) start-time)
                error-msg (.getMessage e)]
            (jobs/update-spring-batch-status! webapi-ds exec-id "FAILED")
            (jobs/update-local-status! trexsql-db database-code "ERROR" error-msg)
            (when progress-fn
              (progress-fn {:phase :job-failed :error error-msg}))
            {:success? false
             :database-code database-code
             :schema-name schema-name
             :tables-copied []
             :tables-failed []
             :duration-ms duration-ms
             :error error-msg}))))))
