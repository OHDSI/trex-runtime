(ns trexsql.native
  "Idiomatic Clojure wrapper around the TrexEngine JNA interface.
   Provides error-checked wrappers for all native C API calls."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [trexsql.errors :as errors])
  (:import [org.trex TrexEngine]
           [com.sun.jna Pointer]
           [java.util ArrayList HashMap]))

(defn- engine
  "Lazy accessor for the TrexEngine JNA instance.
   Defers native library loading to first use rather than AOT compile time."
  ^TrexEngine []
  TrexEngine/INSTANCE)

(defn- check-error!
  "Check trexsql_last_error after a failed call. Throws sql-error."
  [context]
  (let [err (.trexsql_last_error (engine))]
    (throw (errors/sql-error (str context ": " (or err "unknown error")) context))))

;; === Database lifecycle ===

(defn open
  "Open a trexsql database. Returns a Pointer handle.
   path: file path or ':memory:' (nil means ':memory:')
   flags: bit 0 = allow unsigned extensions"
  ([] (open nil 0))
  ([path flags]
   (let [handle (.trexsql_open (engine) (or path ":memory:") (int flags))]
     (when (nil? handle)
       (check-error! "open"))
     handle)))

(defn close!
  "Close a database handle."
  [^Pointer handle]
  (when handle
    (.trexsql_close (engine) handle)))

;; === SQL execution ===

(defn execute!
  "Execute a non-query SQL statement (DDL, DML, LOAD, PRAGMA, etc.).
   Returns true on success. Throws on error."
  [^Pointer handle ^String sql]
  (let [rc (.trexsql_execute (engine) handle sql)]
    (when (not= rc 0)
      (check-error! sql))
    true))

(defn query
  "Execute a SQL query and return results as ArrayList<HashMap>.
   Each row is a HashMap<String, Object> with column names as keys."
  [^Pointer handle ^String sql]
  (let [result (.trexsql_query (engine) handle sql)]
    (when (nil? result)
      (check-error! sql))
    (try
      (let [col-count (.trexsql_result_column_count (engine) result)
            col-names (vec (for [i (range col-count)]
                            (.trexsql_result_column_name (engine) result (int i))))
            rows (ArrayList.)]
        (while (= 1 (.trexsql_result_next (engine) result))
          (let [row (HashMap.)]
            (doseq [i (range col-count)]
              (let [col-name (nth col-names i)]
                (if (= 1 (.trexsql_result_is_null (engine) result (int i)))
                  (.put row col-name nil)
                  (let [str-ptr (.trexsql_result_get_string (engine) result (int i))]
                    (if (nil? str-ptr)
                      (.put row col-name nil)
                      (let [value (.getString str-ptr 0)]
                        (.trexsql_free_string (engine) str-ptr)
                        (.put row col-name value)))))))
            (.add rows row)))
        rows)
      (finally
        (.trexsql_result_close (engine) result)))))

(defn query-long
  "Execute a SQL query and return results with typed long values.
   Useful for queries returning integer results."
  [^Pointer handle ^String sql]
  (let [result (.trexsql_query (engine) handle sql)]
    (when (nil? result)
      (check-error! sql))
    (try
      (let [col-count (.trexsql_result_column_count (engine) result)
            col-names (vec (for [i (range col-count)]
                            (.trexsql_result_column_name (engine) result (int i))))
            rows (ArrayList.)]
        (while (= 1 (.trexsql_result_next (engine) result))
          (let [row (HashMap.)]
            (doseq [i (range col-count)]
              (let [col-name (nth col-names i)]
                (if (= 1 (.trexsql_result_is_null (engine) result (int i)))
                  (.put row col-name nil)
                  (.put row col-name (.trexsql_result_get_long (engine) result (int i))))))
            (.add rows row)))
        rows)
      (finally
        (.trexsql_result_close (engine) result)))))

;; === Parameterized query support ===

(defn- escape-sql-value
  "Escape a value for safe SQL interpolation."
  [v]
  (cond
    (nil? v) "NULL"
    (string? v) (str "'" (str/replace v "'" "''") "'")
    (integer? v) (str v)
    (float? v) (str v)
    (instance? Boolean v) (if v "true" "false")
    (instance? java.math.BigDecimal v) (str v)
    :else (str "'" (str/replace (str v) "'" "''") "'")))

(defn interpolate-sql
  "Replace ? placeholders in SQL with escaped values.
   Mimics JDBC PreparedStatement parameter binding."
  [^String sql params]
  (loop [result (StringBuilder.)
         remaining sql
         params-left params]
    (let [idx (.indexOf remaining "?")]
      (if (or (neg? idx) (empty? params-left))
        (do (.append result remaining)
            (.toString result))
        (do (.append result (subs remaining 0 idx))
            (.append result (escape-sql-value (first params-left)))
            (recur result
                   (subs remaining (inc idx))
                   (rest params-left)))))))

(defn query-with-params
  "Execute a parameterized query. Replaces ? with escaped values."
  [^Pointer handle ^String sql params]
  (query handle (interpolate-sql sql params)))

(defn execute-with-params!
  "Execute a parameterized non-query statement."
  [^Pointer handle ^String sql params]
  (execute! handle (interpolate-sql sql params)))

;; === Appender ===

(defn appender-create
  "Create an appender for schema.table. Returns Pointer handle."
  [^Pointer db-handle ^String schema ^String table]
  (let [app (.trexsql_appender_create (engine) db-handle schema table)]
    (when (nil? app)
      (check-error! (str "appender_create " schema "." table)))
    app))

(defn appender-end-row!
  "Finalize the current row."
  [^Pointer app]
  (let [rc (.trexsql_appender_end_row (engine) app)]
    (when (not= rc 0)
      (check-error! "appender_end_row"))))

(defn appender-append-null!
  "Append a NULL value."
  [^Pointer app]
  (let [rc (.trexsql_appender_append_null (engine) app)]
    (when (not= rc 0)
      (check-error! "appender_append_null"))))

(defn appender-append-string!
  "Append a string value."
  [^Pointer app ^String val]
  (if (nil? val)
    (appender-append-null! app)
    (let [rc (.trexsql_appender_append_string (engine) app val)]
      (when (not= rc 0)
        (check-error! "appender_append_string")))))

(defn appender-append-long!
  "Append a long value."
  [^Pointer app ^long val]
  (let [rc (.trexsql_appender_append_long (engine) app val)]
    (when (not= rc 0)
      (check-error! "appender_append_long"))))

(defn appender-append-int!
  "Append an int value."
  [^Pointer app val]
  (let [rc (.trexsql_appender_append_int (engine) app (int val))]
    (when (not= rc 0)
      (check-error! "appender_append_int"))))

(defn appender-append-double!
  "Append a double value."
  [^Pointer app ^double val]
  (let [rc (.trexsql_appender_append_double (engine) app val)]
    (when (not= rc 0)
      (check-error! "appender_append_double"))))

(defn appender-append-boolean!
  "Append a boolean value."
  [^Pointer app val]
  (let [rc (.trexsql_appender_append_boolean (engine) app (int (if val 1 0)))]
    (when (not= rc 0)
      (check-error! "appender_append_boolean"))))

(defn appender-flush!
  "Flush pending appender data."
  [^Pointer app]
  (let [rc (.trexsql_appender_flush (engine) app)]
    (when (not= rc 0)
      (check-error! "appender_flush"))))

(defn appender-close!
  "Close and free an appender handle."
  [^Pointer app]
  (when app
    (let [rc (.trexsql_appender_close (engine) app)]
      (when (not= rc 0)
        (check-error! "appender_close")))))
