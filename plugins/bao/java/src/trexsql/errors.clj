(ns trexsql.errors
  "Custom exception hierarchy for trexsql.
   Provides typed errors with context for better error handling.")

(def error-types
  "Set of valid error type keywords."
  #{:validation-error
    :connection-error
    :sql-error
    :extension-error
    :config-error
    :resource-error})

(defn validation-error
  "Create a validation error with context.
   Use for input validation failures."
  ([msg]
   (ex-info msg {:type :validation-error}))
  ([msg context]
   (ex-info msg {:type :validation-error :context context})))

(defn connection-error
  "Create a connection error with optional cause.
   Use for database connection failures."
  ([msg]
   (ex-info msg {:type :connection-error}))
  ([msg cause]
   (ex-info msg {:type :connection-error} cause))
  ([msg details cause]
   (ex-info msg {:type :connection-error :details details} cause)))

(defn sql-error
  "Create a SQL error with query context.
   Use for SQL execution failures."
  ([msg]
   (ex-info msg {:type :sql-error}))
  ([msg sql]
   (ex-info msg {:type :sql-error :sql sql}))
  ([msg sql cause]
   (ex-info msg {:type :sql-error :sql sql} cause)))

(defn extension-error
  "Create an extension error with extension name.
   Use for TrexSQL extension loading failures."
  ([msg]
   (ex-info msg {:type :extension-error}))
  ([msg ext-name]
   (ex-info msg {:type :extension-error :extension ext-name}))
  ([msg ext-name cause]
   (ex-info msg {:type :extension-error :extension ext-name} cause)))

(defn config-error
  "Create a configuration error with field context.
   Use for configuration validation failures."
  ([msg]
   (ex-info msg {:type :config-error}))
  ([msg field]
   (ex-info msg {:type :config-error :field field}))
  ([msg field expected]
   (ex-info msg {:type :config-error :field field :expected expected})))

(defn resource-error
  "Create a resource error for lifecycle issues.
   Use for connection closed, resource unavailable, etc."
  ([msg]
   (ex-info msg {:type :resource-error}))
  ([msg resource]
   (ex-info msg {:type :resource-error :resource resource})))

(defn error-type
  "Get the error type from an exception.
   Returns the :type from ex-data, or :unknown for non-ex-info exceptions."
  [e]
  (if (instance? clojure.lang.ExceptionInfo e)
    (get (ex-data e) :type :unknown)
    :unknown))

(defn validation-error?
  "Check if exception is a validation error."
  [e]
  (= :validation-error (error-type e)))

(defn connection-error?
  "Check if exception is a connection error."
  [e]
  (= :connection-error (error-type e)))

(defn sql-error?
  "Check if exception is a SQL error."
  [e]
  (= :sql-error (error-type e)))

(defn extension-error?
  "Check if exception is an extension error."
  [e]
  (= :extension-error (error-type e)))

(defn config-error?
  "Check if exception is a configuration error."
  [e]
  (= :config-error (error-type e)))

(defn resource-error?
  "Check if exception is a resource error."
  [e]
  (= :resource-error (error-type e)))

(defn error-context
  "Extract context map from an exception."
  [e]
  (when (instance? clojure.lang.ExceptionInfo e)
    (ex-data e)))

(defn error-sql
  "Extract SQL query from a SQL error."
  [e]
  (get (error-context e) :sql))

(defn error-extension
  "Extract extension name from an extension error."
  [e]
  (get (error-context e) :extension))

(defn error-field
  "Extract field name from a config/validation error."
  [e]
  (or (get (error-context e) :field)
      (get (error-context e) :context)))

(defn format-error
  "Format an error for display/logging.
   Returns a map with :type, :message, and :details."
  [e]
  (let [ctx (error-context e)]
    {:type (or (:type ctx) :unknown)
     :message (ex-message e)
     :details (dissoc ctx :type)
     :cause (when-let [c (ex-cause e)]
              (ex-message c))}))

(defn error->map
  "Convert an error to a serializable map.
   Suitable for JSON responses."
  [e]
  (let [formatted (format-error e)]
    {:error true
     :error_type (name (:type formatted))
     :message (:message formatted)
     :details (:details formatted)}))

(defmacro throw-validation
  "Throw a validation error with message."
  [msg & {:keys [context]}]
  `(throw (validation-error ~msg ~context)))

(defmacro throw-sql
  "Throw a SQL error with message and optional SQL."
  [msg & {:keys [sql cause]}]
  `(throw (sql-error ~msg ~sql ~cause)))

(defmacro throw-if-invalid
  "Throw validation error if condition is true."
  [condition msg & {:keys [context]}]
  `(when ~condition
     (throw (validation-error ~msg ~context))))

(defmacro when-valid
  "Execute body only if validation-fn returns nil, otherwise throw."
  [validation-fn & body]
  `(if-let [error# ~validation-fn]
     (throw (validation-error error#))
     (do ~@body)))
