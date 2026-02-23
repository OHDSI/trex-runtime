(ns trexsql.servers
  "Server startup for Trexas and PgWire via TrexSQL extension SQL functions."
  (:require [trexsql.db :as db]
            [trexsql.config :as config]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io File]
           [java.sql SQLException]))

(defn validate-sql-password!
  "Validate that TREX_SQL_PASSWORD is set. Exit with error if not."
  []
  (let [password (config/get-sql-password)]
    (when (or (nil? password) (empty? password))
      (binding [*out* *err*]
        (println "Error: TREX_SQL_PASSWORD environment variable is not set"))
      (System/exit 1))
    password))

(defn validate-tls-files!
  "Validate that TLS cert and key files exist and are readable.
   Prints error and exits if validation fails."
  [{:keys [tls-cert tls-key]}]
  (when tls-cert
    (let [cert-file (io/file tls-cert)]
      (when-not (.exists cert-file)
        (binding [*out* *err*]
          (println (str "Error: TLS certificate file not found: " tls-cert)))
        (System/exit 1))
      (when-not (.canRead cert-file)
        (binding [*out* *err*]
          (println (str "Error: TLS certificate file not readable: " tls-cert)))
        (System/exit 1))))
  (when tls-key
    (let [key-file (io/file tls-key)]
      (when-not (.exists key-file)
        (binding [*out* *err*]
          (println (str "Error: TLS key file not found: " tls-key)))
        (System/exit 1))
      (when-not (.canRead key-file)
        (binding [*out* *err*]
          (println (str "Error: TLS key file not readable: " tls-key)))
        (System/exit 1)))))

(defn build-trexas-config
  "Build JSON config string for trex_start_server_with_config SQL function."
  [{:keys [trexas-host trexas-port main-path event-worker-path
           tls-cert tls-key tls-port
           enable-inspector inspector-type inspector-host inspector-port
           allow-main-inspector worker-policy max-parallelism]}]
  (let [config (cond-> {:host trexas-host
                        :port trexas-port
                        :main_service_path main-path}
                 enable-inspector
                 (assoc :inspector (str inspector-type ":" inspector-host ":" inspector-port))

                 allow-main-inspector
                 (assoc :allow_main_inspector true)

                 tls-cert
                 (assoc :tls_cert_path tls-cert)

                 tls-key
                 (assoc :tls_key_path tls-key)

                 (and tls-cert tls-key)
                 (assoc :tls_port tls-port)

                 event-worker-path
                 (assoc :event_worker_path event-worker-path)

                 worker-policy
                 (assoc :user_worker_policy worker-policy)

                 max-parallelism
                 (assoc :max_parallelism max-parallelism))]
    (str "{"
         (->> config
              (map (fn [[k v]]
                     (str "\"" (name k) "\":"
                          (cond
                            (string? v) (str "\"" v "\"")
                            (number? v) v
                            (boolean? v) (if v "true" "false")
                            :else (str "\"" v "\"")))))
              (str/join ","))
         "}")))

(defn- check-port-in-use-error
  "Check if an error message indicates port is already in use."
  [^String msg]
  (when msg
    (or (re-find #"(?i)address already in use" msg)
        (re-find #"(?i)port.*in use" msg)
        (re-find #"(?i)bind.*failed" msg))))

(defn start-pgwire-server
  "Start PgWire server via SQL function.
   Returns result string from extension."
  [database {:keys [pgwire-host pgwire-port]} password]
  (try
    (let [sql (format "SELECT start_pgwire_server('%s', %d, '%s', '') as result"
                      pgwire-host pgwire-port password)
          results (db/query database sql)]
      (if (seq results)
        (let [result (get (first results) "result" "")]
          (when (or (re-find #"(?i)error" result)
                    (re-find #"(?i)failed" result))
            (throw (RuntimeException. (str "Failed to start pgwire server: " result))))
          result)
        "Started"))
    (catch clojure.lang.ExceptionInfo e
      (let [msg (ex-message e)]
        (if (check-port-in-use-error msg)
          (throw (RuntimeException. (str "Error: Failed to start pgwire server: Address already in use (port " pgwire-port ")")))
          (throw (RuntimeException. (str "Error: Failed to start pgwire server on " pgwire-host ":" pgwire-port))))))
    (catch RuntimeException e
      (let [msg (.getMessage e)]
        (if (check-port-in-use-error msg)
          (throw (RuntimeException. (str "Error: Failed to start pgwire server: Address already in use (port " pgwire-port ")")))
          (throw e))))))

(defn start-trexas-server
  "Start Trexas server via SQL function with config.
   Returns result string from extension."
  [database config]
  (try
    (let [config-json (build-trexas-config config)
          sql (format "SELECT trex_start_server_with_config('%s') as result" config-json)
          results (db/query database sql)]
      (if (seq results)
        (let [result (get (first results) "result" "")]
          (when (or (re-find #"(?i)error" result)
                    (re-find #"(?i)failed" result))
            (throw (RuntimeException. (str "Failed to start trexas server: " result))))
          result)
        "Started"))
    (catch RuntimeException e
      (let [msg (.getMessage e)]
        (if (check-port-in-use-error msg)
          (throw (RuntimeException. (str "Error: Failed to start trexas server: Address already in use (port " (:trexas-port config) ")")))
          (throw e))))))

(defn start-servers!
  "Start both PgWire and Trexas servers.
   Validates config and password before starting.
   Returns updated database with :servers-running? true."
  [database config]
  (when-let [err (config/validate-tls-config config)]
    (binding [*out* *err*]
      (println (str "Error: " err)))
    (System/exit 1))
  (validate-tls-files! config)
  (let [password (validate-sql-password!)]
    (println "\n\uD83D\uDE80 Starting servers...")
    (let [pgwire-result (start-pgwire-server database config password)]
      (println (str "PgWire server: " pgwire-result)))
    (let [trexas-result (start-trexas-server database config)]
      (println (str "Trexas server: " trexas-result)))
    (assoc database :servers-running? true)))

(defn print-server-status
  "Print server listening status."
  [{:keys [trexas-host trexas-port pgwire-host pgwire-port
           tls-cert tls-port enable-inspector inspector-type
           inspector-host inspector-port event-worker-path
           worker-policy max-parallelism]}]
  (println)
  (println "\u2705 Servers started successfully")
  (println (str "Trexas listening on "
                (if tls-cert "https://" "http://")
                trexas-host ":" trexas-port
                (when enable-inspector
                  (str " (inspector: " inspector-type ":" inspector-host ":" inspector-port ")"))
                (if event-worker-path
                  " (with event worker)"
                  " (without event worker)")))
  (when (or worker-policy max-parallelism)
    (println (str "  Worker config: "
                  (when worker-policy (str "policy=" worker-policy))
                  (when (and worker-policy max-parallelism) ", ")
                  (when max-parallelism (str "max-parallelism=" max-parallelism)))))
  (println (str "PgWire listening on " pgwire-host ":" pgwire-port))
  (println)
  (println "Press Ctrl+C to stop"))
