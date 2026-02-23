(ns trexsql.core
  "Main entry point for Trexsql - Clojure TrexSQL library."
  (:require [trexsql.db :as db]
            [trexsql.config :as config]
            [trexsql.extensions :as ext]
            [trexsql.servers :as servers]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log])
  (:gen-class))

(def ^:private shutdown-promise (promise))
(defonce current-database (atom nil))

(defn- camel->kebab
  "Convert camelCase string to kebab-case keyword.
   e.g. 'pgwirePort' -> :pgwire-port"
  [s]
  (-> s
      (clojure.string/replace #"([a-z])([A-Z])" "$1-$2")
      clojure.string/lower-case
      keyword))

(defn- parse-trex-init []
  (when-let [init-json (System/getenv "TREX_INIT")]
    (try
      (json/read-str init-json :key-fn camel->kebab)
      (catch Exception e
        (log/warn e "Failed to parse TREX_INIT JSON")
        nil))))

(defn add-shutdown-hook!
  [cleanup-fn]
  (.addShutdownHook
   (Runtime/getRuntime)
   (Thread. ^Runnable cleanup-fn)))

(defn shutdown!
  "Gracefully shutdown the database and any running servers."
  [database]
  (println "\n\nShutting down...")
  (when database
    (db/close! database))
  (reset! current-database nil))

(defn- try-start-servers [database config]
  (let [password (config/get-sql-password)]
    (when-not password
      (log/info "TREX_SQL_PASSWORD not set, skipping server startup"))
    (when password
      (try
        (log/info "Starting PgWire server...")
        (servers/start-pgwire-server database config password)
        (log/info "Starting Trexas server...")
        (servers/start-trexas-server database config)
        (reset! (:servers-running? database) true)
        (log/info "Servers started successfully")
        (catch Exception e
          (log/warn e "Failed to start servers"))))))

(defn init
  ([]
   (init {}))
  ([config]
   (let [env-config (parse-trex-init)
         merged-config (merge config/default-config (or env-config {}) config)
         extensions-path (config/get-extensions-path merged-config)
         handle (db/create-connection merged-config)
         loaded (ext/load-extensions handle extensions-path)
         database (db/make-database handle merged-config)]
     (reset! (:extensions-loaded database) loaded)
     (reset! current-database database)
     (when env-config
       (try-start-servers database merged-config))
     database)))

(defn get-database []
  (or @current-database (init)))

(defn init-with-servers
  ([]
   (init-with-servers {}))
  ([config]
   (let [database (init config)
         merged-config (merge config/default-config config)]
     (if @(:servers-running? database)
       (do
         (servers/print-server-status merged-config)
         database)
       (let [database-with-servers (servers/start-servers! database merged-config)]
         (reset! current-database database-with-servers)
         (servers/print-server-status merged-config)
         database-with-servers)))))

(defn is-running?
  "Check if servers are currently running."
  [database]
  (boolean (:servers-running? database)))

(defn query
  "Execute a SQL query and return results.
   Wrapper around db/query for convenience."
  [database sql]
  (db/query database sql))

(defn execute!
  "Execute a non-query SQL statement.
   Wrapper around db/execute! for convenience."
  [database sql]
  (db/execute! database sql))

(defn loaded-extensions
  "Return set of loaded extension names."
  [database]
  (ext/loaded-extensions database))

(def main-help-text
  "Usage: trexsql <command> [options]

Commands:
  serve      Start Trexas and PgWire servers (default)
  cache      Manage TrexSQL caches from source databases
  bundle     Create an eszip bundle from TypeScript/JavaScript

Use 'trexsql <command> --help' for more information about a command.

Examples:
  trexsql serve --trexas-port 9876
  trexsql cache create -s source -j \"jdbc:...\" -S schema
  trexsql bundle -e main.ts -o output.eszip")

(defn- print-main-help []
  (println main-help-text))

(defn- run-serve [args]
  (let [{:keys [options errors]} (config/parse-args args)]
    (when (:help options)
      (println config/help-text)
      (System/exit 0))
    (when (seq errors)
      (binding [*out* *err*]
        (doseq [err errors]
          (println (str "Error: " err))))
      (System/exit 1))
    (println "\uD83E\uDD95 Starting TREX")
    (let [database (init-with-servers options)]
      (add-shutdown-hook!
       #(shutdown! @current-database))
      @shutdown-promise)))

(defn -main
  "Main entry point - routes to subcommands."
  [& args]
  (let [command (first args)
        sub-args (rest args)]
    (case command
      "serve" (run-serve sub-args)
      "cache" (do
                (require 'trexsql.cli)
                (let [run-cache (resolve 'trexsql.cli/run-cache)
                      {:keys [exit-code]} (run-cache sub-args)]
                  (System/exit (or exit-code 0))))
      "bundle" (do
                 (require 'trexsql.cli)
                 (let [run-bundle (resolve 'trexsql.cli/run-bundle)
                       {:keys [exit-code]} (run-bundle sub-args)]
                   (System/exit (or exit-code 0))))
      "--help" (do (print-main-help) (System/exit 0))
      "-h" (do (print-main-help) (System/exit 0))
      nil (run-serve [])
      (if (or (clojure.string/starts-with? (str command) "-")
              (clojure.string/starts-with? (str command) "--"))
        (run-serve args)
        (do
          (println (format "Unknown command: %s" command))
          (println "\nUse 'trexsql --help' for usage information.")
          (System/exit 1))))))
