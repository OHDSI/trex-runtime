(ns trexsql.config
  "Configuration management for Trexsql - CLI parsing and environment variables."
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]))

(def default-config
  "Default configuration values."
  {:extensions-path (or (System/getenv "TREX_EXTENSIONS_PATH")
                        "node_modules/@trex")
   :trexas-host "0.0.0.0"
   :trexas-port 9876
   :pgwire-host "0.0.0.0"
   :pgwire-port 5433
   :main-path "./main"
   :event-worker-path nil
   :tls-cert nil
   :tls-key nil
   :tls-port 9443
   :enable-inspector false
   :inspector-type "inspect"
   :inspector-host "0.0.0.0"
   :inspector-port 9229
   :allow-main-inspector false
   :worker-policy nil
   :max-parallelism nil})

(def cli-options
  "CLI options matching bao interface."
  [[nil "--trexas-host HOST" "Trexas server host"
    :default (:trexas-host default-config)]
   [nil "--trexas-port PORT" "Trexas server port"
    :default (:trexas-port default-config)
    :parse-fn #(Integer/parseInt %)]
   [nil "--pgwire-host HOST" "PgWire server host"
    :default (:pgwire-host default-config)]
   [nil "--pgwire-port PORT" "PgWire server port"
    :default (:pgwire-port default-config)
    :parse-fn #(Integer/parseInt %)]
   [nil "--main-path PATH" "Path to main service directory"
    :default (:main-path default-config)]
   [nil "--event-worker-path PATH" "Path to event worker directory"
    :default nil]
   [nil "--tls-cert PATH" "Path to TLS certificate file"
    :default nil]
   [nil "--tls-key PATH" "Path to TLS private key file"
    :default nil]
   [nil "--tls-port PORT" "TLS port"
    :default (:tls-port default-config)
    :parse-fn #(Integer/parseInt %)]
   [nil "--enable-inspector" "Enable V8 inspector"
    :default false]
   [nil "--inspector-type TYPE" "Inspector type (inspect, inspect-brk, inspect-wait)"
    :default (:inspector-type default-config)]
   [nil "--inspector-host HOST" "Inspector bind address"
    :default (:inspector-host default-config)]
   [nil "--inspector-port PORT" "Inspector port"
    :default (:inspector-port default-config)
    :parse-fn #(Integer/parseInt %)]
   [nil "--allow-main-inspector" "Allow inspector in main worker"
    :default false]
   [nil "--worker-policy POLICY" "Worker policy (per_worker, per_request, oneshot)"
    :default nil
    :validate [#(contains? #{"per_worker" "per_request" "oneshot"} %)
               "Must be per_worker, per_request, or oneshot"]]
   [nil "--max-parallelism COUNT" "Max workers per service path (1-9999)"
    :default nil
    :parse-fn #(Integer/parseInt %)
    :validate [#(<= 1 % 9999) "Must be between 1 and 9999"]]
   ["-h" "--help" "Show this help message"]])

(defn parse-args
  "Parse command-line arguments using tools.cli.
   Returns {:options {...} :arguments [...] :summary <string> :errors [...]}"
  [args]
  (parse-opts args cli-options))

(defn validate-tls-config
  "Validate TLS configuration - if one of cert/key provided, both must be.
   Returns nil if valid, error message string if invalid."
  [{:keys [tls-cert tls-key]}]
  (cond
    (and tls-cert (not tls-key))
    "TLS certificate provided but TLS key is missing. Both --tls-cert and --tls-key are required together."

    (and tls-key (not tls-cert))
    "TLS key provided but TLS certificate is missing. Both --tls-cert and --tls-key are required together."

    :else nil))

(defn get-extensions-path
  "Get extensions path from config or environment."
  [config]
  (or (:extensions-path config)
      (System/getenv "TREX_EXTENSIONS_PATH")
      "node_modules/@trex"))

(defn get-sql-password
  "Get TREX_SQL_PASSWORD from environment.
   Returns nil if not set."
  []
  (System/getenv "TREX_SQL_PASSWORD"))

(defn merge-cli-config
  "Merge parsed CLI options with defaults."
  [cli-options]
  (merge default-config cli-options))

(def help-text
  "Usage: trexsql [options]

Options:
  --trexas-host HOST        Trexas server host (default: 0.0.0.0)
  --trexas-port PORT        Trexas server port (default: 9876)
  --pgwire-host HOST        PgWire server host (default: 0.0.0.0)
  --pgwire-port PORT        PgWire server port (default: 5433)
  --main-path PATH          Path to main service directory (default: ./main)
  --event-worker-path PATH  Path to event worker directory
  --tls-cert PATH           Path to TLS certificate file
  --tls-key PATH            Path to TLS private key file
  --tls-port PORT           TLS port (default: 9443)
  --enable-inspector        Enable inspector
  --inspector-type TYPE     Inspector type (default: inspect)
  --inspector-host HOST     Inspector host (default: 0.0.0.0)
  --inspector-port PORT     Inspector port (default: 9229)
  --allow-main-inspector    Allow inspector in main worker
  --worker-policy POLICY    Worker policy: per_worker (default), per_request, oneshot
  --max-parallelism COUNT   Max workers per service path (1-9999, default: CPU cores)
  -h, --help                Show this help message")

(defn get-proxy-routes []
  (if-let [routes-json (System/getenv "TREX_ROUTES")]
    (try
      (let [parsed (json/read-str routes-json)]
        (if (map? parsed)
          (do
            (log/info (str "Loaded " (count parsed) " proxy routes from TREX_ROUTES"))
            parsed)
          (do
            (log/warn "TREX_ROUTES must be a JSON object, got:" (type parsed))
            {})))
      (catch Exception e
        (log/warn "Failed to parse TREX_ROUTES:" (.getMessage e))
        {}))
    {}))
