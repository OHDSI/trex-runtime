(ns trexsql.extensions
  "TrexSQL extension discovery and loading."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [trexsql.native :as native])
  (:import [java.io File InputStream FileOutputStream]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [com.sun.jna Pointer]))

(def ^:private embedded-extensions-resource-path "extensions/")
(def ^:private embedded-extensions-temp-dir (atom nil))

(defn- get-embedded-extensions-dir
  "Get or create a temp directory for extracted embedded extensions."
  []
  (when-not @embedded-extensions-temp-dir
    (let [temp-dir (Files/createTempDirectory "trexsql-extensions"
                                               (make-array FileAttribute 0))]
      (reset! embedded-extensions-temp-dir (.toFile temp-dir))
      ;; Register shutdown hook to clean up
      (.addShutdownHook (Runtime/getRuntime)
                        (Thread. #(when-let [dir @embedded-extensions-temp-dir]
                                    (doseq [f (.listFiles dir)]
                                      (.delete f))
                                    (.delete dir))))))
  @embedded-extensions-temp-dir)

(defn- extract-embedded-extension
  "Extract an embedded extension from JAR resources to temp directory.
   Returns the File path or nil if not found."
  [ext-name]
  (let [resource-path (str embedded-extensions-resource-path ext-name ".trex")
        resource (io/resource resource-path)]
    (when resource
      (let [temp-dir (get-embedded-extensions-dir)
            ext-file (File. temp-dir (str ext-name ".trex"))]
        (when-not (.exists ext-file)
          (println (str "Extracting embedded extension: " ext-name))
          (with-open [in (io/input-stream resource)
                      out (FileOutputStream. ext-file)]
            (io/copy in out)))
        ext-file))))

(defn find-embedded-extensions
  "Find all embedded extensions in JAR resources.
   Returns seq of extension names (without .trex suffix)."
  []
  ;; This is tricky - we can't easily list resources in a JAR
  ;; So we check for known extensions
  (let [known-extensions ["circe" "tpm" "pgwire" "llama"]]
    (filter #(io/resource (str embedded-extensions-resource-path % ".trex"))
            known-extensions)))

(defn has-avx-support?
  "Check if the CPU supports AVX instructions.
   Reads /proc/cpuinfo on Linux."
  []
  (try
    (let [cpuinfo (slurp "/proc/cpuinfo")]
      (boolean (re-find #"\bavx\b" cpuinfo)))
    (catch Exception _
      false)))

(defn- extension-file?
  [^File f]
  (and (.isFile f)
       (or (str/ends-with? (.getName f) ".duckdb_extension")
           (str/ends-with? (.getName f) ".trex"))))

(defn- extension-name
  [^File f]
  (str/replace (.getName f) #"\.(duckdb_extension|trex)$" ""))

(defn find-extensions
  "Find all TrexSQL extension files in the given directory.
   Searches recursively in @trex subdirectories.
   Returns seq of {:name <string> :path <string> :requires-avx <boolean>}"
  [extensions-path]
  (let [base-dir (io/file extensions-path)]
    (when (.isDirectory base-dir)
      (for [subdir (.listFiles base-dir)
            :when (.isDirectory subdir)
            ext-file (.listFiles subdir)
            :when (extension-file? ext-file)
            :let [name (extension-name ext-file)]]
        {:name name
         :path (.getAbsolutePath ext-file)
         :requires-avx (= name "llama")}))))

(defn load-extension
  "Load a single extension into the TrexSQL connection.
   Returns {:name <string> :loaded <boolean> :error <string or nil>}"
  [^Pointer handle {:keys [name path requires-avx]}]
  (let [avx-available? (has-avx-support?)]
    (cond
      ;; Skip llama if no AVX support
      (and requires-avx (not avx-available?))
      (do
        (println (str "Skipping " name " extension (no AVX support)"))
        {:name name :loaded false :error "No AVX support"})

      :else
      (try
        (println (str "Loading extension: " name))
        (native/execute! handle (str "LOAD '" path "'"))
        {:name name :loaded true :error nil}
        (catch Exception e
          (println (str "Failed to load extension: " path))
          (println (str "  Error: " (.getMessage e)))
          {:name name :loaded false :error (.getMessage e)})))))

(defn load-embedded-extension
  "Load a single embedded extension by name.
   Extracts from JAR resources if available.
   Returns {:name <string> :loaded <boolean> :error <string or nil>}"
  [^Pointer handle ext-name]
  (let [avx-available? (has-avx-support?)
        requires-avx? (= ext-name "llama")]
    (cond
      ;; Skip llama if no AVX support
      (and requires-avx? (not avx-available?))
      (do
        (println (str "Skipping embedded " ext-name " extension (no AVX support)"))
        {:name ext-name :loaded false :error "No AVX support"})

      :else
      (if-let [ext-file (extract-embedded-extension ext-name)]
        (try
          (println (str "Loading embedded extension: " ext-name))
          (native/execute! handle (str "LOAD '" (.getAbsolutePath ext-file) "'"))
          {:name ext-name :loaded true :error nil}
          (catch Exception e
            (println (str "Failed to load embedded extension: " ext-name))
            (println (str "  Error: " (.getMessage e)))
            {:name ext-name :loaded false :error (.getMessage e)}))
        {:name ext-name :loaded false :error "Not embedded in JAR"}))))

(defn load-all-embedded-extensions
  "Load all embedded extensions from JAR resources.
   Returns set of successfully loaded extension names."
  [^Pointer handle]
  (let [embedded (find-embedded-extensions)]
    (if (empty? embedded)
      (do
        (println "No embedded extensions found in JAR")
        #{})
      (let [results (map #(load-embedded-extension handle %) embedded)
            loaded (filter :loaded results)]
        (println (str "Loaded " (count loaded) " embedded extension(s)"))
        (set (map :name loaded))))))

(defn load-extensions
  "Load all extensions from the configured directory and embedded resources.
   Returns set of successfully loaded extension names."
  [^Pointer handle extensions-path]
  ;; First load embedded extensions
  (let [embedded-loaded (load-all-embedded-extensions handle)
        ;; Then load from external directory
        external-extensions (find-extensions extensions-path)
        ;; Filter out extensions already loaded from embedded
        external-to-load (remove #(contains? embedded-loaded (:name %)) external-extensions)]
    (if (empty? external-to-load)
      embedded-loaded
      (let [results (map #(load-extension handle %) external-to-load)
            loaded (filter :loaded results)]
        (into embedded-loaded (map :name loaded))))))

(defn loaded-extensions
  "Return set of loaded extension names from database state."
  [db]
  (:extensions-loaded db))
