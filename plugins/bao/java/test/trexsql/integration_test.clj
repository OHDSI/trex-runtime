(ns trexsql.integration-test
  "Integration tests for datamart creation using in-memory TrexSQL.
   These tests verify the full datamart creation flow without external databases."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [trexsql.core :as core]
            [trexsql.db :as db]
            [trexsql.datamart :as datamart]
            [trexsql.vocab :as vocab]
            [trexsql.circe :as circe]
            [trexsql.api :as api]
            [trexsql.errors :as errors])
  (:import [java.io File]
           [java.util HashMap ArrayList]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def ^:dynamic *test-db* nil)
(def ^:dynamic *test-cache-path* nil)

(defn with-test-db [f]
  (let [;; Create a temp directory for cache files
        temp-dir (File/createTempFile "trexsql-test" "")
        cache-path (.getAbsolutePath (File. (.getParent temp-dir) "cache"))]
    ;; Delete the temp file and create directory
    (.delete temp-dir)
    (.mkdirs (File. cache-path))

    ;; Initialize database without extensions (for faster tests)
    (let [conn (db/create-connection)
          test-db (db/make-database conn {})]
      (binding [*test-db* test-db
                *test-cache-path* cache-path]
        (try
          (f)
          (finally
            (db/close! test-db)
            ;; Cleanup cache files
            (doseq [f (.listFiles (File. cache-path))]
              (.delete f))
            (.delete (File. cache-path))))))))

(use-fixtures :each with-test-db)

;; =============================================================================
;; Database Attachment Tests
;; =============================================================================

(deftest attach-cache-file-test
  (testing "can attach a cache file"
    (let [alias (db/attach-cache-file! *test-db* "test_cache" *test-cache-path*)]
      (is (= "test_cache" alias))
      (is (db/is-attached? *test-db* "test_cache"))

      ;; Verify cache file was created
      (is (.exists (File. *test-cache-path* "test_cache.db"))))))

(deftest attach-cache-file-idempotent-test
  (testing "attaching same file twice is idempotent"
    (db/attach-cache-file! *test-db* "test_cache" *test-cache-path*)
    (db/attach-cache-file! *test-db* "test_cache" *test-cache-path*)
    (is (db/is-attached? *test-db* "test_cache"))))

(deftest is-attached-false-test
  (testing "is-attached? returns false for non-existent database"
    (is (not (db/is-attached? *test-db* "nonexistent_db")))))

(deftest detach-database-test
  (testing "can detach an attached database"
    (db/attach-cache-file! *test-db* "test_cache" *test-cache-path*)
    (is (db/is-attached? *test-db* "test_cache"))
    (db/detach-database! *test-db* "test_cache")
    (is (not (db/is-attached? *test-db* "test_cache")))))

(deftest detach-nonexistent-throws-test
  (testing "detaching non-existent database throws resource-error"
    (is (thrown? clojure.lang.ExceptionInfo
                 (db/detach-database! *test-db* "nonexistent_db")))
    ;; Verify it's the right error type
    (try
      (db/detach-database! *test-db* "nonexistent_db")
      (catch clojure.lang.ExceptionInfo e
        (is (= :resource-error (errors/error-type e)))))))

;; =============================================================================
;; In-Memory Source Database Tests
;; =============================================================================

(defn create-test-source-db!
  "Create a test source database with sample tables in the main TrexSQL connection."
  [db]
  ;; Create a test schema and tables
  (db/execute! db "CREATE SCHEMA IF NOT EXISTS test_schema")
  (db/execute! db "CREATE TABLE IF NOT EXISTS test_schema.person (person_id INTEGER, year_of_birth INTEGER, gender_concept_id INTEGER)")
  (db/execute! db "INSERT INTO test_schema.person VALUES (1, 1980, 8507), (2, 1990, 8532), (3, 1975, 8507)")
  (db/execute! db "CREATE TABLE IF NOT EXISTS test_schema.concept (concept_id INTEGER, concept_name VARCHAR, vocabulary_id VARCHAR)")
  (db/execute! db "INSERT INTO test_schema.concept VALUES (8507, 'Male', 'Gender'), (8532, 'Female', 'Gender')")
  (db/execute! db "CREATE TABLE IF NOT EXISTS test_schema.observation (observation_id INTEGER, person_id INTEGER, observation_date DATE)")
  (db/execute! db "INSERT INTO test_schema.observation VALUES (1, 1, '2024-01-15'), (2, 2, '2024-02-20'), (3, 1, '2023-06-01')"))

;; =============================================================================
;; Filter Function Integration Tests
;; =============================================================================

(deftest build-select-clause-integration-test
  (testing "select clause generates valid SQL"
    (create-test-source-db! *test-db*)
    (let [select-star (datamart/build-select-clause nil)
          select-cols (datamart/build-select-clause ["person_id" "year_of_birth"])]
      ;; Test with star
      (let [results (db/query *test-db* (str "SELECT " select-star " FROM test_schema.person"))]
        (is (= 3 (count results))))
      ;; Test with specific columns
      (let [results (db/query *test-db* (str "SELECT " select-cols " FROM test_schema.person"))]
        (is (= 3 (count results)))))))

;; =============================================================================
;; Copy Table Tests (using main memory database as source)
;; =============================================================================

(deftest copy-table-basic-test
  (testing "can copy a table from source to cache"
    (create-test-source-db! *test-db*)
    (db/attach-cache-file! *test-db* "target_db" *test-cache-path*)

    ;; Create schema in target
    (db/execute! *test-db* "CREATE SCHEMA IF NOT EXISTS target_db.cdm")

    ;; Copy table using direct SQL (simulating copy-table)
    (db/execute! *test-db*
                 "CREATE OR REPLACE TABLE target_db.cdm.person AS SELECT * FROM test_schema.person")

    ;; Verify data
    (let [results (db/query *test-db* "SELECT COUNT(*) as cnt FROM target_db.cdm.person")]
      (is (= 3 (-> results first (.get "cnt")))))))

(deftest copy-table-with-columns-test
  (testing "can copy specific columns"
    (create-test-source-db! *test-db*)
    (db/attach-cache-file! *test-db* "target_db" *test-cache-path*)
    (db/execute! *test-db* "CREATE SCHEMA IF NOT EXISTS target_db.cdm")

    ;; Copy only specific columns
    (db/execute! *test-db*
                 "CREATE OR REPLACE TABLE target_db.cdm.person AS SELECT person_id, year_of_birth FROM test_schema.person")

    ;; Verify structure
    (let [columns (db/query *test-db*
                            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'cdm' AND table_name = 'person' AND table_catalog = 'target_db'")]
      (is (= 2 (count columns))))))

(deftest copy-table-with-patient-filter-test
  (testing "can copy with patient filter"
    (create-test-source-db! *test-db*)
    (db/attach-cache-file! *test-db* "target_db" *test-cache-path*)
    (db/execute! *test-db* "CREATE SCHEMA IF NOT EXISTS target_db.cdm")

    ;; Copy with patient filter
    (db/execute! *test-db*
                 "CREATE OR REPLACE TABLE target_db.cdm.observation AS SELECT * FROM test_schema.observation WHERE person_id IN (1)")

    ;; Verify only person 1's observations
    (let [results (db/query *test-db* "SELECT COUNT(*) as cnt FROM target_db.cdm.observation")]
      (is (= 2 (-> results first (.get "cnt")))))))

;; =============================================================================
;; Java API Integration Tests
;; =============================================================================

(deftest java-api-is-attached-test
  (testing "Java API isAttached works"
    (is (not (datamart/is-attached? *test-db* "nonexistent")))
    (db/attach-cache-file! *test-db* "java_test" *test-cache-path*)
    (is (datamart/is-attached? *test-db* "java_test"))))

(deftest java-api-detach-test
  (testing "Java API detach works"
    (db/attach-cache-file! *test-db* "detach_test" *test-cache-path*)
    (is (datamart/is-attached? *test-db* "detach_test"))
    (datamart/detach-database! *test-db* "detach_test")
    (is (not (datamart/is-attached? *test-db* "detach_test")))))

;; =============================================================================
;; Record and Conversion Tests
;; =============================================================================

(deftest table-result-creation-test
  (testing "can create and convert TableResult"
    (let [tr (datamart/->TableResult "person" 1000 2)
          java-map (datamart/table-result->java-map tr)]
      (is (= "person" (.get java-map "table-name")))
      (is (= 1000 (.get java-map "rows-copied")))
      (is (= 2 (.get java-map "indexes-created"))))))

(deftest table-error-creation-test
  (testing "can create and convert TableError"
    (let [te (datamart/->TableError "observation" "Connection lost" "copy")
          java-map (datamart/table-error->java-map te)]
      (is (= "observation" (.get java-map "table-name")))
      (is (= "Connection lost" (.get java-map "error")))
      (is (= "copy" (.get java-map "phase"))))))

(deftest cache-result-creation-test
  (testing "can create complete CacheResult"
    (let [copied [(datamart/->TableResult "person" 1000 0)]
          failed [(datamart/->TableError "concept" "Error" "copy")]
          result (datamart/->CacheResult
                  false "testdb" "cdm"
                  copied failed ["person"] 5000 nil)
          java-map (datamart/result->java-map result)]
      (is (= false (.get java-map "success")))
      (is (= "testdb" (.get java-map "database-code")))
      (is (= 1 (.size (.get java-map "tables-copied"))))
      (is (= 1 (.size (.get java-map "tables-failed")))))))

;; =============================================================================
;; Vocabulary Search API Integration Tests
;; =============================================================================

(deftest vocab-api-search-validation-test
  (testing "searchVocab throws on empty search term"
    (is (thrown? clojure.lang.ExceptionInfo
                 (api/-searchVocab *test-db* "" (HashMap.))))
    (try
      (api/-searchVocab *test-db* "" (HashMap.))
      (catch clojure.lang.ExceptionInfo e
        (is (= :validation-error (errors/error-type e))))))

  (testing "searchVocab throws on missing database-code"
    (is (thrown? clojure.lang.ExceptionInfo
                 (api/-searchVocab *test-db* "diabetes" (HashMap.)))))

  (testing "searchVocab throws on invalid database-code"
    (let [opts (doto (HashMap.) (.put "database-code" "bad/code"))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (api/-searchVocab *test-db* "diabetes" opts))))))

(deftest vocab-search-with-mock-data-test
  (testing "searchVocab works with attached concept table"
    ;; Create a test cache with concept table
    (db/attach-cache-file! *test-db* "vocab_test" *test-cache-path*)
    (db/execute! *test-db* "CREATE SCHEMA IF NOT EXISTS vocab_test.vocab_test")
    (db/execute! *test-db* "CREATE TABLE vocab_test.vocab_test.concept (
                             concept_id INTEGER,
                             concept_name VARCHAR,
                             domain_id VARCHAR,
                             vocabulary_id VARCHAR,
                             concept_class_id VARCHAR,
                             standard_concept VARCHAR,
                             concept_code VARCHAR)")
    (db/execute! *test-db* "INSERT INTO vocab_test.vocab_test.concept VALUES
                             (201826, 'Type 2 diabetes mellitus', 'Condition', 'SNOMED', 'Clinical Finding', 'S', '44054006'),
                             (201254, 'Type 1 diabetes mellitus', 'Condition', 'SNOMED', 'Clinical Finding', 'S', '46635009')")

    ;; Search for diabetes
    (let [opts (doto (HashMap.)
                 (.put "database-code" "vocab_test")
                 (.put "max-rows" 10))
          results (api/-searchVocab *test-db* "diabetes" opts)]
      (is (instance? ArrayList results))
      (is (= 2 (.size results)))
      (let [first-result (.get results 0)]
        (is (some? (.get first-result "concept_id")))
        (is (some? (.get first-result "concept_name")))))))

;; =============================================================================
;; Circe API Integration Tests
;; =============================================================================

(deftest circe-api-validation-test
  (testing "executeCirce returns error for empty JSON"
    (let [opts (doto (HashMap.)
                 (.put "cdm-schema" "cdm")
                 (.put "result-schema" "results")
                 (.put "cohort-id" 123))
          result (api/-executeCirce *test-db* "" opts)]
      (is (instance? HashMap result))
      (is (false? (.get result "success")))
      (is (some? (.get result "error")))))

  (testing "executeCirce returns error for missing options"
    (let [result (api/-executeCirce *test-db* "{\"test\": 1}" (HashMap.))]
      (is (false? (.get result "success")))
      (is (re-find #"cdm-schema" (.get result "error"))))))

(deftest circe-render-api-validation-test
  (testing "renderCirceToSql throws on empty JSON"
    (let [opts (doto (HashMap.)
                 (.put "cdm-schema" "cdm")
                 (.put "result-schema" "results")
                 (.put "cohort-id" 123))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (api/-renderCirceToSql *test-db* "" opts)))))

  (testing "renderCirceToSql throws on missing options"
    (is (thrown? clojure.lang.ExceptionInfo
                 (api/-renderCirceToSql *test-db* "{\"test\": 1}" (HashMap.))))))

;; =============================================================================
;; HTTP Request API Integration Tests
;; =============================================================================

(deftest http-api-validation-test
  (testing "httpRequest throws on nil method"
    (is (thrown? clojure.lang.ExceptionInfo
                 (api/-httpRequest *test-db* nil "/test" nil nil nil))))

  (testing "httpRequest throws on nil url"
    (is (thrown? clojure.lang.ExceptionInfo
                 (api/-httpRequest *test-db* "GET" nil nil nil nil))))

  (testing "httpRequest throws on invalid method"
    (is (thrown? clojure.lang.ExceptionInfo
                 (api/-httpRequest *test-db* "INVALID" "/test" nil nil nil))))

  (testing "httpRequest throws on invalid URL format"
    (is (thrown? clojure.lang.ExceptionInfo
                 (api/-httpRequest *test-db* "GET" "not-a-valid-url" nil nil nil)))))

(deftest http-api-options-validation-test
  (testing "httpRequest throws on invalid timeout"
    (let [opts (doto (HashMap.)
                 (.put "timeout-ms" -1))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (api/-httpRequest *test-db* "GET" "/test" nil nil opts)))))

  (testing "httpRequest throws on timeout exceeding max"
    (let [opts (doto (HashMap.)
                 (.put "timeout-ms" 700000))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (api/-httpRequest *test-db* "GET" "/test" nil nil opts)))))

  (testing "httpRequest throws on invalid follow-redirects"
    (let [opts (doto (HashMap.)
                 (.put "follow-redirects" "not-a-boolean"))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (api/-httpRequest *test-db* "GET" "/test" nil nil opts)))))

  (testing "httpRequest throws on invalid max-redirects"
    (let [opts (doto (HashMap.)
                 (.put "max-redirects" -5))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (api/-httpRequest *test-db* "GET" "/test" nil nil opts))))))

(deftest http-api-method-case-insensitive-test
  (testing "httpRequest accepts lowercase methods"
    ;; These should not throw validation errors
    ;; (actual execution may fail due to no trex server, but validation should pass)
    (try
      (api/-httpRequest *test-db* "get" "/test" nil nil nil)
      (catch clojure.lang.ExceptionInfo e
        ;; Check if it's a validation error - that would be a test failure
        (if (= :validation-error (errors/error-type e))
          (is false "Should not throw validation-error for lowercase 'get'")
          ;; Other typed errors (like SQL errors) are expected when extension not loaded
          (is true "Validation passed, execution failed as expected")))
      (catch Exception e
        ;; Other exceptions are expected when extension not loaded
        (is true "Validation passed, execution failed as expected")))))

(deftest http-api-headers-and-body-test
  (testing "httpRequest accepts headers map"
    (let [headers (doto (HashMap.)
                    (.put "Content-Type" "application/json")
                    (.put "Authorization" "Bearer token"))]
      ;; Should not throw validation error for valid headers
      (try
        (api/-httpRequest *test-db* "POST" "/api/test" headers "{\"key\":\"value\"}" nil)
        (catch clojure.lang.ExceptionInfo e
          (if (= :validation-error (errors/error-type e))
            (is false "Should not throw validation-error for valid headers")
            (is true "Validation passed")))
        (catch Exception e
          (is true "Validation passed")))))

  (testing "httpRequest accepts null headers and body"
    (try
      (api/-httpRequest *test-db* "GET" "/test" nil nil nil)
      (catch clojure.lang.ExceptionInfo e
        (if (= :validation-error (errors/error-type e))
          (is false "Should not throw validation-error for null headers/body")
          (is true "Validation passed")))
      (catch Exception e
        (is true "Validation passed")))))

(deftest http-api-options-map-test
  (testing "httpRequest accepts valid options"
    (let [opts (doto (HashMap.)
                 (.put "timeout-ms" 5000)
                 (.put "follow-redirects" false)
                 (.put "max-redirects" 3))]
      (try
        (api/-httpRequest *test-db* "GET" "/test" nil nil opts)
        (catch clojure.lang.ExceptionInfo e
          (if (= :validation-error (errors/error-type e))
            (is false "Should not throw validation-error for valid options")
            (is true "Validation passed")))
        (catch Exception e
          (is true "Validation passed"))))))
