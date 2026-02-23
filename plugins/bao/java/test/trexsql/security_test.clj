(ns trexsql.security-test
  "Security tests for SQL injection prevention and input validation.
   Tests T1.1.11, T1.2.5, T2.2.5, T3.1.6 from improvement plan."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [trexsql.db :as db]
            [trexsql.datamart :as datamart]
            [trexsql.vocab :as vocab]
            [trexsql.circe :as circe]
            [trexsql.errors :as errors]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def ^:dynamic *test-db* nil)

(defn with-test-db [f]
  (let [conn (db/create-connection)
        test-db (db/make-database conn {})]
    (binding [*test-db* test-db]
      (try
        (f)
        (finally
          (db/close! test-db))))))

(use-fixtures :each with-test-db)

;; =============================================================================
;; T1.1.11 - SQL Injection Prevention Tests
;; =============================================================================

(deftest validate-identifier-nil-test
  (testing "nil identifier returns error"
    (is (some? (db/validate-identifier nil)))))

(deftest validate-identifier-empty-test
  (testing "empty identifier returns error"
    (is (some? (db/validate-identifier "")))
    (is (some? (db/validate-identifier "   ")))))

(deftest validate-identifier-valid-test
  (testing "valid identifiers pass validation"
    (is (nil? (db/validate-identifier "my_table")))
    (is (nil? (db/validate-identifier "MyTable123")))
    (is (nil? (db/validate-identifier "_private")))
    (is (nil? (db/validate-identifier "a")))))

(deftest validate-identifier-sql-injection-test
  (testing "SQL injection attempts are rejected"
    ;; Classic SQL injection
    (is (some? (db/validate-identifier "test'; DROP TABLE users;--")))
    (is (some? (db/validate-identifier "test' OR '1'='1")))
    (is (some? (db/validate-identifier "1; DELETE FROM users")))

    ;; Special characters
    (is (some? (db/validate-identifier "table-name")))  ; hyphen not in pattern
    (is (some? (db/validate-identifier "table.name")))
    (is (some? (db/validate-identifier "table name")))
    (is (some? (db/validate-identifier "table\"name")))
    (is (some? (db/validate-identifier "table'name")))
    (is (some? (db/validate-identifier "table;name")))
    (is (some? (db/validate-identifier "table/*comment*/")))

    ;; Unicode injection attempts
    (is (some? (db/validate-identifier "table\u0000name")))
    (is (some? (db/validate-identifier "table\nname")))))

(deftest validate-identifier-length-test
  (testing "identifiers over 128 chars are rejected"
    (let [long-name (apply str (repeat 129 "a"))]
      (is (some? (db/validate-identifier long-name))))
    (let [max-name (apply str (repeat 128 "a"))]
      (is (nil? (db/validate-identifier max-name))))))

(deftest validate-identifier-starting-char-test
  (testing "identifiers must start with letter or underscore"
    (is (some? (db/validate-identifier "1table")))
    (is (some? (db/validate-identifier "123")))
    (is (nil? (db/validate-identifier "_table")))
    (is (nil? (db/validate-identifier "Table")))))

(deftest escape-identifier-test
  (testing "escape-identifier wraps in double quotes"
    (is (= "\"my_table\"" (db/escape-identifier "my_table" "test")))
    (is (= "\"MyTable\"" (db/escape-identifier "MyTable" "test")))))

(deftest escape-identifier-throws-on-invalid-test
  (testing "escape-identifier throws on invalid input"
    (is (thrown? clojure.lang.ExceptionInfo
          (db/escape-identifier "test'; DROP TABLE" "context")))
    (is (thrown? clojure.lang.ExceptionInfo
          (db/escape-identifier nil "context")))
    (is (thrown? clojure.lang.ExceptionInfo
          (db/escape-identifier "" "context")))))

(deftest attach-cache-file-injection-test
  (testing "attach-cache-file! rejects SQL injection in database-code"
    (is (thrown? clojure.lang.ExceptionInfo
          (db/attach-cache-file! *test-db* "test'; DROP TABLE users;--" "./cache")))
    (is (thrown? clojure.lang.ExceptionInfo
          (db/attach-cache-file! *test-db* "test.db" "./cache")))
    (is (thrown? clojure.lang.ExceptionInfo
          (db/attach-cache-file! *test-db* "../../../etc/passwd" "./cache")))))

(deftest datamart-config-validation-test
  (testing "datamart config validates database-code"
    (is (some? (datamart/validate-config
                {:database-code "test'; DROP TABLE"
                 :schema-name "public"
                 :source-credentials {:dialect "postgres"
                                      :host "localhost"
                                      :port 5432
                                      :database-name "test"
                                      :user "user"
                                      :password "pass"}})))))

(deftest vocab-search-options-validation-test
  (testing "vocab search validates database-code"
    (is (some? (vocab/validate-search-options
                {:database-code "test'; DROP TABLE"
                 :schema-name "public"
                 :max-rows 100})))
    (is (some? (vocab/validate-search-options
                {:database-code "valid_db"
                 :schema-name "schema.with.dots"
                 :max-rows 100})))))

(deftest circe-options-validation-test
  (testing "circe options validates schema names"
    (is (some? (circe/validate-circe-options
                {:cdm-schema "test'; DROP TABLE"
                 :result-schema "results"
                 :cohort-id 1})))
    (is (some? (circe/validate-circe-options
                {:cdm-schema "valid"
                 :result-schema nil
                 :cohort-id 1})))
    (is (some? (circe/validate-circe-options
                {:cdm-schema "valid"
                 :result-schema "valid"
                 :cohort-id -1})))))

;; =============================================================================
;; T1.2.5 - Resource Leak / Lifecycle Tests
;; =============================================================================

(deftest ensure-open-throws-after-close-test
  (testing "ensure-open! throws resource-error after close!"
    (let [conn (db/create-connection)
          test-db (db/make-database conn {})]
      ;; Should not throw before close
      (is (nil? (db/ensure-open! test-db)))

      ;; Close the database
      (db/close! test-db)

      ;; Should throw after close
      (is (thrown? clojure.lang.ExceptionInfo
            (db/ensure-open! test-db)))

      ;; Error should be of type :resource-error
      (try
        (db/ensure-open! test-db)
        (catch clojure.lang.ExceptionInfo e
          (is (= :resource-error (errors/error-type e))))))))

(deftest closed-state-is-tracked-test
  (testing "closed? state is properly tracked"
    (let [conn (db/create-connection)
          test-db (db/make-database conn {})]
      ;; Initially not closed
      (is (false? (db/closed? test-db)))

      ;; After close
      (db/close! test-db)
      (is (true? (db/closed? test-db)))

      ;; Double close should not throw
      (db/close! test-db)
      (is (true? (db/closed? test-db))))))

(deftest query-throws-after-close-test
  (testing "query throws after database is closed"
    (let [conn (db/create-connection)
          test-db (db/make-database conn {})]
      (db/close! test-db)
      (is (thrown? clojure.lang.ExceptionInfo
            (db/query test-db "SELECT 1"))))))

(deftest execute-throws-after-close-test
  (testing "execute! throws after database is closed"
    (let [conn (db/create-connection)
          test-db (db/make-database conn {})]
      (db/close! test-db)
      (is (thrown? clojure.lang.ExceptionInfo
            (db/execute! test-db "SELECT 1"))))))

;; =============================================================================
;; T2.2.5 - Extension Caching Tests
;; =============================================================================

(deftest extension-loaded-tracking-test
  (testing "extensions-loaded tracks loaded extensions"
    (is (false? (db/extension-loaded? *test-db* "fts")))

    ;; Load the extension
    (db/load-extension! *test-db* "fts")

    ;; Should now be tracked
    (is (true? (db/extension-loaded? *test-db* "fts")))))

(deftest extension-only-loaded-once-test
  (testing "load-extension! returns false on second call"
    ;; First load should return true
    (is (true? (db/load-extension! *test-db* "fts")))

    ;; Second load should return false (already loaded)
    (is (false? (db/load-extension! *test-db* "fts")))

    ;; Should still be tracked
    (is (true? (db/extension-loaded? *test-db* "fts")))))

;; =============================================================================
;; T3.1.6 - Error Handling Tests
;; =============================================================================

(deftest query-error-includes-message-test
  (testing "query errors include useful message"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"SQL error"
          (db/query *test-db* "SELECT * FROM nonexistent_table_xyz")))))

(deftest execute-error-includes-message-test
  (testing "execute! errors include useful message"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"SQL error"
          (db/execute! *test-db* "DROP TABLE nonexistent_table_xyz")))))

(deftest validation-errors-are-descriptive-test
  (testing "validation errors provide context"
    (try
      (db/escape-identifier "invalid;name" "my-context")
      (is false "Should have thrown")
      (catch clojure.lang.ExceptionInfo e
        (is (re-find #"my-context" (.getMessage e)))
        (is (re-find #"invalid" (.getMessage e)))
        (is (= :validation-error (errors/error-type e)))))))

;; =============================================================================
;; T6.1.2-6 - Additional Security Validation Tests
;; =============================================================================

(deftest reject-path-traversal-test
  (testing "path traversal attempts in database-code are rejected"
    (is (some? (db/validate-identifier "../../../etc")))
    (is (some? (db/validate-identifier "..\\..\\windows")))
    (is (some? (db/validate-identifier "test/../../root")))))

(deftest max-rows-bounds-test
  (testing "max-rows must be within valid bounds"
    (is (some? (vocab/validate-search-options
                {:database-code "test"
                 :schema-name "public"
                 :max-rows 0})))
    (is (some? (vocab/validate-search-options
                {:database-code "test"
                 :schema-name "public"
                 :max-rows -1})))
    (is (some? (vocab/validate-search-options
                {:database-code "test"
                 :schema-name "public"
                 :max-rows 10001})))
    (is (nil? (vocab/validate-search-options
               {:database-code "test"
                :schema-name "test"
                :max-rows 1000})))))

(deftest cohort-id-positive-integer-test
  (testing "cohort-id must be a positive integer"
    (is (some? (circe/validate-circe-options
                {:cdm-schema "cdm"
                 :result-schema "results"
                 :cohort-id 0})))
    (is (some? (circe/validate-circe-options
                {:cdm-schema "cdm"
                 :result-schema "results"
                 :cohort-id -5})))
    (is (some? (circe/validate-circe-options
                {:cdm-schema "cdm"
                 :result-schema "results"
                 :cohort-id "not-an-int"})))
    (is (nil? (circe/validate-circe-options
               {:cdm-schema "cdm"
                :result-schema "results"
                :cohort-id 1})))))
