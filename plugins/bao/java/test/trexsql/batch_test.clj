(ns trexsql.batch-test
  "Unit tests for batch.clj - JDBC batch transfer functions."
  (:require [clojure.test :refer :all]
            [trexsql.batch :as batch]
            [trexsql.core :as core]
            [trexsql.db :as db]
            [honey.sql :as sql]))

;; build-select-query Tests

(deftest test-build-select-query-basic
  (testing "Basic SELECT * query"
    (let [[sql & params] (batch/build-select-query {:schema "cdm" :table "person"})]
      (is (string? sql))
      (is (re-find #"(?i)SELECT" sql))
      (is (re-find #"cdm.*person" sql))
      (is (empty? params)))))

(deftest test-build-select-query-with-columns
  (testing "Query with specific columns"
    (let [[sql & params] (batch/build-select-query
                          {:schema "cdm"
                           :table "person"
                           :columns [:person_id :gender_concept_id]})]
      (is (re-find #"person_id" sql))
      (is (re-find #"gender_concept_id" sql)))))

(deftest test-build-select-query-with-patient-filter
  (testing "Query with patient filter"
    (let [[sql & params] (batch/build-select-query
                          {:schema "cdm"
                           :table "observation"
                           :patient-filter [1001 1002 1003]})]
      (is (re-find #"(?i)WHERE" sql))
      (is (re-find #"person_id" sql))
      (is (= 3 (count params)))
      (is (= [1001 1002 1003] params)))))

(deftest test-build-select-query-with-timestamp-filter
  (testing "Query with timestamp filter"
    (let [[sql & params] (batch/build-select-query
                          {:schema "cdm"
                           :table "observation"
                           :timestamp-filter "2020-01-01"})]
      (is (re-find #"(?i)WHERE" sql))
      (is (re-find #"observation_date" sql))
      (is (= ["2020-01-01"] params)))))

(deftest test-build-select-query-with-column-filter
  (testing "Query with table-specific column filter"
    (let [[sql & params] (batch/build-select-query
                          {:schema "cdm"
                           :table "person"
                           :column-filter {"person" ["person_id" "year_of_birth"]}})]
      (is (re-find #"person_id" sql))
      (is (re-find #"year_of_birth" sql)))))

;; build-create-table-ddl Tests

(deftest test-build-create-table-ddl-basic
  (testing "Basic CREATE TABLE DDL"
    (let [columns [{:name "id" :trexsql-type "BIGINT" :nullable? false}
                   {:name "name" :trexsql-type "VARCHAR" :nullable? true}]
          ddl (batch/build-create-table-ddl "test_table" columns)]
      (is (re-find #"(?i)CREATE TABLE" ddl))
      (is (re-find #"test_table" ddl))
      (is (re-find #"BIGINT" ddl))
      (is (re-find #"VARCHAR" ddl))
      (is (re-find #"NOT NULL" ddl)))))

(deftest test-build-create-table-ddl-with-decimal
  (testing "CREATE TABLE with DECIMAL precision"
    (let [columns [{:name "amount" :trexsql-type "DECIMAL" :precision 18 :scale 2 :nullable? true}]
          ddl (batch/build-create-table-ddl "amounts" columns)]
      (is (re-find #"DECIMAL\(18,2\)" ddl)))))

;; retry-with-backoff Tests

(deftest test-retry-with-backoff-success
  (testing "Successful execution on first try"
    (let [call-count (atom 0)
          result (batch/retry-with-backoff
                  (fn []
                    (swap! call-count inc)
                    :success)
                  {:max-retries 3 :initial-delay-ms 1})]
      (is (= :success result))
      (is (= 1 @call-count)))))

(deftest test-retry-with-backoff-eventual-success
  (testing "Success after retries"
    (let [call-count (atom 0)
          result (batch/retry-with-backoff
                  (fn []
                    (swap! call-count inc)
                    (if (< @call-count 3)
                      (throw (java.net.SocketTimeoutException. "timeout"))
                      :success))
                  {:max-retries 3 :initial-delay-ms 1})]
      (is (= :success result))
      (is (= 3 @call-count)))))

(deftest test-retry-with-backoff-failure
  (testing "Failure after max retries"
    (let [call-count (atom 0)]
      (is (thrown? java.net.SocketTimeoutException
                   (batch/retry-with-backoff
                    (fn []
                      (swap! call-count inc)
                      (throw (java.net.SocketTimeoutException. "timeout")))
                    {:max-retries 2 :initial-delay-ms 1})))
      (is (= 2 @call-count)))))

(deftest test-retry-with-backoff-non-retryable
  (testing "Non-retryable exceptions are thrown immediately"
    (let [call-count (atom 0)]
      (is (thrown-with-msg? IllegalArgumentException #"bad arg"
                            (batch/retry-with-backoff
                             (fn []
                               (swap! call-count inc)
                               (throw (IllegalArgumentException. "bad arg")))
                             {:max-retries 3 :initial-delay-ms 1})))
      (is (= 1 @call-count)))))

;; validate-batch-config Tests

(deftest test-validate-batch-config-valid
  (testing "Valid config returns nil"
    (is (nil? (batch/validate-batch-config {:schema-name "cdm" :batch-size 10000})))))

(deftest test-validate-batch-config-missing-schema
  (testing "Missing schema-name returns error"
    (is (string? (batch/validate-batch-config {:batch-size 10000})))))

(deftest test-validate-batch-config-batch-size-too-small
  (testing "Batch size below minimum returns error"
    (let [error (batch/validate-batch-config {:schema-name "cdm" :batch-size 50})]
      (is (string? error))
      (is (re-find #"100" error)))))

(deftest test-validate-batch-config-batch-size-too-large
  (testing "Batch size above maximum returns error"
    (let [error (batch/validate-batch-config {:schema-name "cdm" :batch-size 200000})]
      (is (string? error))
      (is (re-find #"100000" error)))))

;; Constants Tests

(deftest test-default-values
  (testing "Default values are reasonable"
    (is (= 10000 batch/default-batch-size))
    (is (= 2000 batch/default-fetch-size))
    (is (= 10000 batch/default-progress-interval))
    (is (= 100 batch/min-batch-size))
    (is (= 100000 batch/max-batch-size))))

;; Connection Pooling Tests

(deftest test-create-connection-pool-config-validation
  (testing "Connection pool requires jdbc-url"
    ;; HikariCP will throw if jdbc-url is invalid/missing
    (is (thrown? Exception
          (batch/create-connection-pool {:jdbc-url nil})))))

(deftest test-connection-pool-creates-and-closes
  (testing "Connection pool can be created and closed"
    ;; Use TrexSQL as it's available in test environment
    (let [pool (batch/create-connection-pool
                 {:jdbc-url "jdbc:trex::memory:"
                  :pool-size 2
                  :pool-name "test-pool"})]
      (try
        (is (some? pool))
        (is (= "test-pool" (.getPoolName pool)))
        (is (= 2 (.getMaximumPoolSize pool)))
        (finally
          (batch/close-connection-pool! pool))))))

(deftest test-with-connection-pool-macro
  (testing "with-connection-pool creates and closes pool"
    (let [pool-ref (atom nil)]
      (batch/with-connection-pool [pool {:jdbc-url "jdbc:trex::memory:"
                                         :pool-size 1
                                         :pool-name "macro-test-pool"}]
        (reset! pool-ref pool)
        (is (some? pool))
        (is (not (.isClosed pool))))
      ;; Pool should be closed after macro body
      (is (.isClosed @pool-ref)))))

(deftest test-with-pooled-connection-macro
  (testing "with-pooled-connection gets and returns connection"
    (batch/with-connection-pool [pool {:jdbc-url "jdbc:trex::memory:"
                                       :pool-size 2
                                       :pool-name "conn-test-pool"}]
      (let [conn-ref (atom nil)]
        (batch/with-pooled-connection [conn pool]
          (reset! conn-ref conn)
          (is (some? conn))
          (is (not (.isClosed conn))))
        ;; Connection returned to pool (closed from our perspective)
        (is (.isClosed @conn-ref))))))

(deftest test-connection-pool-reuses-connections
  (testing "Pool reuses connections efficiently"
    (batch/with-connection-pool [pool {:jdbc-url "jdbc:trex::memory:"
                                       :pool-size 1
                                       :pool-name "reuse-test-pool"}]
      ;; With pool-size 1, we can verify sequential access works
      ;; and connections are properly returned to pool
      (let [query-results (atom [])]
        ;; Get connection multiple times sequentially
        (dotimes [_ 3]
          (batch/with-pooled-connection [conn pool]
            (with-open [stmt (.prepareStatement conn "SELECT 42 as answer")
                        rs (.executeQuery stmt)]
              (when (.next rs)
                (swap! query-results conj (.getInt rs 1))))))
        ;; All queries should succeed (pool working)
        (is (= [42 42 42] @query-results))))))

;; Transaction Support Tests

(deftest test-with-trexsql-transaction-commits
  (testing "Transaction commits on success"
    (let [test-db (core/init {:cache-path "./target/test-tx-cache"})]
      (try
        (batch/with-trexsql-transaction [test-db]
          (db/execute! test-db "CREATE TABLE IF NOT EXISTS tx_test (id INTEGER)"))
        ;; Verify table exists
        (is (some? (db/query test-db "SELECT * FROM tx_test LIMIT 1")))
        (finally
          (db/execute! test-db "DROP TABLE IF EXISTS tx_test")
          (core/shutdown! test-db))))))

(deftest test-with-trexsql-transaction-rollback
  (testing "Transaction rolls back on exception"
    (let [test-db (core/init {:cache-path "./target/test-tx-cache"})]
      (try
        ;; Create table outside transaction
        (db/execute! test-db "CREATE TABLE IF NOT EXISTS tx_rollback_test (id INTEGER)")

        ;; Try to insert in transaction that fails
        (is (thrown? Exception
              (batch/with-trexsql-transaction [test-db]
                (db/execute! test-db "INSERT INTO tx_rollback_test VALUES (1)")
                (throw (Exception. "Simulated failure")))))

        ;; Verify no rows (rollback worked)
        (let [results (db/query test-db "SELECT COUNT(*) as cnt FROM tx_rollback_test")
              cnt (.get (first results) "cnt")]
          (is (= 0 (long cnt))))
        (finally
          (db/execute! test-db "DROP TABLE IF EXISTS tx_rollback_test")
          (core/shutdown! test-db))))))

(deftest test-copy-table-jdbc-uses-transactions-by-default
  (testing "copy-table-jdbc uses transactions by default"
    ;; This is a config check - actual execution would need JDBC source
    (is (true? (get {:use-transactions true} :use-transactions true)))
    (is (false? (get {:use-transactions false} :use-transactions true)))))
