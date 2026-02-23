(ns trexsql.datamart-test
  "Unit tests for datamart configuration and validation."
  (:require [clojure.test :refer [deftest is testing]]
            [trexsql.datamart :as datamart]))

;; SourceCredentials Validation Tests

(deftest validate-credentials-nil-test
  (testing "nil credentials returns error"
    (is (= "Missing required config: source-credentials"
           (datamart/validate-credentials nil)))))

(deftest validate-credentials-invalid-dialect-test
  (testing "invalid dialect returns error"
    (is (some? (datamart/validate-credentials
                {:dialect "invalid-dialect"})))))

(deftest validate-credentials-postgres-complete-test
  (testing "complete postgres credentials passes validation"
    (is (nil? (datamart/validate-credentials
               {:dialect "postgres"
                :host "localhost"
                :port 5432
                :database-name "test"
                :user "admin"
                :password "secret"})))))

(deftest validate-credentials-postgres-missing-host-test
  (testing "postgres with missing host fails"
    (is (some? (datamart/validate-credentials
                {:dialect "postgres"
                 :port 5432
                 :database-name "test"
                 :user "admin"
                 :password "secret"})))))

(deftest validate-credentials-postgres-missing-port-test
  (testing "postgres with missing port fails"
    (is (some? (datamart/validate-credentials
                {:dialect "postgres"
                 :host "localhost"
                 :database-name "test"
                 :user "admin"
                 :password "secret"})))))

(deftest validate-credentials-postgres-missing-password-test
  (testing "postgres with missing password fails"
    (is (some? (datamart/validate-credentials
                {:dialect "postgres"
                 :host "localhost"
                 :port 5432
                 :database-name "test"
                 :user "admin"})))))

(deftest validate-credentials-bigquery-complete-test
  (testing "complete bigquery credentials passes validation"
    (is (nil? (datamart/validate-credentials
               {:dialect "bigquery"
                :host "my-project"
                :database-name "my-dataset"})))))

(deftest validate-credentials-bigquery-missing-host-test
  (testing "bigquery with missing host fails"
    (is (some? (datamart/validate-credentials
                {:dialect "bigquery"
                 :database-name "my-dataset"})))))

;; DatamartConfig Validation Tests

(deftest validate-config-nil-test
  (testing "nil config returns error"
    (is (= "Config is nil"
           (datamart/validate-config nil)))))

(deftest validate-config-missing-database-code-test
  (testing "missing database-code returns error"
    (is (some? (datamart/validate-config
                {:schema-name "cdm"
                 :source-credentials {:dialect "postgres"
                                      :host "localhost"
                                      :port 5432
                                      :database-name "test"
                                      :user "admin"
                                      :password "secret"}})))))

(deftest validate-config-invalid-database-code-test
  (testing "database-code with special chars returns error"
    (is (some? (datamart/validate-config
                {:database-code "my/database"
                 :schema-name "cdm"
                 :source-credentials {:dialect "postgres"
                                      :host "localhost"
                                      :port 5432
                                      :database-name "test"
                                      :user "admin"
                                      :password "secret"}})))))

(deftest validate-config-missing-schema-name-test
  (testing "missing schema-name returns error"
    (is (some? (datamart/validate-config
                {:database-code "mydb"
                 :source-credentials {:dialect "postgres"
                                      :host "localhost"
                                      :port 5432
                                      :database-name "test"
                                      :user "admin"
                                      :password "secret"}})))))

(deftest validate-config-complete-test
  (testing "complete config passes validation"
    (is (nil? (datamart/validate-config
               {:database-code "mydb"
                :schema-name "cdm"
                :source-credentials {:dialect "postgres"
                                     :host "localhost"
                                     :port 5432
                                     :database-name "test"
                                     :user "admin"
                                     :password "secret"}})))))

;; Java Map Conversion Tests

(deftest java-map-to-datamart-config-test
  (testing "java map conversion with defaults"
    (let [java-map (java.util.HashMap.
                    {"database-code" "testdb"
                     "schema-name" "public"
                     "source-credentials" (java.util.HashMap.
                                           {"dialect" "postgres"
                                            "host" "localhost"
                                            "port" 5432
                                            "database-name" "mydb"
                                            "user" "admin"
                                            "password" "secret"})})
          config (datamart/java-map->datamart-config java-map)]
      (is (= "testdb" (:database-code config)))
      (is (= "public" (:schema-name config)))
      (is (= "public" (:target-schema-name config)))  ; defaults to schema-name
      (is (= ["concept"] (:fts-tables config)))       ; default FTS tables
      (is (= "./data/cache" (:cache-path config)))    ; default cache path
      (is (= "postgres" (:dialect (:source-credentials config))))
      (is (= "localhost" (:host (:source-credentials config)))))))

(deftest java-map-to-datamart-config-with-overrides-test
  (testing "java map conversion with custom values"
    (let [java-map (java.util.HashMap.
                    {"database-code" "testdb"
                     "schema-name" "cdm"
                     "target-schema-name" "cdm_cache"
                     "fts-tables" (java.util.ArrayList. ["concept" "drug"])
                     "cache-path" "/custom/cache"
                     "source-credentials" (java.util.HashMap.
                                           {"dialect" "bigquery"
                                            "host" "my-project"
                                            "database-name" "my-dataset"})})
          config (datamart/java-map->datamart-config java-map)]
      (is (= "cdm_cache" (:target-schema-name config)))
      (is (= ["concept" "drug"] (:fts-tables config)))
      (is (= "/custom/cache" (:cache-path config)))
      (is (= "bigquery" (:dialect (:source-credentials config)))))))

;; Result Conversion Tests

(deftest result-to-java-map-test
  (testing "CacheResult converts to Java HashMap"
    (let [result (datamart/->CacheResult
                  true
                  "testdb"
                  "cdm"
                  [(datamart/->TableResult "person" 1000 0)
                   (datamart/->TableResult "observation" 5000 0)]
                  []
                  ["concept"]
                  12345
                  nil)
          java-map (datamart/result->java-map result)]
      (is (= true (.get java-map "success")))
      (is (= "testdb" (.get java-map "database-code")))
      (is (= "cdm" (.get java-map "schema-name")))
      (is (= 2 (.size (.get java-map "tables-copied"))))
      (is (= 0 (.size (.get java-map "tables-failed"))))
      (is (= 12345 (.get java-map "duration-ms")))
      (is (nil? (.get java-map "error"))))))

(deftest result-with-errors-to-java-map-test
  (testing "CacheResult with errors converts properly"
    (let [result (datamart/->CacheResult
                  false
                  "testdb"
                  "cdm"
                  [(datamart/->TableResult "person" 1000 0)]
                  [(datamart/->TableError "observation" "Connection lost" "copy")]
                  []
                  5000
                  nil)
          java-map (datamart/result->java-map result)]
      (is (= false (.get java-map "success")))
      (is (= 1 (.size (.get java-map "tables-copied"))))
      (is (= 1 (.size (.get java-map "tables-failed"))))
      (let [failed-table (first (.get java-map "tables-failed"))]
        (is (= "observation" (.get failed-table "table-name")))
        (is (= "Connection lost" (.get failed-table "error")))
        (is (= "copy" (.get failed-table "phase")))))))

;; Filter Function Tests

(deftest apply-table-filter-nil-test
  (testing "nil filter returns all tables"
    (is (= ["a" "b" "c"]
           (datamart/apply-table-filter ["a" "b" "c"] nil)))))

(deftest apply-table-filter-subset-test
  (testing "filter returns only matching tables"
    (is (= ["b"]
           (vec (datamart/apply-table-filter ["a" "b" "c"] {"b" ["*"]}))))))

(deftest build-select-clause-nil-test
  (testing "nil columns returns *"
    (is (= "*" (datamart/build-select-clause nil)))))

(deftest build-select-clause-star-test
  (testing "[*] returns *"
    (is (= "*" (datamart/build-select-clause ["*"])))))

(deftest build-select-clause-columns-test
  (testing "specific columns are quoted and joined"
    (is (= "\"col1\", \"col2\""
           (datamart/build-select-clause ["col1" "col2"])))))

(deftest build-where-clause-nil-test
  (testing "no filters returns nil"
    (is (nil? (datamart/build-where-clause nil nil)))))

(deftest build-where-clause-patient-filter-test
  (testing "patient filter generates IN clause"
    (is (= " WHERE person_id IN (1, 2, 3)"
           (datamart/build-where-clause [1 2 3] nil)))))

(deftest build-where-clause-timestamp-filter-test
  (testing "timestamp filter generates >= clause"
    (is (= " WHERE observation_date >= '2024-01-01'"
           (datamart/build-where-clause nil "2024-01-01")))))

(deftest build-where-clause-combined-test
  (testing "both filters combined with AND"
    (is (= " WHERE person_id IN (1, 2) AND observation_date >= '2024-01-01'"
           (datamart/build-where-clause [1 2] "2024-01-01")))))
