(ns trexsql.circe-test
  "Unit tests for Circe JSON to SQL functionality."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [trexsql.core :as core]
            [trexsql.db :as db]
            [trexsql.circe :as circe]
            [trexsql.errors :as errors])
  (:import [java.io File]
           [java.util HashMap ArrayList]))

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
;; Base64 Encoding Tests
;; =============================================================================

(deftest encode-base64-test
  (testing "encodes empty string"
    (is (= "" (circe/encode-base64 ""))))

  (testing "encodes simple string"
    (is (= "dGVzdA==" (circe/encode-base64 "test"))))

  (testing "encodes JSON-like string"
    (let [json "{\"key\": \"value\"}"
          encoded (circe/encode-base64 json)]
      (is (string? encoded))
      (is (not= json encoded)))))

;; =============================================================================
;; Validation Tests
;; =============================================================================

(deftest validate-circe-options-test
  (testing "nil options returns error"
    (is (some? (circe/validate-circe-options nil))))

  (testing "missing cdm-schema returns error"
    (is (some? (circe/validate-circe-options {}))))

  (testing "missing result-schema returns error"
    (is (some? (circe/validate-circe-options {:cdm-schema "cdm"}))))

  (testing "missing cohort-id returns error"
    (is (some? (circe/validate-circe-options {:cdm-schema "cdm"
                                              :result-schema "results"}))))

  (testing "invalid cohort-id returns error"
    (is (some? (circe/validate-circe-options {:cdm-schema "cdm"
                                              :result-schema "results"
                                              :cohort-id "not-an-int"})))
    (is (some? (circe/validate-circe-options {:cdm-schema "cdm"
                                              :result-schema "results"
                                              :cohort-id 0}))))

  (testing "valid options passes"
    (is (nil? (circe/validate-circe-options {:cdm-schema "cdm"
                                             :result-schema "results"
                                             :cohort-id 123})))))

(deftest validate-circe-json-test
  (testing "empty string returns error"
    (is (some? (circe/validate-circe-json ""))))

  (testing "nil returns error"
    (is (some? (circe/validate-circe-json nil))))

  (testing "non-JSON returns error"
    (is (some? (circe/validate-circe-json "not json"))))

  (testing "valid JSON object passes"
    (is (nil? (circe/validate-circe-json "{\"test\": 1}")))))

;; =============================================================================
;; Java Map Conversion Tests
;; =============================================================================

(deftest java-map-conversion-test
  (testing "converts Java Map to circe options with defaults"
    (let [jmap (doto (HashMap.)
                 (.put "cdm-schema" "cdm")
                 (.put "result-schema" "results")
                 (.put "cohort-id" 123))
          opts (circe/java-map->circe-options jmap)]
      (is (= "cdm" (:cdm-schema opts)))
      (is (= "results" (:result-schema opts)))
      (is (= 123 (:cohort-id opts)))
      (is (= "cohort" (:target-table opts)))      ; default
      (is (false? (:generate-stats opts)))))      ; default

  (testing "respects provided values"
    (let [jmap (doto (HashMap.)
                 (.put "cdm-schema" "cdm")
                 (.put "result-schema" "results")
                 (.put "cohort-id" 456)
                 (.put "target-table" "my_cohort")
                 (.put "generate-stats" true))
          opts (circe/java-map->circe-options jmap)]
      (is (= "cdm" (:cdm-schema opts)))
      (is (= "results" (:result-schema opts)))
      (is (= 456 (:cohort-id opts)))
      (is (= "my_cohort" (:target-table opts)))
      (is (true? (:generate-stats opts))))))

;; =============================================================================
;; Options JSON Builder Tests
;; =============================================================================

(deftest build-circe-options-json-test
  (testing "builds valid JSON string"
    (let [opts {:cdm-schema "cdm"
                :result-schema "results"
                :target-table "cohort"
                :cohort-id 123
                :generate-stats false}
          json (circe/build-circe-options-json opts)]
      (is (string? json))
      (is (re-find #"\"cdmSchema\":\"cdm\"" json))
      (is (re-find #"\"resultSchema\":\"results\"" json))
      (is (re-find #"\"targetTable\":\"cohort\"" json))
      (is (re-find #"\"cohortId\":123" json))
      (is (re-find #"\"generateStats\":false" json))))

  (testing "builds JSON with generate-stats true"
    (let [opts {:cdm-schema "cdm"
                :result-schema "results"
                :target-table "cohort"
                :cohort-id 456
                :generate-stats true}
          json (circe/build-circe-options-json opts)]
      (is (re-find #"\"generateStats\":true" json)))))

;; =============================================================================
;; Error Checking Tests
;; =============================================================================

(deftest check-circe-error-test
  (testing "returns nil for valid SQL"
    (is (nil? (circe/check-circe-error "SELECT * FROM cohort"))))

  (testing "returns nil for nil input"
    (is (nil? (circe/check-circe-error nil))))

  (testing "returns error message for circe error"
    (let [result (circe/check-circe-error "/* circe error: invalid JSON */")]
      (is (some? result))
      (is (re-find #"circe error" result)))))

;; =============================================================================
;; Result Conversion Tests
;; =============================================================================

(deftest circe-result-to-java-map-test
  (testing "converts success result"
    (let [result {:success true
                  :sql "SELECT * FROM cohort"
                  :rows-affected 100
                  :error nil}
          jmap (circe/circe-result->java-map result)]
      (is (instance? HashMap jmap))
      (is (true? (.get jmap "success")))
      (is (= "SELECT * FROM cohort" (.get jmap "sql")))
      (is (= 100 (.get jmap "rows-affected")))
      (is (nil? (.get jmap "error")))))

  (testing "converts failure result"
    (let [result {:success false
                  :sql nil
                  :rows-affected 0
                  :error "Extension not loaded"}
          jmap (circe/circe-result->java-map result)]
      (is (instance? HashMap jmap))
      (is (false? (.get jmap "success")))
      (is (nil? (.get jmap "sql")))
      (is (= 0 (.get jmap "rows-affected")))
      (is (= "Extension not loaded" (.get jmap "error"))))))

;; =============================================================================
;; Execute Circe Tests (Validation Only)
;; =============================================================================

(deftest execute-circe-validation-test
  (testing "returns error for empty JSON"
    (let [result (circe/execute-circe *test-db* "" {:cdm-schema "cdm"
                                                    :result-schema "results"
                                                    :cohort-id 123})]
      (is (false? (:success result)))
      (is (some? (:error result)))))

  (testing "returns error for missing options"
    (let [result (circe/execute-circe *test-db* "{\"test\": 1}" {})]
      (is (false? (:success result)))
      (is (some? (:error result))))))

;; =============================================================================
;; Render Circe to SQL Tests (Validation Only)
;; =============================================================================

(deftest render-circe-to-sql-validation-test
  (testing "throws on empty JSON"
    (is (thrown? clojure.lang.ExceptionInfo
                 (circe/render-circe-to-sql *test-db* "" {:cdm-schema "cdm"
                                                          :result-schema "results"
                                                          :cohort-id 123})))
    (try
      (circe/render-circe-to-sql *test-db* "" {:cdm-schema "cdm"
                                               :result-schema "results"
                                               :cohort-id 123})
      (catch clojure.lang.ExceptionInfo e
        (is (= :validation-error (errors/error-type e))))))

  (testing "throws on invalid options"
    (is (thrown? clojure.lang.ExceptionInfo
                 (circe/render-circe-to-sql *test-db* "{\"test\": 1}" {})))))
