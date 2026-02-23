(ns trexsql.cli-test
  "Unit tests for cli.clj - CLI command handlers."
  (:require [clojure.test :refer :all]
            [trexsql.cli :as cli]))

;; Cache Create Command Tests

(deftest test-cache-create-help
  (testing "Help flag returns exit-code 0"
    (is (= 0 (:exit-code (cli/run-cache-create ["--help"]))))
    (is (= 0 (:exit-code (cli/run-cache-create ["-h"]))))))

(deftest test-cache-create-missing-source
  (testing "Missing source returns error"
    (let [result (cli/run-cache-create ["-S" "cdm"])]
      (is (= 1 (:exit-code result))))))

(deftest test-cache-create-missing-schema
  (testing "Missing schema returns error"
    (let [result (cli/run-cache-create ["-s" "mysource"])]
      (is (= 1 (:exit-code result))))))

(deftest test-cache-create-invalid-batch-size-small
  (testing "Batch size too small returns error"
    (let [result (cli/run-cache-create ["-s" "mysource" "-S" "cdm" "-b" "50"])]
      (is (= 1 (:exit-code result))))))

(deftest test-cache-create-invalid-batch-size-large
  (testing "Batch size too large returns error"
    (let [result (cli/run-cache-create ["-s" "mysource" "-S" "cdm" "-b" "200000"])]
      (is (= 1 (:exit-code result))))))

;; Cache Status Command Tests

(deftest test-cache-status-help
  (testing "Help flag returns exit-code 0"
    (is (= 0 (:exit-code (cli/run-cache-status ["--help"]))))
    (is (= 0 (:exit-code (cli/run-cache-status ["-h"]))))))

(deftest test-cache-status-missing-source
  (testing "Missing source returns error"
    (let [result (cli/run-cache-status [])]
      (is (= 1 (:exit-code result))))))

;; Cache Cancel Command Tests

(deftest test-cache-cancel-help
  (testing "Help flag returns exit-code 0"
    (is (= 0 (:exit-code (cli/run-cache-cancel ["--help"]))))
    (is (= 0 (:exit-code (cli/run-cache-cancel ["-h"]))))))

(deftest test-cache-cancel-missing-source
  (testing "Missing source returns error"
    (let [result (cli/run-cache-cancel [])]
      (is (= 1 (:exit-code result))))))

;; Cache Main Command Routing Tests

(deftest test-cache-routing-create
  (testing "create subcommand routes correctly"
    ;; With --help to avoid actual execution
    (is (= 0 (:exit-code (cli/run-cache ["create" "--help"]))))))

(deftest test-cache-routing-status
  (testing "status subcommand routes correctly"
    (is (= 0 (:exit-code (cli/run-cache ["status" "--help"]))))))

(deftest test-cache-routing-cancel
  (testing "cancel subcommand routes correctly"
    (is (= 0 (:exit-code (cli/run-cache ["cancel" "--help"]))))))

(deftest test-cache-routing-unknown
  (testing "Unknown subcommand returns error"
    (let [result (cli/run-cache ["unknown"])]
      (is (= 1 (:exit-code result))))))

(deftest test-cache-routing-no-command
  (testing "No subcommand shows help"
    (let [result (cli/run-cache [])]
      (is (= 0 (:exit-code result))))))

(deftest test-cache-routing-help
  (testing "Help flags show help"
    (is (= 0 (:exit-code (cli/run-cache ["--help"]))))
    (is (= 0 (:exit-code (cli/run-cache ["-h"]))))))

;; CLI Options Parsing Tests

(deftest test-cache-create-options-parsing
  (testing "Options are parsed correctly"
    ;; We can't test this directly without mocking, but we can verify
    ;; the options definitions are valid by calling with --help
    (is (= 0 (:exit-code (cli/run-cache-create ["--help"]))))))

(deftest test-batch-size-default
  (testing "Default batch size is in valid range"
    ;; Verify the default in cache-create-options is valid
    ;; Options are vectors like ["-b" "--batch-size SIZE" "description" :default 10000 ...]
    (let [opts cli/cache-create-options
          batch-opt (first (filter #(= "-b" (first %)) opts))
          ;; Extract :default value from the option spec (it's in the rest of the vector)
          default (some (fn [[k v]] (when (= k :default) v))
                        (partition 2 (drop 3 batch-opt)))]
      (is (= 10000 default))
      (is (<= 100 default 100000)))))

;; Help Text Tests

(deftest test-cache-create-help-text
  (testing "Help text is returned as string"
    (is (string? (cli/cache-create-help)))
    (is (re-find #"cache create" (cli/cache-create-help)))
    (is (re-find #"--source" (cli/cache-create-help)))))

(deftest test-cache-status-help-text
  (testing "Status help text is returned as string"
    (is (string? (cli/cache-status-help)))
    (is (re-find #"cache status" (cli/cache-status-help)))))

(deftest test-cache-cancel-help-text
  (testing "Cancel help text is returned as string"
    (is (string? (cli/cache-cancel-help)))
    (is (re-find #"cache cancel" (cli/cache-cancel-help)))))

(deftest test-cache-help-text
  (testing "Main cache help text is returned as string"
    (is (string? (cli/cache-help)))
    (is (re-find #"create" (cli/cache-help)))
    (is (re-find #"status" (cli/cache-help)))
    (is (re-find #"cancel" (cli/cache-help)))))

;; Progress Callback Tests

(deftest test-print-progress-job-start
  (testing "Progress callback handles job-start phase"
    ;; Just verify it doesn't throw
    (is (nil? (cli/print-progress {:phase :job-start :total-tables 5})))))

(deftest test-print-progress-table-start
  (testing "Progress callback handles table-start phase"
    (is (nil? (cli/print-progress {:phase :table-start
                                   :table "person"
                                   :table-index 1
                                   :total-tables 5})))))

(deftest test-print-progress-row-progress
  (testing "Progress callback handles row-progress phase"
    (is (nil? (cli/print-progress {:phase :row-progress
                                   :rows-processed 1000})))))

(deftest test-print-progress-table-complete
  (testing "Progress callback handles table-complete phase"
    (is (nil? (cli/print-progress {:phase :table-complete
                                   :rows-copied 5000
                                   :duration-ms 1234})))))

(deftest test-print-progress-table-failed
  (testing "Progress callback handles table-failed phase"
    (is (nil? (cli/print-progress {:phase :table-failed
                                   :error "Connection failed"})))))

(deftest test-print-progress-job-complete
  (testing "Progress callback handles job-complete phase"
    (is (nil? (cli/print-progress {:phase :job-complete
                                   :tables-copied [{:table-name "a"}]
                                   :tables-failed []
                                   :duration-ms 5000})))))

(deftest test-print-progress-job-failed
  (testing "Progress callback handles job-failed phase"
    (is (nil? (cli/print-progress {:phase :job-failed
                                   :error "Fatal error"})))))

(deftest test-print-progress-unknown
  (testing "Progress callback handles unknown phase gracefully"
    (is (nil? (cli/print-progress {:phase :unknown})))))
