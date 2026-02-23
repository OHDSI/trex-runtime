(ns trexsql.vocab-test
  "Unit tests for vocabulary search functionality."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [trexsql.core :as core]
            [trexsql.db :as db]
            [trexsql.vocab :as vocab]
            [trexsql.errors :as errors])
  (:import [java.io File]
           [java.util HashMap ArrayList]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def ^:dynamic *test-db* nil)
(def ^:dynamic *test-cache-path* nil)

(defn with-test-db [f]
  (let [temp-dir (File/createTempFile "trexsql-vocab-test" "")
        cache-path (.getAbsolutePath (File. (.getParent temp-dir) "vocab-cache"))]
    (.delete temp-dir)
    (.mkdirs (File. cache-path))

    (let [conn (db/create-connection)
          test-db (db/make-database conn {})]
      (binding [*test-db* test-db
                *test-cache-path* cache-path]
        (try
          (f)
          (finally
            (db/close! test-db)
            (doseq [f (.listFiles (File. cache-path))]
              (.delete f))
            (.delete (File. cache-path))))))))

(use-fixtures :each with-test-db)

;; =============================================================================
;; Validation Tests
;; =============================================================================

(deftest validate-search-options-test
  (testing "nil options returns error"
    (is (some? (vocab/validate-search-options nil))))

  (testing "missing database-code returns error"
    (is (some? (vocab/validate-search-options {}))))

  (testing "empty database-code returns error"
    (is (some? (vocab/validate-search-options {:database-code ""}))))

  (testing "invalid database-code returns error"
    (is (some? (vocab/validate-search-options {:database-code "bad/code"})))
    (is (some? (vocab/validate-search-options {:database-code "bad code"}))))

  (testing "valid database-code passes"
    (is (nil? (vocab/validate-search-options {:database-code "test_db"}))))

  (testing "invalid schema-name returns error"
    (is (some? (vocab/validate-search-options {:database-code "test_db"
                                               :schema-name "bad/schema"}))))

  (testing "valid schema-name passes"
    (is (nil? (vocab/validate-search-options {:database-code "test_db"
                                              :schema-name "public"}))))

  (testing "invalid max-rows returns error"
    (is (some? (vocab/validate-search-options {:database-code "test_db"
                                               :max-rows 0})))
    (is (some? (vocab/validate-search-options {:database-code "test_db"
                                               :max-rows -1})))
    (is (some? (vocab/validate-search-options {:database-code "test_db"
                                               :max-rows 20000}))))

  (testing "valid max-rows passes"
    (is (nil? (vocab/validate-search-options {:database-code "test_db"
                                              :max-rows 100})))))

;; =============================================================================
;; Java Map Conversion Tests
;; =============================================================================

(deftest java-map-conversion-test
  (testing "converts Java Map to search options with defaults"
    (let [jmap (doto (HashMap.)
                 (.put "database-code" "test_db"))
          opts (vocab/java-map->search-options jmap)]
      (is (= "test_db" (:database-code opts)))
      (is (= "test_db" (:schema-name opts)))  ; defaults to database-code
      (is (= 1000 (:max-rows opts)))))        ; default max-rows

  (testing "respects provided values"
    (let [jmap (doto (HashMap.)
                 (.put "database-code" "test_db")
                 (.put "schema-name" "vocab")
                 (.put "max-rows" 500))
          opts (vocab/java-map->search-options jmap)]
      (is (= "test_db" (:database-code opts)))
      (is (= "vocab" (:schema-name opts)))
      (is (= 500 (:max-rows opts))))))

;; =============================================================================
;; SQL Builder Tests
;; =============================================================================

(deftest build-fts-search-sql-test
  (testing "builds FTS search SQL with correct structure"
    (let [sql (vocab/build-fts-search-sql "mydb" "vocab" 100)]
      (is (string? sql))
      (is (re-find #"SELECT.*concept_id" sql))
      (is (re-find #"match_bm25" sql))
      (is (re-find #"LIMIT 100" sql)))))

(deftest build-fallback-search-sql-test
  (testing "builds fallback ILIKE search SQL with correct structure"
    (let [sql (vocab/build-fallback-search-sql "mydb" "vocab" 50)]
      (is (string? sql))
      (is (re-find #"SELECT.*concept_id" sql))
      (is (re-find #"ILIKE" sql))
      (is (re-find #"LIMIT 50" sql)))))

;; =============================================================================
;; Result Conversion Tests
;; =============================================================================

(deftest results-to-concept-list-test
  (testing "converts empty results to empty ArrayList"
    (let [result (vocab/results->concept-list [])]
      (is (instance? ArrayList result))
      (is (= 0 (.size result)))))

  (testing "converts result rows to ArrayList of HashMaps"
    (let [row (doto (HashMap.)
                (.put "concept_id" 12345)
                (.put "concept_name" "Diabetes mellitus")
                (.put "domain_id" "Condition")
                (.put "vocabulary_id" "SNOMED")
                (.put "concept_class_id" "Clinical Finding")
                (.put "standard_concept" "S")
                (.put "concept_code" "73211009"))
          result (vocab/results->concept-list [row])]
      (is (instance? ArrayList result))
      (is (= 1 (.size result)))
      (let [concept (.get result 0)]
        (is (instance? HashMap concept))
        (is (= 12345 (.get concept "concept_id")))
        (is (= "Diabetes mellitus" (.get concept "concept_name")))))))

;; =============================================================================
;; Search Function Tests
;; =============================================================================

(deftest search-vocab-validation-test
  (testing "throws on empty search term"
    (is (thrown? clojure.lang.ExceptionInfo
                 (vocab/search-vocab *test-db* "" {:database-code "test_db"})))
    (try
      (vocab/search-vocab *test-db* "" {:database-code "test_db"})
      (catch clojure.lang.ExceptionInfo e
        (is (= :validation-error (errors/error-type e))))))

  (testing "throws on nil search term"
    (is (thrown? clojure.lang.ExceptionInfo
                 (vocab/search-vocab *test-db* nil {:database-code "test_db"}))))

  (testing "throws on invalid options"
    (is (thrown? clojure.lang.ExceptionInfo
                 (vocab/search-vocab *test-db* "diabetes" {})))))
