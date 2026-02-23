(ns trexsql.webapi-test
  "Unit tests for webapi.clj - HTTP API handlers."
  (:require [clojure.test :refer :all]
            [trexsql.webapi :as webapi]
            [trexsql.core :as core]
            [clojure.data.json :as json]))

;; Helper Functions

(defn with-test-db [f]
  "Create a temporary database for testing."
  (let [db (core/init {:cache-path "./target/test-webapi-cache"})]
    (try
      (f db)
      (finally
        (core/shutdown! db)))))

;; Response Helper Tests

(deftest test-ok-response
  (testing "ok creates 200 response with body"
    (let [resp (webapi/ok {:data "test"})]
      (is (= 200 (:status resp)))
      (is (= {:data "test"} (:body resp))))))

(deftest test-bad-request-response
  (testing "bad-request creates 400 response"
    (let [resp (webapi/bad-request "error message")]
      (is (= 400 (:status resp)))
      (is (= "BAD_REQUEST" (get-in resp [:body :error])))
      (is (= "error message" (get-in resp [:body :message]))))))

(deftest test-not-found-response
  (testing "not-found creates 404 response"
    (let [resp (webapi/not-found "missing resource")]
      (is (= 404 (:status resp)))
      (is (= "NOT_FOUND" (get-in resp [:body :error])))
      (is (= "missing resource" (get-in resp [:body :message]))))))

(deftest test-internal-error-response
  (testing "internal-error creates 500 response"
    (let [resp (webapi/internal-error "something broke")]
      (is (= 500 (:status resp)))
      (is (= "INTERNAL_ERROR" (get-in resp [:body :error])))
      (is (= "something broke" (get-in resp [:body :message]))))))

;; Validation Tests

(deftest test-validate-database-code-valid
  (testing "Valid database codes pass validation"
    (is (nil? (#'webapi/validate-database-code "mydb")))
    (is (nil? (#'webapi/validate-database-code "my_db")))
    (is (nil? (#'webapi/validate-database-code "my-db")))
    (is (nil? (#'webapi/validate-database-code "MyDB123")))
    (is (nil? (#'webapi/validate-database-code "a")))))

(deftest test-validate-database-code-invalid
  (testing "Invalid database codes fail validation"
    (is (some? (#'webapi/validate-database-code nil)))
    (is (some? (#'webapi/validate-database-code "")))
    (is (some? (#'webapi/validate-database-code "   ")))
    (is (some? (#'webapi/validate-database-code "my db")))
    (is (some? (#'webapi/validate-database-code "my.db")))
    (is (some? (#'webapi/validate-database-code "my/db")))
    (is (some? (#'webapi/validate-database-code "my'db")))
    (is (some? (#'webapi/validate-database-code "my;db")))))

(deftest test-validate-database-code-too-long
  (testing "Database code longer than 128 chars fails"
    (let [long-code (apply str (repeat 129 "a"))]
      (is (some? (#'webapi/validate-database-code long-code))))))

;; Request Routing Tests

(deftest test-handle-request-blank-source
  (testing "Blank source-key returns bad-request"
    (with-test-db
      (fn [db]
        (let [resp (webapi/handle-request db "GET" "" nil nil nil)]
          (is (= 400 (:status resp))))))))

(deftest test-handle-request-unknown-resource
  (testing "Unknown resource returns not-found"
    (with-test-db
      (fn [db]
        (let [resp (webapi/handle-request db "GET" "mysource/unknown" nil nil nil)]
          (is (= 404 (:status resp))))))))

(deftest test-handle-request-unknown-source
  (testing "Unknown source returns not-found for cache status"
    (with-test-db
      (fn [db]
        ;; Note: This depends on source-repository being empty
        (let [resp (webapi/handle-request db "GET" "unknown-source/cache" nil nil nil)]
          (is (= 404 (:status resp))))))))

;; JSON Parsing Tests

(deftest test-parse-json-valid
  (testing "Valid JSON is parsed correctly"
    (let [result (#'webapi/parse-json "{\"key\": \"value\"}")]
      (is (= "value" (:key result))))))

(deftest test-parse-json-invalid
  (testing "Invalid JSON returns nil"
    (is (nil? (#'webapi/parse-json "not json")))
    (is (nil? (#'webapi/parse-json "{broken")))
    (is (nil? (#'webapi/parse-json nil)))))

;; java-map->clj Conversion Tests

(deftest test-java-map-conversion
  (testing "Java HashMap is converted to Clojure map"
    (let [java-map (java.util.HashMap. {"key" "value" "num" 42})
          result (#'webapi/java-map->clj java-map)]
      (is (map? result))
      (is (= "value" (:key result)))
      (is (= 42 (:num result))))))

(deftest test-java-list-conversion
  (testing "Java ArrayList is converted to Clojure vector"
    (let [java-list (java.util.ArrayList. ["a" "b" "c"])
          result (#'webapi/java-map->clj java-list)]
      (is (vector? result))
      (is (= ["a" "b" "c"] result)))))

(deftest test-nested-conversion
  (testing "Nested Java collections are converted"
    (let [inner-map (java.util.HashMap. {"inner" "value"})
          java-map (java.util.HashMap. {"outer" inner-map})
          result (#'webapi/java-map->clj java-map)]
      (is (= "value" (get-in result [:outer :inner]))))))

;; Cache Path Generation Tests

(deftest test-get-cache-path
  (testing "Cache path is correctly generated"
    (is (= "./data/cache/mydb.db" (#'webapi/get-cache-path "./data/cache" "mydb")))
    (is (= "/tmp/mydb.db" (#'webapi/get-cache-path "/tmp" "mydb")))))
