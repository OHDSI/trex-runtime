(ns trexsql.http-test
  "Unit tests for HTTP request functionality."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [trexsql.core :as core]
            [trexsql.db :as db]
            [trexsql.http :as http]
            [clojure.data.json :as json])
  (:import [java.util HashMap]))

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
;; Validation Tests
;; =============================================================================

(deftest validate-http-options-test
  (testing "nil method returns error"
    (is (some? (http/validate-http-options {:url "/test"}))))

  (testing "nil url returns error"
    (is (some? (http/validate-http-options {:method "GET"}))))

  (testing "invalid method returns error"
    (is (some? (http/validate-http-options {:method "INVALID" :url "/test"}))))

  (testing "valid GET request passes"
    (is (nil? (http/validate-http-options {:method "GET" :url "/test"}))))

  (testing "valid POST request passes"
    (is (nil? (http/validate-http-options {:method "POST" :url "/test"}))))

  (testing "valid PUT request passes"
    (is (nil? (http/validate-http-options {:method "PUT" :url "/test"}))))

  (testing "valid DELETE request passes"
    (is (nil? (http/validate-http-options {:method "DELETE" :url "/test"}))))

  (testing "valid PATCH request passes"
    (is (nil? (http/validate-http-options {:method "PATCH" :url "/test"}))))

  (testing "case-insensitive method validation"
    (is (nil? (http/validate-http-options {:method "get" :url "/test"})))
    (is (nil? (http/validate-http-options {:method "Post" :url "/test"}))))

  (testing "URL validation - worker paths allowed"
    (is (nil? (http/validate-http-options {:method "GET" :url "/my-worker/api"}))))

  (testing "URL validation - http:// allowed"
    (is (nil? (http/validate-http-options {:method "GET" :url "http://example.com"}))))

  (testing "URL validation - https:// allowed"
    (is (nil? (http/validate-http-options {:method "GET" :url "https://example.com"}))))

  (testing "URL validation - invalid URL rejected"
    (is (some? (http/validate-http-options {:method "GET" :url "not-a-url"}))))

  (testing "headers must be a map"
    (is (some? (http/validate-http-options {:method "GET" :url "/test" :headers "invalid"}))))

  (testing "body must be a string"
    (is (some? (http/validate-http-options {:method "GET" :url "/test" :body 123}))))

  (testing "options must be a map"
    (is (some? (http/validate-http-options {:method "GET" :url "/test" :options "invalid"})))))

(deftest validate-http-options-timeout-test
  (testing "timeout-ms must be positive"
    (is (some? (http/validate-http-options
                {:method "GET" :url "/test" :options {:timeout-ms 0}})))
    (is (some? (http/validate-http-options
                {:method "GET" :url "/test" :options {:timeout-ms -1}}))))

  (testing "timeout-ms must be within limit"
    (is (some? (http/validate-http-options
                {:method "GET" :url "/test" :options {:timeout-ms 700000}}))))

  (testing "valid timeout-ms passes"
    (is (nil? (http/validate-http-options
               {:method "GET" :url "/test" :options {:timeout-ms 5000}})))))

(deftest validate-http-options-redirects-test
  (testing "follow-redirects must be boolean"
    (is (some? (http/validate-http-options
                {:method "GET" :url "/test" :options {:follow-redirects "true"}}))))

  (testing "valid follow-redirects passes"
    (is (nil? (http/validate-http-options
               {:method "GET" :url "/test" :options {:follow-redirects true}})))
    (is (nil? (http/validate-http-options
               {:method "GET" :url "/test" :options {:follow-redirects false}}))))

  (testing "max-redirects must be non-negative"
    (is (some? (http/validate-http-options
                {:method "GET" :url "/test" :options {:max-redirects -1}}))))

  (testing "max-redirects must be within limit"
    (is (some? (http/validate-http-options
                {:method "GET" :url "/test" :options {:max-redirects 25}}))))

  (testing "valid max-redirects passes"
    (is (nil? (http/validate-http-options
               {:method "GET" :url "/test" :options {:max-redirects 5}})))))

;; =============================================================================
;; JSON Building Tests
;; =============================================================================

(deftest build-request-json-test
  (testing "minimal GET request"
    (let [result (json/read-str (http/build-request-json {:method "get" :url "/test"})
                                :key-fn keyword)]
      (is (= "GET" (:method result)))
      (is (= "/test" (:url result)))))

  (testing "POST request with headers and body"
    (let [result (json/read-str
                  (http/build-request-json
                   {:method "POST"
                    :url "/api/data"
                    :headers {"Content-Type" "application/json"}
                    :body "{\"key\":\"value\"}"})
                  :key-fn keyword)]
      (is (= "POST" (:method result)))
      (is (= "/api/data" (:url result)))
      (is (= {:Content-Type "application/json"} (:headers result)))
      (is (= "{\"key\":\"value\"}" (:body result)))))

  (testing "request with options"
    (let [result (json/read-str
                  (http/build-request-json
                   {:method "GET"
                    :url "/test"
                    :options {:timeout-ms 5000
                              :follow-redirects false
                              :max-redirects 3}})
                  :key-fn keyword)]
      (is (= 5000 (get-in result [:options :timeout_ms])))
      (is (= false (get-in result [:options :follow_redirects])))
      (is (= 3 (get-in result [:options :max_redirects]))))))

;; =============================================================================
;; Response Parsing Tests
;; =============================================================================

(deftest parse-response-json-test
  (testing "successful response"
    (let [json-str "{\"success\":true,\"status_code\":200,\"headers\":{\"content-type\":\"application/json\"},\"body\":\"{}\",\"encoding\":\"utf8\"}"
          result (http/parse-response-json json-str)]
      (is (true? (:success result)))
      (is (= 200 (:status-code result)))
      (is (= "application/json" (get-in result [:headers :content-type])))
      (is (= "{}" (:body result)))
      (is (= "utf8" (:encoding result)))))

  (testing "error response"
    (let [json-str "{\"success\":false,\"error\":\"Connection timeout\"}"
          result (http/parse-response-json json-str)]
      (is (false? (:success result)))
      (is (= "Connection timeout" (:error result)))
      (is (nil? (:status-code result)))))

  (testing "truncated response"
    (let [json-str "{\"success\":true,\"status_code\":200,\"body\":\"...\",\"truncated\":true}"
          result (http/parse-response-json json-str)]
      (is (true? (:truncated result)))))

  (testing "nil input returns nil"
    (is (nil? (http/parse-response-json nil)))))

;; =============================================================================
;; Java Map Conversion Tests
;; =============================================================================

(deftest http-response-java-map-test
  (testing "successful response conversion"
    (let [response {:success true
                    :status-code 200
                    :headers {:content-type "application/json"}
                    :body "{\"data\":\"test\"}"
                    :encoding "utf8"}
          result (http/http-response->java-map response)]
      (is (instance? HashMap result))
      (is (true? (.get result "success")))
      (is (= 200 (.get result "status-code")))
      (is (= "application/json" (.get (.get result "headers") "content-type")))
      (is (= "{\"data\":\"test\"}" (.get result "body")))
      (is (= "utf8" (.get result "encoding")))))

  (testing "error response conversion"
    (let [response {:success false
                    :error "Network error"}
          result (http/http-response->java-map response)]
      (is (false? (.get result "success")))
      (is (= "Network error" (.get result "error")))
      (is (nil? (.get result "status-code")))))

  (testing "truncated response conversion"
    (let [response {:success true
                    :status-code 200
                    :body "truncated..."
                    :truncated true}
          result (http/http-response->java-map response)]
      (is (true? (.get result "truncated"))))))

;; =============================================================================
;; Edge Case Tests
;; =============================================================================

(deftest edge-cases-test
  (testing "empty headers allowed"
    (is (nil? (http/validate-http-options {:method "GET" :url "/test" :headers {}}))))

  (testing "nil body allowed"
    (is (nil? (http/validate-http-options {:method "POST" :url "/test" :body nil}))))

  (testing "empty options allowed"
    (is (nil? (http/validate-http-options {:method "GET" :url "/test" :options {}})))))
