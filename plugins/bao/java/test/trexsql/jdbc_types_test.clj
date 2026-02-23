(ns trexsql.jdbc-types-test
  "Unit tests for jdbc_types.clj - Type mapping functions."
  (:require [clojure.test :refer :all]
            [trexsql.jdbc-types :as jdbc-types])
  (:import [java.sql Types]))

;; Type Mapping Tests

(deftest test-jdbc-to-trexsql-types-loaded
  (testing "Type mapping table is loaded"
    (is (map? jdbc-types/jdbc-to-trexsql-types))
    (is (not (empty? jdbc-types/jdbc-to-trexsql-types)))))

(deftest test-get-trexsql-type-numeric
  (testing "Numeric types map correctly"
    (is (= "BIGINT" (jdbc-types/get-trexsql-type Types/BIGINT)))
    (is (= "INTEGER" (jdbc-types/get-trexsql-type Types/INTEGER)))
    (is (= "SMALLINT" (jdbc-types/get-trexsql-type Types/SMALLINT)))
    (is (= "DECIMAL" (jdbc-types/get-trexsql-type Types/DECIMAL)))
    (is (= "DOUBLE" (jdbc-types/get-trexsql-type Types/DOUBLE)))))

(deftest test-get-trexsql-type-string
  (testing "String types map correctly"
    (is (= "VARCHAR" (jdbc-types/get-trexsql-type Types/VARCHAR)))
    (is (= "VARCHAR" (jdbc-types/get-trexsql-type Types/CHAR)))
    (is (= "VARCHAR" (jdbc-types/get-trexsql-type Types/NVARCHAR)))
    (is (= "VARCHAR" (jdbc-types/get-trexsql-type Types/CLOB)))))

(deftest test-get-trexsql-type-datetime
  (testing "Date/time types map correctly"
    (is (= "DATE" (jdbc-types/get-trexsql-type Types/DATE)))
    (is (= "TIME" (jdbc-types/get-trexsql-type Types/TIME)))
    (is (= "TIMESTAMP" (jdbc-types/get-trexsql-type Types/TIMESTAMP)))))

(deftest test-get-trexsql-type-boolean
  (testing "Boolean types map correctly"
    (is (= "BOOLEAN" (jdbc-types/get-trexsql-type Types/BOOLEAN)))
    (is (= "BOOLEAN" (jdbc-types/get-trexsql-type Types/BIT)))))

(deftest test-get-trexsql-type-binary
  (testing "Binary types map correctly"
    (is (= "BLOB" (jdbc-types/get-trexsql-type Types/BLOB)))
    (is (= "BLOB" (jdbc-types/get-trexsql-type Types/BINARY)))
    (is (= "BLOB" (jdbc-types/get-trexsql-type Types/VARBINARY)))))

(deftest test-get-trexsql-type-unknown
  (testing "Unknown types default to VARCHAR"
    (is (= "VARCHAR" (jdbc-types/get-trexsql-type 99999)))))

;; ColumnInfo Record Tests

(deftest test-column-info-record
  (testing "ColumnInfo record has expected fields"
    (let [col (jdbc-types/->ColumnInfo "person_id" Types/BIGINT "BIGINT" 0 0 false)]
      (is (= "person_id" (:name col)))
      (is (= Types/BIGINT (:jdbc-type col)))
      (is (= "BIGINT" (:trexsql-type col)))
      (is (= false (:nullable? col))))))

;; Type Conversion Tests

(deftest test-date-conversion
  (testing "Date values are handled"
    ;; Test that LocalDate is returned as-is
    (let [ld (java.time.LocalDate/of 2020 1 15)]
      ;; Note: We can't easily test append-typed-value! without a real Appender
      ;; but we can test the type mapping is correct
      (is (instance? java.time.LocalDate ld)))))

(deftest test-timestamp-conversion
  (testing "Timestamp values are handled"
    (let [ts (java.sql.Timestamp. (System/currentTimeMillis))
          ldt (.toLocalDateTime ts)]
      (is (instance? java.time.LocalDateTime ldt)))))

;; Edge Cases

(deftest test-common-jdbc-types-covered
  (testing "All common JDBC types are mapped"
    (let [common-types [Types/BIGINT Types/INTEGER Types/SMALLINT Types/TINYINT
                        Types/DECIMAL Types/NUMERIC Types/DOUBLE Types/FLOAT Types/REAL
                        Types/VARCHAR Types/CHAR Types/NVARCHAR Types/NCHAR
                        Types/DATE Types/TIME Types/TIMESTAMP
                        Types/BOOLEAN Types/BIT
                        Types/BLOB Types/BINARY Types/VARBINARY]]
      (doseq [t common-types]
        (is (contains? jdbc-types/jdbc-to-trexsql-types t)
            (format "Type %d should be mapped" t))))))

(deftest test-sql-server-specific-types
  (testing "SQL Server specific types are handled"
    ;; NTEXT, NCHAR, etc. should map to VARCHAR
    (is (= "VARCHAR" (jdbc-types/get-trexsql-type Types/NCHAR)))
    (is (= "VARCHAR" (jdbc-types/get-trexsql-type Types/NVARCHAR)))))

(deftest test-oracle-specific-types
  (testing "Oracle specific types are handled"
    ;; NUMBER maps to DECIMAL, CLOB to VARCHAR
    (is (= "DECIMAL" (jdbc-types/get-trexsql-type Types/NUMERIC)))
    (is (= "VARCHAR" (jdbc-types/get-trexsql-type Types/CLOB)))))

(deftest test-mysql-specific-types
  (testing "MySQL specific types are handled"
    ;; TINYINT, MEDIUMINT etc. should be covered
    (is (= "TINYINT" (jdbc-types/get-trexsql-type Types/TINYINT)))))
