(ns trexsql.jdbc-types
  "JDBC to TrexSQL type mapping and value conversion."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [trexsql.native :as native])
  (:import [java.sql ResultSet ResultSetMetaData Types]
           [java.time LocalDate LocalDateTime LocalTime]
           [java.math BigDecimal]
           [com.sun.jna Pointer]))

(def jdbc-to-trexsql-types
  "JDBC type codes to TrexSQL type names. Loaded from jdbc-type-mappings.edn."
  (if-let [resource (io/resource "jdbc-type-mappings.edn")]
    (edn/read-string (slurp resource))
    {Types/BIGINT     "BIGINT"
     Types/INTEGER    "INTEGER"
     Types/SMALLINT   "SMALLINT"
     Types/TINYINT    "TINYINT"
     Types/DECIMAL    "DECIMAL"
     Types/NUMERIC    "DECIMAL"
     Types/DOUBLE     "DOUBLE"
     Types/FLOAT      "FLOAT"
     Types/REAL       "REAL"
     Types/VARCHAR    "VARCHAR"
     Types/CHAR       "VARCHAR"
     Types/NVARCHAR   "VARCHAR"
     Types/NCHAR      "VARCHAR"
     Types/LONGVARCHAR "VARCHAR"
     Types/CLOB       "VARCHAR"
     Types/NCLOB      "VARCHAR"
     Types/DATE       "DATE"
     Types/TIME       "TIME"
     Types/TIMESTAMP  "TIMESTAMP"
     Types/BOOLEAN    "BOOLEAN"
     Types/BIT        "BOOLEAN"
     Types/BLOB       "BLOB"
     Types/BINARY     "BLOB"
     Types/VARBINARY  "BLOB"
     Types/NULL       "VARCHAR"}))

(defn get-trexsql-type
  "Map JDBC type code to TrexSQL type. Defaults to VARCHAR."
  [jdbc-type]
  (let [trexsql-type (get jdbc-to-trexsql-types jdbc-type "VARCHAR")]
    (when-not (contains? jdbc-to-trexsql-types jdbc-type)
      (log/warn (format "Unmapped JDBC type: %d, defaulting to VARCHAR" jdbc-type)))
    trexsql-type))

(defrecord ColumnInfo [name jdbc-type trexsql-type precision scale nullable?])

(defn get-column-info
  "Extract column metadata from JDBC ResultSetMetaData."
  [^ResultSetMetaData metadata]
  (let [col-count (.getColumnCount metadata)]
    (vec
     (for [i (range 1 (inc col-count))]
       (let [jdbc-type (.getColumnType metadata i)]
         (->ColumnInfo
          (.getColumnName metadata i)
          jdbc-type
          (get-trexsql-type jdbc-type)
          (.getPrecision metadata i)
          (.getScale metadata i)
          (= (.isNullable metadata i) ResultSetMetaData/columnNullable)))))))

(defn- to-local-date [value]
  (cond
    (instance? LocalDate value) value
    (instance? java.sql.Date value) (.toLocalDate ^java.sql.Date value)
    (instance? java.util.Date value) (-> ^java.util.Date value .toInstant (.atZone (java.time.ZoneId/systemDefault)) .toLocalDate)
    (string? value) (LocalDate/parse value)
    :else (LocalDate/parse (str value))))

(defn- to-local-time [value]
  (cond
    (instance? LocalTime value) value
    (instance? java.sql.Time value) (.toLocalTime ^java.sql.Time value)
    (string? value) (LocalTime/parse value)
    :else (LocalTime/parse (str value))))

(defn- to-local-datetime [value]
  (cond
    (instance? LocalDateTime value) value
    (instance? java.sql.Timestamp value) (.toLocalDateTime ^java.sql.Timestamp value)
    (instance? java.util.Date value) (-> ^java.util.Date value .toInstant (.atZone (java.time.ZoneId/systemDefault)) .toLocalDateTime)
    (string? value) (LocalDateTime/parse value)
    :else (LocalDateTime/parse (str value))))

(defn append-typed-value!
  "Append value to TrexSQL native Appender with type conversion.
   Uses native C API appender functions."
  [^Pointer appender value trexsql-type]
  (if (nil? value)
    (native/appender-append-null! appender)
    (case trexsql-type
      "BIGINT"    (native/appender-append-long! appender (long value))
      "INTEGER"   (native/appender-append-int! appender (int value))
      "SMALLINT"  (native/appender-append-int! appender (int (short value)))
      "TINYINT"   (native/appender-append-int! appender (int (byte value)))
      "DOUBLE"    (native/appender-append-double! appender (double value))
      "FLOAT"     (native/appender-append-double! appender (double (float value)))
      "REAL"      (native/appender-append-double! appender (double (float value)))
      "DECIMAL"   (native/appender-append-string! appender (str (if (instance? BigDecimal value)
                                                                    value
                                                                    (BigDecimal. (str value)))))
      "VARCHAR"   (native/appender-append-string! appender (str value))
      "BOOLEAN"   (native/appender-append-boolean! appender (boolean value))
      "DATE"      (native/appender-append-string! appender (str (to-local-date value)))
      "TIME"      (native/appender-append-string! appender (str (to-local-time value)))
      "TIMESTAMP" (native/appender-append-string! appender (str (to-local-datetime value)))
      "BLOB"      (native/appender-append-string! appender (if (bytes? value)
                                                               (String. ^bytes value "UTF-8")
                                                               (str value)))
      (native/appender-append-string! appender (str value)))))

(defn read-typed-value
  "Read value from ResultSet with type handling. Returns nil for SQL NULL."
  [^ResultSet rs ^long col-idx trexsql-type]
  (let [value (case trexsql-type
                "BIGINT"    (.getLong rs col-idx)
                "INTEGER"   (.getInt rs col-idx)
                "SMALLINT"  (.getShort rs col-idx)
                "TINYINT"   (.getByte rs col-idx)
                "DOUBLE"    (.getDouble rs col-idx)
                "FLOAT"     (.getFloat rs col-idx)
                "REAL"      (.getFloat rs col-idx)
                "DECIMAL"   (.getBigDecimal rs col-idx)
                "VARCHAR"   (.getString rs col-idx)
                "BOOLEAN"   (.getBoolean rs col-idx)
                "DATE"      (.getDate rs col-idx)
                "TIME"      (.getTime rs col-idx)
                "TIMESTAMP" (.getTimestamp rs col-idx)
                "BLOB"      (.getBytes rs col-idx)
                (.getObject rs col-idx))]
    (when-not (.wasNull rs)
      value)))
