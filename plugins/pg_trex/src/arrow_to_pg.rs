// Converts Arrow RecordBatch data to PostgreSQL-compatible string representations.
// Supports tab-separated row output for SETOF RECORD functions and per-value
// formatting for all common Arrow types.

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use std::io::Cursor;

/// Convert Arrow RecordBatches into tab-separated string rows.
/// NULL values are represented as "NULL".
pub fn batches_to_string_rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();

    for batch in batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            let mut parts = Vec::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let array = batch.column(col_idx);
                parts.push(arrow_value_to_string(array.as_ref(), row_idx));
            }
            rows.push(parts.join("\t"));
        }
    }

    rows
}

/// Convert Arrow RecordBatches into structured rows with proper null tracking.
///
/// Each row is a `Vec<Option<String>>` where `None` represents a SQL NULL and
/// `Some(val)` is the string representation of the value. This avoids the
/// ambiguity of tab-separated encoding where tab characters in data or empty
/// strings could be confused with delimiters or NULLs.
pub fn batches_to_structured_rows(batches: &[RecordBatch]) -> Vec<Vec<Option<String>>> {
    let mut rows = Vec::new();

    for batch in batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            let mut row = Vec::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let array = batch.column(col_idx);
                if array.is_null(row_idx) {
                    row.push(None);
                } else {
                    row.push(Some(arrow_value_to_string(array.as_ref(), row_idx)));
                }
            }
            rows.push(row);
        }
    }

    rows
}

/// Convert a single Arrow array value at the given row index to its PostgreSQL
/// string representation.
///
/// Type mappings:
/// - Int8/Int16/Int32/Int64        -> decimal string
/// - UInt8/UInt16/UInt32/UInt64    -> decimal string
/// - Float32/Float64               -> decimal string
/// - Utf8/LargeUtf8                -> the string value
/// - Boolean                       -> "true"/"false"
/// - Date32                        -> "YYYY-MM-DD" (days since Unix epoch)
/// - Timestamp(Microsecond, _)     -> "YYYY-MM-DD HH:MM:SS.ffffff"
/// - Decimal128(_, scale)          -> formatted decimal string
/// - Binary/LargeBinary            -> hex-encoded ("\\x" prefix)
/// - Null / is_null                -> "NULL"
pub fn arrow_value_to_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(row).to_string()
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            if arr.value(row) {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row);
            format_date_from_unix_days(days)
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let ms = arr.value(row);
            let days = (ms / 86_400_000) as i32;
            format_date_from_unix_days(days)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let us = arr.value(row);
            format_timestamp_from_unix_us(us)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let ms = arr.value(row);
            format_timestamp_from_unix_us(ms * 1_000)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let s = arr.value(row);
            format_timestamp_from_unix_us(s * 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let ns = arr.value(row);
            format_timestamp_from_unix_us(ns / 1_000)
        }
        DataType::Time32(TimeUnit::Second) => {
            let arr = array.as_any().downcast_ref::<Time32SecondArray>().unwrap();
            let s = arr.value(row);
            format_time_from_seconds(s as i64)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .unwrap();
            let ms = arr.value(row) as i64;
            let total_secs = ms / 1_000;
            let frac_ms = (ms % 1_000).unsigned_abs();
            let h = total_secs / 3600;
            let m = (total_secs % 3600) / 60;
            let s = total_secs % 60;
            format!("{:02}:{:02}:{:02}.{:03}", h, m, s, frac_ms)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            let us = arr.value(row);
            let total_secs = us / 1_000_000;
            let frac_us = (us % 1_000_000).unsigned_abs();
            let h = total_secs / 3600;
            let m = (total_secs % 3600) / 60;
            let s = total_secs % 60;
            format!("{:02}:{:02}:{:02}.{:06}", h, m, s, frac_us)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap();
            let ns = arr.value(row);
            let total_secs = ns / 1_000_000_000;
            let frac_ns = (ns % 1_000_000_000).unsigned_abs();
            let h = total_secs / 3600;
            let m = (total_secs % 3600) / 60;
            let s = total_secs % 60;
            format!("{:02}:{:02}:{:02}.{:09}", h, m, s, frac_ns)
        }
        DataType::Decimal128(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            let raw = arr.value(row);
            if *scale >= 0 {
                format_decimal128(raw, *scale as u32)
            } else {
                // Negative scale means multiply by 10^|scale|; just show as integer.
                let multiplier = 10i128.pow((-*scale) as u32);
                (raw * multiplier).to_string()
            }
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let bytes = arr.value(row);
            format_hex(bytes)
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let bytes = arr.value(row);
            format_hex(bytes)
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let bytes = arr.value(row);
            format_hex(bytes)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let arr = array
                .as_any()
                .downcast_ref::<IntervalDayTimeArray>()
                .unwrap();
            let val = arr.value(row);
            format!("{} days {} ms", val.days, val.milliseconds)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            let arr = array
                .as_any()
                .downcast_ref::<IntervalYearMonthArray>()
                .unwrap();
            let months = arr.value(row);
            let years = months / 12;
            let rem = months % 12;
            format!("{} years {} mons", years, rem)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<DurationMicrosecondArray>()
                .unwrap();
            let us = arr.value(row);
            format!("{} us", us)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<DurationMillisecondArray>()
                .unwrap();
            let ms = arr.value(row);
            format!("{} ms", ms)
        }
        DataType::Duration(TimeUnit::Second) => {
            let arr = array
                .as_any()
                .downcast_ref::<DurationSecondArray>()
                .unwrap();
            let s = arr.value(row);
            format!("{} s", s)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<DurationNanosecondArray>()
                .unwrap();
            let ns = arr.value(row);
            format!("{} ns", ns)
        }
        DataType::List(_) => {
            let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
            let inner = arr.value(row);
            format_list_array(inner.as_ref())
        }
        DataType::LargeList(_) => {
            let arr = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let inner = arr.value(row);
            format_list_array(inner.as_ref())
        }
        DataType::Struct(_) => {
            let arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            format_struct_value(arr, row)
        }
        DataType::Null => "NULL".to_string(),
        other => format!("<unsupported:{}>", other),
    }
}

/// Extract column names from an Arrow schema.
pub fn arrow_schema_to_column_names(schema: &Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// Deserialize Arrow IPC stream bytes into schema + record batches.
///
/// The input `data` should be a complete Arrow IPC stream (stream format,
/// not file format), as produced by `arrow_ipc::writer::StreamWriter`.
pub fn deserialize_arrow_ipc(data: &[u8]) -> Result<(Schema, Vec<RecordBatch>), String> {
    let cursor = Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
        format!("failed to create Arrow IPC StreamReader: {}", e)
    })?;

    let schema = reader.schema().as_ref().clone();
    let mut batches = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            format!("failed to read Arrow IPC RecordBatch: {}", e)
        })?;
        batches.push(batch);
    }

    Ok((schema, batches))
}

/// Format a Unix epoch day count as "YYYY-MM-DD".
fn format_date_from_unix_days(days: i32) -> String {
    // Howard Hinnant's civil_from_days algorithm.
    let z = days as i64 + 719468; // shift to epoch 0000-03-01
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // month index [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // day [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]
    let y = if m <= 2 { y + 1 } else { y };

    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// Format microseconds since Unix epoch as "YYYY-MM-DD HH:MM:SS.ffffff".
fn format_timestamp_from_unix_us(us: i64) -> String {
    let (total_secs, frac_us) = if us >= 0 {
        (us / 1_000_000, (us % 1_000_000) as u32)
    } else {
        // Negative timestamps (before 1970): use Euclidean division for correct fractional part.
        let s = us.div_euclid(1_000_000);
        let f = us.rem_euclid(1_000_000) as u32;
        (s, f)
    };

    let days = total_secs.div_euclid(86_400) as i32;
    let day_secs = total_secs.rem_euclid(86_400) as u32;

    let date = format_date_from_unix_days(days);
    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;

    format!("{} {:02}:{:02}:{:02}.{:06}", date, h, m, s, frac_us)
}

/// Format seconds since midnight as "HH:MM:SS".
fn format_time_from_seconds(total_secs: i64) -> String {
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

/// Format an i128 Decimal128 value with the given scale.
///
/// For example, raw=12345 with scale=2 produces "123.45".
fn format_decimal128(raw: i128, scale: u32) -> String {
    if scale == 0 {
        return raw.to_string();
    }

    let is_negative = raw < 0;
    let abs_val = raw.unsigned_abs();
    let divisor = 10u128.pow(scale);
    let integer_part = abs_val / divisor;
    let fractional_part = abs_val % divisor;

    if is_negative {
        format!(
            "-{}.{:0>width$}",
            integer_part,
            fractional_part,
            width = scale as usize
        )
    } else {
        format!(
            "{}.{:0>width$}",
            integer_part,
            fractional_part,
            width = scale as usize
        )
    }
}

/// Format bytes as PostgreSQL-style hex string ("\\x" prefix + lowercase hex).
fn format_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("\\x");
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Format a list/array value as "{elem1,elem2,...}" (PostgreSQL array literal).
fn format_list_array(array: &dyn Array) -> String {
    let mut parts = Vec::with_capacity(array.len());
    for i in 0..array.len() {
        parts.push(arrow_value_to_string(array, i));
    }
    format!("{{{}}}", parts.join(","))
}

/// Format a struct value as "(field1,field2,...)" (PostgreSQL composite literal).
fn format_struct_value(array: &StructArray, row: usize) -> String {
    let mut parts = Vec::with_capacity(array.num_columns());
    for col_idx in 0..array.num_columns() {
        let col = array.column(col_idx);
        parts.push(arrow_value_to_string(col.as_ref(), row));
    }
    format!("({})", parts.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::*;
    use std::sync::Arc;

    #[test]
    fn test_int_types() {
        let arr = Int32Array::from(vec![Some(42), None, Some(-1)]);
        assert_eq!(arrow_value_to_string(&arr, 0), "42");
        assert_eq!(arrow_value_to_string(&arr, 1), "NULL");
        assert_eq!(arrow_value_to_string(&arr, 2), "-1");

        let arr = Int64Array::from(vec![Some(9_999_999_999i64)]);
        assert_eq!(arrow_value_to_string(&arr, 0), "9999999999");
    }

    #[test]
    fn test_uint_types() {
        let arr = UInt8Array::from(vec![Some(255u8)]);
        assert_eq!(arrow_value_to_string(&arr, 0), "255");

        let arr = UInt64Array::from(vec![Some(u64::MAX)]);
        assert_eq!(arrow_value_to_string(&arr, 0), u64::MAX.to_string());
    }

    #[test]
    fn test_float_types() {
        let arr = Float32Array::from(vec![Some(3.14f32)]);
        assert_eq!(arrow_value_to_string(&arr, 0), "3.14");

        let arr = Float64Array::from(vec![Some(2.718281828f64)]);
        assert_eq!(arrow_value_to_string(&arr, 0), "2.718281828");
    }

    #[test]
    fn test_string_types() {
        let arr = StringArray::from(vec![Some("hello"), None, Some("")]);
        assert_eq!(arrow_value_to_string(&arr, 0), "hello");
        assert_eq!(arrow_value_to_string(&arr, 1), "NULL");
        assert_eq!(arrow_value_to_string(&arr, 2), "");

        let arr = LargeStringArray::from(vec![Some("large string")]);
        assert_eq!(arrow_value_to_string(&arr, 0), "large string");
    }

    #[test]
    fn test_boolean() {
        let arr = BooleanArray::from(vec![Some(true), Some(false), None]);
        assert_eq!(arrow_value_to_string(&arr, 0), "true");
        assert_eq!(arrow_value_to_string(&arr, 1), "false");
        assert_eq!(arrow_value_to_string(&arr, 2), "NULL");
    }

    #[test]
    fn test_date32() {
        // 1970-01-01 = day 0
        let arr = Date32Array::from(vec![Some(0)]);
        assert_eq!(arrow_value_to_string(&arr, 0), "1970-01-01");

        // 2024-01-15 = day 19737 (19723 for 2024-01-01 + 14)
        let arr = Date32Array::from(vec![Some(19737)]);
        assert_eq!(arrow_value_to_string(&arr, 0), "2024-01-15");

        // 2000-01-01 = day 10957
        let arr = Date32Array::from(vec![Some(10957)]);
        assert_eq!(arrow_value_to_string(&arr, 0), "2000-01-01");
    }

    #[test]
    fn test_timestamp_microsecond() {
        // 1970-01-01 00:00:00.000000
        let arr = TimestampMicrosecondArray::from(vec![Some(0i64)]);
        assert_eq!(
            arrow_value_to_string(&arr, 0),
            "1970-01-01 00:00:00.000000"
        );

        // 2024-01-15 12:30:45.123456
        // 2024-01-15 = 19737 days from epoch (19723 for 2024-01-01 + 14)
        // 12:30:45 = 45045 seconds
        // total us = (19737 * 86400 + 45045) * 1_000_000 + 123456
        let us = (19737i64 * 86400 + 45045) * 1_000_000 + 123456;
        let arr = TimestampMicrosecondArray::from(vec![Some(us)]);
        assert_eq!(
            arrow_value_to_string(&arr, 0),
            "2024-01-15 12:30:45.123456"
        );
    }

    #[test]
    fn test_decimal128() {
        assert_eq!(format_decimal128(12345, 2), "123.45");
        assert_eq!(format_decimal128(-12345, 2), "-123.45");
        assert_eq!(format_decimal128(100, 2), "1.00");
        assert_eq!(format_decimal128(5, 3), "0.005");
        assert_eq!(format_decimal128(42, 0), "42");
    }

    #[test]
    fn test_binary() {
        let arr = BinaryArray::from(vec![Some(&[0xDE, 0xAD, 0xBE, 0xEF][..])]);
        assert_eq!(arrow_value_to_string(&arr, 0), "\\xdeadbeef");
    }

    #[test]
    fn test_null_array() {
        let arr = NullArray::new(3);
        assert_eq!(arrow_value_to_string(&arr, 0), "NULL");
        assert_eq!(arrow_value_to_string(&arr, 2), "NULL");
    }

    #[test]
    fn test_batches_to_string_rows() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), None, Some("charlie")])),
            ],
        )
        .unwrap();

        let rows = batches_to_string_rows(&[batch]);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], "1\talice");
        assert_eq!(rows[1], "2\tNULL");
        assert_eq!(rows[2], "3\tcharlie");
    }

    #[test]
    fn test_batches_to_string_rows_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let b1 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();
        let b2 = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![3]))]).unwrap();

        let rows = batches_to_string_rows(&[b1, b2]);
        assert_eq!(rows, vec!["1", "2", "3"]);
    }

    #[test]
    fn test_arrow_schema_to_column_names() {
        let schema = Schema::new(vec![
            Field::new("col_a", DataType::Int32, false),
            Field::new("col_b", DataType::Utf8, true),
            Field::new("col_c", DataType::Float64, false),
        ]);
        let names = arrow_schema_to_column_names(&schema);
        assert_eq!(names, vec!["col_a", "col_b", "col_c"]);
    }

    #[test]
    fn test_deserialize_arrow_ipc_roundtrip() {
        use arrow_ipc::writer::StreamWriter;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec![Some("foo"), Some("bar")])),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let (out_schema, out_batches) = deserialize_arrow_ipc(&buf).unwrap();
        assert_eq!(out_schema.fields().len(), 2);
        assert_eq!(out_schema.field(0).name(), "id");
        assert_eq!(out_schema.field(1).name(), "val");
        assert_eq!(out_batches.len(), 1);
        assert_eq!(out_batches[0].num_rows(), 2);
    }

    #[test]
    fn test_deserialize_arrow_ipc_invalid_data() {
        let result = deserialize_arrow_ipc(&[0xFF, 0x00, 0x01]);
        assert!(result.is_err());
    }

    #[test]
    fn test_format_date_known_dates() {
        // 1969-12-31 = day -1
        assert_eq!(format_date_from_unix_days(-1), "1969-12-31");
        // 1970-01-02 = day 1
        assert_eq!(format_date_from_unix_days(1), "1970-01-02");
        // 2000-02-29 (leap year) = day 11016 (10957 for 2000-01-01 + 31 Jan + 28 Feb)
        assert_eq!(format_date_from_unix_days(11016), "2000-02-29");
    }

    #[test]
    fn test_batches_to_structured_rows() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), None, Some("")])),
            ],
        )
        .unwrap();

        let rows = batches_to_structured_rows(&[batch]);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![Some("1".to_string()), Some("alice".to_string())]);
        assert_eq!(rows[1], vec![Some("2".to_string()), None]);
        // Empty string is preserved as Some(""), not treated as NULL
        assert_eq!(rows[2], vec![Some("3".to_string()), Some("".to_string())]);
    }

    #[test]
    fn test_structured_rows_with_tabs() {
        let schema = Schema::new(vec![
            Field::new("val", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![Some("hello\tworld")])),
            ],
        )
        .unwrap();

        let rows = batches_to_structured_rows(&[batch]);
        assert_eq!(rows.len(), 1);
        // Tab character is preserved in the value, not split
        assert_eq!(rows[0], vec![Some("hello\tworld".to_string())]);
    }

    #[test]
    fn test_list_array() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
        ]);
        assert_eq!(arrow_value_to_string(&list, 0), "{1,2,3}");
        assert_eq!(arrow_value_to_string(&list, 1), "{4,5}");
    }
}
