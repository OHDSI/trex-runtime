use chrono::{DateTime, TimeDelta};
use duckdb::Rows;
use futures::stream;
use futures::Stream;
use pgwire::messages::data::DataRow;
use std::sync::Arc;

use duckdb::arrow::datatypes::DataType;
use duckdb::types::ValueRef;
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

pub fn into_pg_type(df_type: &DataType) -> PgWireResult<Type> {
  Ok(match df_type {
    DataType::Null => Type::UNKNOWN,
    DataType::Boolean => Type::BOOL,
    DataType::Int8 | DataType::UInt8 => Type::CHAR,
    DataType::Int16 | DataType::UInt16 => Type::INT2,
    DataType::Int32 | DataType::UInt32 => Type::INT4,
    DataType::Int64 | DataType::UInt64 => Type::INT8,
    DataType::Timestamp(_, _) => Type::TIMESTAMP,
    DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
    DataType::Date32 | DataType::Date64 => Type::DATE,
    DataType::Binary => Type::BYTEA,
    DataType::Float32 => Type::FLOAT4,
    DataType::Float64 => Type::FLOAT8,
    DataType::Decimal128(_, _) => Type::INT8,
    DataType::Utf8 => Type::VARCHAR,
    DataType::List(field) => match field.data_type() {
      DataType::Boolean => Type::BOOL_ARRAY,
      DataType::Int8 | DataType::UInt8 => Type::CHAR_ARRAY,
      DataType::Int16 | DataType::UInt16 => Type::INT2_ARRAY,
      DataType::Int32 | DataType::UInt32 => Type::INT4_ARRAY,
      DataType::Int64 | DataType::UInt64 => Type::INT8_ARRAY,
      DataType::Timestamp(_, _) => Type::TIMESTAMP_ARRAY,
      DataType::Time32(_) | DataType::Time64(_) => Type::TIME_ARRAY,
      DataType::Date32 | DataType::Date64 => Type::DATE_ARRAY,
      DataType::Binary => Type::BYTEA_ARRAY,
      DataType::Float32 => Type::FLOAT4_ARRAY,
      DataType::Float64 => Type::FLOAT8_ARRAY,
      DataType::Utf8 => Type::VARCHAR_ARRAY,
      list_type => {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
          "ERROR".to_owned(),
          "XX000".to_owned(),
          format!("Unsupported List Datatype {list_type}"),
        ))));
      }
    },
    _ => {
      return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        format!("Unsupported Datatype {df_type}"),
      ))));
    }
  })
}

pub fn encode_row_data(
  mut rows: Rows<'_>,
  schema: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
  let mut results = Vec::new();
  let ncols = schema.len();
  while let Ok(Some(row)) = rows.next() {
    let mut encoder = DataRowEncoder::new(schema.clone());
    for idx in 0..ncols {
      let data = row.get_ref_unwrap::<usize>(idx);
      match data {
        ValueRef::Null => encoder.encode_field(&None::<i8>).unwrap(),
        ValueRef::Boolean(b) => {
          encoder.encode_field(&b).unwrap();
        }
        ValueRef::TinyInt(i) => {
          encoder.encode_field(&i).unwrap();
        }
        ValueRef::SmallInt(i) => {
          encoder.encode_field(&i).unwrap();
        }
        ValueRef::Int(i) => {
          encoder.encode_field(&i).unwrap();
        }
        ValueRef::BigInt(i) => {
          encoder.encode_field(&i).unwrap();
        }
        ValueRef::Float(f) => {
          encoder.encode_field(&f).unwrap();
        }
        ValueRef::Double(f) => {
          encoder.encode_field(&f).unwrap();
        }
        ValueRef::Text(t) => {
          encoder
            .encode_field(&String::from_utf8_lossy(t).as_ref())
            .unwrap();
        }
        ValueRef::Date32(d) => {
          let mut t2 = i64::from(d);
          let mut t3 = 0;
          if t2 < 0 {
            t3 = -t2;
            t2 = 0;
          }
          encoder
            .encode_field(
              &DateTime::from_timestamp(t2, 0)
                .expect("TREX: Date conversion failed")
                .checked_sub_signed(TimeDelta::seconds(t3))
                .expect("TREX: Date conversion failed")
                .format("%Y-%m-%d")
                .to_string(),
            )
            .unwrap();
        }
        ValueRef::Timestamp(_unit, time) => {
          let mut t2 = time / 1000 / 1000;
          let mut t3 = 0;
          if t2 < 0 {
            t3 = -t2;
            t2 = 0;
          }
          encoder
            .encode_field(
              &DateTime::from_timestamp(t2, 0)
                .expect("TREX: Date conversion failed")
                .checked_sub_signed(TimeDelta::seconds(t3))
                .expect("TREX: Date conversion failed")
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            )
            .unwrap();
        }
        ValueRef::Blob(b) => {
          encoder.encode_field(&b).unwrap();
        }
        _ => {
          unimplemented!("More types to be supported. {:?}", data)
        }
      }
    }

    results.push(encoder.finish());
  }

  stream::iter(results)
}
