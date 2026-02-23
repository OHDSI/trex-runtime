use etl_lib::types::Cell;
use etl_lib::types::Type;

pub fn pg_type_to_duckdb(typ: &Type) -> &'static str {
    match *typ {
        Type::BOOL => "BOOLEAN",
        Type::INT2 => "SMALLINT",
        Type::INT4 => "INTEGER",
        Type::INT8 => "BIGINT",
        Type::FLOAT4 => "FLOAT",
        Type::FLOAT8 => "DOUBLE",
        Type::NUMERIC => "DECIMAL",
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => "VARCHAR",
        Type::BYTEA => "BLOB",
        Type::DATE => "DATE",
        Type::TIME => "TIME",
        Type::TIMESTAMP => "TIMESTAMP",
        Type::TIMESTAMPTZ => "TIMESTAMPTZ",
        Type::UUID => "UUID",
        Type::JSON | Type::JSONB => "JSON",
        Type::BOOL_ARRAY
        | Type::INT2_ARRAY
        | Type::INT4_ARRAY
        | Type::INT8_ARRAY
        | Type::FLOAT4_ARRAY
        | Type::FLOAT8_ARRAY
        | Type::TEXT_ARRAY
        | Type::VARCHAR_ARRAY => "VARCHAR",
        Type::INTERVAL => "VARCHAR",
        Type::OID => "UINTEGER",
        _ => "VARCHAR",
    }
}

pub fn cell_to_sql_literal(cell: &Cell) -> String {
    match cell {
        Cell::Null => "NULL".to_string(),
        Cell::Bool(v) => {
            if *v {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Cell::String(v) => format!("'{}'", v.replace('\'', "''")),
        Cell::I16(v) => v.to_string(),
        Cell::I32(v) => v.to_string(),
        Cell::U32(v) => v.to_string(),
        Cell::I64(v) => v.to_string(),
        Cell::F32(v) => format!("{}", v),
        Cell::F64(v) => format!("{}", v),
        Cell::Numeric(v) => format!("'{}'", v.to_string().replace('\'', "''")),
        Cell::Date(v) => format!("'{}'", v.to_string().replace('\'', "''")),
        Cell::Time(v) => format!("'{}'", v.to_string().replace('\'', "''")),
        Cell::Timestamp(v) => format!("'{}'", v.to_string().replace('\'', "''")),
        Cell::TimestampTz(v) => format!("'{}'", v.to_string().replace('\'', "''")),
        Cell::Uuid(v) => format!("'{}'", v.to_string().replace('\'', "''")),
        Cell::Json(v) => format!("'{}'", v.to_string().replace('\'', "''")),
        Cell::Bytes(v) => {
            let hex: String = v.iter().map(|b| format!("{:02x}", b)).collect();
            format!("'\\x{}'::BLOB", hex)
        }
        Cell::Array(v) => format!("'{}'", format!("{:?}", v).replace('\'', "''")),
    }
}
