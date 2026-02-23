use duckdb::{
    vtab::{BindInfo, InitInfo, VTab, TableFunctionInfo},
    core::{LogicalTypeId, LogicalTypeHandle, DataChunkHandle, Inserter},
    vscalar::{VScalar, ScalarFunctionSignature},
    vtab::arrow::WritableVector,
};
use std::error::Error;
use std::sync::RwLock;

use crate::hana_scan::{validate_hana_connection, safe_hana_connect, HanaError};
use crate::hana_state::{
    self, TableAttachmentInfo, AttachedDatabase,
    prefixed_name, duckdb_schema_name, attachment_key, escape_identifier,
    execute_ddl, execute_ddl_batch,
};

#[derive(Debug)]
pub struct HanaAttachBindData {
    url: String,
    dbname: String,
    schema: String,
    table_names: Vec<String>,
}

#[derive(Debug)]
pub struct HanaAttachInitData {
    tables: Vec<(String, String)>,
    ddl_statements: Vec<String>,
    current_row: RwLock<usize>,
    ddl_done: RwLock<bool>,
}

pub struct HanaAttachVTab;

impl VTab for HanaAttachVTab {
    type InitData = HanaAttachInitData;
    type BindData = HanaAttachBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let url = bind.get_parameter(0).to_string();
        let dbname = bind.get_parameter(1).to_string();
        let schema = bind.get_parameter(2).to_string();

        validate_hana_connection(&url)?;

        if dbname.is_empty() {
            return Err(HanaError::configuration(
                "dbname cannot be empty", Some("dbname"), None, Some("non-empty string"),
            ));
        }
        if schema.is_empty() {
            return Err(HanaError::configuration(
                "schema cannot be empty", Some("schema"), None, Some("non-empty string"),
            ));
        }

        let hana_conn = safe_hana_connect(url.clone())?;
        let discover_query = format!(
            "SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = '{}'",
            schema.replace('\'', "''")
        );
        let result_set = hana_conn.query(&discover_query).map_err(|e| {
            HanaError::query(
                &format!("Table discovery failed: {}", e),
                Some(&discover_query),
                None,
                "hana_attach bind",
            )
        })?;

        let mut table_names: Vec<String> = Vec::new();
        for row_result in result_set {
            let row = row_result.map_err(|e| {
                HanaError::query(
                    &format!("Row read failed during discovery: {}", e),
                    Some(&discover_query),
                    None,
                    "hana_attach bind",
                )
            })?;
            if let Ok(Some(name)) = row[0].clone().try_into::<Option<String>>() {
                table_names.push(name);
            }
        }

        if table_names.is_empty() {
            return Err(HanaError::schema(
                &format!("No tables found in schema '{}'", schema),
                None,
                "hana_attach bind",
            ));
        }

        bind.add_result_column("table_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("full_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        Ok(HanaAttachBindData { url, dbname, schema, table_names })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = init.get_bind_data::<Self::BindData>();
        let bind_ref = unsafe { &*bind_data };

        let url = &bind_ref.url;
        let dbname = &bind_ref.dbname;
        let schema = &bind_ref.schema;
        let table_names = &bind_ref.table_names;

        let mut tables: Vec<(String, String)> = Vec::new();

        hana_state::write_state(|state| {
            let att_key = attachment_key(dbname, schema);
            if let Some(prev) = state.attachments.remove(&att_key) {
                for t in &prev.table_names {
                    let key = prefixed_name(&prev.dbname, &prev.schema, t).to_uppercase();
                    state.table_registry.remove(&key);
                }
            }

            for table in table_names {
                let flat = prefixed_name(dbname, schema, table);
                let key = flat.to_uppercase();
                state.table_registry.insert(
                    key,
                    TableAttachmentInfo {
                        url: url.clone(),
                        hana_schema: schema.clone(),
                        hana_table: table.clone(),
                    },
                );
                tables.push((table.clone(), flat));
            }

            state.attachments.insert(
                att_key,
                AttachedDatabase {
                    url: url.clone(),
                    dbname: dbname.clone(),
                    schema: schema.clone(),
                    table_names: table_names.clone(),
                },
            );
        });

        // DDL is deferred to func phase to avoid DuckDB catalog lock re-entrancy.
        let duck_schema = escape_identifier(&duckdb_schema_name(dbname, schema));
        let safe_url = url.replace('\'', "''");
        let safe_hana_schema = escape_identifier(schema);
        let mut ddl_statements: Vec<String> = Vec::with_capacity(table_names.len() + 1);
        ddl_statements.push(format!(
            "CREATE SCHEMA IF NOT EXISTS \"{}\"",
            duck_schema
        ));
        for table in table_names {
            let safe_table = escape_identifier(table);
            ddl_statements.push(format!(
                "CREATE OR REPLACE VIEW \"{}\".\"{}\" AS SELECT * FROM hana_scan('SELECT * FROM \"{}\".\"{}\"', '{}')",
                duck_schema, safe_table, safe_hana_schema, safe_table, safe_url,
            ));
        }

        Ok(HanaAttachInitData {
            tables,
            ddl_statements,
            current_row: RwLock::new(0),
            ddl_done: RwLock::new(false),
        })
    }

    fn func(
        info: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = &*(info.get_init_data());

        // Best-effort DDL; replacement scan works regardless.
        {
            let done = *init_data.ddl_done.read().map_err(|_| HanaError::new("lock error"))?;
            if !done {
                let ddl_refs: Vec<&str> = init_data.ddl_statements.iter().map(|s| s.as_str()).collect();
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    execute_ddl_batch(&ddl_refs)
                })) {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        eprintln!("[WARN] ATTACH Could not create schema/views (replacement scan still works): {}", e);
                    }
                    Err(_) => {
                        eprintln!("[WARN] ATTACH Schema/view creation panicked (replacement scan still works)");
                    }
                }
                *init_data.ddl_done.write().map_err(|_| HanaError::new("lock error"))? = true;
            }
        }

        let current = *init_data.current_row.read().map_err(|_| HanaError::new("lock error"))?;
        let remaining = init_data.tables.len().saturating_sub(current);
        if remaining == 0 {
            output.set_len(0);
            return Ok(());
        }
        let batch = std::cmp::min(remaining, 2048);
        output.set_len(batch);

        let name_vec = output.flat_vector(0);
        let full_vec = output.flat_vector(1);
        for i in 0..batch {
            let (ref tname, ref fname) = init_data.tables[current + i];
            name_vec.insert(i, tname.as_str());
            full_vec.insert(i, fname.as_str());
        }

        *init_data.current_row.write().map_err(|_| HanaError::new("lock error"))? += batch;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // url
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // dbname
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // schema
        ])
    }
}

pub struct HanaDetachScalar;

impl VScalar for HanaDetachScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let dbname_vec = input.flat_vector(0);
        let schema_vec = input.flat_vector(1);
        let dbname_slice = dbname_vec.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let schema_slice = schema_vec.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        let dbname = {
            let mut binding = dbname_slice[0];
            duckdb::types::DuckString::new(&mut binding).as_str().to_string()
        };
        let schema = {
            let mut binding = schema_slice[0];
            duckdb::types::DuckString::new(&mut binding).as_str().to_string()
        };

        let removed_count = hana_state::write_state(|state| {
            let att_key = attachment_key(&dbname, &schema);
            let mut count = 0usize;
            if let Some(att) = state.attachments.remove(&att_key) {
                for table in &att.table_names {
                    let key = prefixed_name(&att.dbname, &att.schema, table).to_uppercase();
                    if state.table_registry.remove(&key).is_some() {
                        count += 1;
                    }
                }
            }
            count
        });

        let duck_schema = escape_identifier(&duckdb_schema_name(&dbname, &schema));
        let _ = execute_ddl(&format!(
            "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
            duck_schema
        ));

        let msg = format!("Detached {} tables from {}.{}", removed_count, dbname, schema);
        let flat = output.flat_vector();
        flat.insert(0, &msg);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(), // dbname
                LogicalTypeId::Varchar.into(), // schema
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

#[derive(Debug)]
pub struct HanaTablesBindData {
    rows: Vec<(String, String, String, String)>,
}

#[derive(Debug)]
pub struct HanaTablesInitData {
    rows: Vec<(String, String, String, String)>,
    current_row: RwLock<usize>,
}

pub struct HanaTablesVTab;

impl VTab for HanaTablesVTab {
    type InitData = HanaTablesInitData;
    type BindData = HanaTablesBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("table_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("schema_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("dbname", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("full_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let rows = hana_state::read_state(|state| {
            let mut out = Vec::new();
            for att in state.attachments.values() {
                for table in &att.table_names {
                    let flat = prefixed_name(&att.dbname, &att.schema, table);
                    out.push((
                        table.clone(),
                        att.schema.clone(),
                        att.dbname.clone(),
                        flat,
                    ));
                }
            }
            out
        });

        Ok(HanaTablesBindData { rows })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = init.get_bind_data::<Self::BindData>();
        let bind_ref = unsafe { &*bind_data };
        Ok(HanaTablesInitData {
            rows: bind_ref.rows.clone(),
            current_row: RwLock::new(0),
        })
    }

    fn func(
        info: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = &*(info.get_init_data());
        let current = *init_data.current_row.read().map_err(|_| HanaError::new("lock error"))?;
        let remaining = init_data.rows.len().saturating_sub(current);
        if remaining == 0 {
            output.set_len(0);
            return Ok(());
        }
        let batch = std::cmp::min(remaining, 2048);
        output.set_len(batch);

        let col0 = output.flat_vector(0);
        let col1 = output.flat_vector(1);
        let col2 = output.flat_vector(2);
        let col3 = output.flat_vector(3);
        for i in 0..batch {
            let (ref tname, ref sname, ref dname, ref fname) = init_data.rows[current + i];
            col0.insert(i, tname.as_str());
            col1.insert(i, sname.as_str());
            col2.insert(i, dname.as_str());
            col3.insert(i, fname.as_str());
        }

        *init_data.current_row.write().map_err(|_| HanaError::new("lock error"))? += batch;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![]) // no parameters
    }
}
