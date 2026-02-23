use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use std::error::Error;
use std::sync::RwLock;

use crate::pg_state::{self, escape_identifier, execute_ddl_batch, AttachedSchema};
use crate::spi_bridge;

// ── pg_attach(schema) ────────────────────────────────────────────────────

#[derive(Debug)]
pub struct PgAttachBindData {
    schema: String,
    table_names: Vec<String>,
}

#[derive(Debug)]
pub struct PgAttachInitData {
    tables: Vec<(String, String)>,
    ddl_statements: Vec<String>,
    current_row: RwLock<usize>,
    ddl_done: RwLock<bool>,
}

pub struct PgAttachVTab;

impl VTab for PgAttachVTab {
    type InitData = PgAttachInitData;
    type BindData = PgAttachBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let schema = bind.get_parameter(0).to_string();
        if schema.is_empty() {
            return Err("pg_attach: schema name cannot be empty".into());
        }

        let safe_schema = schema.replace('\'', "''");
        let discover_sql = format!(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = '{}' AND table_type = 'BASE TABLE'",
            safe_schema
        );

        let response = spi_bridge::request(&discover_sql)
            .map_err(|e| format!("pg_attach: table discovery failed: {e}"))?;

        if let Some(err) = response.error {
            return Err(format!("pg_attach: {err}").into());
        }

        let table_names: Vec<String> = response
            .rows
            .into_iter()
            .filter_map(|row| row.into_iter().next().flatten())
            .collect();

        if table_names.is_empty() {
            return Err(format!("pg_attach: no tables found in schema '{}'", schema).into());
        }

        bind.add_result_column("table_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "schema_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );

        Ok(PgAttachBindData {
            schema,
            table_names,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = init.get_bind_data::<Self::BindData>();
        let bind_ref = unsafe { &*bind_data };

        let schema = &bind_ref.schema;
        let table_names = &bind_ref.table_names;

        let mut tables: Vec<(String, String)> = Vec::new();

        pg_state::write_state(|state| {
            // Remove previous attachment for this schema
            state.attachments.remove(schema);

            for table in table_names {
                tables.push((table.clone(), schema.clone()));
            }

            state.attachments.insert(
                schema.clone(),
                AttachedSchema {
                    pg_schema: schema.clone(),
                    table_names: table_names.clone(),
                },
            );
        });

        // DDL deferred to func phase to avoid DuckDB catalog lock re-entrancy
        let safe_schema = escape_identifier(schema);
        let mut ddl_statements: Vec<String> = Vec::with_capacity(table_names.len() + 1);
        ddl_statements.push(format!(
            "CREATE SCHEMA IF NOT EXISTS \"{}\"",
            safe_schema
        ));
        for table in table_names {
            let safe_table = escape_identifier(table);
            ddl_statements.push(format!(
                "CREATE OR REPLACE VIEW \"{}\".\"{}\" AS \
                 SELECT * FROM pg_scan('SELECT * FROM \"{}\".\"{}\"')",
                safe_schema, safe_table, safe_schema, safe_table,
            ));
        }

        Ok(PgAttachInitData {
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

        // Best-effort DDL — pg_scan still works without views
        {
            let done = *init_data.ddl_done.read().map_err(|_| "lock error")?;
            if !done {
                let ddl_refs: Vec<&str> =
                    init_data.ddl_statements.iter().map(|s| s.as_str()).collect();
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    execute_ddl_batch(&ddl_refs)
                })) {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        eprintln!(
                            "[WARN] pg_attach: could not create schema/views: {}",
                            e
                        );
                    }
                    Err(_) => {
                        eprintln!("[WARN] pg_attach: schema/view creation panicked");
                    }
                }
                *init_data.ddl_done.write().map_err(|_| "lock error")? = true;
            }
        }

        let current = *init_data.current_row.read().map_err(|_| "lock error")?;
        let remaining = init_data.tables.len().saturating_sub(current);
        if remaining == 0 {
            output.set_len(0);
            return Ok(());
        }
        let batch = std::cmp::min(remaining, 2048);
        output.set_len(batch);

        let name_vec = output.flat_vector(0);
        let schema_vec = output.flat_vector(1);
        for i in 0..batch {
            let (ref tname, ref sname) = init_data.tables[current + i];
            name_vec.insert(i, tname.as_str());
            schema_vec.insert(i, sname.as_str());
        }

        *init_data.current_row.write().map_err(|_| "lock error")? += batch;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ── pg_detach(schema) ────────────────────────────────────────────────────

#[derive(Debug)]
pub struct PgDetachBindData {
    schema: String,
}

#[derive(Debug)]
pub struct PgDetachInitData {
    ddl: Vec<String>,
    done: RwLock<bool>,
    status: String,
}

pub struct PgDetachVTab;

impl VTab for PgDetachVTab {
    type InitData = PgDetachInitData;
    type BindData = PgDetachBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let schema = bind.get_parameter(0).to_string();
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(PgDetachBindData { schema })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = init.get_bind_data::<Self::BindData>();
        let bind_ref = unsafe { &*bind_data };
        let schema = &bind_ref.schema;

        let removed_count = pg_state::write_state(|state| {
            state
                .attachments
                .remove(schema)
                .map_or(0, |a| a.table_names.len())
        });

        let safe_schema = escape_identifier(schema);
        let ddl = vec![format!(
            "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
            safe_schema
        )];

        let status = format!(
            "Detached {} tables from schema '{}'",
            removed_count, schema
        );

        Ok(PgDetachInitData {
            ddl,
            done: RwLock::new(false),
            status,
        })
    }

    fn func(
        info: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = &*(info.get_init_data());

        let done = *init_data.done.read().map_err(|_| "lock error")?;
        if done {
            output.set_len(0);
            return Ok(());
        }

        let ddl_refs: Vec<&str> = init_data.ddl.iter().map(|s| s.as_str()).collect();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execute_ddl_batch(&ddl_refs)
        })) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("[WARN] pg_detach: DDL failed: {}", e),
            Err(_) => eprintln!("[WARN] pg_detach: DDL panicked"),
        }

        output.set_len(1);
        let col = output.flat_vector(0);
        col.insert(0, init_data.status.as_str());

        *init_data.done.write().map_err(|_| "lock error")? = true;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ── pg_tables() ──────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct PgTablesBindData {
    rows: Vec<(String, String)>,
}

#[derive(Debug)]
pub struct PgTablesInitData {
    rows: Vec<(String, String)>,
    current_row: RwLock<usize>,
}

pub struct PgTablesVTab;

impl VTab for PgTablesVTab {
    type InitData = PgTablesInitData;
    type BindData = PgTablesBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("table_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "schema_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );

        let rows = pg_state::read_state(|state| {
            let mut out = Vec::new();
            for att in state.attachments.values() {
                for table in &att.table_names {
                    out.push((table.clone(), att.pg_schema.clone()));
                }
            }
            out
        });

        Ok(PgTablesBindData { rows })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = init.get_bind_data::<Self::BindData>();
        let bind_ref = unsafe { &*bind_data };
        Ok(PgTablesInitData {
            rows: bind_ref.rows.clone(),
            current_row: RwLock::new(0),
        })
    }

    fn func(
        info: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = &*(info.get_init_data());
        let current = *init_data.current_row.read().map_err(|_| "lock error")?;
        let remaining = init_data.rows.len().saturating_sub(current);
        if remaining == 0 {
            output.set_len(0);
            return Ok(());
        }
        let batch = std::cmp::min(remaining, 2048);
        output.set_len(batch);

        let col0 = output.flat_vector(0);
        let col1 = output.flat_vector(1);
        for i in 0..batch {
            let (ref tname, ref sname) = init_data.rows[current + i];
            col0.insert(i, tname.as_str());
            col1.insert(i, sname.as_str());
        }

        *init_data.current_row.write().map_err(|_| "lock error")? += batch;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![])
    }
}
