use crate::project::load_project;
use crate::state::{ensure_state_table, query_state, upsert_state};
use crate::{escape_sql_ident, escape_sql_str, execute_sql};
use chrono::Utc;
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use siphasher::sip::SipHasher13;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

struct SeedResult {
    name: String,
    action: String,
    rows: String,
    message: String,
}

fn compute_seed_checksum(name: &str, path: &str) -> String {
    let content = std::fs::read_to_string(path).unwrap_or_default();
    let mut hasher = SipHasher13::new();
    name.hash(&mut hasher);
    content.hash(&mut hasher);
    hasher.finish().to_string()
}

#[repr(C)]
pub struct SeedBindData {
    path: String,
    schema: String,
}

#[repr(C)]
pub struct SeedInitData {
    results: Vec<SeedResult>,
    index: AtomicUsize,
}

pub struct SeedVTab;

impl VTab for SeedVTab {
    type InitData = SeedInitData;
    type BindData = SeedBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("action", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("rows", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("message", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let path = bind.get_parameter(0).to_string();
        let schema = bind.get_parameter(1).to_string();
        Ok(SeedBindData { path, schema })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = init.get_bind_data::<Self::BindData>();
        if bind_data.is_null() {
            return Err("Bind data is null".into());
        }
        let (path, schema) = unsafe {
            (
                (*bind_data).path.clone(),
                (*bind_data).schema.clone(),
            )
        };

        let project = load_project(&path)?;

        execute_sql(&format!(
            "CREATE SCHEMA IF NOT EXISTS \"{}\"",
            escape_sql_ident(&schema)
        ))?;
        ensure_state_table(&schema)?;

        let existing_state = query_state(&schema)?;
        let mut results = Vec::new();

        for seed in &project.seeds {
            let checksum = compute_seed_checksum(&seed.name, &seed.path);
            let needs_load = match existing_state.get(&seed.name) {
                Some(state) => state.checksum != checksum,
                None => true,
            };

            if !needs_load {
                results.push(SeedResult {
                    name: seed.name.clone(),
                    action: "no_change".to_string(),
                    rows: String::new(),
                    message: String::new(),
                });
                continue;
            }

            let action = if existing_state.contains_key(&seed.name) {
                "update"
            } else {
                "create"
            };

            let sql = format!(
                "CREATE OR REPLACE TABLE \"{schema}\".\"{name}\" AS SELECT * FROM read_csv_auto('{path}')",
                schema = escape_sql_ident(&schema),
                name = escape_sql_ident(&seed.name),
                path = escape_sql_str(&seed.path),
            );

            match execute_sql(&sql) {
                Ok(_) => {
                    let deployed_at = Utc::now().to_rfc3339();
                    upsert_state(&schema, &seed.name, "seed", &checksum, &deployed_at, None, None)?;
                    results.push(SeedResult {
                        name: seed.name.clone(),
                        action: action.to_string(),
                        rows: String::new(),
                        message: "ok".to_string(),
                    });
                }
                Err(e) => {
                    results.push(SeedResult {
                        name: seed.name.clone(),
                        action: "error".to_string(),
                        rows: String::new(),
                        message: format!("{}", e),
                    });
                }
            }
        }

        Ok(SeedInitData {
            results,
            index: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let current_index = init_data.index.fetch_add(1, Ordering::Relaxed);

        if current_index >= init_data.results.len() {
            output.set_len(0);
            return Ok(());
        }

        let result = &init_data.results[current_index];

        let name_vector = output.flat_vector(0);
        name_vector.insert(0, result.name.as_str());

        let action_vector = output.flat_vector(1);
        action_vector.insert(0, result.action.as_str());

        let rows_vector = output.flat_vector(2);
        rows_vector.insert(0, result.rows.as_str());

        let msg_vector = output.flat_vector(3);
        msg_vector.insert(0, result.message.as_str());

        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}
