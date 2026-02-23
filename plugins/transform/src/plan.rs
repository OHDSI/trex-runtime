use crate::compile::compile_project;
use crate::dag::transitive_dependents;
use crate::parser::extract_dependencies;
use crate::project::{load_project, Materialization};
use crate::state::{ensure_state_table, query_state};
use crate::{escape_sql_ident, execute_sql};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use siphasher::sip::SipHasher13;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

struct PlanResult {
    name: String,
    action: String,
    materialized: String,
    reason: String,
}

fn compute_model_checksum(name: &str, sql: &str, yaml: Option<&str>) -> String {
    let mut hasher = SipHasher13::new();
    name.hash(&mut hasher);
    sql.hash(&mut hasher);
    if let Some(y) = yaml {
        y.hash(&mut hasher);
    }
    hasher.finish().to_string()
}

#[repr(C)]
pub struct PlanBindData {
    path: String,
    schema: String,
    source_schema: Option<String>,
}

#[repr(C)]
pub struct PlanInitData {
    results: Vec<PlanResult>,
    index: AtomicUsize,
}

pub struct PlanVTab;

impl VTab for PlanVTab {
    type InitData = PlanInitData;
    type BindData = PlanBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("action", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "materialized",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("reason", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let path = bind.get_parameter(0).to_string();
        let schema = bind.get_parameter(1).to_string();
        let source_schema = bind
            .get_named_parameter("source_schema")
            .map(|v| v.to_string())
            .filter(|s| !s.is_empty());
        Ok(PlanBindData { path, schema, source_schema })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = init.get_bind_data::<Self::BindData>();
        if bind_data.is_null() {
            return Err("Bind data is null".into());
        }
        let (path, schema, _source_schema) = unsafe {
            (
                (*bind_data).path.clone(),
                (*bind_data).schema.clone(),
                (*bind_data).source_schema.clone(),
            )
        };

        let project = load_project(&path)?;
        let compiled = compile_project(&project)?;

        let _ = execute_sql(&format!(
            "CREATE SCHEMA IF NOT EXISTS \"{}\"",
            escape_sql_ident(&schema)
        ));
        let _ = ensure_state_table(&schema);

        let existing_state = query_state(&schema).unwrap_or_default();

        let known_names: HashSet<String> = project
            .models
            .iter()
            .map(|m| m.name.clone())
            .chain(project.seeds.iter().map(|s| s.name.clone()))
            .collect();

        let mut edges: HashMap<String, HashSet<String>> = HashMap::new();
        for model in &project.models {
            if let Ok(all_refs) = extract_dependencies(&model.sql) {
                let deps: HashSet<String> = all_refs
                    .into_iter()
                    .filter(|r| known_names.contains(r) && *r != model.name)
                    .collect();
                edges.insert(model.name.clone(), deps);
            }
        }

        let mut directly_changed: HashSet<String> = HashSet::new();
        for model in &project.models {
            if model.materialization == Materialization::Ephemeral {
                continue;
            }
            let checksum =
                compute_model_checksum(&model.name, &model.sql, model.yaml_content.as_deref());
            match existing_state.get(&model.name) {
                Some(state) => {
                    if state.checksum != checksum {
                        directly_changed.insert(model.name.clone());
                    }
                }
                None => {
                    directly_changed.insert(model.name.clone());
                }
            }
        }

        let all_nodes: Vec<String> = project.models.iter().map(|m| m.name.clone()).collect();
        let affected = transitive_dependents(&directly_changed, &all_nodes, &edges);

        let project_names: HashSet<String> = project
            .models
            .iter()
            .map(|m| m.name.clone())
            .chain(project.seeds.iter().map(|s| s.name.clone()))
            .collect();

        let mut results = Vec::new();

        for cr in &compiled {
            if cr.materialized == "seed" || cr.materialized == "ephemeral" {
                continue;
            }

            if !existing_state.contains_key(&cr.name) {
                results.push(PlanResult {
                    name: cr.name.clone(),
                    action: "create".to_string(),
                    materialized: cr.materialized.clone(),
                    reason: "new model".to_string(),
                });
            } else if directly_changed.contains(&cr.name) {
                results.push(PlanResult {
                    name: cr.name.clone(),
                    action: "update".to_string(),
                    materialized: cr.materialized.clone(),
                    reason: "model changed".to_string(),
                });
            } else if affected.contains(&cr.name) {
                results.push(PlanResult {
                    name: cr.name.clone(),
                    action: "update".to_string(),
                    materialized: cr.materialized.clone(),
                    reason: "dependency changed".to_string(),
                });
            } else {
                results.push(PlanResult {
                    name: cr.name.clone(),
                    action: "no_change".to_string(),
                    materialized: cr.materialized.clone(),
                    reason: String::new(),
                });
            }
        }

        for (name, state) in &existing_state {
            if !project_names.contains(name) && state.materialized != "seed" {
                results.push(PlanResult {
                    name: name.clone(),
                    action: "drop".to_string(),
                    materialized: state.materialized.clone(),
                    reason: "model removed from project".to_string(),
                });
            }
        }

        Ok(PlanInitData {
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

        let mat_vector = output.flat_vector(2);
        mat_vector.insert(0, result.materialized.as_str());

        let reason_vector = output.flat_vector(3);
        reason_vector.insert(0, result.reason.as_str());

        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            ("source_schema".to_string(), LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ])
    }
}
