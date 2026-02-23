use crate::dag::topological_sort;
use crate::parser::extract_dependencies;
use crate::project::{load_project, Project};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct CompileResult {
    pub name: String,
    pub materialized: String,
    pub dependencies: String,
    pub order: i32,
    pub status: String,
    pub message: String,
    pub endpoint_path: String,
    pub endpoint_roles: String,
    pub endpoint_formats: String,
}

pub fn compile_project(project: &Project) -> Result<Vec<CompileResult>, Box<dyn Error>> {
    let mut results = Vec::new();

    let known_names: HashSet<String> = project
        .models
        .iter()
        .map(|m| m.name.clone())
        .chain(project.seeds.iter().map(|s| s.name.clone()))
        .collect();

    let mut edges: HashMap<String, HashSet<String>> = HashMap::new();
    let mut parse_errors: Vec<CompileResult> = Vec::new();

    for model in &project.models {
        match extract_dependencies(&model.sql) {
            Ok(all_refs) => {
                let deps: HashSet<String> = all_refs
                    .into_iter()
                    .filter(|r| known_names.contains(r) && *r != model.name)
                    .collect();
                edges.insert(model.name.clone(), deps);
            }
            Err(e) => {
                parse_errors.push(CompileResult {
                    name: model.name.clone(),
                    materialized: model.materialization.as_str().to_string(),
                    dependencies: String::new(),
                    order: -1,
                    status: "error".to_string(),
                    message: format!("SQL parse error: {}", e),
                    endpoint_path: String::new(),
                    endpoint_roles: String::new(),
                    endpoint_formats: String::new(),
                });
            }
        }
    }

    if !parse_errors.is_empty() {
        return Ok(parse_errors);
    }

    for seed in &project.seeds {
        edges.insert(seed.name.clone(), HashSet::new());
    }

    let all_nodes: Vec<String> = project
        .seeds
        .iter()
        .map(|s| s.name.clone())
        .chain(project.models.iter().map(|m| m.name.clone()))
        .collect();

    let sorted = topological_sort(&all_nodes, &edges)?;

    let model_map: HashMap<&str, &crate::project::Model> = project
        .models
        .iter()
        .map(|m| (m.name.as_str(), m))
        .collect();

    let seed_names: HashSet<&str> = project.seeds.iter().map(|s| s.name.as_str()).collect();

    for (order, name) in sorted.iter().enumerate() {
        let deps = edges.get(name).cloned().unwrap_or_default();
        let deps_str = {
            let mut d: Vec<&String> = deps.iter().collect();
            d.sort();
            d.iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };

        let model = model_map.get(name.as_str()).copied();

        let materialized = if seed_names.contains(name.as_str()) {
            "seed".to_string()
        } else if let Some(m) = model {
            m.materialization.as_str().to_string()
        } else {
            "unknown".to_string()
        };

        let (ep_path, ep_roles, ep_formats) = match model.and_then(|m| m.endpoint.as_ref()) {
            Some(ep) => (
                ep.path.clone(),
                ep.roles.join(","),
                ep.formats.join(","),
            ),
            None => (String::new(), String::new(), String::new()),
        };

        results.push(CompileResult {
            name: name.clone(),
            materialized,
            dependencies: deps_str,
            order: order as i32,
            status: "ok".to_string(),
            message: String::new(),
            endpoint_path: ep_path,
            endpoint_roles: ep_roles,
            endpoint_formats: ep_formats,
        });
    }

    Ok(results)
}

#[repr(C)]
pub struct CompileBindData {
    path: String,
}

#[repr(C)]
pub struct CompileInitData {
    results: Vec<CompileResult>,
    index: AtomicUsize,
}

pub struct CompileVTab;

impl VTab for CompileVTab {
    type InitData = CompileInitData;
    type BindData = CompileBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "materialized",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "dependencies",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("order", LogicalTypeHandle::from(LogicalTypeId::Integer));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("message", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "endpoint_path",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "endpoint_roles",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "endpoint_formats",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );

        let path = bind.get_parameter(0).to_string();
        Ok(CompileBindData { path })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = init.get_bind_data::<Self::BindData>();
        if bind_data.is_null() {
            return Err("Bind data is null".into());
        }
        let path = unsafe { (*bind_data).path.clone() };

        let project = load_project(&path)?;
        let results = compile_project(&project)?;

        Ok(CompileInitData {
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

        let mat_vector = output.flat_vector(1);
        mat_vector.insert(0, result.materialized.as_str());

        let deps_vector = output.flat_vector(2);
        deps_vector.insert(0, result.dependencies.as_str());

        let mut order_vector = output.flat_vector(3);
        order_vector.as_mut_slice::<i32>()[0] = result.order;

        let status_vector = output.flat_vector(4);
        status_vector.insert(0, result.status.as_str());

        let msg_vector = output.flat_vector(5);
        msg_vector.insert(0, result.message.as_str());

        let ep_path_vector = output.flat_vector(6);
        ep_path_vector.insert(0, result.endpoint_path.as_str());

        let ep_roles_vector = output.flat_vector(7);
        ep_roles_vector.insert(0, result.endpoint_roles.as_str());

        let ep_formats_vector = output.flat_vector(8);
        ep_formats_vector.insert(0, result.endpoint_formats.as_str());

        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}
