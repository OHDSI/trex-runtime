use crate::parser::{rewrite_table_references, rewrite_table_references_dual};
use crate::project::load_project;
use crate::{escape_sql_ident, query_sql};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::collections::HashSet;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};

struct TestResult {
    name: String,
    status: String,
    rows_returned: String,
    message: String,
}

fn run_tests(path: &str, schema: &str, source_schema: Option<&str>) -> Result<Vec<TestResult>, Box<dyn Error>> {
    let project = load_project(path)?;
    let mut results = Vec::new();

    let known_names: HashSet<String> = project
        .models
        .iter()
        .map(|m| m.name.clone())
        .chain(project.seeds.iter().map(|s| s.name.clone()))
        .collect();

    let src_names: Option<HashSet<String>> = source_schema.map(|_| {
        project.source_tables.iter().cloned().collect()
    });

    for test in &project.tests {
        let rewritten = if let (Some(sn), Some(ss)) = (&src_names, source_schema) {
            rewrite_table_references_dual(&test.sql, &known_names, sn, schema, ss)?
        } else {
            rewrite_table_references(&test.sql, &known_names, schema)?
        };
        match query_sql(&rewritten) {
            Ok(rows) => {
                let count = rows.len();
                let status = if count == 0 { "pass" } else { "fail" };
                results.push(TestResult {
                    name: test.name.clone(),
                    status: status.to_string(),
                    rows_returned: count.to_string(),
                    message: if count > 0 {
                        format!("Test returned {} rows (expected 0)", count)
                    } else {
                        String::new()
                    },
                });
            }
            Err(e) => {
                results.push(TestResult {
                    name: test.name.clone(),
                    status: "error".to_string(),
                    rows_returned: String::new(),
                    message: format!("{}", e),
                });
            }
        }
    }

    for model in &project.models {
        for col_test in &model.column_tests {
            for test_type in &col_test.tests {
                let test_name = format!("{}_{}", model.name, col_test.name);
                let esc_schema = escape_sql_ident(schema);
                let esc_table = escape_sql_ident(&model.name);
                let esc_col = escape_sql_ident(&col_test.name);

                let sql = match test_type.as_str() {
                    "not_null" => {
                        format!(
                            "SELECT \"{esc_col}\" FROM \"{esc_schema}\".\"{esc_table}\" WHERE \"{esc_col}\" IS NULL"
                        )
                    }
                    "unique" => {
                        format!(
                            "SELECT \"{esc_col}\", COUNT(*) as cnt \
                             FROM \"{esc_schema}\".\"{esc_table}\" \
                             GROUP BY \"{esc_col}\" HAVING COUNT(*) > 1"
                        )
                    }
                    other => {
                        results.push(TestResult {
                            name: format!("{}_{}_{}", model.name, col_test.name, other),
                            status: "error".to_string(),
                            rows_returned: String::new(),
                            message: format!("Unknown test type: {}", other),
                        });
                        continue;
                    }
                };

                match query_sql(&sql) {
                    Ok(rows) => {
                        let count = rows.len();
                        let status = if count == 0 { "pass" } else { "fail" };
                        results.push(TestResult {
                            name: format!("{}_{}", test_name, test_type),
                            status: status.to_string(),
                            rows_returned: count.to_string(),
                            message: if count > 0 {
                                format!(
                                    "{} test failed: {} rows with violations",
                                    test_type, count
                                )
                            } else {
                                String::new()
                            },
                        });
                    }
                    Err(e) => {
                        results.push(TestResult {
                            name: format!("{}_{}", test_name, test_type),
                            status: "error".to_string(),
                            rows_returned: String::new(),
                            message: format!("{}", e),
                        });
                    }
                }
            }
        }
    }

    Ok(results)
}

#[repr(C)]
pub struct TestBindData {
    path: String,
    schema: String,
    source_schema: Option<String>,
}

#[repr(C)]
pub struct TestInitData {
    results: Vec<TestResult>,
    index: AtomicUsize,
}

pub struct TestVTab;

impl VTab for TestVTab {
    type InitData = TestInitData;
    type BindData = TestBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "rows_returned",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("message", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let path = bind.get_parameter(0).to_string();
        let schema = bind.get_parameter(1).to_string();
        let source_schema = bind
            .get_named_parameter("source_schema")
            .map(|v| v.to_string())
            .filter(|s| !s.is_empty());
        Ok(TestBindData { path, schema, source_schema })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = init.get_bind_data::<Self::BindData>();
        if bind_data.is_null() {
            return Err("Bind data is null".into());
        }
        let (path, schema, source_schema) = unsafe {
            (
                (*bind_data).path.clone(),
                (*bind_data).schema.clone(),
                (*bind_data).source_schema.clone(),
            )
        };

        let results = run_tests(&path, &schema, source_schema.as_deref())?;

        Ok(TestInitData {
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

        let status_vector = output.flat_vector(1);
        status_vector.insert(0, result.status.as_str());

        let rows_vector = output.flat_vector(2);
        rows_vector.insert(0, result.rows_returned.as_str());

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

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            ("source_schema".to_string(), LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ])
    }
}
