use crate::project::{load_project, FreshnessThreshold, SourceDef};
use crate::{escape_sql_ident, query_sql};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};

struct FreshnessResult {
    name: String,
    status: String,
    max_loaded_at: String,
    age_hours: f64,
    warn_after: String,
    error_after: String,
}

fn threshold_to_hours(threshold: &FreshnessThreshold) -> f64 {
    let count = threshold.count as f64;
    match threshold.period.as_str() {
        "minute" => count / 60.0,
        "hour" => count,
        "day" => count * 24.0,
        _ => count,
    }
}

fn threshold_to_string(threshold: &Option<FreshnessThreshold>) -> String {
    match threshold {
        Some(t) => format!("{} {}", t.count, t.period),
        None => String::new(),
    }
}

fn check_freshness(
    sources: &[SourceDef],
    schema: &str,
) -> Result<Vec<FreshnessResult>, Box<dyn Error>> {
    let mut results = Vec::new();
    let esc_schema = escape_sql_ident(schema);

    for source in sources {
        let esc_name = escape_sql_ident(&source.name);
        let esc_field = escape_sql_ident(&source.loaded_at_field);

        let max_rows = query_sql(&format!(
            "SELECT MAX(\"{esc_field}\")::VARCHAR FROM \"{esc_schema}\".\"{esc_name}\""
        ));

        let (max_loaded_at, age_hours, status) = match max_rows {
            Ok(rows) => {
                let max_val = rows
                    .first()
                    .map(|r| r.columns[0].clone())
                    .unwrap_or_default();

                if max_val.is_empty() {
                    ("NULL".to_string(), f64::INFINITY, "error".to_string())
                } else {
                    let age_rows = query_sql(&format!(
                        "SELECT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - '{}'::TIMESTAMP) / 3600.0",
                        crate::escape_sql_str(&max_val)
                    ))?;

                    let age = age_rows
                        .first()
                        .and_then(|r| r.columns[0].parse::<f64>().ok())
                        .unwrap_or(f64::INFINITY);

                    let status = if let Some(error_threshold) = &source.error_after {
                        if age >= threshold_to_hours(error_threshold) {
                            "error".to_string()
                        } else if let Some(warn_threshold) = &source.warn_after {
                            if age >= threshold_to_hours(warn_threshold) {
                                "warn".to_string()
                            } else {
                                "pass".to_string()
                            }
                        } else {
                            "pass".to_string()
                        }
                    } else if let Some(warn_threshold) = &source.warn_after {
                        if age >= threshold_to_hours(warn_threshold) {
                            "warn".to_string()
                        } else {
                            "pass".to_string()
                        }
                    } else {
                        "pass".to_string()
                    };

                    (max_val, age, status)
                }
            }
            Err(_) => {
                results.push(FreshnessResult {
                    name: source.name.clone(),
                    status: "error".to_string(),
                    max_loaded_at: String::new(),
                    age_hours: -1.0,
                    warn_after: threshold_to_string(&source.warn_after),
                    error_after: threshold_to_string(&source.error_after),
                });
                continue;
            }
        };

        results.push(FreshnessResult {
            name: source.name.clone(),
            status,
            max_loaded_at,
            age_hours,
            warn_after: threshold_to_string(&source.warn_after),
            error_after: threshold_to_string(&source.error_after),
        });
    }

    Ok(results)
}

#[repr(C)]
pub struct FreshnessBindData {
    path: String,
    schema: String,
}

#[repr(C)]
pub struct FreshnessInitData {
    results: Vec<FreshnessResult>,
    index: AtomicUsize,
}

pub struct FreshnessVTab;

impl VTab for FreshnessVTab {
    type InitData = FreshnessInitData;
    type BindData = FreshnessBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "max_loaded_at",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("age_hours", LogicalTypeHandle::from(LogicalTypeId::Double));
        bind.add_result_column(
            "warn_after",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "error_after",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );

        let path = bind.get_parameter(0).to_string();
        let schema = bind.get_parameter(1).to_string();
        Ok(FreshnessBindData { path, schema })
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

        if project.sources.is_empty() {
            return Ok(FreshnessInitData {
                results: Vec::new(),
                index: AtomicUsize::new(0),
            });
        }

        let results = check_freshness(&project.sources, &schema)?;

        Ok(FreshnessInitData {
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

        let loaded_at_vector = output.flat_vector(2);
        loaded_at_vector.insert(0, result.max_loaded_at.as_str());

        let mut age_vector = output.flat_vector(3);
        age_vector.as_mut_slice::<f64>()[0] = result.age_hours;

        let warn_vector = output.flat_vector(4);
        warn_vector.insert(0, result.warn_after.as_str());

        let error_vector = output.flat_vector(5);
        error_vector.insert(0, result.error_after.as_str());

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
