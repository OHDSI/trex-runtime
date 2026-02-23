use serde::Deserialize;
use std::{collections::HashMap, error::Error, fs, path::Path};

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectConfig {
    #[allow(dead_code)]
    pub name: String,
    #[serde(default = "default_models_path")]
    pub models_path: String,
    #[serde(default = "default_seeds_path")]
    pub seeds_path: String,
    #[serde(default = "default_tests_path")]
    pub tests_path: String,
    #[serde(default)]
    pub source_tables: Vec<String>,
}

fn default_models_path() -> String {
    "models".to_string()
}

fn default_seeds_path() -> String {
    "seeds".to_string()
}

fn default_tests_path() -> String {
    "tests".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Materialization {
    View,
    Table,
    Incremental,
    Snapshot,
    Ephemeral,
}

impl Materialization {
    pub fn as_str(&self) -> &'static str {
        match self {
            Materialization::View => "view",
            Materialization::Table => "table",
            Materialization::Incremental => "incremental",
            Materialization::Snapshot => "snapshot",
            Materialization::Ephemeral => "ephemeral",
        }
    }
}

impl Default for Materialization {
    fn default() -> Self {
        Materialization::View
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncrementalStrategy {
    Append,
    DeleteInsert,
    Merge,
    Microbatch,
}

impl IncrementalStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            IncrementalStrategy::Append => "append",
            IncrementalStrategy::DeleteInsert => "delete_insert",
            IncrementalStrategy::Merge => "merge",
            IncrementalStrategy::Microbatch => "microbatch",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum UniqueKeyConfig {
    Single(String),
    Composite(Vec<String>),
}

impl UniqueKeyConfig {
    pub fn columns(&self) -> Vec<String> {
        match self {
            UniqueKeyConfig::Single(s) => vec![s.clone()],
            UniqueKeyConfig::Composite(v) => v.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchSize {
    Hour,
    Day,
    Month,
}

impl BatchSize {
    pub fn as_interval(&self) -> &'static str {
        match self {
            BatchSize::Hour => "1 HOUR",
            BatchSize::Day => "1 DAY",
            BatchSize::Month => "1 MONTH",
        }
    }

    pub fn as_trunc(&self) -> &'static str {
        match self {
            BatchSize::Hour => "hour",
            BatchSize::Day => "day",
            BatchSize::Month => "month",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStrategy {
    Timestamp,
    Check,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EndpointConfig {
    pub path: String,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default = "default_formats")]
    pub formats: Vec<String>,
}

fn default_formats() -> Vec<String> {
    vec!["json".into(), "csv".into(), "arrow".into()]
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelYaml {
    #[serde(default)]
    pub materialized: Option<String>,
    #[serde(default)]
    pub endpoint: Option<EndpointConfig>,
    #[serde(default)]
    pub unique_key: Option<UniqueKeyConfig>,
    #[serde(default)]
    pub incremental_strategy: Option<IncrementalStrategy>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub batch_size: Option<BatchSize>,
    #[serde(default)]
    pub lookback: Option<u32>,
    #[serde(default)]
    pub merge_update_columns: Option<Vec<String>>,
    #[serde(default)]
    pub merge_exclude_columns: Option<Vec<String>>,
    #[serde(default)]
    pub strategy: Option<SnapshotStrategy>,
    #[serde(default)]
    pub check_cols: Option<Vec<String>>,
    #[serde(default)]
    pub pre_hooks: Option<Vec<String>>,
    #[serde(default)]
    pub post_hooks: Option<Vec<String>>,
    #[serde(default)]
    pub columns: Vec<ColumnTest>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnTest {
    pub name: String,
    #[serde(default)]
    pub tests: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Model {
    pub name: String,
    pub sql: String,
    pub materialization: Materialization,
    pub unique_key: Option<Vec<String>>,
    pub incremental_strategy: Option<IncrementalStrategy>,
    pub updated_at: Option<String>,
    pub batch_size: Option<BatchSize>,
    pub lookback: Option<u32>,
    pub merge_update_columns: Option<Vec<String>>,
    pub merge_exclude_columns: Option<Vec<String>>,
    pub strategy: Option<SnapshotStrategy>,
    pub check_cols: Option<Vec<String>>,
    pub pre_hooks: Option<Vec<String>>,
    pub post_hooks: Option<Vec<String>>,
    pub column_tests: Vec<ColumnTest>,
    pub yaml_content: Option<String>,
    pub endpoint: Option<EndpointConfig>,
}

#[derive(Debug, Clone)]
pub struct Seed {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone)]
pub struct TestFile {
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourcesConfig {
    pub sources: Vec<SourceDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceDef {
    pub name: String,
    pub loaded_at_field: String,
    #[serde(default)]
    pub warn_after: Option<FreshnessThreshold>,
    #[serde(default)]
    pub error_after: Option<FreshnessThreshold>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FreshnessThreshold {
    pub count: u32,
    pub period: String,
}

#[derive(Debug)]
pub struct Project {
    #[allow(dead_code)]
    pub config: ProjectConfig,
    pub models: Vec<Model>,
    pub seeds: Vec<Seed>,
    pub tests: Vec<TestFile>,
    pub sources: Vec<SourceDef>,
    pub source_tables: Vec<String>,
    #[allow(dead_code)]
    pub base_path: String,
}

pub fn load_project(path: &str) -> Result<Project, Box<dyn Error>> {
    let base = Path::new(path);
    if !base.exists() {
        return Err(format!("Project directory not found: {}", path).into());
    }

    let config_path = base.join("project.yml");
    if !config_path.exists() {
        return Err(format!("project.yml not found in: {}", path).into());
    }

    let config_str = fs::read_to_string(&config_path)?;
    let config: ProjectConfig = serde_yaml::from_str(&config_str)?;

    let models = discover_models(base, &config.models_path)?;
    let seeds = discover_seeds(base, &config.seeds_path)?;
    let tests = discover_tests(base, &config.tests_path)?;

    let sources_path = base.join("sources.yml");
    let sources = if sources_path.exists() {
        let sources_str = fs::read_to_string(&sources_path)?;
        let sources_config: SourcesConfig = serde_yaml::from_str(&sources_str)?;
        sources_config.sources
    } else {
        Vec::new()
    };

    let source_tables = config.source_tables.clone();

    Ok(Project {
        config,
        models,
        seeds,
        tests,
        sources,
        source_tables,
        base_path: path.to_string(),
    })
}

fn discover_models(base: &Path, models_path: &str) -> Result<Vec<Model>, Box<dyn Error>> {
    let models_dir = base.join(models_path);
    if !models_dir.exists() {
        return Ok(Vec::new());
    }

    let mut sql_files: HashMap<String, String> = HashMap::new();
    let mut yaml_files: HashMap<String, String> = HashMap::new();

    collect_files(&models_dir, &mut sql_files, &mut yaml_files)?;

    let mut models = Vec::new();
    for (name, sql) in &sql_files {
        let yaml_content = yaml_files.get(name);
        let model_yaml = match yaml_content {
            Some(yaml_str) => Some(serde_yaml::from_str::<ModelYaml>(yaml_str)?),
            None => None,
        };

        let materialization = match model_yaml.as_ref().and_then(|y| y.materialized.as_deref()) {
            Some("table") => Materialization::Table,
            Some("incremental") => Materialization::Incremental,
            Some("snapshot") => Materialization::Snapshot,
            Some("ephemeral") => Materialization::Ephemeral,
            Some("view") | None => Materialization::View,
            Some(other) => {
                return Err(format!(
                    "Unknown materialization '{}' for model '{}'",
                    other, name
                )
                .into())
            }
        };

        let model = Model {
            name: name.clone(),
            sql: sql.clone(),
            materialization,
            unique_key: model_yaml.as_ref().and_then(|y| y.unique_key.as_ref().map(|u| u.columns())),
            incremental_strategy: model_yaml.as_ref().and_then(|y| y.incremental_strategy),
            updated_at: model_yaml.as_ref().and_then(|y| y.updated_at.clone()),
            batch_size: model_yaml.as_ref().and_then(|y| y.batch_size),
            lookback: model_yaml.as_ref().and_then(|y| y.lookback),
            merge_update_columns: model_yaml.as_ref().and_then(|y| y.merge_update_columns.clone()),
            merge_exclude_columns: model_yaml.as_ref().and_then(|y| y.merge_exclude_columns.clone()),
            strategy: model_yaml.as_ref().and_then(|y| y.strategy),
            check_cols: model_yaml.as_ref().and_then(|y| y.check_cols.clone()),
            pre_hooks: model_yaml.as_ref().and_then(|y| y.pre_hooks.clone()),
            post_hooks: model_yaml.as_ref().and_then(|y| y.post_hooks.clone()),
            column_tests: model_yaml.as_ref().map(|y| y.columns.clone()).unwrap_or_default(),
            yaml_content: yaml_content.cloned(),
            endpoint: model_yaml.as_ref().and_then(|y| y.endpoint.clone()),
        };

        if model.materialization == Materialization::Incremental {
            validate_incremental_config(&model)?;
        }
        if model.materialization == Materialization::Snapshot {
            validate_snapshot_config(&model)?;
        }

        models.push(model);
    }

    models.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(models)
}

fn collect_files(
    dir: &Path,
    sql_files: &mut HashMap<String, String>,
    yaml_files: &mut HashMap<String, String>,
) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, sql_files, yaml_files)?;
        } else if let Some(ext) = path.extension() {
            let stem = path.file_stem().unwrap().to_string_lossy().to_string();
            if ext == "sql" {
                let content = fs::read_to_string(&path)?;
                sql_files.insert(stem, content);
            } else if ext == "yml" || ext == "yaml" {
                let content = fs::read_to_string(&path)?;
                yaml_files.insert(stem, content);
            }
        }
    }
    Ok(())
}

fn validate_incremental_config(model: &Model) -> Result<(), Box<dyn Error>> {
    let strategy = model.incremental_strategy.unwrap_or(IncrementalStrategy::DeleteInsert);
    match strategy {
        IncrementalStrategy::Merge => {
            if model.unique_key.is_none() {
                return Err(format!(
                    "Model '{}': merge strategy requires unique_key",
                    model.name
                )
                .into());
            }
        }
        IncrementalStrategy::Microbatch => {
            if model.updated_at.is_none() {
                return Err(format!(
                    "Model '{}': microbatch strategy requires updated_at",
                    model.name
                )
                .into());
            }
            if model.batch_size.is_none() {
                return Err(format!(
                    "Model '{}': microbatch strategy requires batch_size",
                    model.name
                )
                .into());
            }
        }
        IncrementalStrategy::Append | IncrementalStrategy::DeleteInsert => {}
    }
    Ok(())
}

fn validate_snapshot_config(model: &Model) -> Result<(), Box<dyn Error>> {
    if model.unique_key.is_none() {
        return Err(format!(
            "Model '{}': snapshot requires unique_key",
            model.name
        )
        .into());
    }
    let strategy = model.strategy.unwrap_or(SnapshotStrategy::Timestamp);
    match strategy {
        SnapshotStrategy::Timestamp => {
            if model.updated_at.is_none() {
                return Err(format!(
                    "Model '{}': snapshot timestamp strategy requires updated_at",
                    model.name
                )
                .into());
            }
        }
        SnapshotStrategy::Check => {
            if model.check_cols.is_none() {
                return Err(format!(
                    "Model '{}': snapshot check strategy requires check_cols",
                    model.name
                )
                .into());
            }
        }
    }
    Ok(())
}

fn discover_seeds(base: &Path, seeds_path: &str) -> Result<Vec<Seed>, Box<dyn Error>> {
    let seeds_dir = base.join(seeds_path);
    if !seeds_dir.exists() {
        return Ok(Vec::new());
    }

    let mut seeds = Vec::new();
    for entry in fs::read_dir(&seeds_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "csv" {
                    let name = path.file_stem().unwrap().to_string_lossy().to_string();
                    seeds.push(Seed {
                        name,
                        path: path.to_string_lossy().to_string(),
                    });
                }
            }
        }
    }
    seeds.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(seeds)
}

fn discover_tests(base: &Path, tests_path: &str) -> Result<Vec<TestFile>, Box<dyn Error>> {
    let tests_dir = base.join(tests_path);
    if !tests_dir.exists() {
        return Ok(Vec::new());
    }

    let mut tests = Vec::new();
    for entry in fs::read_dir(&tests_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "sql" {
                    let name = path.file_stem().unwrap().to_string_lossy().to_string();
                    let sql = fs::read_to_string(&path)?;
                    tests.push(TestFile { name, sql });
                }
            }
        }
    }
    tests.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(tests)
}
