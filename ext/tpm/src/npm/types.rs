use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type NpmResult<T> = Result<T, NpmError>;

#[derive(Debug)]
pub enum NpmError {
  Network(String),
  PackageNotFound(String),
  InvalidPackageName(String),
  Serialization(String),
  Other(String),
}

impl std::fmt::Display for NpmError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      NpmError::Network(msg) => write!(f, "Network error: {}", msg),
      NpmError::PackageNotFound(pkg) => write!(f, "Package not found: {}", pkg),
      NpmError::InvalidPackageName(name) => {
        write!(f, "Invalid package name: {}", name)
      }
      NpmError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
      NpmError::Other(msg) => write!(f, "Error: {}", msg),
    }
  }
}

impl std::error::Error for NpmError {}

impl From<reqwest::Error> for NpmError {
  fn from(err: reqwest::Error) -> Self {
    NpmError::Network(err.to_string())
  }
}

impl From<serde_json::Error> for NpmError {
  fn from(err: serde_json::Error) -> Self {
    NpmError::Serialization(err.to_string())
  }
}

#[derive(Debug, Deserialize)]
pub struct NpmPackageMetadata {
  pub name: String,
  #[serde(default)]
  pub description: Option<String>,
  #[serde(rename = "dist-tags")]
  pub dist_tags: HashMap<String, String>,
  pub versions: HashMap<String, NpmVersionMetadata>,
}

#[derive(Debug, Deserialize)]
pub struct NpmVersionMetadata {
  pub version: String,
  #[serde(default)]
  pub description: Option<String>,
  #[serde(default)]
  pub dependencies: HashMap<String, String>,
  #[serde(rename = "devDependencies", default)]
  pub dev_dependencies: HashMap<String, String>,
  #[serde(default)]
  pub dist: Option<DistInfo>,
}

#[derive(Debug, Serialize)]
pub struct PackageInfoResponse {
  pub name: String,
  pub description: Option<String>,
  pub latest_version: Option<String>,
  pub versions: Vec<String>,
  pub dist_tags: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct DistInfo {
  pub tarball: String,
  pub shasum: String,
  #[serde(default)]
  pub integrity: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct NpmVersionMetadataExt {
  pub version: String,
  #[serde(default)]
  pub description: Option<String>,
  #[serde(default)]
  pub dependencies: HashMap<String, String>,
  #[serde(rename = "devDependencies", default)]
  pub dev_dependencies: HashMap<String, String>,
  pub dist: DistInfo,
}

#[derive(Debug, Serialize)]
pub struct ResolveResponse {
  pub package: String,
  pub resolved_version: String,
  pub tarball_url: String,
  pub dependencies: HashMap<String, String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub shasum: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InstallResponse {
  pub package: String,
  pub version: String,
  pub install_path: String,
  pub success: bool,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub error: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct DependencyNode {
  pub package: String,
  pub version: String,
  pub depth: usize,
  pub parent: Option<String>,
  #[serde(skip_serializing_if = "HashMap::is_empty")]
  pub dependencies: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct DependencyTreeResponse {
  pub package: String,
  pub version: String,
  pub depth: usize,
  pub parent: Option<String>,
  pub tree_line: String,
}

#[derive(Debug, Serialize)]
pub struct ListResponse {
  pub package: String,
  pub version: String,
  pub install_path: String,
}
