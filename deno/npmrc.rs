use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use deno_npmrc::NpmRc;
use deno_npmrc::NpmRegistryUrl;
use deno_npmrc::ResolvedNpmRc;
use tokio::fs;

use crate::args::npm_registry_url;

/// Adapter that routes `EnvVar` lookups through a supplied HashMap, falling back
/// to the process environment. Needed because `deno_npmrc::NpmRc::parse` now
/// takes a `&impl EnvVar` instead of a closure (deno 2.7.12).
struct MapEnvVar<'a> {
  overrides: Option<&'a HashMap<String, String>>,
}

impl<'a> sys_traits::BaseEnvVar for MapEnvVar<'a> {
  fn base_env_var_os(
    &self,
    key: &std::ffi::OsStr,
  ) -> Option<std::ffi::OsString> {
    if let Some(map) = self.overrides
      && let Some(k) = key.to_str()
      && let Some(v) = map.get(k)
    {
      return Some(std::ffi::OsString::from(v));
    }
    std::env::var_os(key)
  }
}

pub async fn create_npmrc<P>(
  path: P,
  maybe_env_vars: Option<&HashMap<String, String>>,
) -> Result<Arc<ResolvedNpmRc>, anyhow::Error>
where
  P: AsRef<Path>,
{
  let env = MapEnvVar {
    overrides: maybe_env_vars,
  };
  let source = fs::read_to_string(path)
    .await
    .context("failed to read path")?;
  NpmRc::parse(&env, &source)
    .context("failed to parse .npmrc file")?
    .as_resolved(&NpmRegistryUrl {
      url: npm_registry_url().clone(),
      from_env: false,
    })
    .context("failed to resolve .npmrc file")
    .map(Arc::new)
}
