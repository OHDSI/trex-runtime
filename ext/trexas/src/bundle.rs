use anyhow::{bail, Context, Result};
use base::{get_default_permissions, CacheSetting, WorkerKind};
use deno::DenoOptionsBuilder;
use deno_facade::{generate_binary_eszip, EmitterFactory, Metadata};
use serde::Deserialize;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct BundleOptions {
  #[serde(default)]
  pub checksum: Option<String>,

  #[serde(default)]
  pub static_patterns: Vec<String>,

  #[serde(default)]
  pub no_module_cache: bool,

  #[serde(default)]
  pub timeout_sec: Option<u64>,
}

impl BundleOptions {
  pub fn get_checksum(&self) -> Result<Option<deno_facade::Checksum>> {
    use deno_facade::Checksum;
    match self.checksum.as_deref() {
      None | Some("none") | Some("") => Ok(None),
      Some("sha256") => Ok(Checksum::from_u8(1)),
      Some("xxhash3") => Ok(Checksum::from_u8(2)),
      Some(other) => bail!(
        "Invalid checksum type '{}'. Expected 'none', 'sha256', or 'xxhash3'",
        other
      ),
    }
  }
}

pub fn create_bundle_sync(
  entrypoint: &str,
  output: &str,
  options: Option<BundleOptions>,
) -> Result<String> {
  // Install rustls crypto provider (ring) before any TLS operations
  let _ = rustls::crypto::ring::default_provider().install_default();

  let options = options.unwrap_or_default();
  let entrypoint = entrypoint.to_string();
  let output = output.to_string();

  let entrypoint_path = PathBuf::from(&entrypoint);
  if !entrypoint_path.exists() {
    bail!("Entrypoint path does not exist: {}", entrypoint);
  }
  if !entrypoint_path.is_file() {
    bail!("Entrypoint path is not a file: {}", entrypoint);
  }
  let entrypoint_path = entrypoint_path
    .canonicalize()
    .context("Failed to canonicalize entrypoint path")?;

  let checksum = options.get_checksum()?;
  let static_patterns = options.static_patterns.clone();
  let no_module_cache = options.no_module_cache;
  let timeout_sec = options.timeout_sec;

  let handle = thread::spawn(move || -> Result<Vec<u8>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .thread_name("trex-bundle")
      .build()
      .context("Failed to create tokio runtime")?;

    runtime.block_on(async {
      let mut emitter_factory = EmitterFactory::new();

      if no_module_cache {
        emitter_factory.set_cache_strategy(Some(CacheSetting::ReloadAll));
      }

      emitter_factory.set_permissions_options(Some(get_default_permissions(
        WorkerKind::MainWorker,
      )));

      let deno_options = DenoOptionsBuilder::new()
        .entrypoint(entrypoint_path)
        .build()
        .await
        .context("Failed to build DenoOptions")?;

      emitter_factory.set_deno_options(deno_options);

      let static_pattern_refs: Vec<&str> =
        static_patterns.iter().map(|s| s.as_str()).collect();

      let mut metadata = Metadata::default();

      #[allow(clippy::arc_with_non_send_sync)]
      let eszip_fut = generate_binary_eszip(
        &mut metadata,
        Arc::new(emitter_factory),
        None,
        checksum,
        if static_pattern_refs.is_empty() {
          None
        } else {
          Some(static_pattern_refs)
        },
      );

      let eszip = if let Some(secs) = timeout_sec {
        match tokio::time::timeout(
          std::time::Duration::from_secs(secs),
          eszip_fut,
        )
        .await
        {
          Ok(result) => result,
          Err(_) => {
            bail!("Bundle operation timed out after {} seconds", secs)
          }
        }
      } else {
        eszip_fut.await
      }?;

      Ok(eszip.into_bytes())
    })
  });

  let bytes = handle
    .join()
    .map_err(|_| anyhow::anyhow!("Bundle thread panicked"))??;

  let mut file = File::create(&output)
    .with_context(|| format!("Failed to create output file: {}", output))?;

  file
    .write_all(&bytes)
    .with_context(|| format!("Failed to write bundle to: {}", output))?;

  Ok(format!(
    "Bundle created successfully: {} ({} bytes)",
    output,
    bytes.len()
  ))
}
