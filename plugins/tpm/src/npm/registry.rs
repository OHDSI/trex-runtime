use reqwest::blocking::Client;
use semver::{Version, VersionReq};
use sha1::{Digest, Sha1};
use std::time::Duration;

use super::types::{
  DeleteResponse, DependencyTreeResponse, InstallResponse, ListResponse,
  NpmError, NpmPackageMetadata, NpmResult, NpmVersionMetadataExt,
  PackageInfoResponse, ResolveResponse, SeedResponse,
};

pub struct NpmRegistry {
  client: Client,
  registry_url: String,
}

impl NpmRegistry {
  pub fn new() -> NpmResult<Self> {
    Self::with_registry_url(None)
  }

  pub fn with_registry_url(registry_url: Option<String>) -> NpmResult<Self> {
    let registry_url =
      registry_url.unwrap_or_else(|| "https://registry.npmjs.org".to_string());

    let client = Client::builder()
      .user_agent("tpm-duckdb-extension/0.1.0")
      .timeout(Duration::from_secs(300))
      .redirect(reqwest::redirect::Policy::limited(10))
      .build()
      .map_err(|e| NpmError::Network(e.to_string()))?;

    Ok(Self {
      client,
      registry_url,
    })
  }

  pub fn get_package_info(&self, name: &str) -> NpmResult<PackageInfoResponse> {
    let url = format!("{}/{}", self.registry_url, name);

    let response = self.client.get(&url).send()?;

    if response.status() == 404 {
      return Err(NpmError::PackageNotFound(name.to_string()));
    }

    if !response.status().is_success() {
      return Err(NpmError::Network(format!(
        "HTTP {} for package {}",
        response.status(),
        name
      )));
    }

    let text = response.text()?;
    let metadata: NpmPackageMetadata = serde_json::from_str(&text)?;

    let latest_version = metadata.dist_tags.get("latest").cloned();

    let mut versions: Vec<String> = metadata.versions.keys().cloned().collect();
    versions.sort();

    let description = latest_version
      .as_ref()
      .and_then(|v| metadata.versions.get(v))
      .and_then(|info| info.description.clone())
      .or(metadata.description);

    Ok(PackageInfoResponse {
      name: metadata.name,
      description,
      latest_version,
      versions,
      dist_tags: metadata.dist_tags,
    })
  }
}

impl Default for NpmRegistry {
  fn default() -> Self {
    Self::new().expect("Failed to create default NpmRegistry")
  }
}

impl NpmRegistry {
  fn verify_integrity(
    &self,
    data: &[u8],
    expected_shasum: &str,
  ) -> NpmResult<()> {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let result = hasher.finalize();
    let computed = format!("{:x}", result);

    if computed != expected_shasum {
      return Err(NpmError::Other(format!(
        "Integrity check failed: expected {}, got {}",
        expected_shasum, computed
      )));
    }

    Ok(())
  }
}

impl NpmRegistry {
  pub fn resolve_package(
    &self,
    package_spec: &str,
  ) -> NpmResult<ResolveResponse> {
    let (name, version_req) = if let Some(pos) = package_spec.rfind('@') {
      if pos == 0 {
        (package_spec, "latest")
      } else {
        let (n, v) = package_spec.split_at(pos);
        (n, &v[1..])
      }
    } else {
      (package_spec, "latest")
    };

    let url = format!("{}/{}", self.registry_url, name);
    let response = self.client.get(&url).send()?;

    if response.status() == 404 {
      return Err(NpmError::PackageNotFound(name.to_string()));
    }

    if !response.status().is_success() {
      return Err(NpmError::Network(format!(
        "HTTP {} for package {}",
        response.status(),
        name
      )));
    }

    let text = response.text()?;
    let metadata: NpmPackageMetadata = serde_json::from_str(&text)?;

    let resolved_version = if version_req == "latest" || version_req.is_empty()
    {
      metadata
        .dist_tags
        .get("latest")
        .ok_or_else(|| NpmError::Other("No latest tag found".to_string()))?
        .clone()
    } else if version_req.starts_with('^')
      || version_req.starts_with('~')
      || version_req.contains('*')
      || version_req.contains('>')
      || version_req.contains('<')
    {
      let req = VersionReq::parse(version_req).map_err(|e| {
        NpmError::Other(format!(
          "Invalid semver requirement '{}': {}",
          version_req, e
        ))
      })?;

      let mut versions: Vec<(Version, String)> = metadata
        .versions
        .keys()
        .filter_map(|v| {
          Version::parse(v).ok().map(|parsed| (parsed, v.clone()))
        })
        .collect();

      versions.sort_by(|a, b| b.0.cmp(&a.0));

      versions
        .into_iter()
        .find(|(v, _)| req.matches(v))
        .map(|(_, s)| s)
        .ok_or_else(|| {
          NpmError::Other(format!(
            "No version matching '{}' found",
            version_req
          ))
        })?
    } else {
      version_req.to_string()
    };

    let version_meta =
      metadata.versions.get(&resolved_version).ok_or_else(|| {
        NpmError::Other(format!(
          "Version {} not found in package metadata for {}",
          resolved_version, name
        ))
      })?;

    let dist = version_meta.dist.as_ref().ok_or_else(|| {
      NpmError::Other(format!(
        "No dist information found for {}@{}",
        name, resolved_version
      ))
    })?;

    Ok(ResolveResponse {
      package: name.to_string(),
      resolved_version: version_meta.version.clone(),
      tarball_url: dist.tarball.clone(),
      dependencies: version_meta.dependencies.clone(),
      shasum: Some(dist.shasum.clone()),
    })
  }

  pub fn install_package(
    &self,
    package_spec: &str,
    install_dir: &str,
  ) -> NpmResult<InstallResponse> {
    let resolved = self.resolve_package(package_spec)?;

    let shasum = resolved.shasum.clone();

    let tarball_response = self.client.get(&resolved.tarball_url).send()?;

    if !tarball_response.status().is_success() {
      return Ok(InstallResponse {
        package: resolved.package.clone(),
        version: resolved.resolved_version.clone(),
        install_path: String::new(),
        success: false,
        error: Some(format!(
          "Failed to download tarball: HTTP {}",
          tarball_response.status()
        )),
      });
    }

    let tarball_bytes = tarball_response.bytes()?;

    if let Some(ref expected_shasum) = shasum {
      if let Err(e) = self.verify_integrity(&tarball_bytes, expected_shasum) {
        return Ok(InstallResponse {
          package: resolved.package.clone(),
          version: resolved.resolved_version.clone(),
          install_path: String::new(),
          success: false,
          error: Some(e.to_string()),
        });
      }
    }

    let package_dir = if resolved.package.starts_with('@') {
      let parts: Vec<&str> = resolved.package.splitn(2, '/').collect();
      if parts.len() == 2 {
        std::path::Path::new(install_dir).join(parts[0]).join(parts[1])
      } else {
        std::path::Path::new(install_dir).join(&resolved.package)
      }
    } else {
      std::path::Path::new(install_dir).join(&resolved.package)
    };

    std::fs::create_dir_all(&package_dir).map_err(|e| {
      NpmError::Other(format!("Failed to create install directory: {}", e))
    })?;

    use flate2::read::GzDecoder;
    use std::io::Cursor;

    let cursor = Cursor::new(tarball_bytes);
    let decoder = GzDecoder::new(cursor);
    let mut archive = tar::Archive::new(decoder);

    archive.set_preserve_permissions(true);
    archive.set_preserve_mtime(true);
    archive.set_unpack_xattrs(false);

    for entry in archive
      .entries()
      .map_err(|e| NpmError::Other(e.to_string()))?
    {
      let mut entry = entry.map_err(|e| NpmError::Other(e.to_string()))?;
      let path = entry.path().map_err(|e| NpmError::Other(e.to_string()))?;

      let stripped_path = path.strip_prefix("package").unwrap_or(&path);

      let dest_path = package_dir.join(stripped_path);

      if let Some(parent) = dest_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
          if e.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(NpmError::Other(format!(
              "Failed to create directory {}: {}",
              parent.display(),
              e
            )));
          }
        }
      }

      if let Err(e) = entry.unpack(&dest_path) {
        if e.kind() == std::io::ErrorKind::NotFound {
          use std::io::{Read, Write};

          let mut content = Vec::new();
          entry.read_to_end(&mut content).map_err(|e| {
            NpmError::Other(format!("Failed to read entry content: {}", e))
          })?;

          let mut file = std::fs::File::create(&dest_path).map_err(|e| {
            NpmError::Other(format!(
              "Failed to create file {}: {}",
              dest_path.display(),
              e
            ))
          })?;

          file.write_all(&content).map_err(|e| {
            NpmError::Other(format!(
              "Failed to write file {}: {}",
              dest_path.display(),
              e
            ))
          })?;
        } else {
          return Err(NpmError::Other(format!(
            "Failed to extract file {}: {}",
            dest_path.display(),
            e
          )));
        }
      }
    }

    Ok(InstallResponse {
      package: resolved.package,
      version: resolved.resolved_version,
      install_path: package_dir.to_string_lossy().to_string(),
      success: true,
      error: None,
    })
  }

  pub fn install_package_with_deps(
    &self,
    package_spec: &str,
    install_dir: &str,
  ) -> NpmResult<Vec<InstallResponse>> {
    use std::collections::HashSet;

    let mut results = Vec::new();
    let mut to_install: Vec<(String, usize)> =
      vec![(package_spec.to_string(), 0)];
    let mut installed: HashSet<String> = HashSet::new();

    while let Some((spec, depth)) = to_install.pop() {
      let (name, _) = if let Some(pos) = spec.rfind('@') {
        if pos == 0 {
          (spec.as_str(), "latest")
        } else {
          let (n, v) = spec.split_at(pos);
          (n, &v[1..])
        }
      } else {
        (spec.as_str(), "latest")
      };

      if installed.contains(name) {
        continue;
      }

      match self.install_package(&spec, install_dir) {
        Ok(install_result) => {
          if install_result.success {
            installed.insert(name.to_string());

            if let Ok(resolved) = self.resolve_package(&spec) {
              if depth < 10 && !resolved.dependencies.is_empty() {
                for (dep_name, dep_version) in resolved.dependencies.iter() {
                  let dep_spec = format!("{}@{}", dep_name, dep_version);
                  to_install.push((dep_spec, depth + 1));
                }
              }
            }
          }
          results.push(install_result);
        }
        Err(e) => {
          results.push(InstallResponse {
            package: name.to_string(),
            version: String::new(),
            install_path: String::new(),
            success: false,
            error: Some(e.to_string()),
          });
        }
      }
    }

    Ok(results)
  }

  pub fn get_dependency_tree(
    &self,
    package_spec: &str,
  ) -> NpmResult<Vec<DependencyTreeResponse>> {
    use std::collections::{HashSet, VecDeque};

    let mut result = Vec::new();
    let mut to_process: VecDeque<(String, usize, Option<String>)> =
      VecDeque::new();
    let mut processed: HashSet<String> = HashSet::new();

    to_process.push_back((package_spec.to_string(), 0, None));

    while let Some((spec, depth, parent)) = to_process.pop_front() {
      let (name, _) = if let Some(pos) = spec.rfind('@') {
        if pos == 0 {
          (spec.as_str(), "latest")
        } else {
          let (n, v) = spec.split_at(pos);
          (n, &v[1..])
        }
      } else {
        (spec.as_str(), "latest")
      };

      if processed.contains(name) {
        continue;
      }
      processed.insert(name.to_string());

      match self.resolve_package(&spec) {
        Ok(resolved) => {
          let tree_line = if depth == 0 {
            format!("{} {}", resolved.package, resolved.resolved_version)
          } else {
            let prefix = "  ".repeat(depth - 1);
            let connector = if depth > 0 { "├── " } else { "" };
            format!(
              "{}{}{} {}",
              prefix, connector, resolved.package, resolved.resolved_version
            )
          };

          result.push(DependencyTreeResponse {
            package: resolved.package.clone(),
            version: resolved.resolved_version.clone(),
            depth,
            parent: parent.clone(),
            tree_line,
          });

          if depth < 5 && !resolved.dependencies.is_empty() {
            for (dep_name, dep_version) in resolved.dependencies.iter() {
              let dep_spec = format!("{}@{}", dep_name, dep_version);
              to_process.push_back((
                dep_spec,
                depth + 1,
                Some(resolved.package.clone()),
              ));
            }
          }
        }
        Err(_) => {
          continue;
        }
      }
    }

    Ok(result)
  }

  pub fn list_installed_packages(
    install_dir: &str,
  ) -> NpmResult<Vec<ListResponse>> {
    use std::fs;
    use std::path::Path;

    let mut results = Vec::new();
    let base_path = Path::new(install_dir);

    if !base_path.exists() {
      return Ok(results);
    }

    let try_read_package =
      |dir_path: &Path, results: &mut Vec<ListResponse>| {
        let package_json_path = dir_path.join("package.json");
        if package_json_path.exists() {
          if let Ok(content) = fs::read_to_string(&package_json_path) {
            if let Ok(pkg) =
              serde_json::from_str::<serde_json::Value>(&content)
            {
              let name = pkg
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string();
              let version = pkg
                .get("version")
                .and_then(|v| v.as_str())
                .unwrap_or("0.0.0")
                .to_string();
              results.push(ListResponse {
                package: name,
                version,
                install_path: dir_path.to_string_lossy().to_string(),
              });
            }
          }
        }
      };

    if let Ok(entries) = fs::read_dir(base_path) {
      for entry in entries.flatten() {
        let entry_path = entry.path();
        if !entry_path.is_dir() {
          continue;
        }

        if let Ok(dir_name) = entry.file_name().into_string() {
          if dir_name.starts_with('@') {
            if let Ok(scoped_entries) = fs::read_dir(&entry_path) {
              for scoped_entry in scoped_entries.flatten() {
                let scoped_path = scoped_entry.path();
                if scoped_path.is_dir() {
                  try_read_package(&scoped_path, &mut results);
                }
              }
            }
          } else {
            try_read_package(&entry_path, &mut results);
          }
        }
      }
    }

    results.sort_by(|a, b| {
      a.package.cmp(&b.package).then(a.version.cmp(&b.version))
    });

    Ok(results)
  }

  pub fn delete_package(
    package_name: &str,
    install_dir: &str,
  ) -> NpmResult<DeleteResponse> {
    let package_dir = if package_name.starts_with('@') {
      let parts: Vec<&str> = package_name.splitn(2, '/').collect();
      if parts.len() == 2 {
        std::path::Path::new(install_dir).join(parts[0]).join(parts[1])
      } else {
        std::path::Path::new(install_dir).join(package_name)
      }
    } else {
      std::path::Path::new(install_dir).join(package_name)
    };

    if !package_dir.exists() {
      return Ok(DeleteResponse {
        package: package_name.to_string(),
        deleted: false,
        error: Some(format!(
          "Package directory not found: {}",
          package_dir.display()
        )),
      });
    }

    match std::fs::remove_dir_all(&package_dir) {
      Ok(_) => {
        if let Some(parent) = package_dir.parent() {
          if parent != std::path::Path::new(install_dir) {
            if let Ok(mut entries) = std::fs::read_dir(parent) {
              if entries.next().is_none() {
                let _ = std::fs::remove_dir(parent);
              }
            }
          }
        }
        Ok(DeleteResponse {
          package: package_name.to_string(),
          deleted: true,
          error: None,
        })
      }
      Err(e) => Ok(DeleteResponse {
        package: package_name.to_string(),
        deleted: false,
        error: Some(format!(
          "Failed to delete {}: {}",
          package_dir.display(),
          e
        )),
      }),
    }
  }

  pub fn seed_packages(
    &self,
    install_dir: &str,
  ) -> NpmResult<Vec<SeedResponse>> {
    let seed_env = std::env::var("PLUGINS_SEED").unwrap_or_default();
    if seed_env.is_empty() {
      return Ok(Vec::new());
    }

    let packages: Vec<String> = serde_json::from_str(&seed_env)
      .map_err(|e| NpmError::Other(format!("Invalid PLUGINS_SEED JSON: {}", e)))?;

    let update = std::env::var("PLUGINS_SEED_UPDATE")
      .map(|v| v.to_lowercase() == "true")
      .unwrap_or(false);

    let api_version =
      std::env::var("PLUGINS_API_VERSION").unwrap_or_else(|_| "latest".to_string());

    let mut results = Vec::new();

    for pkg_name in &packages {
      let package_spec = if pkg_name.contains('@')
        && pkg_name.rfind('@').unwrap_or(0) > 0
      {
        pkg_name.clone()
      } else {
        format!("{}@{}", pkg_name, api_version)
      };

      let name_part = if let Some(pos) = package_spec.rfind('@') {
        if pos == 0 {
          &package_spec
        } else {
          &package_spec[..pos]
        }
      } else {
        &package_spec
      };

      let pkg_dir = if name_part.starts_with('@') {
        let parts: Vec<&str> = name_part.splitn(2, '/').collect();
        if parts.len() == 2 {
          std::path::Path::new(install_dir).join(parts[0]).join(parts[1])
        } else {
          std::path::Path::new(install_dir).join(name_part)
        }
      } else {
        std::path::Path::new(install_dir).join(name_part)
      };
      let is_installed = pkg_dir.join("package.json").exists();

      if is_installed && !update {
        results.push(SeedResponse {
          package: pkg_name.clone(),
          version: String::new(),
          success: true,
          skipped: true,
          error: None,
        });
        continue;
      }

      match self.install_package(&package_spec, install_dir) {
        Ok(install_result) => {
          results.push(SeedResponse {
            package: pkg_name.clone(),
            version: install_result.version,
            success: install_result.success,
            skipped: false,
            error: install_result.error,
          });
        }
        Err(e) => {
          results.push(SeedResponse {
            package: pkg_name.clone(),
            version: String::new(),
            success: false,
            skipped: false,
            error: Some(e.to_string()),
          });
        }
      }
    }

    Ok(results)
  }
}
