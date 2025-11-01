use reqwest::blocking::Client;
use std::time::Duration;
use semver::{Version, VersionReq};
use sha1::{Sha1, Digest};

use super::types::{
    NpmError, NpmPackageMetadata, NpmResult, PackageInfoResponse,
    ResolveResponse, NpmVersionMetadataExt, InstallResponse,
    DependencyTreeResponse, ListResponse,
};

pub struct NpmRegistry {
    client: Client,
    registry_url: String,
}

impl NpmRegistry {
    /// Create a new NpmRegistry with default npm registry
    pub fn new() -> NpmResult<Self> {
        Self::with_registry_url(None)
    }

    /// Create a new NpmRegistry with custom registry URL (or None for default npm registry)
    pub fn with_registry_url(registry_url: Option<String>) -> NpmResult<Self> {
        let registry_url = registry_url.unwrap_or_else(|| "https://registry.npmjs.org".to_string());

        let client = Client::builder()
            .user_agent("tpm-duckdb-extension/0.1.0")
            .timeout(Duration::from_secs(30))
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

        let metadata: NpmPackageMetadata = response.json()?;

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
    /// Verify tarball integrity using SHA-1 (as used by npm)
    fn verify_integrity(&self, data: &[u8], expected_shasum: &str) -> NpmResult<()> {
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
    /// Resolve a package spec (name@version or name) to a specific version
    pub fn resolve_package(&self, package_spec: &str) -> NpmResult<ResolveResponse> {
        // Parse package spec: "name@version" or just "name"
        let (name, version_req) = if let Some(pos) = package_spec.rfind('@') {
            if pos == 0 {
                // Scoped package like @types/node
                (package_spec, "latest")
            } else {
                let (n, v) = package_spec.split_at(pos);
                (n, &v[1..]) // Skip the @ character
            }
        } else {
            (package_spec, "latest")
        };

        // Fetch package metadata
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

        let metadata: NpmPackageMetadata = response.json()?;

        // Resolve version using proper semver
        let resolved_version = if version_req == "latest" || version_req.is_empty() {
            metadata
                .dist_tags
                .get("latest")
                .ok_or_else(|| NpmError::Other("No latest tag found".to_string()))?
                .clone()
        } else if version_req.starts_with('^') || version_req.starts_with('~') || version_req.contains('*') || version_req.contains('>') || version_req.contains('<') {
            // Parse as semver requirement
            let req = VersionReq::parse(version_req)
                .map_err(|e| NpmError::Other(format!("Invalid semver requirement '{}': {}", version_req, e)))?;

            // Get all versions and find the best match
            let mut versions: Vec<(Version, String)> = metadata.versions
                .keys()
                .filter_map(|v| {
                    Version::parse(v).ok().map(|parsed| (parsed, v.clone()))
                })
                .collect();

            // Sort by version (highest first)
            versions.sort_by(|a, b| b.0.cmp(&a.0));

            // Find first matching version
            versions
                .into_iter()
                .find(|(v, _)| req.matches(v))
                .map(|(_, s)| s)
                .ok_or_else(|| NpmError::Other(format!("No version matching '{}' found", version_req)))?
        } else {
            // Exact version
            version_req.to_string()
        };

        // Get version metadata with dist info
        let version_url = format!("{}/{}/{}", self.registry_url, name, resolved_version);
        let version_response = self.client.get(&version_url).send()?;

        if !version_response.status().is_success() {
            return Err(NpmError::Other(format!(
                "Version {} not found for package {}",
                resolved_version, name
            )));
        }

        let version_meta: NpmVersionMetadataExt = version_response.json()?;

        Ok(ResolveResponse {
            package: name.to_string(),
            resolved_version: version_meta.version,
            tarball_url: version_meta.dist.tarball.clone(),
            dependencies: version_meta.dependencies,
            shasum: Some(version_meta.dist.shasum),
        })
    }

    /// Download and install a package to a specific directory
    pub fn install_package(
        &self,
        package_spec: &str,
        install_dir: &str,
    ) -> NpmResult<InstallResponse> {
        // First resolve the package
        let resolved = self.resolve_package(package_spec)?;

        // Get shasum from resolve response for integrity check
        let shasum = resolved.shasum.clone();

        // Download tarball
        let tarball_response = self.client.get(&resolved.tarball_url).send()?;

        if !tarball_response.status().is_success() {
            return Ok(InstallResponse {
                package: resolved.package.clone(),
                version: resolved.resolved_version.clone(),
                install_path: String::new(),
                success: false,
                error: Some(format!("Failed to download tarball: HTTP {}", tarball_response.status())),
            });
        }

        let tarball_bytes = tarball_response.bytes()?;

        // Verify integrity if shasum is provided
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

        // Create installation directory
        let package_dir = std::path::Path::new(install_dir)
            .join(&resolved.package)
            .join(&resolved.resolved_version);

        std::fs::create_dir_all(&package_dir).map_err(|e| {
            NpmError::Other(format!("Failed to create install directory: {}", e))
        })?;

        // Extract tarball (wrapped in a tar.gz)
        use flate2::read::GzDecoder;
        use std::io::Cursor;

        let cursor = Cursor::new(tarball_bytes);
        let decoder = GzDecoder::new(cursor);
        let mut archive = tar::Archive::new(decoder);

        // Extract, stripping the "package/" prefix that npm tarballs have
        for entry in archive.entries().map_err(|e| NpmError::Other(e.to_string()))? {
            let mut entry = entry.map_err(|e| NpmError::Other(e.to_string()))?;
            let path = entry.path().map_err(|e| NpmError::Other(e.to_string()))?;

            // Strip "package/" prefix
            let stripped_path = path
                .strip_prefix("package")
                .unwrap_or(&path);

            let dest_path = package_dir.join(stripped_path);

            // Create parent directories
            if let Some(parent) = dest_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    NpmError::Other(format!("Failed to create directory: {}", e))
                })?;
            }

            entry.unpack(&dest_path).map_err(|e| {
                NpmError::Other(format!("Failed to extract file: {}", e))
            })?;
        }

        Ok(InstallResponse {
            package: resolved.package,
            version: resolved.resolved_version,
            install_path: package_dir.to_string_lossy().to_string(),
            success: true,
            error: None,
        })
    }

    /// Install a package with all its dependencies recursively
    pub fn install_package_with_deps(
        &self,
        package_spec: &str,
        install_dir: &str,
    ) -> NpmResult<Vec<InstallResponse>> {
        use std::collections::HashSet;

        let mut results = Vec::new();
        let mut to_install: Vec<(String, usize)> = vec![(package_spec.to_string(), 0)];
        let mut installed: HashSet<String> = HashSet::new();

        while let Some((spec, depth)) = to_install.pop() {
            // Skip if already installed
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

            // Install the package
            match self.install_package(&spec, install_dir) {
                Ok(install_result) => {
                    if install_result.success {
                        installed.insert(name.to_string());

                        // Resolve to get dependencies
                        if let Ok(resolved) = self.resolve_package(&spec) {
                            // Add dependencies to install queue (limit depth to prevent infinite recursion)
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

    /// Build a dependency tree for visualization
    pub fn get_dependency_tree(
        &self,
        package_spec: &str,
    ) -> NpmResult<Vec<DependencyTreeResponse>> {
        use std::collections::{HashSet, VecDeque};

        let mut result = Vec::new();
        let mut to_process: VecDeque<(String, usize, Option<String>)> = VecDeque::new();
        let mut processed: HashSet<String> = HashSet::new();

        // Start with root package
        to_process.push_back((package_spec.to_string(), 0, None));

        while let Some((spec, depth, parent)) = to_process.pop_front() {
            // Parse package name
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

            // Skip if already processed
            if processed.contains(name) {
                continue;
            }
            processed.insert(name.to_string());

            // Resolve the package
            match self.resolve_package(&spec) {
                Ok(resolved) => {
                    // Create tree line with proper indentation
                    let tree_line = if depth == 0 {
                        format!("{} {}", resolved.package, resolved.resolved_version)
                    } else {
                        let prefix = "  ".repeat(depth - 1);
                        let connector = if depth > 0 { "├── " } else { "" };
                        format!("{}{}{} {}", prefix, connector, resolved.package, resolved.resolved_version)
                    };

                    result.push(DependencyTreeResponse {
                        package: resolved.package.clone(),
                        version: resolved.resolved_version.clone(),
                        depth,
                        parent: parent.clone(),
                        tree_line,
                    });

                    // Add dependencies to queue (limit depth)
                    if depth < 5 && !resolved.dependencies.is_empty() {
                        for (dep_name, dep_version) in resolved.dependencies.iter() {
                            let dep_spec = format!("{}@{}", dep_name, dep_version);
                            to_process.push_back((dep_spec, depth + 1, Some(resolved.package.clone())));
                        }
                    }
                }
                Err(_) => {
                    // Skip packages that can't be resolved
                    continue;
                }
            }
        }

        Ok(result)
    }

    /// List installed packages in a directory
    pub fn list_installed_packages(install_dir: &str) -> NpmResult<Vec<ListResponse>> {
        use std::fs;
        use std::path::Path;

        let mut results = Vec::new();
        let base_path = Path::new(install_dir);

        if !base_path.exists() {
            return Ok(results);
        }

        // Iterate through package directories
        if let Ok(entries) = fs::read_dir(base_path) {
            for entry in entries.flatten() {
                if let Ok(package_name) = entry.file_name().into_string() {
                    let package_path = entry.path();

                    // Iterate through version directories
                    if let Ok(version_entries) = fs::read_dir(&package_path) {
                        for version_entry in version_entries.flatten() {
                            if let Ok(version) = version_entry.file_name().into_string() {
                                let install_path = version_entry.path();

                                // Verify it's a valid package by checking for package.json
                                let package_json = install_path.join("package.json");
                                if package_json.exists() {
                                    results.push(ListResponse {
                                        package: package_name.clone(),
                                        version,
                                        install_path: install_path.to_string_lossy().to_string(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sort by package name then version
        results.sort_by(|a, b| a.package.cmp(&b.package).then(a.version.cmp(&b.version)));

        Ok(results)
    }
}
