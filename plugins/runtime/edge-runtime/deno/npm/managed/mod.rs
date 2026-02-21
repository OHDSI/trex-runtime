// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

#![allow(clippy::collapsible_if)]

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use deno_ast::ModuleSpecifier;
use deno_cache_dir::npm::NpmCacheDir;
use deno_core::anyhow::Context;
use deno_core::error::AnyError;
use deno_core::unsync::sync::AtomicFlag;
use deno_core::url::Url;
use deno_fs::FileSystem;
use deno_npm::NpmPackageId;
use deno_npm::NpmResolutionPackage;
use deno_npm::NpmSystemInfo;
use deno_npm::npm_rc::ResolvedNpmRc;
use deno_npm::registry::NpmPackageInfo;
use deno_npm::registry::NpmRegistryApi;
use deno_npm::resolution::NpmResolutionSnapshot;
use deno_npm::resolution::PackageReqNotFoundError;
use deno_npm::resolution::ValidSerializedNpmResolutionSnapshot;
use deno_resolver::npm::ResolvePkgFolderFromDenoReqError;
use deno_semver::package::PackageNv;
use deno_semver::package::PackageReq;
use ext_node::NodePermissions;
use node_resolver::InNpmPackageChecker;
use node_resolver::NpmPackageFolderResolver;
use node_resolver::UrlOrPathRef;
use node_resolver::errors::PackageFolderResolveError;
use node_resolver::errors::PackageFolderResolveIoError;
use resolution::AddPkgReqsResult;

use crate::args::CliLockfile;
use crate::args::NpmInstallDepsProvider;
use crate::args::PackageJsonDepValueParseWithLocationError;
use crate::cache::FastInsecureHasher;
use crate::util::fs::canonicalize_path_maybe_not_exists_with_fs;
use crate::util::progress_bar::ProgressBar;
use crate::util::progress_bar::ProgressBarStyle;

use self::resolution::NpmResolution;
use self::resolvers::NpmPackageFsResolver;
use self::resolvers::create_npm_fs_resolver;

use super::CliNpmCache;
use super::CliNpmCacheHttpClient;
use super::CliNpmRegistryInfoProvider;
use super::CliNpmResolver;
use super::CliNpmSys;
use super::CliNpmTarballCache;
use super::InnerCliNpmResolverRef;

mod resolution;
mod resolvers;

pub enum CliNpmResolverManagedSnapshotOption {
  ResolveFromLockfile(Arc<CliLockfile>),
  Specified(Option<ValidSerializedNpmResolutionSnapshot>),
}

pub struct CliManagedNpmResolverCreateOptions {
  pub snapshot: CliNpmResolverManagedSnapshotOption,
  pub maybe_lockfile: Option<Arc<CliLockfile>>,
  pub fs: Arc<dyn deno_fs::FileSystem>,
  pub http_client_provider: Arc<crate::http_util::HttpClientProvider>,
  pub npm_cache_dir: Arc<NpmCacheDir>,
  pub cache_setting: crate::args::CacheSetting,
  pub maybe_node_modules_path: Option<PathBuf>,
  pub npm_system_info: NpmSystemInfo,
  pub npm_install_deps_provider: Arc<NpmInstallDepsProvider>,
  pub npmrc: Arc<ResolvedNpmRc>,
}

pub async fn create_managed_npm_resolver(
  options: CliManagedNpmResolverCreateOptions,
) -> Result<Arc<dyn CliNpmResolver>, AnyError> {
  let pb = ProgressBar::new(ProgressBarStyle::TextOnly);
  let http_client = Arc::new(CliNpmCacheHttpClient::new(
    options.http_client_provider.clone(),
    pb,
  ));
  let npm_cache = create_cache(sys_traits::impls::RealSys, &options);
  let api = create_api(npm_cache.clone(), http_client.clone(), &options);
  let snapshot = resolve_snapshot(&api, options.snapshot).await?;
  Ok(create_inner(
    sys_traits::impls::RealSys,
    http_client,
    options.fs,
    options.maybe_lockfile,
    api,
    npm_cache,
    options.npmrc,
    options.npm_install_deps_provider,
    options.maybe_node_modules_path,
    options.npm_system_info,
    snapshot,
  ))
}

#[allow(clippy::too_many_arguments)]
fn create_inner(
  sys: CliNpmSys,
  http_client: Arc<CliNpmCacheHttpClient>,
  fs: Arc<dyn deno_fs::FileSystem>,
  maybe_lockfile: Option<Arc<CliLockfile>>,
  registry_info_provider: Arc<CliNpmRegistryInfoProvider>,
  npm_cache: Arc<CliNpmCache>,
  npm_rc: Arc<ResolvedNpmRc>,
  npm_install_deps_provider: Arc<NpmInstallDepsProvider>,
  node_modules_dir_path: Option<PathBuf>,
  npm_system_info: NpmSystemInfo,
  snapshot: Option<ValidSerializedNpmResolutionSnapshot>,
) -> Arc<dyn CliNpmResolver> {
  let resolution = Arc::new(NpmResolution::from_serialized(
    registry_info_provider.clone(),
    snapshot,
    maybe_lockfile.clone(),
  ));
  let tarball_cache = Arc::new(CliNpmTarballCache::new(
    npm_cache.clone(),
    http_client.clone(),
    sys,
    npm_rc.clone(),
    None, // reporter
  ));
  let fs_resolver = create_npm_fs_resolver(
    fs.clone(),
    npm_cache.clone(),
    &npm_install_deps_provider,
    resolution.clone(),
    tarball_cache.clone(),
    node_modules_dir_path,
    npm_system_info.clone(),
  );
  Arc::new(ManagedCliNpmResolver::new(
    fs,
    fs_resolver,
    maybe_lockfile,
    registry_info_provider,
    npm_cache,
    npm_install_deps_provider,
    resolution,
    tarball_cache,
    npm_system_info,
    npm_rc,
  ))
}

fn create_cache(
  sys: CliNpmSys,
  options: &CliManagedNpmResolverCreateOptions,
) -> Arc<CliNpmCache> {
  Arc::new(CliNpmCache::new(
    options.npm_cache_dir.clone(),
    sys,
    options.cache_setting.as_npm_cache_setting(),
    options.npmrc.clone(),
  ))
}

fn create_api(
  cache: Arc<CliNpmCache>,
  http_client: Arc<CliNpmCacheHttpClient>,
  options: &CliManagedNpmResolverCreateOptions,
) -> Arc<CliNpmRegistryInfoProvider> {
  Arc::new(CliNpmRegistryInfoProvider::new(
    cache,
    http_client.clone(),
    options.npmrc.clone(),
  ))
}

async fn resolve_snapshot(
  registry_info_provider: &Arc<CliNpmRegistryInfoProvider>,
  snapshot: CliNpmResolverManagedSnapshotOption,
) -> Result<Option<ValidSerializedNpmResolutionSnapshot>, AnyError> {
  match snapshot {
    CliNpmResolverManagedSnapshotOption::ResolveFromLockfile(lockfile) => {
      let (overwrite, filename) = {
        let guard = lockfile.lock();
        (guard.overwrite, guard.filename.clone())
      };
      if !overwrite {
        let snapshot = snapshot_from_lockfile(
          lockfile.clone(),
          registry_info_provider.as_ref(),
        )
        .await
        .with_context(|| {
          format!("failed reading lockfile '{}'", filename.display())
        })?;
        Ok(Some(snapshot))
      } else {
        Ok(None)
      }
    }
    CliNpmResolverManagedSnapshotOption::Specified(snapshot) => Ok(snapshot),
  }
}

async fn snapshot_from_lockfile(
  lockfile: Arc<CliLockfile>,
  _api: &dyn NpmRegistryApi,
) -> Result<ValidSerializedNpmResolutionSnapshot, AnyError> {
  let lock = lockfile.lock();

  // Note: In the new deno_npm API, incomplete_snapshot_from_lockfile has been removed
  // and snapshot_from_lockfile is now synchronous and takes different parameters.
  // We need to provide link_packages which we can get from the registry.

  // For now, use an empty link_packages map. In a production environment,
  // you may want to populate this from the registry or package.json.
  let link_packages = HashMap::new();

  let snapshot = deno_npm::resolution::snapshot_from_lockfile(
    deno_npm::resolution::SnapshotFromLockfileParams {
      link_packages: &link_packages,
      lockfile: &lock,
      default_tarball_url:
        &deno_npm::resolution::NpmRegistryDefaultTarballUrlProvider,
    },
  )?;

  drop(lock);
  Ok(snapshot)
}

#[derive(Debug)]
struct ManagedInNpmPackageChecker {
  root_dir: Url,
}

impl InNpmPackageChecker for ManagedInNpmPackageChecker {
  fn in_npm_package(&self, specifier: &Url) -> bool {
    specifier.as_ref().starts_with(self.root_dir.as_str())
  }
}

pub struct CliManagedInNpmPkgCheckerCreateOptions<'a> {
  pub root_cache_dir_url: &'a Url,
  pub maybe_node_modules_path: Option<&'a Path>,
}

pub fn create_managed_in_npm_pkg_checker(
  options: CliManagedInNpmPkgCheckerCreateOptions,
) -> Arc<dyn InNpmPackageChecker> {
  let root_dir = match options.maybe_node_modules_path {
    Some(node_modules_folder) => {
      deno_path_util::url_from_directory_path(node_modules_folder).unwrap()
    }
    None => options.root_cache_dir_url.clone(),
  };
  debug_assert!(root_dir.as_str().ends_with('/'));
  Arc::new(ManagedInNpmPackageChecker { root_dir })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackageCaching<'a> {
  Only(Cow<'a, [PackageReq]>),
  All,
}

/// An npm resolver where the resolution is managed by Deno rather than
/// the user bringing their own node_modules (BYONM) on the file system.
pub struct ManagedCliNpmResolver {
  fs: Arc<dyn FileSystem>,
  fs_resolver: Arc<dyn NpmPackageFsResolver>,
  maybe_lockfile: Option<Arc<CliLockfile>>,
  registry_info_provider: Arc<CliNpmRegistryInfoProvider>,
  npm_cache: Arc<CliNpmCache>,
  npm_install_deps_provider: Arc<NpmInstallDepsProvider>,
  resolution: Arc<NpmResolution>,
  tarball_cache: Arc<CliNpmTarballCache>,
  npm_system_info: NpmSystemInfo,
  npmrc: Arc<ResolvedNpmRc>,
  top_level_install_flag: AtomicFlag,
}

impl std::fmt::Debug for ManagedCliNpmResolver {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ManagedNpmResolver")
      .field("<omitted>", &"<omitted>")
      .finish()
  }
}

impl ManagedCliNpmResolver {
  #[allow(clippy::too_many_arguments)]
  pub fn new(
    fs: Arc<dyn FileSystem>,
    fs_resolver: Arc<dyn NpmPackageFsResolver>,
    maybe_lockfile: Option<Arc<CliLockfile>>,
    registry_info_provider: Arc<CliNpmRegistryInfoProvider>,
    npm_cache: Arc<CliNpmCache>,
    npm_install_deps_provider: Arc<NpmInstallDepsProvider>,
    resolution: Arc<NpmResolution>,
    tarball_cache: Arc<CliNpmTarballCache>,
    npm_system_info: NpmSystemInfo,
    npmrc: Arc<ResolvedNpmRc>,
  ) -> Self {
    Self {
      fs,
      fs_resolver,
      maybe_lockfile,
      registry_info_provider,
      npm_cache,
      npm_install_deps_provider,
      resolution,
      tarball_cache,
      npm_system_info,
      npmrc,
      top_level_install_flag: Default::default(),
    }
  }

  pub fn resolve_pkg_folder_from_pkg_id(
    &self,
    pkg_id: &NpmPackageId,
  ) -> Result<PathBuf, AnyError> {
    let path = self.fs_resolver.package_folder(pkg_id)?;
    let path =
      canonicalize_path_maybe_not_exists_with_fs(&path, self.fs.as_ref())?;
    log::debug!(
      "Resolved package folder of {} to {}",
      pkg_id.as_serialized(),
      path.display()
    );
    Ok(path)
  }

  /// Resolves the package id from the provided specifier.
  pub fn resolve_pkg_id_from_specifier(
    &self,
    specifier: &ModuleSpecifier,
  ) -> Result<Option<NpmPackageId>, AnyError> {
    let Some(cache_folder_id) = self
      .fs_resolver
      .resolve_package_cache_folder_id_from_specifier(specifier)?
    else {
      return Ok(None);
    };
    Ok(Some(
      self
        .resolution
        .resolve_pkg_id_from_pkg_cache_folder_id(&cache_folder_id)?,
    ))
  }

  pub fn resolve_pkg_reqs_from_pkg_id(
    &self,
    id: &NpmPackageId,
  ) -> Vec<PackageReq> {
    self.resolution.resolve_pkg_reqs_from_pkg_id(id)
  }

  /// Attempts to get the package size in bytes.
  pub fn package_size(
    &self,
    package_id: &NpmPackageId,
  ) -> Result<u64, AnyError> {
    let package_folder = self.fs_resolver.package_folder(package_id)?;
    Ok(crate::util::fs::dir_size(&package_folder)?)
  }

  pub fn all_system_packages(
    &self,
    system_info: &NpmSystemInfo,
  ) -> Vec<NpmResolutionPackage> {
    self.resolution.all_system_packages(system_info)
  }

  /// Checks if the provided package req's folder is cached.
  pub fn is_pkg_req_folder_cached(&self, req: &PackageReq) -> bool {
    self
      .resolve_pkg_id_from_pkg_req(req)
      .ok()
      .and_then(|id| self.fs_resolver.package_folder(&id).ok())
      .map(|folder| folder.exists())
      .unwrap_or(false)
  }

  /// Adds package requirements to the resolver and ensures everything is setup.
  /// This includes setting up the `node_modules` directory, if applicable.
  pub async fn add_and_cache_package_reqs(
    &self,
    packages: &[PackageReq],
  ) -> Result<(), AnyError> {
    self
      .add_package_reqs_raw(
        packages,
        Some(PackageCaching::Only(packages.into())),
      )
      .await
      .dependencies_result
  }

  pub async fn add_package_reqs_no_cache(
    &self,
    packages: &[PackageReq],
  ) -> Result<(), AnyError> {
    self
      .add_package_reqs_raw(packages, None)
      .await
      .dependencies_result
  }

  pub async fn add_package_reqs(
    &self,
    packages: &[PackageReq],
    caching: PackageCaching<'_>,
  ) -> Result<(), AnyError> {
    self
      .add_package_reqs_raw(packages, Some(caching))
      .await
      .dependencies_result
  }

  pub async fn add_package_reqs_raw<'a>(
    &self,
    packages: &[PackageReq],
    caching: Option<PackageCaching<'a>>,
  ) -> AddPkgReqsResult {
    if packages.is_empty() {
      return AddPkgReqsResult {
        dependencies_result: Ok(()),
        results: vec![],
        unmet_peer_diagnostics: vec![],
      };
    }

    let mut result = self.resolution.add_package_reqs(packages).await;

    if result.dependencies_result.is_ok() {
      if let Some(_lockfile) = self.maybe_lockfile.as_ref() {
        result.dependencies_result = {
          Ok(())
          // NOTE(Nyannyacha): If the edge runtime implements the frozen option for
          // lockfile, the comment below should be uncommented.

          // let lockfile = lockfile.lock();

          // if lockfile.has_content_changed {
          //     Err(anyhow!("The lockfile is out of date."))
          // } else {
          //     Ok(())
          // }
        };
      }
    }
    if result.dependencies_result.is_ok() {
      if let Some(caching) = caching {
        result.dependencies_result = self.cache_packages(caching).await;
      }
    }

    result
  }

  /// Sets package requirements to the resolver, removing old requirements and adding new ones.
  ///
  /// This will retrieve and resolve package information, but not cache any package files.
  pub async fn set_package_reqs(
    &self,
    packages: &[PackageReq],
  ) -> Result<(), AnyError> {
    self.resolution.set_package_reqs(packages).await
  }

  pub fn snapshot(&self) -> NpmResolutionSnapshot {
    self.resolution.snapshot()
  }

  pub fn top_package_req_for_name(&self, name: &str) -> Option<PackageReq> {
    let package_reqs = self.resolution.package_reqs();
    let mut entries = package_reqs
      .iter()
      .filter(|(_, nv)| nv.name == name)
      .collect::<Vec<_>>();
    entries.sort_by_key(|(_, nv)| &nv.version);
    Some(entries.last()?.0.clone())
  }

  pub fn serialized_valid_snapshot_for_system(
    &self,
    system_info: &NpmSystemInfo,
  ) -> ValidSerializedNpmResolutionSnapshot {
    self
      .resolution
      .serialized_valid_snapshot_for_system(system_info)
  }

  pub async fn inject_synthetic_types_node_package(
    &self,
  ) -> Result<(), AnyError> {
    let reqs = &[PackageReq::from_str("@types/node").unwrap()];
    // add and ensure this isn't added to the lockfile
    self
      .add_package_reqs(reqs, PackageCaching::Only(reqs.into()))
      .await?;

    Ok(())
  }

  pub async fn cache_packages(
    &self,
    caching: PackageCaching<'_>,
  ) -> Result<(), AnyError> {
    self.fs_resolver.cache_packages(caching).await
  }

  pub fn resolve_pkg_folder_from_deno_module(
    &self,
    nv: &PackageNv,
  ) -> Result<PathBuf, AnyError> {
    let pkg_id = self.resolution.resolve_pkg_id_from_deno_module(nv)?;
    self.resolve_pkg_folder_from_pkg_id(&pkg_id)
  }

  pub fn resolve_pkg_id_from_pkg_req(
    &self,
    req: &PackageReq,
  ) -> Result<NpmPackageId, PackageReqNotFoundError> {
    self.resolution.resolve_pkg_id_from_pkg_req(req)
  }

  pub fn ensure_no_pkg_json_dep_errors(
    &self,
  ) -> Result<(), Box<PackageJsonDepValueParseWithLocationError>> {
    for err in self.npm_install_deps_provider.pkg_json_dep_errors() {
      match err.source.0.as_ref() {
        deno_package_json::PackageJsonDepValueParseErrorKind::VersionReq(_) => {
          return Err(Box::new(err.clone()));
        }
        deno_package_json::PackageJsonDepValueParseErrorKind::Unsupported {
          ..
        } => {
          // only warn for this one
          log::warn!("{} {}\n    at {}", "Warning", err.source, err.location)
        }
        deno_package_json::PackageJsonDepValueParseErrorKind::JsrVersionReq(
          _,
        ) => {
          return Err(Box::new(err.clone()));
        }
      }
    }
    Ok(())
  }

  /// Ensures that the top level `package.json` dependencies are installed.
  /// This may set up the `node_modules` directory.
  ///
  /// Returns `true` if the top level packages are already installed. A
  /// return value of `false` means that new packages were added to the NPM resolution.
  pub async fn ensure_top_level_package_json_install(
    &self,
  ) -> Result<bool, AnyError> {
    if !self.top_level_install_flag.raise() {
      return Ok(true); // already did this
    }
    let pkg_json_remote_pkgs = self.npm_install_deps_provider.remote_pkgs();
    if pkg_json_remote_pkgs.is_empty() {
      return Ok(true);
    }

    // check if something needs resolving before bothering to load all
    // the package information (which is slow)
    if pkg_json_remote_pkgs.iter().all(|pkg| {
      self
        .resolution
        .resolve_pkg_id_from_pkg_req(&pkg.req)
        .is_ok()
    }) {
      log::debug!(
        "All package.json deps resolvable. Skipping top level install."
      );
      return Ok(true); // everything is already resolvable
    }

    let pkg_reqs = pkg_json_remote_pkgs
      .iter()
      .map(|pkg| pkg.req.clone())
      .collect::<Vec<_>>();
    self.add_package_reqs_no_cache(&pkg_reqs).await?;

    Ok(false)
  }

  pub async fn cache_package_info(
    &self,
    package_name: &str,
  ) -> Result<Arc<NpmPackageInfo>, AnyError> {
    // this will internally cache the package information
    self
      .registry_info_provider
      .package_info(package_name)
      .await
      .map_err(|err| err.into())
  }

  pub fn maybe_node_modules_path(&self) -> Option<&Path> {
    self.fs_resolver.node_modules_path()
  }

  pub fn global_cache_root_path(&self) -> &Path {
    self.npm_cache.root_dir_path()
  }

  pub fn global_cache_root_url(&self) -> &Url {
    self.npm_cache.root_dir_url()
  }

  // Public accessors for conversion to upstream ManagedNpmResolver
  // These allow extracting internal components for creating an upstream resolver

  pub fn npm_cache(&self) -> &Arc<CliNpmCache> {
    &self.npm_cache
  }

  pub fn npm_system_info(&self) -> &NpmSystemInfo {
    &self.npm_system_info
  }

  pub fn resolution(&self) -> &Arc<NpmResolution> {
    &self.resolution
  }

  pub fn fs_resolver(&self) -> &Arc<dyn NpmPackageFsResolver> {
    &self.fs_resolver
  }

  pub fn npmrc(&self) -> &Arc<ResolvedNpmRc> {
    &self.npmrc
  }
}

impl NpmPackageFolderResolver for ManagedCliNpmResolver {
  fn resolve_package_folder_from_package(
    &self,
    name: &str,
    referrer: &UrlOrPathRef<'_>,
  ) -> Result<PathBuf, PackageFolderResolveError> {
    let path = self
      .fs_resolver
      .resolve_package_folder_from_package(name, referrer)?;
    let path =
      canonicalize_path_maybe_not_exists_with_fs(&path, self.fs.as_ref())
        .map_err(|err| PackageFolderResolveIoError {
          package_name: name.to_string(),
          referrer: referrer.display(),
          source: err,
        })?;
    log::debug!(
      "Resolved {} from {} to {}",
      name,
      referrer.display(),
      path.display()
    );
    Ok(path)
  }
}

impl crate::resolver::CliNpmReqResolver for ManagedCliNpmResolver {
  fn resolve_pkg_folder_from_deno_module_req(
    &self,
    req: &PackageReq,
    _referrer: &ModuleSpecifier,
  ) -> Result<PathBuf, ResolvePkgFolderFromDenoReqError> {
    let pkg_id = self
      .resolve_pkg_id_from_pkg_req(req)
      .map_err(|err| ResolvePkgFolderFromDenoReqError::Managed(err.into()))?;
    // Edge runtime's resolve_pkg_folder_from_pkg_id returns AnyError instead of
    // ResolvePkgFolderFromPkgIdError, so we propagate it directly without wrapping
    self
      .resolve_pkg_folder_from_pkg_id(&pkg_id)
      .map_err(|_err| {
        // TODO: proper error conversion when aligning with upstream error types
        // Note: This creates a PackageReqNotFoundError even though the req was found,
        // because resolve_pkg_folder_from_pkg_id returns AnyError instead of
        // the proper ResolvePkgFolderFromPkgIdError type.
        ResolvePkgFolderFromDenoReqError::Managed(
          deno_npm::resolution::PackageReqNotFoundError(req.clone()).into(),
        )
      })
  }
}

impl CliNpmResolver for ManagedCliNpmResolver {
  fn into_npm_pkg_folder_resolver(
    self: Arc<Self>,
  ) -> Arc<dyn NpmPackageFolderResolver> {
    self
  }

  fn into_npm_req_resolver(
    self: Arc<Self>,
  ) -> Arc<dyn crate::resolver::CliNpmReqResolver> {
    self
  }

  fn clone_snapshotted(&self) -> Arc<dyn CliNpmResolver> {
    // create a new snapshotted npm resolution and resolver
    let npm_resolution = Arc::new(NpmResolution::new(
      self.registry_info_provider.clone(),
      self.resolution.snapshot(),
      self.maybe_lockfile.clone(),
    ));

    Arc::new(ManagedCliNpmResolver::new(
      self.fs.clone(),
      create_npm_fs_resolver(
        self.fs.clone(),
        self.npm_cache.clone(),
        &self.npm_install_deps_provider,
        npm_resolution.clone(),
        self.tarball_cache.clone(),
        self.root_node_modules_path().map(ToOwned::to_owned),
        self.npm_system_info.clone(),
      ),
      self.maybe_lockfile.clone(),
      self.registry_info_provider.clone(),
      self.npm_cache.clone(),
      self.npm_install_deps_provider.clone(),
      npm_resolution,
      self.tarball_cache.clone(),
      self.npm_system_info.clone(),
      self.npmrc.clone(),
    ))
  }

  fn as_inner(&self) -> InnerCliNpmResolverRef {
    InnerCliNpmResolverRef::Managed(self)
  }

  fn root_node_modules_path(&self) -> Option<&Path> {
    self.fs_resolver.node_modules_path()
  }

  fn ensure_read_permission<'a>(
    &self,
    permissions: &mut dyn NodePermissions,
    path: &'a Path,
  ) -> Result<Cow<'a, Path>, AnyError> {
    self.fs_resolver.ensure_read_permission(permissions, path)
  }

  fn check_state_hash(&self) -> Option<u64> {
    // We could go further and check all the individual
    // npm packages, but that's probably overkill.
    let mut package_reqs = self
      .resolution
      .package_reqs()
      .into_iter()
      .collect::<Vec<_>>();
    package_reqs.sort_by(|a, b| a.0.cmp(&b.0)); // determinism
    let mut hasher = FastInsecureHasher::new_without_deno_version();
    // ensure the cache gets busted when turning nodeModulesDir on or off
    // as this could cause changes in resolution
    hasher.write_hashable(self.fs_resolver.node_modules_path().is_some());
    for (pkg_req, pkg_nv) in package_reqs {
      hasher.write_hashable(&pkg_req);
      hasher.write_hashable(&pkg_nv);
    }
    Some(hasher.finish())
  }
}
