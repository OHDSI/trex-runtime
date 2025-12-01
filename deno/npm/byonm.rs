// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

use std::borrow::Cow;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use deno_core::ModuleSpecifier;
use deno_core::error::AnyError;
use deno_permissions::OpenAccessKind;
use deno_resolver::npm::ByonmNpmResolver;
use deno_resolver::npm::ByonmNpmResolverCreateOptions;
use deno_resolver::npm::ResolvePkgFolderFromDenoReqError;
use deno_semver::package::PackageReq;
use ext_node::NodePermissions;
use node_resolver::NpmPackageFolderResolver;
use sys_traits::impls::RealSys;

use super::CliNpmResolver;
use super::InnerCliNpmResolverRef;

pub type CliByonmNpmResolverCreateOptions =
  ByonmNpmResolverCreateOptions<RealSys>;
pub type CliByonmNpmResolver = ByonmNpmResolver<RealSys>;

impl crate::resolver::CliNpmReqResolver for CliByonmNpmResolver {
  fn resolve_pkg_folder_from_deno_module_req(
    &self,
    req: &PackageReq,
    referrer: &ModuleSpecifier,
  ) -> Result<PathBuf, ResolvePkgFolderFromDenoReqError> {
    self
      .resolve_pkg_folder_from_deno_module_req(req, referrer)
      .map_err(ResolvePkgFolderFromDenoReqError::Byonm)
  }
}

impl CliNpmResolver for CliByonmNpmResolver {
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

  fn into_maybe_byonm(self: Arc<Self>) -> Option<Arc<CliByonmNpmResolver>> {
    Some(self)
  }

  fn clone_snapshotted(&self) -> Arc<dyn CliNpmResolver> {
    Arc::new(self.clone())
  }

  fn as_inner(&self) -> InnerCliNpmResolverRef {
    InnerCliNpmResolverRef::Byonm(self)
  }

  fn root_node_modules_path(&self) -> Option<&Path> {
    self.root_node_modules_path()
  }

  #[allow(clippy::manual_ignore_case_cmp)]
  fn ensure_read_permission<'a>(
    &self,
    permissions: &mut dyn NodePermissions,
    path: &'a Path,
  ) -> Result<Cow<'a, Path>, AnyError> {
    if !path
      .components()
      .any(|c| c.as_os_str().to_ascii_lowercase() == "node_modules")
    {
      permissions
        .check_open(Cow::Borrowed(path), OpenAccessKind::Read, None)
        .map(|checked| checked.into_path())
        .map_err(Into::into)
    } else {
      Ok(Cow::Borrowed(path))
    }
  }

  fn check_state_hash(&self) -> Option<u64> {
    // it is very difficult to determine the check state hash for byonm
    // so we just return None to signify check caching is not supported
    None
  }
}
