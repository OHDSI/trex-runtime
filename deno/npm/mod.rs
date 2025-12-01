// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

pub mod byonm;
pub mod managed;

use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;

use byonm::CliByonmNpmResolver;
use byonm::CliByonmNpmResolverCreateOptions;
use deno_core::error::AnyError;
use deno_core::url::Url;
use deno_error::JsErrorBox;
use deno_fs::FileSystem;
use deno_resolver::npm::ByonmInNpmPackageChecker;
use deno_resolver::npm::ByonmNpmResolver;
use ext_node::NodePermissions;
pub use managed::*;
use node_resolver::InNpmPackageChecker;
use node_resolver::NpmPackageFolderResolver;

use sys_traits::impls::RealSys;

use crate::http_util::DownloadErrorKind;
use crate::http_util::HttpClientProvider;
use crate::util::progress_bar::ProgressBar;
use deno_npm_cache::NpmCacheHttpClientBytesResponse;
use deno_npm_cache::NpmCacheHttpClientResponse;

pub type CliNpmSys = RealSys;
pub type CliNpmTarballCache =
  deno_npm_cache::TarballCache<CliNpmCacheHttpClient, CliNpmSys>;
pub type CliNpmCache = deno_npm_cache::NpmCache<CliNpmSys>;
pub type CliNpmRegistryInfoProvider =
  deno_npm_cache::RegistryInfoProvider<CliNpmCacheHttpClient, CliNpmSys>;

#[derive(Debug)]
pub struct CliNpmCacheHttpClient {
  http_client_provider: Arc<HttpClientProvider>,
  progress_bar: ProgressBar,
}

impl CliNpmCacheHttpClient {
  pub fn new(
    http_client_provider: Arc<HttpClientProvider>,
    progress_bar: ProgressBar,
  ) -> Self {
    Self {
      http_client_provider,
      progress_bar,
    }
  }
}

#[async_trait::async_trait(?Send)]
impl deno_npm_cache::NpmCacheHttpClient for CliNpmCacheHttpClient {
  async fn download_with_retries_on_any_tokio_runtime(
    &self,
    url: Url,
    maybe_auth: Option<String>,
    maybe_etag: Option<String>,
  ) -> Result<NpmCacheHttpClientResponse, deno_npm_cache::DownloadError> {
    let guard = self.progress_bar.update(url.as_str());
    let client = self.http_client_provider.get_or_create().map_err(|err| {
      deno_npm_cache::DownloadError {
        status_code: None,
        error: err,
      }
    })?;
    let mut headers = http::HeaderMap::new();
    if let Some(auth) = maybe_auth {
      headers.append(
        http::header::AUTHORIZATION,
        http::header::HeaderValue::try_from(auth).unwrap(),
      );
    }
    if let Some(etag) = maybe_etag {
      headers.append(
        http::header::IF_NONE_MATCH,
        http::header::HeaderValue::try_from(etag).unwrap(),
      );
    }
    client
      .download_with_progress_and_retries(url, &headers, &guard)
      .await
      .map(|response| match response {
        crate::http_util::HttpClientResponse::Success { headers, body } => {
          NpmCacheHttpClientResponse::Bytes(NpmCacheHttpClientBytesResponse {
            etag: headers
              .get(http::header::ETAG)
              .and_then(|e| e.to_str().map(|t| t.to_string()).ok()),
            bytes: body,
          })
        }
        crate::http_util::HttpClientResponse::NotFound => {
          NpmCacheHttpClientResponse::NotFound
        }
        crate::http_util::HttpClientResponse::NotModified => {
          NpmCacheHttpClientResponse::NotModified
        }
      })
      .map_err(|err| {
        use crate::http_util::DownloadErrorKind::*;
        let status_code = match err.as_kind() {
          Fetch { .. }
          | UrlParse { .. }
          | HttpParse { .. }
          | Json { .. }
          | ToStr { .. }
          | RedirectHeaderParse { .. }
          | TooManyRedirects
          | UnhandledNotModified
          | NotFound
          | Other(_) => None,
          BadResponse(bad_response_error) => {
            Some(bad_response_error.status_code.as_u16())
          }
        };
        deno_npm_cache::DownloadError {
          status_code,
          error: deno_error::JsErrorBox::from_err(err),
        }
      })
  }
}

// /// State provided to the process via an environment variable.
// #[derive(Clone, Debug, Serialize, Deserialize)]
// pub struct NpmProcessState {
//   pub kind: NpmProcessStateKind,
//   pub local_node_modules_path: Option<String>,
// }

// #[derive(Clone, Debug, Serialize, Deserialize)]
// pub enum NpmProcessStateKind {
//   Snapshot(deno_npm::resolution::SerializedNpmResolutionSnapshot),
//   Byonm,
// }

pub enum CliNpmResolverCreateOptions {
  Managed(CliManagedNpmResolverCreateOptions),
  Byonm(CliByonmNpmResolverCreateOptions),
}

pub async fn create_cli_npm_resolver(
  options: CliNpmResolverCreateOptions,
) -> Result<Arc<dyn CliNpmResolver>, AnyError> {
  use CliNpmResolverCreateOptions::*;
  match options {
    Managed(options) => managed::create_managed_npm_resolver(options).await,
    Byonm(options) => Ok(Arc::new(ByonmNpmResolver::new(options))),
  }
}

pub enum CreateInNpmPkgCheckerOptions<'a> {
  Managed(CliManagedInNpmPkgCheckerCreateOptions<'a>),
  Byonm,
}

pub fn create_in_npm_pkg_checker(
  options: CreateInNpmPkgCheckerOptions,
) -> Arc<dyn InNpmPackageChecker> {
  match options {
    CreateInNpmPkgCheckerOptions::Managed(options) => {
      create_managed_in_npm_pkg_checker(options)
    }
    CreateInNpmPkgCheckerOptions::Byonm => Arc::new(ByonmInNpmPackageChecker),
  }
}

pub enum InnerCliNpmResolverRef<'a> {
  Managed(&'a ManagedCliNpmResolver),
  #[allow(dead_code)]
  Byonm(&'a CliByonmNpmResolver),
}

pub trait CliNpmResolver:
  NpmPackageFolderResolver + crate::resolver::CliNpmReqResolver
{
  fn into_npm_pkg_folder_resolver(
    self: Arc<Self>,
  ) -> Arc<dyn NpmPackageFolderResolver>;
  fn into_npm_req_resolver(
    self: Arc<Self>,
  ) -> Arc<dyn crate::resolver::CliNpmReqResolver>;
  fn into_maybe_byonm(self: Arc<Self>) -> Option<Arc<CliByonmNpmResolver>> {
    None
  }

  fn clone_snapshotted(&self) -> Arc<dyn CliNpmResolver>;

  fn as_inner(&self) -> InnerCliNpmResolverRef;

  fn as_managed(&self) -> Option<&ManagedCliNpmResolver> {
    match self.as_inner() {
      InnerCliNpmResolverRef::Managed(inner) => Some(inner),
      InnerCliNpmResolverRef::Byonm(_) => None,
    }
  }

  fn as_byonm(&self) -> Option<&CliByonmNpmResolver> {
    match self.as_inner() {
      InnerCliNpmResolverRef::Managed(_) => None,
      InnerCliNpmResolverRef::Byonm(inner) => Some(inner),
    }
  }

  fn root_node_modules_path(&self) -> Option<&Path>;

  fn ensure_read_permission<'a>(
    &self,
    permissions: &mut dyn NodePermissions,
    path: &'a Path,
  ) -> Result<Cow<'a, Path>, AnyError>;

  /// Returns a hash returning the state of the npm resolver
  /// or `None` if the state currently can't be determined.
  fn check_state_hash(&self) -> Option<u64>;
}
