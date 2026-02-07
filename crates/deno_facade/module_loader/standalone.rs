use fs::VfsSys;
use std::borrow::Cow;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use crate::source_map_store;

use anyhow::Context;
use anyhow::anyhow;
use base64::Engine;
use deno::PermissionsContainer;
use deno::args::CacheSetting;
use deno::args::NpmInstallDepsProvider;
use deno::cache::Caches;
use deno::cache::DenoDirProvider;
use deno::cache::NodeAnalysisCache;
use deno::deno_ast::MediaType;
use deno::deno_cache_dir::npm::NpmCacheDir;
use deno::deno_fs::RealFs;
use deno::deno_package_json;
use deno::deno_package_json::PackageJsonDepValue;
use deno::deno_permissions::PermissionDescriptorParser;
use deno::deno_permissions::Permissions;
use deno::deno_permissions::PermissionsOptions;
use deno::deno_resolver::cjs::IsCjsResolutionMode;
use deno::deno_resolver::npm::DenoInNpmPackageChecker;
use deno::deno_resolver::npm::NpmReqResolver;
use deno::deno_resolver::npm::NpmReqResolverOptions;
use deno::deno_resolver::workspace::MappedResolution;
use deno::deno_resolver::workspace::WorkspaceResolver;
use deno::deno_semver::npm::NpmPackageReqReference;
use deno::deno_tls::RootCertStoreProvider;
use deno::deno_tls::rustls::RootCertStore;
use deno::http_util::HttpClientProvider;
use deno::node::GenericCjsCodeAnalyzer;
use deno::node::GenericCjsTracker;
use deno::node_resolver;
use deno::node_resolver::NodeResolutionKind;
use deno::node_resolver::PackageJsonResolver;
use deno::node_resolver::ResolutionMode;
use deno::node_resolver::UrlOrPathRef;
use deno::node_resolver::analyze::NodeCodeTranslator;
use deno::npm::CliManagedInNpmPkgCheckerCreateOptions;
use deno::npm::CliManagedNpmResolverCreateOptions;
use deno::npm::CliNpmResolver;
use deno::npm::CliNpmResolverCreateOptions;
use deno::npm::CliNpmResolverManagedSnapshotOption;
use deno::npm::CreateInNpmPkgCheckerOptions;
use deno::npm::byonm::CliByonmNpmResolverCreateOptions;
use deno::npm::create_cli_npm_resolver;
use deno::npm::create_in_npm_pkg_checker;
use deno::resolver::CliSloppyImportsResolver;
use deno::resolver::GenericNpmModuleLoader;
use deno::standalone::binary;
use deno::util::text_encoding::from_utf8_lossy_cow;
use deno_config::workspace::ResolverWorkspaceJsrPackage;
use deno_core::FastString;
use deno_core::ModuleLoader;
use deno_core::ModuleSourceCode;
use deno_core::ModuleSpecifier;
use deno_core::ModuleType;
use deno_core::RequestedModuleType;
use deno_core::ResolutionKind;
use deno_core::error::AnyError;
use deno_core::futures::FutureExt;
use deno_core::url::Url;
use deno_error::JsErrorBox;
use deno_maybe_sync::new_rc;
use eszip::EszipRelativeFileBaseUrl;
use eszip::ModuleKind;
use eszip_trait::AsyncEszipDataRead;
use ext_node::NodeExtInitServices;
use ext_node::NodeRequireLoader;
use ext_node::NodeResolver;
use ext_node::create_host_defined_options;
use ext_runtime::cert::CaData;
use ext_runtime::cert::get_root_cert_store;
use fs::deno_compile_fs::DenoCompileFileSystem;
use fs::virtual_fs::FileBackedVfs;
use futures_util::future::OptionFuture;
use tracing::instrument;

use crate::EszipPayloadKind;
use crate::LazyLoadableEszip;
use crate::eszip::vfs::load_npm_vfs;
use crate::metadata::Metadata;
use crate::migrate;
use crate::migrate::MigrateOptions;
use crate::payload_to_eszip;
use crate::permissions::RuntimePermissionDescriptorParser;

use super::RuntimeProviders;
use super::util::arc_u8_to_arc_str;

pub struct WorkspaceEszipModule {
  specifier: ModuleSpecifier,
  inner: eszip::Module,
}

pub struct WorkspaceEszip {
  pub eszip: LazyLoadableEszip,
  pub root_dir_url: Arc<Url>,
}

const SLOPPY_IMPORT_EXTENSIONS: &[&str] =
  &[".ts", ".tsx", ".mts", ".cts", ".js", ".jsx", ".mjs", ".cjs"];

const SLOPPY_INDEX_FILES: &[&str] = &[
  "/index.ts",
  "/index.tsx",
  "/index.mts",
  "/index.cts",
  "/index.js",
  "/index.jsx",
  "/index.mjs",
  "/index.cjs",
];

impl WorkspaceEszip {
  pub fn get_module(
    &self,
    specifier: &ModuleSpecifier,
  ) -> Option<WorkspaceEszipModule> {
    if specifier.scheme() == "file" {
      let base_url = EszipRelativeFileBaseUrl::new(&self.root_dir_url);
      let specifier_key = base_url.specifier_key(specifier);

      if let Some(module) = self.eszip.ensure_module(&specifier_key) {
        let specifier = self.root_dir_url.join(&module.specifier).unwrap();
        return Some(WorkspaceEszipModule {
          specifier,
          inner: module,
        });
      }

      let has_extension = SLOPPY_IMPORT_EXTENSIONS
        .iter()
        .any(|ext| specifier_key.ends_with(ext));

      if !has_extension {
        for ext in SLOPPY_IMPORT_EXTENSIONS {
          let key_with_ext = format!("{}{}", specifier_key, ext);
          if let Some(module) = self.eszip.ensure_module(&key_with_ext) {
            let specifier = self.root_dir_url.join(&module.specifier).unwrap();
            return Some(WorkspaceEszipModule {
              specifier,
              inner: module,
            });
          }
        }

        for index in SLOPPY_INDEX_FILES {
          let key_with_index = format!("{}{}", specifier_key, index);
          if let Some(module) = self.eszip.ensure_module(&key_with_index) {
            let specifier = self.root_dir_url.join(&module.specifier).unwrap();
            return Some(WorkspaceEszipModule {
              specifier,
              inner: module,
            });
          }
        }
      }

      None
    } else {
      let module = self.eszip.ensure_module(specifier.as_str())?;

      Some(WorkspaceEszipModule {
        specifier: ModuleSpecifier::parse(&module.specifier).unwrap(),
        inner: module,
      })
    }
  }
}

// Type aliases for VfsSys-based node resolution
type VfsNpmResolver = deno::deno_resolver::npm::NpmResolver<VfsSys>;

type VfsNodeResolver = NodeResolver<
  deno::deno_resolver::npm::DenoInNpmPackageChecker,
  VfsNpmResolver,
  VfsSys,
>;
type VfsNpmReqResolver = NpmReqResolver<
  deno::deno_resolver::npm::DenoInNpmPackageChecker,
  node_resolver::DenoIsBuiltInNodeModuleChecker,
  VfsNpmResolver,
  VfsSys,
>;

// VfsSys-based CjsTracker (using GenericCjsTracker from deno/node.rs)
type VfsCjsTracker = GenericCjsTracker<VfsSys>;

// VfsSys-based CjsCodeAnalyzer
type VfsCjsCodeAnalyzer = GenericCjsCodeAnalyzer<VfsSys>;

// VFS-aware NodeCodeTranslator
type VfsNodeCodeTranslator = deno::node_resolver::analyze::NodeCodeTranslator<
  VfsCjsCodeAnalyzer,
  deno::deno_resolver::npm::DenoInNpmPackageChecker,
  node_resolver::DenoIsBuiltInNodeModuleChecker,
  VfsNpmResolver,
  VfsSys,
>;

// VfsSys-based NpmModuleLoader
type VfsNpmModuleLoader = GenericNpmModuleLoader<
  deno::deno_resolver::npm::DenoInNpmPackageChecker,
  VfsSys,
  VfsCjsCodeAnalyzer,
  node_resolver::DenoIsBuiltInNodeModuleChecker,
  VfsNpmResolver,
>;

pub struct SharedModuleLoaderState {
  pub(crate) root_path: PathBuf,
  pub(crate) service_path: Option<PathBuf>,
  pub(crate) eszip: WorkspaceEszip,
  pub(crate) workspace_resolver: WorkspaceResolver<VfsSys>,
  pub(crate) cjs_tracker: Arc<VfsCjsTracker>,
  pub(crate) node_code_translator: Arc<VfsNodeCodeTranslator>,
  pub(crate) npm_module_loader: Arc<VfsNpmModuleLoader>,
  pub(crate) npm_req_resolver: Arc<VfsNpmReqResolver>,
  pub(crate) npm_resolver: Arc<dyn CliNpmResolver>,
  pub(crate) node_resolver: Arc<VfsNodeResolver>,
  pub(crate) vfs: Arc<FileBackedVfs>,
  #[allow(dead_code)]
  pub(crate) sloppy_imports_resolver: Option<Arc<CliSloppyImportsResolver>>,
  pub(crate) disable_fs_fallback: bool,
}

#[derive(Clone)]
pub struct EmbeddedModuleLoader {
  pub(crate) shared: Arc<SharedModuleLoaderState>,
  pub(crate) include_source_map: bool,
}

impl ModuleLoader for EmbeddedModuleLoader {
  #[instrument(level = "debug", skip(self))]
  fn resolve(
    &self,
    specifier: &str,
    referrer: &str,
    kind: ResolutionKind,
  ) -> Result<ModuleSpecifier, deno_core::error::ModuleLoaderError> {
    let referrer = if referrer == "." {
      if kind != ResolutionKind::MainModule {
        return Err(JsErrorBox::type_error(format!(
          "Expected to resolve main module, got {:?} instead.",
          kind
        )));
      }

      deno_core::resolve_path(".", &self.shared.root_path)
        .map_err(|e| JsErrorBox::type_error(format!("{:#}", e)))?
    } else {
      ModuleSpecifier::parse(referrer).map_err(|err| {
        JsErrorBox::type_error(format!(
          "Referrer uses invalid specifier: {}",
          err
        ))
      })?
    };
    let referrer_kind = if self
      .shared
      .cjs_tracker
      .is_maybe_cjs(&referrer, MediaType::from_specifier(&referrer))
      .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?
    {
      ResolutionMode::Require
    } else {
      ResolutionMode::Import
    };

    if self.shared.node_resolver.in_npm_package(&referrer) {
      let url_or_path = self
        .shared
        .node_resolver
        .resolve(
          specifier,
          &referrer,
          referrer_kind,
          NodeResolutionKind::Execution,
        )
        .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;
      return url_or_path
        .into_url()
        .map_err(|e| JsErrorBox::generic(format!("{:#}", e)));
    }

    let mapped_resolution = self.shared.workspace_resolver.resolve(
      specifier,
      &referrer,
      deno::deno_resolver::workspace::ResolutionKind::Execution,
    );

    match mapped_resolution {
      Ok(MappedResolution::WorkspaceJsrPackage { specifier, .. }) => {
        Ok(specifier)
      }
      Ok(MappedResolution::WorkspaceNpmPackage {
        target_pkg_json: pkg_json,
        sub_path,
        ..
      }) => {
        let url_or_path = self
          .shared
          .node_resolver
          .resolve_package_subpath_from_deno_module(
            pkg_json.dir_path(),
            sub_path.as_deref(),
            Some(&referrer),
            referrer_kind,
            NodeResolutionKind::Execution,
          )
          .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;
        Ok(
          url_or_path
            .into_url()
            .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?,
        )
      }
      Ok(MappedResolution::PackageJsonImport { pkg_json }) => {
        let referrer_path_ref = UrlOrPathRef::from_url(&referrer);
        let url_or_path = self
          .shared
          .node_resolver
          .resolve_package_import(
            specifier,
            Some(&referrer_path_ref),
            Some(pkg_json),
            referrer_kind,
            NodeResolutionKind::Execution,
          )
          .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;
        url_or_path
          .into_url()
          .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))
      }
      Ok(MappedResolution::PackageJson {
        dep_result,
        sub_path,
        alias,
        ..
      }) => match dep_result.as_ref().map_err(|e| {
        JsErrorBox::generic(format!("{:#}", AnyError::from(e.clone())))
      })? {
        PackageJsonDepValue::Req(req) => {
          let url_or_path = self
            .shared
            .npm_req_resolver
            .resolve_req_with_sub_path(
              req,
              sub_path.as_deref(),
              &referrer,
              referrer_kind,
              NodeResolutionKind::Execution,
            )
            .map_err(|e| {
              JsErrorBox::generic(format!("{:#}", AnyError::from(e)))
            })?;
          Ok(
            url_or_path
              .into_url()
              .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?,
          )
        }

        PackageJsonDepValue::Workspace(version_req) => {
          let pkg_folder = self
            .shared
            .workspace_resolver
            .resolve_workspace_pkg_json_folder_for_pkg_json_dep(
              alias,
              version_req,
            )
            .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;
          let url_or_path = self
            .shared
            .node_resolver
            .resolve_package_subpath_from_deno_module(
              pkg_folder,
              sub_path.as_deref(),
              Some(&referrer),
              referrer_kind,
              NodeResolutionKind::Execution,
            )
            .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;
          let url = url_or_path
            .into_url()
            .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;
          Ok(url)
        }

        PackageJsonDepValue::File(_) => {
          // file: protocol dependencies are not supported in standalone mode
          Err(JsErrorBox::type_error(format!(
            "file: protocol dependencies are not supported in package.json (dependency: {})",
            alias
          )))
        }

        PackageJsonDepValue::JsrReq(_) => {
          // jsr: protocol dependencies are not supported in standalone mode
          Err(JsErrorBox::type_error(format!(
            "jsr: protocol dependencies are not supported in package.json (dependency: {})",
            alias
          )))
        }
      },
      Ok(MappedResolution::Normal { specifier, .. }) => {
        if let Ok(reference) =
          NpmPackageReqReference::from_specifier(&specifier)
        {
          let url_or_path = self
            .shared
            .npm_req_resolver
            .resolve_req_reference(
              &reference,
              &referrer,
              referrer_kind,
              NodeResolutionKind::Execution,
            )
            .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;
          let url = url_or_path
            .into_url()
            .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;
          return Ok(url);
        }

        if specifier.scheme() == "jsr"
          && let Some(module) = self.shared.eszip.get_module(&specifier)
        {
          return Ok(module.specifier);
        }

        let final_specifier = self
          .shared
          .node_resolver
          .handle_if_in_node_modules(&specifier)
          .unwrap_or_else(|| specifier.clone());

        Ok(final_specifier)
      }
      Err(err)
        if err.is_unmapped_bare_specifier() && referrer.scheme() == "file" =>
      {
        let maybe_res = self.shared.npm_req_resolver.resolve_if_for_npm_pkg(
          specifier,
          &referrer,
          referrer_kind,
          NodeResolutionKind::Execution,
        );
        if let Ok(Some(res)) = maybe_res {
          return res
            .into_url()
            .map_err(|e| JsErrorBox::generic(format!("{:#}", e)));
        }
        Err(JsErrorBox::type_error(format!("{:#}", err)))
      }
      Err(err) => Err(JsErrorBox::type_error(format!("{:#}", err))),
    }
  }

  fn get_host_defined_options<'s>(
    &self,
    scope: &mut deno_core::v8::PinScope<'s, '_>,
    name: &str,
  ) -> Option<deno_core::v8::Local<'s, deno_core::v8::Data>> {
    let name = deno_core::ModuleSpecifier::parse(name).ok()?;
    if self.shared.node_resolver.in_npm_package(&name) {
      Some(create_host_defined_options(scope))
    } else {
      None
    }
  }

  #[instrument(level = "debug", skip_all, fields(specifier = original_specifier.as_str()))]
  fn load(
    &self,
    original_specifier: &ModuleSpecifier,
    maybe_referrer: Option<&deno_core::ModuleLoadReferrer>,
    _is_dynamic: bool,
    _requested_module_type: RequestedModuleType,
  ) -> deno_core::ModuleLoadResponse {
    let include_source_map = self.include_source_map;

    if original_specifier.scheme() == "data" {
      let data_url_text =
        match data_url::DataUrl::process(original_specifier.as_str()) {
          Ok(data_url) => {
            let (bytes, _) = match data_url.decode_to_vec() {
              Ok(result) => result,
              Err(err) => {
                return deno_core::ModuleLoadResponse::Sync(Err(
                  JsErrorBox::type_error(format!(
                    "Failed to decode data URL: {:#}",
                    err
                  )),
                ));
              }
            };
            match String::from_utf8(bytes) {
              Ok(text) => text,
              Err(err) => {
                return deno_core::ModuleLoadResponse::Sync(Err(
                  JsErrorBox::type_error(format!(
                    "Data URL is not valid UTF-8: {:#}",
                    err
                  )),
                ));
              }
            }
          }
          Err(err) => {
            return deno_core::ModuleLoadResponse::Sync(Err(
              JsErrorBox::type_error(format!("Invalid data URL: {:#}", err)),
            ));
          }
        };

      return deno_core::ModuleLoadResponse::Sync(Ok(
        deno_core::ModuleSource::new(
          deno_core::ModuleType::JavaScript,
          ModuleSourceCode::String(data_url_text.into()),
          original_specifier,
          None,
        ),
      ));
    }

    if self.shared.node_resolver.in_npm_package(original_specifier) {
      let npm_module_loader = self.shared.npm_module_loader.clone();
      let original_specifier = original_specifier.clone();
      let maybe_referrer = maybe_referrer.cloned();

      return deno_core::ModuleLoadResponse::Async(
        async move {
          let code_source = npm_module_loader
            .load(
              &original_specifier,
              maybe_referrer.as_ref().map(|r| &r.specifier),
            )
            .await
            .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;

          Ok(deno_core::ModuleSource::new_with_redirect(
            match code_source.media_type {
              MediaType::Json => ModuleType::Json,
              _ => ModuleType::JavaScript,
            },
            code_source.code,
            &original_specifier,
            &code_source.found_url,
            None,
          ))
        }
        .boxed_local(),
      );
    }

    let Some(module) = self.shared.eszip.get_module(original_specifier) else {
      #[allow(clippy::collapsible_if)]
      if !self.shared.disable_fs_fallback
        && original_specifier.scheme() == "file"
      {
        if let Ok(path) = original_specifier.to_file_path() {
          let paths_to_try: Vec<PathBuf> = {
            let mut paths = vec![path.clone()];
            let path_str = path.to_string_lossy();
            if let Some(rest) =
              path_str.strip_prefix("/var/tmp/sb-compile-trex/")
            {
              if let (Some(relative), Some(svc_path)) = (
                rest.split_once('/').map(|(_, r)| r),
                &self.shared.service_path,
              ) {
                paths.push(svc_path.join(relative));
              }
            }
            paths
          };

          let code = paths_to_try
            .iter()
            .find_map(|p| std::fs::read_to_string(p).ok());
          if let Some(code) = code {
            let media_type = MediaType::from_specifier(original_specifier);
            let (final_code, module_type) = match media_type {
              MediaType::TypeScript
              | MediaType::Mts
              | MediaType::Cts
              | MediaType::Tsx => {
                match deno::deno_ast::parse_module(
                  deno::deno_ast::ParseParams {
                    specifier: original_specifier.clone(),
                    text: code.into(),
                    media_type,
                    capture_tokens: false,
                    scope_analysis: false,
                    maybe_syntax: None,
                  },
                ) {
                  Ok(parsed) => {
                    match parsed.transpile(
                      &deno::deno_ast::TranspileOptions {
                        imports_not_used_as_values:
                          deno::deno_ast::ImportsNotUsedAsValues::Remove,
                        ..Default::default()
                      },
                      &deno::deno_ast::TranspileModuleOptions::default(),
                      &deno::deno_ast::EmitOptions::default(),
                    ) {
                      Ok(transpiled) => {
                        let source = transpiled.into_source();
                        (source.text, ModuleType::JavaScript)
                      }
                      Err(e) => {
                        return deno_core::ModuleLoadResponse::Sync(Err(
                          JsErrorBox::type_error(format!(
                            "Failed to transpile {}: {:?}",
                            original_specifier, e
                          )),
                        ));
                      }
                    }
                  }
                  Err(e) => {
                    return deno_core::ModuleLoadResponse::Sync(Err(
                      JsErrorBox::type_error(format!(
                        "Failed to parse {}: {:?}",
                        original_specifier, e
                      )),
                    ));
                  }
                }
              }
              MediaType::Json => (code, ModuleType::Json),
              _ => (code, ModuleType::JavaScript),
            };

            return deno_core::ModuleLoadResponse::Sync(Ok(
              deno_core::ModuleSource::new(
                module_type,
                ModuleSourceCode::String(final_code.into()),
                original_specifier,
                None,
              ),
            ));
          }
        }
      }
      return deno_core::ModuleLoadResponse::Sync(Err(JsErrorBox::type_error(
        format!("Module not found: {}", original_specifier),
      )));
    };

    let original_specifier = original_specifier.clone();
    let media_type = MediaType::from_specifier(&module.specifier);
    let shared = self.shared.clone();
    let is_maybe_cjs = match shared
      .cjs_tracker
      .is_maybe_cjs(&original_specifier, media_type)
    {
      Ok(is_maybe_cjs) => is_maybe_cjs,
      Err(err) => {
        return deno_core::ModuleLoadResponse::Sync(Err(
          JsErrorBox::type_error(format!("{:?}", err)),
        ));
      }
    };

    deno_core::ModuleLoadResponse::Async(
      async move {
        let code = module.inner.source().await.ok_or_else(|| {
          JsErrorBox::type_error(format!(
            "Module not found: {}",
            original_specifier
          ))
        })?;

        if module.inner.kind == ModuleKind::Wasm {
          return Ok(deno_core::ModuleSource::new_with_redirect(
            ModuleType::Wasm,
            ModuleSourceCode::Bytes(code.into()),
            &original_specifier,
            &module.specifier,
            None,
          ));
        }

        let code = arc_u8_to_arc_str(code)
          .map_err(|_| JsErrorBox::type_error("Module source is not utf-8"))?;

        if is_maybe_cjs {
          let source = shared
            .node_code_translator
            .translate_cjs_to_esm(
              &module.specifier,
              Some(code.to_string().into()),
            )
            .await
            .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;
          let module_source = match source {
            Cow::Owned(source) => ModuleSourceCode::String(source.into()),
            Cow::Borrowed(source) => {
              ModuleSourceCode::String(FastString::from_static(source))
            }
          };
          Ok(deno_core::ModuleSource::new_with_redirect(
            ModuleType::JavaScript,
            module_source,
            &original_specifier,
            &module.specifier,
            None,
          ))
        } else {
          let source_map = module.inner.source_map().await;
          let maybe_code_with_source_map = 'scope: {
            if !include_source_map {
              break 'scope code;
            }
            if !matches!(module.inner.kind, ModuleKind::JavaScript) {
              break 'scope code;
            }

            let Some(source_map) = source_map else {
              break 'scope code;
            };
            if source_map.is_empty() {
              break 'scope code;
            }

            source_map_store::store_source_map(
              original_specifier.as_str(),
              &source_map,
            );

            let mut src = code.to_string();

            if !src.ends_with('\n') {
              src.push('\n');
            }

            const SOURCE_MAP_PREFIX: &str =
              "//# sourceMappingURL=data:application/json;base64,";

            src.push_str(SOURCE_MAP_PREFIX);

            base64::prelude::BASE64_STANDARD
              .encode_string(source_map, &mut src);
            Arc::from(src)
          };

          Ok(deno_core::ModuleSource::new_with_redirect(
            match module.inner.kind {
              ModuleKind::JavaScript => ModuleType::JavaScript,
              ModuleKind::Json => ModuleType::Json,
              ModuleKind::Jsonc => {
                return Err(JsErrorBox::type_error(
                  "jsonc modules not supported",
                ));
              }
              ModuleKind::OpaqueData | ModuleKind::Wasm => {
                unreachable!();
              }
            },
            ModuleSourceCode::String(maybe_code_with_source_map.into()),
            &original_specifier,
            &module.specifier,
            None,
          ))
        }
      }
      .boxed_local(),
    )
  }
}

impl NodeRequireLoader for EmbeddedModuleLoader {
  fn ensure_read_permission<'a>(
    &self,
    permissions: &mut dyn ext_node::NodePermissions,
    path: Cow<'a, Path>,
  ) -> Result<Cow<'a, Path>, JsErrorBox> {
    if self.shared.vfs.open_file(&path).is_ok() {
      // allow reading if the file is in the virtual fs
      return Ok(path);
    }

    self
      .shared
      .npm_resolver
      .ensure_read_permission(permissions, path.as_ref())
      .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;
    Ok(path)
  }

  fn load_text_file_lossy(
    &self,
    path: &Path,
  ) -> Result<deno_core::FastString, JsErrorBox> {
    let file_entry = self
      .shared
      .vfs
      .open_file(path)
      .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;
    let file_bytes = file_entry
      .read_all_sync()
      .map_err(|e| JsErrorBox::generic(format!("{:#}", e)))?;
    Ok(from_utf8_lossy_cow(file_bytes).to_string().into())
  }

  fn is_maybe_cjs(
    &self,
    specifier: &Url,
  ) -> Result<bool, deno::node_resolver::errors::PackageJsonLoadError> {
    let media_type = MediaType::from_specifier(specifier);
    self.shared.cjs_tracker.is_maybe_cjs(specifier, media_type)
  }
}

pub struct StandaloneModuleLoaderFactory {
  shared: Arc<SharedModuleLoaderState>,
}

struct StandaloneRootCertStoreProvider {
  ca_stores: Option<Vec<String>>,
  ca_data: Option<CaData>,
  cell: once_cell::sync::OnceCell<RootCertStore>,
}

impl RootCertStoreProvider for StandaloneRootCertStoreProvider {
  fn get_or_try_init(&self) -> Result<&RootCertStore, JsErrorBox> {
    self.cell.get_or_try_init(|| {
      get_root_cert_store(None, self.ca_stores.clone(), self.ca_data.clone())
        .map_err(|err| JsErrorBox::generic(format!("{:#}", err)))
    })
  }
}

pub async fn create_module_loader_for_eszip(
  mut eszip: LazyLoadableEszip,
  permissions_options: PermissionsOptions,
  include_source_map: bool,
  service_path: Option<&str>,
  disable_fs_fallback: bool,
) -> Result<RuntimeProviders, AnyError> {
  let migrated = eszip.migrated();
  let current_exe_path = std::env::current_exe().unwrap();
  let _current_exe_name =
    current_exe_path.file_name().unwrap().to_string_lossy();

  let permission_desc_parser =
    Arc::new(RuntimePermissionDescriptorParser::new(Arc::new(RealFs)))
      as Arc<dyn PermissionDescriptorParser>;
  let permissions =
    Permissions::from_options(&*permission_desc_parser, &permissions_options)?;
  let permissions_container =
    PermissionsContainer::new(permission_desc_parser.clone(), permissions);

  let mut metadata = OptionFuture::<_>::from(
    eszip
      .ensure_module(eszip_trait::v2::METADATA_KEY)
      .map(|it| async move { it.source().await }),
  )
  .await
  .flatten()
  .map(|it| {
    rkyv::from_bytes::<Metadata>(it.as_ref())
      .map_err(|_| anyhow!("failed to deserialize metadata from eszip"))
  })
  .transpose()?
  .unwrap_or_default();

  let root_path = if cfg!(target_family = "unix") {
    // Canonicalize /var/tmp to resolve symlinks (e.g., /var -> /private/var on macOS)
    // This ensures VFS paths match the canonical paths Deno resolves to
    std::fs::canonicalize("/var/tmp")
      .unwrap_or_else(|_| PathBuf::from("/var/tmp"))
  } else {
    std::env::temp_dir()
  }
  .join("sb-compile-trex");

  let node_modules = metadata.node_modules()?;
  let root_dir_url =
    Arc::new(ModuleSpecifier::from_directory_path(&root_path).unwrap());

  // Check if npm packages are embedded in VFS
  let has_embedded_npm = metadata.virtual_dir.is_some();

  let deno_dir_provider = Arc::new(DenoDirProvider::new(None));

  // If no embedded npm packages, use global Deno npm cache with real registry
  let (root_node_modules_path, npm_registry_url, use_real_fs) =
    if has_embedded_npm {
      // Compiled binary with embedded npm packages - use dummy localhost
      let root_node_modules_path = match &node_modules {
        Some(binary::NodeModules::Managed { .. }) | None => {
          root_path.join("node_modules")
        }
        Some(binary::NodeModules::Byonm { .. }) => root_path.clone(),
      };
      let npm_registry_url =
        ModuleSpecifier::parse("https://localhost/").unwrap();
      (root_node_modules_path, npm_registry_url, false)
    } else {
      // Development mode - use global Deno npm cache with real registry
      let deno_dir = deno_dir_provider
        .get_or_create()
        .map_err(|e| anyhow!("failed to get deno dir: {}", e))?;
      let npm_folder = deno_dir.npm_folder_path();
      let npm_registry_url =
        ModuleSpecifier::parse("https://registry.npmjs.org/").unwrap();
      (npm_folder, npm_registry_url, true)
    };

  // Log npm cache path for debugging
  tracing::debug!(
    "npm cache path: {}, use_real_fs: {}, has_embedded_npm: {}",
    root_node_modules_path.display(),
    use_real_fs,
    has_embedded_npm
  );

  let static_files = metadata.static_assets_lookup(&root_path);
  let npmrc = metadata.resolved_npmrc(&npm_registry_url)?;
  let root_cert_store_provider = Arc::new(StandaloneRootCertStoreProvider {
    ca_stores: metadata.ca_stores.take(),
    ca_data: metadata.ca_data.take().map(CaData::Bytes),
    cell: Default::default(),
  });

  let http_client_provider = Arc::new(HttpClientProvider::new(
    Some(root_cert_store_provider.clone()),
    metadata.unsafely_ignore_certificate_errors.clone(),
  ));

  let (fs, vfs): (Arc<dyn deno::deno_fs::FileSystem>, Arc<FileBackedVfs>) =
    if use_real_fs {
      // Development mode: use real filesystem for npm packages
      // Create a minimal VFS for non-npm modules from eszip, but allow FS fallback
      let vfs = load_npm_vfs(
        Arc::new(eszip.clone()),
        root_node_modules_path.clone(),
        None, // No virtual_dir - will create empty VFS
      )
      .context("Failed to load npm vfs.")?;

      let fs = DenoCompileFileSystem::new(vfs).use_real_fs(true);
      let fs_backed_vfs = fs.file_backed_vfs().clone();

      (
        Arc::new(fs) as Arc<dyn deno::deno_fs::FileSystem>,
        fs_backed_vfs,
      )
    } else {
      // Compiled binary mode: use VFS with embedded npm packages
      // Use root_node_modules_path as VFS root because the virtual_dir is the node_modules
      // directory itself. This ensures path resolution works correctly:
      // - npm paths like /root/node_modules/localhost/pkg/... get root stripped to localhost/pkg/...
      // - VFS root dir (node_modules) contains localhost as an entry, so lookup succeeds
      let vfs = load_npm_vfs(
        Arc::new(eszip.clone()),
        root_node_modules_path.clone(),
        metadata.virtual_dir.take(),
      )
      .context("Failed to load npm vfs.")?;

      let fs = DenoCompileFileSystem::new(vfs).use_real_fs(false);
      let fs_backed_vfs = fs.file_backed_vfs().clone();

      (
        Arc::new(fs) as Arc<dyn deno::deno_fs::FileSystem>,
        fs_backed_vfs,
      )
    };

  // Create VfsSys for reading files from the eszip VFS.
  // VfsSys automatically falls back to RealSys for paths outside the VFS,
  // so it works correctly for both bundle mode (reads from VFS) and development mode
  // (falls back to real filesystem).
  // This is critical for npm package resolution - the PackageJsonResolver needs to read
  // package.json files from the VFS to determine entry points (e.g., "main": "fastify.js").
  let vfs_sys = VfsSys::new(vfs.clone());

  let npm_cache_dir = Arc::new(NpmCacheDir::new(
    &vfs_sys,
    root_node_modules_path,
    npmrc.get_all_known_registries_urls(),
  ));

  let snapshot = eszip.take_npm_snapshot();

  let pkg_json_resolver =
    Arc::new(PackageJsonResolver::new(vfs_sys.clone(), None));
  let (_in_npm_pkg_checker, npm_resolver) = match node_modules {
    Some(binary::NodeModules::Managed { .. }) | None => {
      let in_npm_pkg_checker =
        create_in_npm_pkg_checker(CreateInNpmPkgCheckerOptions::Managed(
          CliManagedInNpmPkgCheckerCreateOptions {
            root_cache_dir_url: npm_cache_dir.root_dir_url(),
            maybe_node_modules_path: None,
          },
        ));
      // In development mode (use_real_fs), don't use the eszip snapshot as it contains
      // localhost-based package IDs. Instead use ResolveFromLockfile or no snapshot
      // so the resolver will look for packages in the real npm cache.
      let snapshot_option = if use_real_fs {
        CliNpmResolverManagedSnapshotOption::Specified(None)
      } else {
        CliNpmResolverManagedSnapshotOption::Specified(snapshot.clone())
      };
      let npm_resolver =
        create_cli_npm_resolver(CliNpmResolverCreateOptions::Managed(
          CliManagedNpmResolverCreateOptions {
            snapshot: snapshot_option,
            maybe_lockfile: None,
            fs: fs.clone(),
            http_client_provider,
            npm_cache_dir: npm_cache_dir.clone(),
            cache_setting: CacheSetting::Use,
            maybe_node_modules_path: None,
            npm_system_info: Default::default(),
            npm_install_deps_provider: Arc::new(
              // this is only used for installing packages, which isn't necessary
              // with deno compile
              NpmInstallDepsProvider::empty(),
            ),
            npmrc,
          },
        ))
        .await?;
      (in_npm_pkg_checker, npm_resolver)
    }
    Some(binary::NodeModules::Byonm {
      ref root_node_modules_dir,
    }) => {
      let root_node_modules_dir =
        root_node_modules_dir.as_ref().map(|p| vfs.root().join(p));
      let in_npm_pkg_checker =
        create_in_npm_pkg_checker(CreateInNpmPkgCheckerOptions::Byonm);
      // For Byonm, create with RealSys (cli_sys) since the CLI npm resolver types require CliSys
      // The concrete_npm_resolver will be created later with VfsSys
      let cli_sys = sys_traits::impls::RealSys;
      let cli_pkg_json_resolver =
        Arc::new(PackageJsonResolver::new(cli_sys.clone(), None));
      let npm_resolver = create_cli_npm_resolver(
        CliNpmResolverCreateOptions::Byonm(CliByonmNpmResolverCreateOptions {
          sys: deno::node_resolver::cache::NodeResolutionSys::new(
            cli_sys.clone(),
            None,
          ),
          pkg_json_resolver: cli_pkg_json_resolver.clone(),
          root_node_modules_dir,
        }),
      )
      .await?;
      (in_npm_pkg_checker, npm_resolver)
    }
  };

  // Convert trait objects to concrete types for Deno 2.5.6 upstream APIs
  // Create DenoInNpmPackageChecker directly from options (using deno_resolver types)
  let concrete_in_npm_pkg_checker = match node_modules {
    Some(binary::NodeModules::Managed { .. }) | None => {
      DenoInNpmPackageChecker::new(
        deno::deno_resolver::npm::CreateInNpmPkgCheckerOptions::Managed(
          deno::deno_resolver::npm::managed::ManagedInNpmPkgCheckerCreateOptions {
            root_cache_dir_url: npm_cache_dir.root_dir_url(),
            maybe_node_modules_path: None,
          },
        )
      )
    }
    Some(binary::NodeModules::Byonm { .. }) => {
      DenoInNpmPackageChecker::new(
        deno::deno_resolver::npm::CreateInNpmPkgCheckerOptions::Byonm
      )
    }
  };
  let concrete_npm_resolver = match npm_resolver.as_inner() {
    deno::npm::InnerCliNpmResolverRef::Managed(inner) => {
      // Convert CLI's ManagedCliNpmResolver to upstream ManagedNpmResolver
      // Strategy: Use the CLI resolver's snapshot to create a fresh upstream resolver

      // Get the snapshot from the CLI resolver
      let snapshot = inner.snapshot();

      // Create an upstream NpmResolutionCell from the snapshot
      use deno::deno_resolver::npm::managed::NpmResolutionCell;
      let npm_resolution_cell = NpmResolutionCell::new(snapshot);
      let npm_resolution = std::sync::Arc::new(npm_resolution_cell);

      // Get node_modules path from the CLI resolver
      let maybe_node_modules_path =
        inner.maybe_node_modules_path().map(|p| p.to_path_buf());

      // Create upstream ManagedNpmResolver
      use deno::deno_resolver::npm::managed::{
        ManagedNpmResolver, ManagedNpmResolverCreateOptions,
      };
      let upstream_resolver =
        ManagedNpmResolver::<VfsSys>::new(ManagedNpmResolverCreateOptions {
          npm_cache_dir: npm_cache_dir.clone(),
          sys: vfs_sys.clone(),
          maybe_node_modules_path,
          npm_system_info: inner.npm_system_info().clone(),
          npmrc: inner.npmrc().clone(),
          npm_resolution,
        });

      deno::deno_resolver::npm::NpmResolver::Managed(new_rc(upstream_resolver))
    }
    deno::npm::InnerCliNpmResolverRef::Byonm(inner) => {
      // Create a new ByonmNpmResolver with VfsSys instead of using the CliSys-typed inner
      use deno::deno_resolver::npm::{
        ByonmNpmResolver, ByonmNpmResolverCreateOptions,
      };
      let byonm_resolver =
        ByonmNpmResolver::new(ByonmNpmResolverCreateOptions {
          root_node_modules_dir: inner
            .root_node_modules_path()
            .map(|p| p.to_path_buf()),
          sys: node_resolver::cache::NodeResolutionSys::new(
            vfs_sys.clone(),
            None,
          ),
          pkg_json_resolver: pkg_json_resolver.clone(),
        });
      deno::deno_resolver::npm::NpmResolver::Byonm(new_rc(byonm_resolver))
    }
  };

  let node_resolver = Arc::new(NodeResolver::new(
    concrete_in_npm_pkg_checker.clone(),
    node_resolver::DenoIsBuiltInNodeModuleChecker,
    concrete_npm_resolver.clone(),
    pkg_json_resolver.clone(),
    node_resolver::cache::NodeResolutionSys::new(vfs_sys.clone(), None),
    Default::default(), // NodeResolverOptions
  ));

  // Use VfsSys-based CjsTracker - the pkg_json_resolver already uses VfsSys
  let cjs_tracker: Arc<VfsCjsTracker> =
    Arc::new(deno::deno_resolver::cjs::CjsTracker::new(
      concrete_in_npm_pkg_checker.clone(),
      pkg_json_resolver.clone(),
      IsCjsResolutionMode::ExplicitTypeCommonJs,
    ));

  let cache_db = Caches::new(deno_dir_provider.clone());
  let node_analysis_cache = NodeAnalysisCache::new(cache_db.node_analysis_db());
  let npm_req_resolver = Arc::new(NpmReqResolver::new(NpmReqResolverOptions {
    in_npm_pkg_checker: concrete_in_npm_pkg_checker.clone(),
    node_resolver: Arc::clone(&node_resolver),
    npm_resolver: concrete_npm_resolver.clone(),
    sys: vfs_sys.clone(),
  }));

  let cjs_esm_code_analyzer = GenericCjsCodeAnalyzer::new(
    node_analysis_cache,
    cjs_tracker.clone(),
    fs.clone(),
    None,
  );
  let cjs_module_export_analyzer =
    Arc::new(node_resolver::analyze::CjsModuleExportAnalyzer::new(
      cjs_esm_code_analyzer,
      concrete_in_npm_pkg_checker.clone(),
      Arc::clone(&node_resolver),
      concrete_npm_resolver.clone(),
      pkg_json_resolver.clone(),
      vfs_sys.clone(),
    ));
  let node_code_translator = Arc::new(NodeCodeTranslator::new(
    cjs_module_export_analyzer,
    node_resolver::analyze::NodeCodeTranslatorMode::ModuleLoader,
  ));

  let serialized_workspace_resolver =
    metadata.serialized_workspace_resolver()?;

  let module_loader_factory = StandaloneModuleLoaderFactory {
    shared: Arc::new(SharedModuleLoaderState {
      root_path,
      service_path: service_path.map(PathBuf::from),
      eszip: WorkspaceEszip {
        eszip,
        root_dir_url: root_dir_url.clone(),
      },
      workspace_resolver: {
        let import_map = match serialized_workspace_resolver.import_map {
          Some(import_map) => Some(
            import_map::parse_from_json_with_options(
              root_dir_url.join(&import_map.specifier)?,
              &import_map.json,
              import_map::ImportMapOptions {
                address_hook: None,
                expand_imports: true,
              },
            )?
            .import_map,
          ),
          None => None,
        };
        let pkg_jsons = serialized_workspace_resolver
          .package_jsons
          .into_iter()
          .map(|(relative_path, json)| {
            let path =
              root_dir_url.join(&relative_path)?.to_file_path().map_err(
                |_| anyhow!("failed to convert to file path from url"),
              )?;
            let pkg_json =
              deno_package_json::PackageJson::load_from_value(path, json)?;
            Ok::<_, AnyError>(Arc::new(pkg_json))
          })
          .collect::<Result<_, _>>()?;
        log::debug!("WorkspaceResolver root_dir_url: {}", root_dir_url);
        for jsr_pkg in &serialized_workspace_resolver.jsr_pkgs {
          log::debug!("JSR pkg relative_base: {}", jsr_pkg.relative_base);
        }
        WorkspaceResolver::new_raw(
          root_dir_url.clone(),
          import_map,
          serialized_workspace_resolver
            .jsr_pkgs
            .iter()
            .map(|it| {
              let base = root_dir_url
                .join(&it.relative_base)
                .with_context(|| "failed to parse base url")?;
              log::debug!("JSR pkg joined base: {}", base);
              Ok::<_, AnyError>(ResolverWorkspaceJsrPackage {
                is_link: false,
                base,
                name: it.name.clone(),
                version: it.version.clone(),
                exports: it.exports.clone(),
              })
            })
            .collect::<Result<_, _>>()?,
          pkg_jsons,
          serialized_workspace_resolver.pkg_json_resolution,
          deno::deno_resolver::workspace::SloppyImportsOptions::Enabled, // sloppy_imports_options
          Default::default(), // fs_cache_options
          vfs_sys.clone(),    // sys
        )
      },
      cjs_tracker: cjs_tracker.clone(),
      node_code_translator: node_code_translator.clone(),
      npm_module_loader: Arc::new(GenericNpmModuleLoader::new(
        cjs_tracker.clone(),
        fs.clone(),
        node_code_translator,
      )),
      npm_req_resolver,
      npm_resolver: npm_resolver.clone(),
      node_resolver: node_resolver.clone(),
      vfs: vfs.clone(),
      sloppy_imports_resolver: None,
      disable_fs_fallback,
    }),
  };

  let module_loader = Rc::new(EmbeddedModuleLoader {
    shared: module_loader_factory.shared.clone(),
    include_source_map,
  });

  Ok(RuntimeProviders {
    migrated,
    module_loader: module_loader.clone(),
    node_services: NodeExtInitServices {
      node_require_loader: module_loader.clone(),
      node_resolver,
      pkg_json_resolver,
      sys: vfs_sys.clone(),
    },
    npm_snapshot: snapshot,
    permissions: permissions_container,
    metadata,
    static_files,
    vfs_path: npm_cache_dir.root_dir().to_path_buf(),
    vfs,
    base_url: root_dir_url,
  })
}

pub async fn create_module_loader_for_standalone_from_eszip_kind(
  eszip_payload_kind: EszipPayloadKind,
  permissions_options: PermissionsOptions,
  include_source_map: bool,
  options: Option<MigrateOptions>,
  service_path: Option<&str>,
  disable_fs_fallback: bool,
) -> Result<RuntimeProviders, AnyError> {
  let eszip = migrate::try_migrate_if_needed(
    payload_to_eszip(eszip_payload_kind).await?,
    options,
  )
  .await?;

  create_module_loader_for_eszip(
    eszip,
    permissions_options,
    include_source_map,
    service_path,
    disable_fs_fallback,
  )
  .await
}
