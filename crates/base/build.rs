use std::env;
use std::path::PathBuf;

mod supabase_startup_snapshot {
  use std::io::Write;
  use std::rc::Rc;

  use deno_core::snapshot::create_snapshot;
  use deno_core::snapshot::CreateSnapshotOptions;
  use deno_core::v8;
  use deno_core::Extension;
  use deno_core::ExtensionFileSource;
  use deno_core::ExtensionFileSourceCode;

  use super::*;

  fn transpile_ts(
    specifier: deno_core::ModuleName,
    code: deno_core::ModuleCodeString,
  ) -> Result<
    (
      deno_core::ModuleCodeString,
      Option<deno_core::SourceMapData>,
    ),
    deno_error::JsErrorBox,
  > {
    deno::transpile::maybe_transpile_source(specifier, code)
  }

  // Name-only stub (base_runtime_permissions ships no ESM/ops; it only injects
  // state at runtime) to hold its slot in the list.
  deno_core::extension!(base_runtime_permissions);

  /// NOTE(deno-2.9.5): a single `lazy_loaded_js` / `lazy_loaded_esm` entry
  /// declared by an extension, as `(specifier, on-disk source path)`. Mirrors
  /// upstream's `deno_runtime::snapshot::LazyExtensionFile`.
  #[derive(Clone, Copy, Debug, PartialEq, Eq)]
  enum LazyKind {
    /// `lazy_loaded_js` - loaded on demand via `core.loadExtScript()`.
    Js,
    /// `lazy_loaded_esm` - loaded on demand via `op_lazy_load_esm()`.
    Esm,
  }

  struct LazyFile {
    specifier: String,
    path: PathBuf,
    kind: LazyKind,
  }

  fn lazy_file_entry(
    file: &ExtensionFileSource,
    kind: LazyKind,
  ) -> Option<LazyFile> {
    #[allow(deprecated)]
    let path = match &file.code {
      ExtensionFileSourceCode::LoadedFromFsDuringSnapshot(p) => {
        PathBuf::from(p)
      }
      // Entries pushed by customizers have no on-disk path and so cannot be
      // re-embedded by the build script.
      ExtensionFileSourceCode::IncludedInBinary(_)
      | ExtensionFileSourceCode::LoadedFromMemoryDuringSnapshot(_)
      | ExtensionFileSourceCode::Computed(_) => return None,
    };
    Some(LazyFile {
      specifier: file.specifier.to_string(),
      path,
      kind,
    })
  }

  fn collect_lazy_extension_files(extensions: &[Extension]) -> Vec<LazyFile> {
    let mut out = Vec::new();
    for ext in extensions {
      for file in &*ext.lazy_loaded_js_files {
        if let Some(entry) = lazy_file_entry(file, LazyKind::Js) {
          out.push(entry);
        }
      }
      for file in &*ext.lazy_loaded_esm_files {
        if let Some(entry) = lazy_file_entry(file, LazyKind::Esm) {
          out.push(entry);
        }
      }
    }
    out.sort_by(|a, b| a.specifier.cmp(&b.specifier));
    out.dedup_by(|a, b| a.specifier == b.specifier);
    out
  }

  /// Residual lazy sources arrive at the runtime untranspiled and unwrapped
  /// (only *consumed* sources go through the snapshot's transpiler), so do
  /// both here and write the result under `$OUT_DIR/residual_sources`.
  fn transpile_residual_source(
    out_dir: &std::path::Path,
    specifier: &str,
    src_path: &std::path::Path,
  ) -> PathBuf {
    let source = std::fs::read_to_string(src_path).unwrap_or_else(|e| {
      panic!(
        "failed to read residual lazy source {}: {e}",
        src_path.display()
      )
    });
    let (transpiled, _source_map) = transpile_ts(
      deno_core::ModuleName::from(specifier.to_string()),
      deno_core::ModuleCodeString::from(source),
    )
    .unwrap_or_else(|e| {
      panic!("failed to transpile residual lazy source {specifier}: {e}")
    });

    // `ext:deno_node/https.ts` -> `ext_deno_node_https_ts.js`
    let sanitized: String = specifier
      .chars()
      .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
      .collect();
    let out_path = out_dir.join(format!("{sanitized}.js"));
    std::fs::write(&out_path, transpiled.as_bytes()).unwrap();
    out_path
  }

  fn write_residual_table(
    f: &mut std::fs::File,
    out_dir: &std::path::Path,
    name: &str,
    entries: &[(String, PathBuf)],
  ) {
    writeln!(f, "pub static {name}: &[(&str, &str)] = &[").unwrap();
    let mut entries = entries.iter().collect::<Vec<_>>();
    // `ModuleMap::add_residual_lazy_loaded_sources` binary-searches these.
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    for (specifier, transpiled_path) in entries {
      let rel = transpiled_path.strip_prefix(out_dir).unwrap();
      writeln!(
        f,
        "  ({specifier:?}, include_str!(concat!(env!(\"OUT_DIR\"), {:?}))),",
        format!("/{}", rel.display()),
      )
      .unwrap();
    }
    writeln!(f, "];\n").unwrap();
  }

  pub fn create_runtime_snapshot(
    snapshot_path: PathBuf,
    residual_path: PathBuf,
  ) {
    println!("Creating runtime snapshot...");

    // Must mirror runtime/mod.rs's list (same names + order) or deno_core hits
    // ExtensionSnapshotMismatch. Build-safe subs: runtime_bootstrap::init(None);
    // build-available deno_node types; base_runtime_permissions stub; trex_core
    // included only when the `trex` feature is on, matching runtime/mod.rs.
    let extensions: Vec<Extension> = vec![
      deno_telemetry::deno_telemetry::init(),
      deno_webidl::deno_webidl::init(),
      deno_web::deno_web::lazy_init(),
      deno_webgpu::deno_webgpu::init(),
      deno_image::deno_image::init(),
      deno_fetch::deno_fetch::lazy_init(),
      deno_websocket::deno_websocket::lazy_init(),
      // TODO: support providing a custom seed for crypto
      deno_crypto::deno_crypto::lazy_init(),
      deno_net::deno_net::lazy_init(),
      deno_tls::deno_tls::init(),
      deno_node_crypto::deno_node_crypto::init(),
      deno_node_sqlite::deno_node_sqlite::init(),
      deno_http::deno_http::lazy_init(),
      deno_io::deno_io::lazy_init(),
      deno_fs::deno_fs::lazy_init(),
      ext_ai::ai::init(),
      #[cfg(feature = "trex")]
      trex_core::trex::init(),
      ext_env::env::init(),
      deno_process::deno_process::init(None),
      ext_workers::user_workers::init(),
      ext_event_worker::user_event_worker::init(),
      ext_event_worker::js_interceptors::js_interceptors::init(),
      ext_runtime::runtime_bootstrap::init(None),
      ext_runtime::runtime_net::init(),
      ext_runtime::runtime_http::init(),
      ext_runtime::runtime_http_start::init(),
      // NOTE: Order matters (see runtime/mod.rs).
      ext_node::deno_node::lazy_init::<
        deno_resolver::npm::DenoInNpmPackageChecker,
        deno_resolver::npm::NpmResolver<sys_traits::impls::RealSys>,
        sys_traits::impls::RealSys,
      >(),
      deno_cache::deno_cache::lazy_init(),
      deno::runtime::ops::permissions::deno_permissions::init(),
      base_runtime_permissions::init(),
      ext_os::os::init(None),
      ext_os::deno_os::init(),
      ext_runtime::runtime::init(),
    ];

    // Must be collected before `create_snapshot` takes ownership.
    let lazy_extension_files = collect_lazy_extension_files(&extensions);

    let snapshot = create_snapshot(
      CreateSnapshotOptions {
        cargo_manifest_dir: env!("CARGO_MANIFEST_DIR"),
        startup_snapshot: None,
        extensions,
        extension_transpiler: Some(Rc::new(transpile_ts)),
        skip_op_registration: false,
        // Bake ext_node's global template + VM context (at VM_CONTEXT_INDEX)
        // into the snapshot, matching deno's runtime/snapshot.rs. Without this
        // the runtime indexes a missing context slot (out-of-bounds).
        with_runtime_cb: Some(Box::new(|rt| {
          let isolate = rt.v8_isolate();
          v8::scope!(scope, isolate);
          let tmpl = ext_node::init_global_template(
            scope,
            ext_node::ContextInitMode::ForSnapshot,
          );
          let ctx = ext_node::create_v8_context(
            scope,
            tmpl,
            ext_node::ContextInitMode::ForSnapshot,
            std::ptr::null_mut(),
          );
          assert_eq!(scope.add_context(ctx), ext_node::VM_CONTEXT_INDEX);
        })),
      },
      None,
    );

    let output = snapshot.unwrap();

    let mut snapshot_file = std::fs::File::create(snapshot_path).unwrap();
    snapshot_file.write_all(&output.output).unwrap();

    println!("Snapshot created successfully");

    // NOTE(deno-2.9.5): deno_core stores *empty* bytes in the snapshot for any
    // `lazy_loaded_*` source that snapshot-time evaluation did not consume, on
    // the assumption that the embedder ships those sources separately. Without
    // this table every deferred module - the whole node polyfill closure
    // included - fails at runtime with "cannot be lazy-loaded as it was not
    // included in the binary". Mirrors upstream's `cli/snapshot/build.rs`.
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let consumed: std::collections::HashSet<&str> = output
      .consumed_lazy_specifiers
      .iter()
      .map(String::as_str)
      .collect();

    let residual_sources_dir = out_dir.join("residual_sources");
    std::fs::create_dir_all(&residual_sources_dir).unwrap();

    let mut residual_js: Vec<(String, PathBuf)> = Vec::new();
    let mut residual_esm: Vec<(String, PathBuf)> = Vec::new();
    for file in &lazy_extension_files {
      if consumed.contains(file.specifier.as_str()) {
        continue;
      }
      println!("cargo:rerun-if-changed={}", file.path.display());
      let transpiled_path = transpile_residual_source(
        &residual_sources_dir,
        &file.specifier,
        &file.path,
      );
      match file.kind {
        LazyKind::Js => {
          // `loadExtScript` evaluates the source as the body of a
          // `compile_function` call; doing the wrap here lets the runtime hand
          // V8 a `&'static` external string.
          let src = std::fs::read_to_string(&transpiled_path).unwrap();
          let wrapped = deno_core::wrap_lazy_ext_script(&src);
          std::fs::write(&transpiled_path, wrapped.as_bytes()).unwrap();
          residual_js.push((file.specifier.clone(), transpiled_path));
        }
        LazyKind::Esm => {
          residual_esm.push((file.specifier.clone(), transpiled_path));
        }
      }
    }

    println!(
      "Residual lazy sources: {} js, {} esm ({} consumed at snapshot time)",
      residual_js.len(),
      residual_esm.len(),
      consumed.len()
    );

    let mut f = std::fs::File::create(&residual_path).unwrap();
    writeln!(f, "// @generated by crates/base/build.rs - do not edit.\n")
      .unwrap();
    write_residual_table(&mut f, &out_dir, "RESIDUAL_LAZY_JS", &residual_js);
    write_residual_table(&mut f, &out_dir, "RESIDUAL_LAZY_ESM", &residual_esm);

    for path in output.files_loaded_during_snapshot {
      println!("cargo:rerun-if-changed={}", path.display());
    }
  }
}

fn main() {
  // Rebuild if build script changes
  println!("cargo:rerun-if-changed=build.rs");

  println!("cargo:rustc-env=TARGET={}", env::var("TARGET").unwrap());
  println!("cargo:rustc-env=PROFILE={}", env::var("PROFILE").unwrap());

  let o = PathBuf::from(env::var_os("OUT_DIR").unwrap());
  let runtime_snapshot_path = o.join("RUNTIME_SNAPSHOT.bin");
  let residual_path = o.join("EXTENSION_RESIDUAL_SOURCES.rs");
  supabase_startup_snapshot::create_runtime_snapshot(
    runtime_snapshot_path.clone(),
    residual_path,
  );
}
