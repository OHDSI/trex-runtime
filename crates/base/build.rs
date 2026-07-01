use std::env;
use std::path::PathBuf;

mod supabase_startup_snapshot {
  use std::io::Write;
  use std::rc::Rc;

  use deno_core::snapshot::create_snapshot;
  use deno_core::snapshot::CreateSnapshotOptions;
  use deno_core::v8;
  use deno_core::Extension;

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

  pub fn create_runtime_snapshot(snapshot_path: PathBuf) {
    println!("Creating runtime snapshot...");

    // Must mirror runtime/mod.rs's list (same names + order) or deno_core hits
    // ExtensionSnapshotMismatch. Build-safe subs: runtime_bootstrap::init(None);
    // build-available deno_node types; base_runtime_permissions stub; trex_core
    // omitted (feature-gated).
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
  supabase_startup_snapshot::create_runtime_snapshot(
    runtime_snapshot_path.clone(),
  );
}
