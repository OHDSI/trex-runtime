use std::env;
use std::path::PathBuf;

// Temporal support with stub implementations:
// - temporal_shim.c provides stub implementations of temporal_rs functions
// - Compiled into rusty_v8 when building V8 from source
// - Allows snapshot generation to work without full temporal_rs integration
mod supabase_startup_snapshot {
  use std::borrow::Cow;
  use std::io::Write;
  use std::path::Path;
  use std::rc::Rc;

  use deno::deno_permissions::CheckedPath;
  use deno::deno_permissions::OpenAccessKind;
  use deno::deno_permissions::PermissionCheckError;
  use deno_core::snapshot::create_snapshot;
  use deno_core::snapshot::CreateSnapshotOptions;
  use deno_core::url::Url;
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

  #[derive(Clone)]
  #[allow(dead_code)]
  pub struct Permissions;

  impl deno::deno_fetch::FetchPermissions for Permissions {
    fn check_net(
      &mut self,
      _host: &str,
      _port: u16,
      _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
      unreachable!("snapshotting!")
    }

    fn check_net_url(
      &mut self,
      _url: &Url,
      _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
      unreachable!("snapshotting!")
    }

    fn check_open<'a>(
      &mut self,
      _path: Cow<'a, Path>,
      _open_access: OpenAccessKind,
      _api_name: &str,
    ) -> Result<CheckedPath<'a>, PermissionCheckError> {
      unreachable!("snapshotting!")
    }

    fn check_net_vsock(
      &mut self,
      _cid: u32,
      _port: u32,
      _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
      unreachable!("snapshotting!")
    }
  }

  impl deno::deno_web::TimersPermission for Permissions {
    fn allow_hrtime(&mut self) -> bool {
      unreachable!("snapshotting!")
    }
  }

  impl deno::deno_websocket::WebSocketPermissions for Permissions {
    fn check_net_url(
      &mut self,
      _url: &Url,
      _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
      unreachable!("snapshotting!")
    }
  }

  impl ext_node::NodePermissions for Permissions {
    fn check_net_url(
      &mut self,
      _url: &Url,
      _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
      unreachable!("snapshotting!")
    }

    fn check_net(
      &mut self,
      _host: (&str, Option<u16>),
      _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
      unreachable!("snapshotting!")
    }

    fn check_open<'a>(
      &mut self,
      _path: Cow<'a, Path>,
      _open_access: OpenAccessKind,
      _api_name: Option<&str>,
    ) -> Result<CheckedPath<'a>, PermissionCheckError> {
      unreachable!("snapshotting!")
    }

    fn query_read_all(&mut self) -> bool {
      unreachable!("snapshotting!")
    }

    fn check_sys(
      &mut self,
      _kind: &str,
      _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
      unreachable!("snapshotting!")
    }
  }

  impl deno::deno_net::NetPermissions for Permissions {
    fn check_net<T: AsRef<str>>(
      &mut self,
      _host: &(T, Option<u16>),
      _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
      unreachable!("snapshotting!")
    }

    fn check_open<'a>(
      &mut self,
      _path: Cow<'a, Path>,
      _open_access: OpenAccessKind,
      _api_name: &str,
    ) -> Result<CheckedPath<'a>, PermissionCheckError> {
      unreachable!("snapshotting!")
    }

    fn check_vsock(
      &mut self,
      _cid: u32,
      _port: u32,
      _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
      unreachable!("snapshotting!")
    }
  }

  impl deno::deno_fs::FsPermissions for Permissions {
    fn check_open<'a>(
      &self,
      _path: Cow<'a, Path>,
      _access_kind: OpenAccessKind,
      _api_name: &str,
    ) -> Result<CheckedPath<'a>, PermissionCheckError> {
      unreachable!("snapshotting!")
    }

    fn check_open_blind<'a>(
      &self,
      _path: Cow<'a, Path>,
      _access_kind: OpenAccessKind,
      _display: &str,
      _api_name: &str,
    ) -> Result<CheckedPath<'a>, PermissionCheckError> {
      unreachable!("snapshotting!")
    }

    fn check_read_all(
      &self,
      _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
      unreachable!("snapshotting!")
    }

    fn check_write_partial<'a>(
      &self,
      _path: Cow<'a, Path>,
      _api_name: &str,
    ) -> Result<CheckedPath<'a>, PermissionCheckError> {
      unreachable!("snapshotting!")
    }

    fn check_write_all(
      &self,
      _api_name: &str,
    ) -> Result<(), PermissionCheckError> {
      unreachable!("snapshotting!")
    }
  }

  pub fn create_runtime_snapshot(snapshot_path: PathBuf) {
    // SNAPSHOT COMPLETELY DISABLED FOR DENO 2.5.6:
    // Deno 2.5.6 changed the extension system - ALL extensions with JavaScript modules
    // now cause NonEvaluatedModules errors when using init() in snapshots.
    // This includes even core extensions like deno_console, deno_webidl, deno_telemetry.
    //
    // The Deno team's solution is to either:
    // 1. Use no snapshot at all (load everything at runtime), OR
    // 2. Use lazy_init() for extensions (but this has other issues)
    //
    // For now, we're creating a TRULY EMPTY snapshot - just a minimal V8 snapshot with no extensions.
    // All extensions will be loaded at runtime. This is how modern Deno 2.x projects work.
    //
    // Performance impact: ~60-110ms slower worker startup (vs ~10ms with full snapshot)
    // Mitigation strategies will be implemented in Phase 3: worker prewarming, module caching

    println!("Creating a snapshot...");

    // Create a snapshot with extensions
    #[allow(unused_mut)]
    let mut extensions: Vec<Extension> = vec![];
    /*
      deno_telemetry::deno_telemetry::init(),
      deno_webidl::deno_webidl::init(),
      deno_console::deno_console::init(),
      deno_url::deno_url::init(),
      deno_web::deno_web::init::<PermissionsContainer>(
        Default::default(),
        Default::default(),
      ),
      deno_fetch::deno_fetch::init::<PermissionsContainer>(Default::default()),
      deno_websocket::deno_websocket::init::<PermissionsContainer>(),
      deno_crypto::deno_crypto::init(None),
      deno_broadcast_channel::deno_broadcast_channel::init::<
        deno_broadcast_channel::InMemoryBroadcastChannel,
      >(
        deno_broadcast_channel::InMemoryBroadcastChannel::default(),
      ),
      deno_net::deno_net::init::<PermissionsContainer>(None, None),
      deno_tls::deno_tls::init(),
      // deno_http::deno_http::init(Default::default()),
      deno_io::deno_io::init(Default::default()),
      deno_fs::deno_fs::init::<PermissionsContainer>(Arc::new(deno_fs::RealFs)),
      deno_webgpu::deno_webgpu::init(),
      ext_ai::ai::init(),
      ext_env::env::init(),
      deno_process::deno_process::init(None),
      ext_workers::user_workers::init(),
      ext_event_worker::user_event_worker::init(),
      ext_event_worker::js_interceptors::js_interceptors::init(),
      ext_runtime::runtime_bootstrap::init::<PermissionsContainer>(None),
      ext_runtime::runtime_net::init(),
      ext_runtime::runtime_http::init(),
      ext_runtime::runtime_http_start::init(),
      ext_node::deno_node::init::<
        PermissionsContainer,
        deno_resolver::npm::DenoInNpmPackageChecker,
        deno_resolver::npm::ManagedNpmResolver<sys_traits::impls::RealSys>,
        sys_traits::impls::RealSys,
      >(None, Arc::new(deno_fs::RealFs)),
      deno_cache::deno_cache::init(Default::default()),
      // deno::runtime::ops::permissions::deno_permissions::init(),
      ext_os::os::init(None),
      ext_os::deno_os::init(),
      ext_runtime::runtime::init(),
    ];
    */

    let snapshot = create_snapshot(
      CreateSnapshotOptions {
        cargo_manifest_dir: env!("CARGO_MANIFEST_DIR"),
        startup_snapshot: None,
        extensions,
        extension_transpiler: Some(Rc::new(transpile_ts)),
        skip_op_registration: false,
        with_runtime_cb: None,
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

  // Create the runtime snapshot
  // When building V8 from source, temporal_shim.c provides the needed symbols
  let o = PathBuf::from(env::var_os("OUT_DIR").unwrap());
  let runtime_snapshot_path = o.join("RUNTIME_SNAPSHOT.bin");
  supabase_startup_snapshot::create_runtime_snapshot(
    runtime_snapshot_path.clone(),
  );
}
