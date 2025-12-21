use deno::deno_permissions::PermissionsOptions;
use ext_workers::context::WorkerKind;

pub fn get_default_permissions(kind: WorkerKind) -> PermissionsOptions {
  match kind {
    WorkerKind::MainWorker | WorkerKind::EventsWorker => PermissionsOptions {
      allow_env: Some(vec![]),
      allow_net: Some(vec![]),
      allow_ffi: Some(vec![]),
      allow_read: Some(vec![]),
      allow_run: Some(vec![]),
      allow_sys: Some(vec![]),
      allow_write: Some(vec![]),
      allow_import: Some(vec![]),
      ..Default::default()
    },

    WorkerKind::UserWorker => PermissionsOptions {
      allow_env: Some(vec![]),
      allow_net: Some(vec![]),
      allow_read: Some(vec![]),
      allow_write: Some(vec![]),
      allow_import: Some(vec![]),
      allow_sys: Some(vec![
        "hostname".to_string(),
        "userInfo".to_string(),
        "cpus".to_string(),
      ]),
      ..Default::default()
    },
  }
}
