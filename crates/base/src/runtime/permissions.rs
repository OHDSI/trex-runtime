use deno::deno_permissions::PermissionsOptions;
use ext_workers::context::WorkerKind;

pub fn get_default_permissions(kind: WorkerKind) -> PermissionsOptions {
  match kind {
    WorkerKind::MainWorker | WorkerKind::EventsWorker => PermissionsOptions {
      // allow_all has been removed in Deno 2.5.6
      // Setting each permission to Some(vec![]) grants all permissions
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
      // User worker has restricted permissions
      allow_env: Some(Default::default()),
      allow_net: Some(Default::default()),
      allow_read: Some(Default::default()),
      allow_write: Some(Default::default()),
      allow_import: Some(Default::default()),
      allow_sys: Some(vec!["hostname".to_string()]),
      ..Default::default()
    },
  }
}
