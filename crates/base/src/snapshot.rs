// The snapshot bakes the extension ESM (see build.rs) so user workers restore
// the runtime instead of rebuilding it per spawn. build.rs mirrors the
// feature-gated trex_core ext (cfg(feature = "trex")) so `trex`-feature builds
// restore the same extension list and avoid ExtensionSnapshotMismatch.
pub static CLI_SNAPSHOT: &[u8] =
  include_bytes!(concat!(env!("OUT_DIR"), "/RUNTIME_SNAPSHOT.bin"));

pub fn snapshot() -> Option<&'static [u8]> {
  Some(CLI_SNAPSHOT)
}
