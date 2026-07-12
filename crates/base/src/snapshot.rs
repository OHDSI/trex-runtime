// The snapshot bakes the extension ESM (see build.rs) so user workers restore
// the runtime instead of rebuilding it per spawn. build.rs omits the
// feature-gated trex_core ext, so a `trex`-feature build must add it there or
// hit ExtensionSnapshotMismatch.
pub static CLI_SNAPSHOT: &[u8] =
  include_bytes!(concat!(env!("OUT_DIR"), "/RUNTIME_SNAPSHOT.bin"));

pub fn snapshot() -> Option<&'static [u8]> {
  Some(CLI_SNAPSHOT)
}
