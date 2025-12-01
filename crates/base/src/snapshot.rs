// DISABLED: Snapshot causes SIGBUS crash on ARM64 macOS with V8 140.2.0
// when built from source. Returning None causes ~100ms slower startup but
// avoids snapshot corruption issues.
// See: architecture-advisor analysis for details
pub static CLI_SNAPSHOT: &[u8] =
  include_bytes!(concat!(env!("OUT_DIR"), "/RUNTIME_SNAPSHOT.bin"));

pub fn snapshot() -> Option<&'static [u8]> {
  // Return None to disable snapshot and initialize V8 from scratch
  None
}
