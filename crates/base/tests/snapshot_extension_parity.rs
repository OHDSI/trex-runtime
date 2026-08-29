//! The V8 startup snapshot is created in build.rs from one extension list and
//! consumed in runtime/mod.rs from another. deno_core requires the two to have
//! identical names in identical order, or it raises ExtensionSnapshotMismatch
//! at runtime -- a panic no functional test would otherwise catch.
//!
//! This test compares the two lists as source text so it fails at `cargo test`
//! rather than when a worker boots in production.

use std::fs;

/// Extracts extension identifiers from a `vec![...]` extension list.
///
/// Entries are split on top-level commas (tracking `()`, `<>`, `[]`, `{}`
/// nesting) so multi-line calls -- generic turbofish args, multi-line
/// function arguments -- collapse into a single entry rather than spilling
/// spurious extra names. Within an entry, only the call path before the
/// first `(` or `<` is considered, and the identifier immediately before the
/// trailing `init`/`lazy_init` segment is taken as the extension name. This
/// way stylistically different but semantically identical call paths (e.g.
/// `base_runtime_permissions::init()` vs.
/// `ops::permissions::base_runtime_permissions::init(permissions)`) compare
/// equal, while genuine drift (different extension, different order) does
/// not.
fn extension_names(source: &str, start_marker: &str) -> Vec<String> {
  let marker_at = source
    .find(start_marker)
    .unwrap_or_else(|| panic!("marker not found: {start_marker}"));
  let tail = &source[marker_at + start_marker.len()..];
  let end = tail.find("];").expect("unterminated extension list");
  let body = &tail[..end];

  // Drop full-line comments and attributes; join what remains into one
  // stream so multi-line calls parse as a single entry.
  let joined: String = body
    .lines()
    .map(str::trim)
    .filter(|line| {
      !line.is_empty() && !line.starts_with("//") && !line.starts_with('#')
    })
    .collect::<Vec<_>>()
    .join(" ");

  // Split on top-level commas (depth 0 across (), <>, [], {}).
  let mut entries: Vec<String> = Vec::new();
  let mut depth: i32 = 0;
  let mut current = String::new();
  for c in joined.chars() {
    match c {
      '(' | '<' | '[' | '{' => {
        depth += 1;
        current.push(c);
      }
      ')' | '>' | ']' | '}' => {
        depth -= 1;
        current.push(c);
      }
      ',' if depth == 0 => {
        entries.push(std::mem::take(&mut current));
      }
      _ => current.push(c),
    }
  }
  if !current.trim().is_empty() {
    entries.push(current);
  }

  entries
    .iter()
    .filter_map(|entry| {
      let entry = entry.trim();
      if entry.is_empty() {
        return None;
      }
      // The call path is everything before the first `(` or `<` (the
      // start of arguments or turbofish generics).
      let cut = entry
        .char_indices()
        .find(|(_, c)| *c == '(' || *c == '<')
        .map(|(i, _)| i)
        .unwrap_or(entry.len());
      let path = entry[..cut].trim_end_matches(':').trim();
      let segments: Vec<&str> =
        path.split("::").filter(|s| !s.is_empty()).collect();
      // The extension identifier is the segment immediately before the
      // trailing `init`/`lazy_init` call.
      let name = if segments.len() >= 2 {
        segments[segments.len() - 2]
      } else {
        segments.last().copied().unwrap_or_default()
      };
      if name.is_empty() {
        None
      } else {
        Some(name.to_string())
      }
    })
    .collect()
}

#[test]
fn build_and_runtime_extension_lists_match() {
  let build =
    fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/build.rs"))
      .expect("read build.rs");
  let runtime = fs::read_to_string(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/runtime/mod.rs"
  ))
  .expect("read runtime/mod.rs");

  let from_build =
    extension_names(&build, "let extensions: Vec<Extension> = vec![");
  let from_runtime = extension_names(&runtime, "let extensions = vec![");

  assert!(
        !from_build.is_empty(),
        "parsed an empty extension list from build.rs -- the marker or format changed"
    );
  assert!(
        !from_runtime.is_empty(),
        "parsed an empty extension list from runtime/mod.rs -- the marker or format changed"
    );
  assert_eq!(
    from_build, from_runtime,
    "\nbuild.rs and runtime/mod.rs extension lists differ.\n\
         deno_core will raise ExtensionSnapshotMismatch at runtime.\n\
         build.rs:   {from_build:?}\n\
         runtime:    {from_runtime:?}\n"
  );
}
