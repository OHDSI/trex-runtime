use once_cell::sync::Lazy;
use parking_lot::RwLock;
use regex::Regex;
use sourcemap::SourceMap;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

static SOURCE_MAPS: Lazy<RwLock<HashMap<String, Arc<SourceMap>>>> =
  Lazy::new(|| RwLock::new(HashMap::new()));

static LOCATION_RE: Lazy<Regex> =
  Lazy::new(|| Regex::new(r"(file://[^:]+):(\d+):(\d+)").unwrap());

pub fn store_source_map(specifier: &str, source_map_bytes: &[u8]) {
  let result =
    std::panic::catch_unwind(|| SourceMap::from_slice(source_map_bytes));

  match result {
    Ok(Ok(sm)) => {
      if let Some(mut maps) = SOURCE_MAPS.try_write() {
        maps.insert(specifier.to_string(), Arc::new(sm));
      }
    }
    Ok(Err(e)) => {
      debug!("Failed to parse source map for {}: {}", specifier, e);
    }
    Err(_) => {
      debug!("Panic while parsing source map for {}", specifier);
    }
  }
}

pub fn translate_location(
  specifier: &str,
  line: u32,
  column: u32,
) -> Option<(String, u32, u32)> {
  let maps = SOURCE_MAPS.try_read()?;
  let sm = maps.get(specifier)?;
  let token =
    sm.lookup_token(line.saturating_sub(1), column.saturating_sub(1))?;
  let src = token.get_source()?;
  Some((
    src.to_string(),
    token.get_src_line() + 1,
    token.get_src_col() + 1,
  ))
}

pub fn translate_error_locations(error_msg: &str) -> String {
  LOCATION_RE
    .replace_all(error_msg, |caps: &regex::Captures| {
      let full_path = &caps[1];
      let line: u32 = caps[2].parse().unwrap_or(0);
      let col: u32 = caps[3].parse().unwrap_or(0);

      if let Some((src_file, src_line, src_col)) =
        translate_location(full_path, line, col)
      {
        format!("{}:{}:{}", src_file, src_line, src_col)
      } else {
        format!("{}:{}:{}", full_path, line, col)
      }
    })
    .to_string()
}

#[allow(dead_code)]
pub fn clear_source_maps() {
  if let Some(mut maps) = SOURCE_MAPS.try_write() {
    maps.clear();
  }
}
