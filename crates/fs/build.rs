use std::path::Path;

fn main() {
  let env_file = "tests/.env";
  let env_path = Path::new(env_file);

  println!("cargo::rustc-check-cfg=cfg(dotenv)");

  // Only enable S3 tests when .env exists and has actual content.
  // CI creates this file with MinIO credentials; locally it should
  // only exist if you have a running MinIO instance.
  if env_path.exists() {
    let content = std::fs::read_to_string(env_path).unwrap_or_default();
    if content.lines().any(|l| l.contains('=')) {
      println!("cargo:rustc-cfg=dotenv");
    }
  }

  println!("cargo::rerun-if-changed={}", env_file);
}
