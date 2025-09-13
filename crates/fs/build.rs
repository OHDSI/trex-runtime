use std::fs::File;
use std::path::Path;

fn main() {
  let env_file = "tests/.env";
  let env_path = Path::new(env_file);

  println!("cargo::rustc-check-cfg=cfg(dotenv)");

  if env_path.exists() {
    println!("cargo:rustc-cfg=dotenv")
  } else {
    match File::create_new(env_path) {
      Ok(_) => {}
      Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
        println!("cargo:rustc-cfg=dotenv")
      }
      Err(e) => {
        eprintln!("Warning: Could not create {}: {}", env_file, e);
      }
    }
  }

  println!("cargo::rerun-if-changed={}", env_file);
}
