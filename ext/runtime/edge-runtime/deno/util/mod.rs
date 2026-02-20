pub mod archive;
pub mod checksum;
pub mod console;
pub mod diff;
pub mod display;
pub mod draw_thread;
// pub mod extract; // Commented out: CLI-only archive extraction, missing mapped_specifier_for_tsc
// pub mod file_watcher; // Commented out: CLI-only file watching, depends on removed deno_runtime crate
pub mod fs;
pub mod path;
pub mod progress_bar;
pub mod retry;
pub mod sync;
pub mod text_encoding;
// pub mod v8; // Commented out: CLI-only V8 flag construction, depends on removed deno_runtime crate
// pub mod watch_env_tracker; // Commented out: CLI-only environment tracking utilities

#[cfg(unix)]
pub mod unix;

#[cfg(windows)]
pub mod windows;
