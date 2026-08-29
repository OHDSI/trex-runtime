use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;

use deno_fs::FsDirEntry;
use deno_fs::FsReadDir;
use deno_fs::FsReadDirRc;
use deno_io::fs::FsResult;
use normalize_path::NormalizePath;

pub mod deno_compile_fs;
pub mod prefix_fs;
pub mod s3_fs;
pub mod static_fs;
pub mod tmp_fs;
pub mod virtual_fs;

trait TryNormalizePath {
  fn try_normalize(&self) -> FsResult<PathBuf>;
}

impl TryNormalizePath for Path {
  fn try_normalize(&self) -> FsResult<PathBuf> {
    NormalizePath::try_normalize(self).ok_or(deno_io::fs::FsError::Io(
      io::Error::from(io::ErrorKind::InvalidInput),
    ))
  }
}

/// NOTE(deno-2.9.5): `FileSystem::read_dir_async` used to return a
/// `Vec<FsDirEntry>`; deno_fs 0.167 changed it to a lazily-pulled
/// `FsReadDirRc` so `RealFs` can stream `tokio::fs::ReadDir`. Every trex
/// filesystem produces its listing eagerly, so they wrap the finished vector
/// in this adapter. Entries and their order are unchanged.
#[derive(Debug)]
pub(crate) struct VecFsReadDir(Mutex<std::vec::IntoIter<FsDirEntry>>);

impl VecFsReadDir {
  pub fn new(entries: Vec<FsDirEntry>) -> Self {
    Self(Mutex::new(entries.into_iter()))
  }
}

#[async_trait::async_trait(?Send)]
impl FsReadDir for VecFsReadDir {
  async fn next(&self) -> FsResult<Option<FsDirEntry>> {
    let mut iter = self
      .0
      .try_lock()
      .map_err(|_| deno_io::fs::FsError::FileBusy)?;
    Ok(iter.next())
  }
}

/// Wraps an already-materialised listing as an [`FsReadDirRc`].
pub(crate) fn vec_read_dir(entries: Vec<FsDirEntry>) -> FsReadDirRc {
  deno_maybe_sync::new_rc(VecFsReadDir::new(entries))
}
