use std::borrow::Cow;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use deno::standalone::binary::NodeModules;
use deno_core::normalize_path;
use deno_fs::FsDirEntry;
use deno_fs::FsFileType;
use deno_fs::OpenOptions;
use deno_io::fs::File;
use deno_io::fs::FsError;
use deno_io::fs::FsResult;
use deno_io::fs::FsStat;
use deno_npm::resolution::ValidSerializedNpmResolutionSnapshot;
use deno_permissions::{CheckedPath, CheckedPathBuf};
use eszip_trait::EszipStaticFiles;

use crate::FileBackedVfs;
use crate::rt::IO_RT;

#[derive(Debug, Clone)]
pub struct StaticFs {
  static_files: EszipStaticFiles,
  base_dir_path: PathBuf,
  vfs_path: PathBuf,
  byonm_node_modules_path: Option<PathBuf>,
  snapshot: Option<ValidSerializedNpmResolutionSnapshot>,
  vfs: Arc<FileBackedVfs>,
}

impl StaticFs {
  pub fn new(
    node_modules: Option<NodeModules>,
    static_files: EszipStaticFiles,
    base_dir_path: PathBuf,
    vfs_path: PathBuf,
    vfs: Arc<FileBackedVfs>,
    snapshot: Option<ValidSerializedNpmResolutionSnapshot>,
  ) -> Self {
    let byonm_node_modules_path = if let Some(NodeModules::Byonm {
      root_node_modules_dir: Some(path),
    }) = node_modules
    {
      Some(vfs_path.join(path))
    } else {
      None
    };

    Self {
      vfs,
      static_files,
      base_dir_path,
      byonm_node_modules_path,
      vfs_path,
      snapshot,
    }
  }

  pub fn is_valid_npm_package(&self, path: &Path) -> bool {
    if self.snapshot.is_some() {
      let vfs_path = self.vfs_path.clone();
      path.starts_with(vfs_path)
    } else {
      false
    }
  }
}

#[async_trait::async_trait(?Send)]
impl deno_fs::FileSystem for StaticFs {
  fn cwd(&self) -> FsResult<PathBuf> {
    Ok(PathBuf::new())
  }

  fn tmp_dir(&self) -> FsResult<PathBuf> {
    Err(FsError::NotSupported)
  }

  fn chdir(&self, _path: &CheckedPath) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn umask(&self, _mask: Option<u32>) -> FsResult<u32> {
    Err(FsError::NotSupported)
  }

  fn open_sync(
    &self,
    path: &CheckedPath,
    _options: OpenOptions,
  ) -> FsResult<Rc<dyn File>> {
    if self.vfs.is_path_within(&**path) {
      Ok(self.vfs.open_file(&**path)?)
    } else {
      Err(FsError::Io(io::Error::from(io::ErrorKind::NotFound)))
    }
  }

  async fn open_async<'a>(
    &'a self,
    path: CheckedPathBuf,
    _options: OpenOptions,
  ) -> FsResult<Rc<dyn File>> {
    if self.vfs.is_path_within(&*path) {
      Ok(self.vfs.open_file(&*path)?)
    } else {
      Err(FsError::Io(io::Error::from(io::ErrorKind::NotFound)))
    }
  }

  fn mkdir_sync(
    &self,
    _path: &CheckedPath,
    _recursive: bool,
    _mode: Option<u32>,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn mkdir_async(
    &self,
    _path: CheckedPathBuf,
    _recursive: bool,
    _mode: Option<u32>,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  #[cfg(unix)]
  fn chmod_sync(&self, _path: &CheckedPath, _mode: u32) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  #[cfg(not(unix))]
  fn chmod_sync(&self, _path: &CheckedPath, _mode: i32) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  #[cfg(unix)]
  async fn chmod_async(
    &self,
    _path: CheckedPathBuf,
    _mode: u32,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  #[cfg(not(unix))]
  async fn chmod_async(
    &self,
    _path: CheckedPathBuf,
    _mode: i32,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn lchmod_sync(&self, _path: &CheckedPath, _mode: u32) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn lchmod_async(
    &self,
    _path: CheckedPathBuf,
    _mode: u32,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn chown_sync(
    &self,
    _path: &CheckedPath,
    _uid: Option<u32>,
    _gid: Option<u32>,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn chown_async(
    &self,
    _path: CheckedPathBuf,
    _uid: Option<u32>,
    _gid: Option<u32>,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn lchown_sync(
    &self,
    _path: &CheckedPath,
    _uid: Option<u32>,
    _gid: Option<u32>,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn lchown_async(
    &self,
    _path: CheckedPathBuf,
    _uid: Option<u32>,
    _gid: Option<u32>,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn remove_sync(&self, _path: &CheckedPath, _recursive: bool) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn remove_async(
    &self,
    _path: CheckedPathBuf,
    _recursive: bool,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn copy_file_sync(
    &self,
    _oldpath: &CheckedPath,
    _newpath: &CheckedPath,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn copy_file_async(
    &self,
    _oldpath: CheckedPathBuf,
    _newpath: CheckedPathBuf,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn cp_sync(
    &self,
    _path: &CheckedPath,
    _new_path: &CheckedPath,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn cp_async(
    &self,
    _path: CheckedPathBuf,
    _new_path: CheckedPathBuf,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn stat_sync(&self, path: &CheckedPath) -> FsResult<FsStat> {
    if self.vfs.is_path_within(&**path) {
      Ok(self.vfs.stat(&**path)?)
    } else {
      Err(FsError::NotSupported)
    }
  }

  async fn stat_async(&self, path: CheckedPathBuf) -> FsResult<FsStat> {
    if self.vfs.is_path_within(&*path) {
      Ok(self.vfs.stat(&*path)?)
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn lstat_sync(&self, path: &CheckedPath) -> FsResult<FsStat> {
    if self.vfs.is_path_within(&**path) {
      Ok(self.vfs.lstat(&**path)?)
    } else {
      Err(FsError::NotSupported)
    }
  }

  async fn lstat_async(&self, path: CheckedPathBuf) -> FsResult<FsStat> {
    if self.vfs.is_path_within(&*path) {
      Ok(self.vfs.lstat(&*path)?)
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn realpath_sync(&self, path: &CheckedPath) -> FsResult<PathBuf> {
    if self.vfs.is_path_within(&**path) {
      Ok(self.vfs.canonicalize(&**path)?)
    } else {
      Err(FsError::NotSupported)
    }
  }

  async fn realpath_async(&self, path: CheckedPathBuf) -> FsResult<PathBuf> {
    if self.vfs.is_path_within(&*path) {
      Ok(self.vfs.canonicalize(&*path)?)
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn read_dir_sync(&self, path: &CheckedPath) -> FsResult<Vec<FsDirEntry>> {
    if self.vfs.is_path_within(&**path) {
      Ok(self.vfs.read_dir(&**path)?)
    } else {
      Err(FsError::NotSupported)
    }
  }

  async fn read_dir_async(
    &self,
    path: CheckedPathBuf,
  ) -> FsResult<Vec<FsDirEntry>> {
    if self.vfs.is_path_within(&*path) {
      Ok(self.vfs.read_dir(&*path)?)
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn rename_sync(
    &self,
    _oldpath: &CheckedPath,
    _newpath: &CheckedPath,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn rename_async(
    &self,
    _oldpath: CheckedPathBuf,
    _newpath: CheckedPathBuf,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn link_sync(
    &self,
    _oldpath: &CheckedPath,
    _newpath: &CheckedPath,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn link_async(
    &self,
    _oldpath: CheckedPathBuf,
    _newpath: CheckedPathBuf,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn symlink_sync(
    &self,
    _oldpath: &CheckedPath,
    _newpath: &CheckedPath,
    _file_type: Option<FsFileType>,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn symlink_async(
    &self,
    _oldpath: CheckedPathBuf,
    _newpath: CheckedPathBuf,
    _file_type: Option<FsFileType>,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn read_link_sync(&self, path: &CheckedPath) -> FsResult<PathBuf> {
    if self.vfs.is_path_within(&**path) {
      Ok(self.vfs.read_link(&**path)?)
    } else {
      Err(FsError::NotSupported)
    }
  }

  async fn read_link_async(&self, path: CheckedPathBuf) -> FsResult<PathBuf> {
    if self.vfs.is_path_within(&*path) {
      Ok(self.vfs.read_link(&*path)?)
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn truncate_sync(&self, _path: &CheckedPath, _len: u64) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn truncate_async(
    &self,
    _path: CheckedPathBuf,
    _len: u64,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn utime_sync(
    &self,
    _path: &CheckedPath,
    _atime_secs: i64,
    _atime_nanos: u32,
    _mtime_secs: i64,
    _mtime_nanos: u32,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn utime_async(
    &self,
    _path: CheckedPathBuf,
    _atime_secs: i64,
    _atime_nanos: u32,
    _mtime_secs: i64,
    _mtime_nanos: u32,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn lutime_sync(
    &self,
    _path: &CheckedPath,
    _atime_secs: i64,
    _atime_nanos: u32,
    _mtime_secs: i64,
    _mtime_nanos: u32,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  async fn lutime_async(
    &self,
    _path: CheckedPathBuf,
    _atime_secs: i64,
    _atime_nanos: u32,
    _mtime_secs: i64,
    _mtime_nanos: u32,
  ) -> FsResult<()> {
    Err(FsError::NotSupported)
  }

  fn read_file_sync(&self, path: &CheckedPath) -> FsResult<Cow<'static, [u8]>> {
    let is_npm = self.is_valid_npm_package(&**path);
    let is_byonm_path = self
      .byonm_node_modules_path
      .as_ref()
      .map(|it| (&**path).starts_with(it))
      .unwrap_or_default();

    if is_npm || is_byonm_path {
      let options = OpenOptions::read();
      let file = self.open_sync(path, options)?;
      let buf = file.read_all_sync()?;
      Ok(buf)
    } else {
      let eszip = self.vfs.eszip.as_ref();
      let path_ref = &**path;
      let path_buf = if path_ref.is_relative() {
        self.base_dir_path.join(path_ref)
      } else {
        path_ref.to_path_buf()
      };

      let normalized = normalize_path(Cow::Owned(path_buf));

      if let Some(file) = self
        .static_files
        .get(normalized.as_ref())
        .and_then(|it| eszip.ensure_module(it))
      {
        let Some(res) = std::thread::scope(|s| {
          s.spawn(move || IO_RT.block_on(async move { file.source().await }))
            .join()
            .unwrap()
        }) else {
          return Err(
            std::io::Error::new(
              std::io::ErrorKind::NotFound,
              "No content available",
            )
            .into(),
          );
        };

        Ok(Cow::Owned(res.to_vec()))
      } else {
        Err(
          std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("path not found: {}", normalized.to_string_lossy()),
          )
          .into(),
        )
      }
    }
  }

  async fn read_file_async<'a>(
    &'a self,
    path: CheckedPathBuf,
  ) -> FsResult<Cow<'static, [u8]>> {
    let checked = CheckedPath::unsafe_new(Cow::Borrowed(&*path));
    self.read_file_sync(&checked)
  }

  fn exists_sync(&self, path: &CheckedPath) -> bool {
    self.vfs.is_path_within(&**path)
  }

  async fn exists_async(&self, path: CheckedPathBuf) -> FsResult<bool> {
    Ok(self.vfs.is_path_within(&*path))
  }
}
