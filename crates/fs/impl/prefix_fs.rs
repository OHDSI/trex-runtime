use std::borrow::Cow;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::anyhow;
use base_rt::RuntimeState;
use deno_fs::FsDirEntry;
use deno_fs::FsFileType;
use deno_fs::OpenOptions;
use deno_io::fs::File;
use deno_io::fs::FsError;
use deno_io::fs::FsResult;
use deno_io::fs::FsStat;
use deno_permissions::{CheckedPath, CheckedPathBuf};

#[derive(Debug, Clone)]
pub struct PrefixFs<FileSystem> {
  prefix: PathBuf,
  cwd: Option<PathBuf>,
  tmp_dir: Option<PathBuf>,
  fs: Arc<FileSystem>,
  base_fs: Option<Arc<dyn deno_fs::FileSystem>>,
  runtime_state: Option<Arc<RuntimeState>>,
  check_sync_api: bool,
}

impl<FileSystem> PrefixFs<FileSystem>
where
  FileSystem: deno_fs::FileSystem,
{
  pub fn new<P>(
    prefix: P,
    fs: FileSystem,
    base_fs: Option<Arc<dyn deno_fs::FileSystem>>,
  ) -> Self
  where
    P: AsRef<Path>,
  {
    Self {
      prefix: prefix.as_ref().to_path_buf(),
      cwd: None,
      tmp_dir: None,
      fs: Arc::new(fs),
      base_fs,
      runtime_state: None,
      check_sync_api: false,
    }
  }

  pub fn cwd<P>(mut self, v: P) -> Self
  where
    P: AsRef<Path>,
  {
    self.cwd = Some(v.as_ref().to_path_buf());
    self
  }

  pub fn tmp_dir<P>(mut self, v: P) -> Self
  where
    P: AsRef<Path>,
  {
    self.tmp_dir = Some(v.as_ref().to_path_buf());
    self
  }

  pub fn set_cwd<P>(&mut self, v: P) -> &mut Self
  where
    P: AsRef<Path>,
  {
    self.cwd = Some(v.as_ref().to_path_buf());
    self
  }

  pub fn set_tmp_dir<P>(&mut self, v: P) -> &mut Self
  where
    P: AsRef<Path>,
  {
    self.tmp_dir = Some(v.as_ref().to_path_buf());
    self
  }

  pub fn set_runtime_state(&mut self, v: &Arc<RuntimeState>) -> &mut Self {
    self.runtime_state = Some(v.clone());
    self
  }

  pub fn set_check_sync_api(&mut self, v: bool) -> &mut Self {
    self.check_sync_api = v;
    self
  }
}

impl<FileSystem> PrefixFs<FileSystem>
where
  FileSystem: deno_fs::FileSystem + 'static,
{
  pub fn add_fs<P, FileSystemInner>(
    mut self,
    prefix: P,
    fs: FileSystemInner,
  ) -> PrefixFs<FileSystemInner>
  where
    P: AsRef<Path>,
    FileSystemInner: deno_fs::FileSystem,
  {
    PrefixFs {
      prefix: prefix.as_ref().to_path_buf(),
      fs: Arc::new(fs),
      cwd: self.cwd.take(),
      tmp_dir: self.tmp_dir.take(),
      runtime_state: self.runtime_state.clone(),
      check_sync_api: self.check_sync_api,
      base_fs: Some(Arc::new(self)),
    }
  }
}

#[async_trait::async_trait(?Send)]
impl<FileSystem> deno_fs::FileSystem for PrefixFs<FileSystem>
where
  FileSystem: deno_fs::FileSystem,
{
  fn cwd(&self) -> FsResult<PathBuf> {
    self
      .cwd
      .clone()
      .map(Ok)
      .or_else(|| self.base_fs.as_ref().map(|it| it.cwd()))
      .unwrap_or_else(|| Ok(PathBuf::new()))
  }

  #[allow(clippy::unnecessary_lazy_evaluations)]
  fn tmp_dir(&self) -> FsResult<PathBuf> {
    self
      .tmp_dir
      .clone()
      .map(Ok)
      .or_else(|| self.base_fs.as_ref().map(|it| it.tmp_dir()))
      .unwrap_or_else(|| Err(FsError::NotSupported))
  }

  fn chdir(&self, path: &CheckedPath) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.chdir(&checked)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.chdir(path))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  fn umask(&self, mask: Option<u32>) -> FsResult<u32> {
    self
      .base_fs
      .as_ref()
      .map(|it| it.umask(mask))
      .unwrap_or_else(|| Err(FsError::NotSupported))
  }

  fn open_sync(
    &self,
    path: &CheckedPath,
    options: OpenOptions,
  ) -> FsResult<Rc<dyn File>> {
    self.check_sync_api_allowed("open_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.open_sync(&checked, options)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.open_sync(path, options))
        .unwrap_or_else(|| {
          Err(FsError::Io(io::Error::from(io::ErrorKind::NotFound)))
        })
    }
  }

  async fn open_async<'a>(
    &'a self,
    path: CheckedPathBuf,
    options: OpenOptions,
  ) -> FsResult<Rc<dyn File>> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.open_async(checked, options).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.open_async(path, options).await
    } else {
      Err(FsError::Io(io::Error::from(io::ErrorKind::NotFound)))
    }
  }

  fn mkdir_sync(
    &self,
    path: &CheckedPath,
    recursive: bool,
    mode: Option<u32>,
  ) -> FsResult<()> {
    self.check_sync_api_allowed("mkdir_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.mkdir_sync(&checked, recursive, mode)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.mkdir_sync(path, recursive, mode))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn mkdir_async(
    &self,
    path: CheckedPathBuf,
    recursive: bool,
    mode: Option<u32>,
  ) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.mkdir_async(checked, recursive, mode).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.mkdir_async(path, recursive, mode).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  #[cfg(unix)]
  fn chmod_sync(&self, path: &CheckedPath, mode: u32) -> FsResult<()> {
    self.check_sync_api_allowed("chmod_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.chmod_sync(&checked, mode)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.chmod_sync(path, mode))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  #[cfg(not(unix))]
  fn chmod_sync(&self, path: &CheckedPath, mode: i32) -> FsResult<()> {
    self.check_sync_api_allowed("chmod_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.chmod_sync(&checked, mode)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.chmod_sync(path, mode))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  #[cfg(unix)]
  async fn chmod_async(&self, path: CheckedPathBuf, mode: u32) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.chmod_async(checked, mode).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.chmod_async(path, mode).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  #[cfg(not(unix))]
  async fn chmod_async(&self, path: CheckedPathBuf, mode: i32) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.chmod_async(checked, mode).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.chmod_async(path, mode).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  #[cfg(unix)]
  fn lchmod_sync(&self, path: &CheckedPath, mode: u32) -> FsResult<()> {
    self.check_sync_api_allowed("lchmod_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.lchmod_sync(&checked, mode)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.lchmod_sync(path, mode))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  #[cfg(not(unix))]
  fn lchmod_sync(&self, path: &CheckedPath, mode: i32) -> FsResult<()> {
    self.check_sync_api_allowed("lchmod_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.lchmod_sync(&checked, mode)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.lchmod_sync(path, mode))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  #[cfg(unix)]
  async fn lchmod_async(
    &self,
    path: CheckedPathBuf,
    mode: u32,
  ) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.lchmod_async(checked, mode).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.lchmod_async(path, mode).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  #[cfg(not(unix))]
  async fn lchmod_async(
    &self,
    path: CheckedPathBuf,
    mode: i32,
  ) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.lchmod_async(checked, mode).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.lchmod_async(path, mode).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn chown_sync(
    &self,
    path: &CheckedPath,
    uid: Option<u32>,
    gid: Option<u32>,
  ) -> FsResult<()> {
    self.check_sync_api_allowed("chown_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.chown_sync(&checked, uid, gid)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.chown_sync(path, uid, gid))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn chown_async(
    &self,
    path: CheckedPathBuf,
    uid: Option<u32>,
    gid: Option<u32>,
  ) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.chown_async(checked, uid, gid).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.chown_async(path, uid, gid).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn lchown_sync(
    &self,
    path: &CheckedPath,
    uid: Option<u32>,
    gid: Option<u32>,
  ) -> FsResult<()> {
    self.check_sync_api_allowed("lchown_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.lchown_sync(&checked, uid, gid)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.lchown_sync(path, uid, gid))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn lchown_async(
    &self,
    path: CheckedPathBuf,
    uid: Option<u32>,
    gid: Option<u32>,
  ) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.lchown_async(checked, uid, gid).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.lchown_async(path, uid, gid).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn remove_sync(&self, path: &CheckedPath, recursive: bool) -> FsResult<()> {
    self.check_sync_api_allowed("remove_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.remove_sync(&checked, recursive)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.remove_sync(path, recursive))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn remove_async(
    &self,
    path: CheckedPathBuf,
    recursive: bool,
  ) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.remove_async(checked, recursive).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.remove_async(path, recursive).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn rmdir_sync(&self, path: &CheckedPath) -> FsResult<()> {
    self.check_sync_api_allowed("rmdir_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.rmdir_sync(&checked)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.rmdir_sync(path))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn rmdir_async(&self, path: CheckedPathBuf) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.rmdir_async(checked).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.rmdir_async(path).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn copy_file_sync(
    &self,
    oldpath: &CheckedPath,
    newpath: &CheckedPath,
  ) -> FsResult<()> {
    self.check_sync_api_allowed("copy_file_sync")?;

    let oldpath_matches = oldpath.starts_with(&self.prefix);
    let newpath_matches = newpath.starts_with(&self.prefix);
    if oldpath_matches || newpath_matches {
      let old_stripped;
      let old = if oldpath_matches {
        old_stripped = CheckedPath::unsafe_new(Cow::Borrowed(
          oldpath.strip_prefix(&self.prefix).unwrap(),
        ));
        &old_stripped
      } else {
        oldpath
      };
      let new_stripped;
      let new = if newpath_matches {
        new_stripped = CheckedPath::unsafe_new(Cow::Borrowed(
          newpath.strip_prefix(&self.prefix).unwrap(),
        ));
        &new_stripped
      } else {
        newpath
      };
      self.fs.copy_file_sync(old, new)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.copy_file_sync(oldpath, newpath))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn copy_file_async(
    &self,
    oldpath: CheckedPathBuf,
    newpath: CheckedPathBuf,
  ) -> FsResult<()> {
    let oldpath_matches = oldpath.starts_with(&self.prefix);
    let newpath_matches = newpath.starts_with(&self.prefix);
    if oldpath_matches || newpath_matches {
      self
        .fs
        .copy_file_async(
          if oldpath_matches {
            let stripped = oldpath.strip_prefix(&self.prefix).unwrap();
            CheckedPathBuf::unsafe_new(stripped.to_path_buf())
          } else {
            oldpath
          },
          if newpath_matches {
            let stripped = newpath.strip_prefix(&self.prefix).unwrap();
            CheckedPathBuf::unsafe_new(stripped.to_path_buf())
          } else {
            newpath
          },
        )
        .await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.copy_file_async(oldpath, newpath).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn cp_sync(
    &self,
    path: &CheckedPath,
    new_path: &CheckedPath,
  ) -> FsResult<()> {
    self.check_sync_api_allowed("cp_sync")?;

    let path_matches = path.starts_with(&self.prefix);
    let new_path_matches = new_path.starts_with(&self.prefix);
    if path_matches || new_path_matches {
      let p_stripped;
      let p = if path_matches {
        p_stripped = CheckedPath::unsafe_new(Cow::Borrowed(
          path.strip_prefix(&self.prefix).unwrap(),
        ));
        &p_stripped
      } else {
        path
      };
      let np_stripped;
      let np = if new_path_matches {
        np_stripped = CheckedPath::unsafe_new(Cow::Borrowed(
          new_path.strip_prefix(&self.prefix).unwrap(),
        ));
        &np_stripped
      } else {
        new_path
      };
      self.fs.cp_sync(p, np)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.cp_sync(path, new_path))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn cp_async(
    &self,
    path: CheckedPathBuf,
    new_path: CheckedPathBuf,
  ) -> FsResult<()> {
    let path_matches = path.starts_with(&self.prefix);
    let new_path_matches = new_path.starts_with(&self.prefix);
    if path_matches || new_path_matches {
      let p = if path_matches {
        let stripped = path.strip_prefix(&self.prefix).unwrap();
        CheckedPathBuf::unsafe_new(stripped.to_path_buf())
      } else {
        path
      };
      let np = if new_path_matches {
        let stripped = new_path.strip_prefix(&self.prefix).unwrap();
        CheckedPathBuf::unsafe_new(stripped.to_path_buf())
      } else {
        new_path
      };
      self.fs.cp_async(p, np).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.cp_async(path, new_path).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn stat_sync(&self, path: &CheckedPath) -> FsResult<FsStat> {
    self.check_sync_api_allowed("stat_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.stat_sync(&checked)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.stat_sync(path))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn stat_async(&self, path: CheckedPathBuf) -> FsResult<FsStat> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.stat_async(checked).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.stat_async(path).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn lstat_sync(&self, path: &CheckedPath) -> FsResult<FsStat> {
    self.check_sync_api_allowed("lstat_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.lstat_sync(&checked)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.lstat_sync(path))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn lstat_async(&self, path: CheckedPathBuf) -> FsResult<FsStat> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.lstat_async(checked).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.lstat_async(path).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn realpath_sync(&self, path: &CheckedPath) -> FsResult<PathBuf> {
    self.check_sync_api_allowed("realpath_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.realpath_sync(&checked)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.realpath_sync(path))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn realpath_async(&self, path: CheckedPathBuf) -> FsResult<PathBuf> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.realpath_async(checked).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.realpath_async(path).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn read_dir_sync(&self, path: &CheckedPath) -> FsResult<Vec<FsDirEntry>> {
    self.check_sync_api_allowed("read_dir_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.read_dir_sync(&checked)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.read_dir_sync(path))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn read_dir_async(
    &self,
    path: CheckedPathBuf,
  ) -> FsResult<Vec<FsDirEntry>> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.read_dir_async(checked).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.read_dir_async(path).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn rename_sync(
    &self,
    oldpath: &CheckedPath,
    newpath: &CheckedPath,
  ) -> FsResult<()> {
    self.check_sync_api_allowed("rename_sync")?;

    let oldpath_matches = oldpath.starts_with(&self.prefix);
    let newpath_matches = newpath.starts_with(&self.prefix);
    if oldpath_matches || newpath_matches {
      let old_stripped;
      let old = if oldpath_matches {
        old_stripped = CheckedPath::unsafe_new(Cow::Borrowed(
          oldpath.strip_prefix(&self.prefix).unwrap(),
        ));
        &old_stripped
      } else {
        oldpath
      };
      let new_stripped;
      let new = if newpath_matches {
        new_stripped = CheckedPath::unsafe_new(Cow::Borrowed(
          newpath.strip_prefix(&self.prefix).unwrap(),
        ));
        &new_stripped
      } else {
        newpath
      };
      self.fs.rename_sync(old, new)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.rename_sync(oldpath, newpath))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn rename_async(
    &self,
    oldpath: CheckedPathBuf,
    newpath: CheckedPathBuf,
  ) -> FsResult<()> {
    let oldpath_matches = oldpath.starts_with(&self.prefix);
    let newpath_matches = newpath.starts_with(&self.prefix);
    if oldpath_matches || newpath_matches {
      self
        .fs
        .rename_async(
          if oldpath_matches {
            let stripped = oldpath.strip_prefix(&self.prefix).unwrap();
            CheckedPathBuf::unsafe_new(stripped.to_path_buf())
          } else {
            oldpath
          },
          if newpath_matches {
            let stripped = newpath.strip_prefix(&self.prefix).unwrap();
            CheckedPathBuf::unsafe_new(stripped.to_path_buf())
          } else {
            newpath
          },
        )
        .await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.rename_async(oldpath, newpath).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn link_sync(
    &self,
    oldpath: &CheckedPath,
    newpath: &CheckedPath,
  ) -> FsResult<()> {
    self.check_sync_api_allowed("link_sync")?;

    let oldpath_matches = oldpath.starts_with(&self.prefix);
    let newpath_matches = newpath.starts_with(&self.prefix);
    if oldpath_matches || newpath_matches {
      let old_stripped;
      let old = if oldpath_matches {
        old_stripped = CheckedPath::unsafe_new(Cow::Borrowed(
          oldpath.strip_prefix(&self.prefix).unwrap(),
        ));
        &old_stripped
      } else {
        oldpath
      };
      let new_stripped;
      let new = if newpath_matches {
        new_stripped = CheckedPath::unsafe_new(Cow::Borrowed(
          newpath.strip_prefix(&self.prefix).unwrap(),
        ));
        &new_stripped
      } else {
        newpath
      };
      self.fs.link_sync(old, new)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.link_sync(oldpath, newpath))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn link_async(
    &self,
    oldpath: CheckedPathBuf,
    newpath: CheckedPathBuf,
  ) -> FsResult<()> {
    let oldpath_matches = oldpath.starts_with(&self.prefix);
    let newpath_matches = newpath.starts_with(&self.prefix);
    if oldpath_matches || newpath_matches {
      self
        .fs
        .link_async(
          if oldpath_matches {
            let stripped = oldpath.strip_prefix(&self.prefix).unwrap();
            CheckedPathBuf::unsafe_new(stripped.to_path_buf())
          } else {
            oldpath
          },
          if newpath_matches {
            let stripped = newpath.strip_prefix(&self.prefix).unwrap();
            CheckedPathBuf::unsafe_new(stripped.to_path_buf())
          } else {
            newpath
          },
        )
        .await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.link_async(oldpath, newpath).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn symlink_sync(
    &self,
    oldpath: &CheckedPath,
    newpath: &CheckedPath,
    file_type: Option<FsFileType>,
  ) -> FsResult<()> {
    self.check_sync_api_allowed("symlink_sync")?;

    let oldpath_matches = oldpath.starts_with(&self.prefix);
    let newpath_matches = newpath.starts_with(&self.prefix);
    if oldpath_matches || newpath_matches {
      if oldpath_matches && newpath_matches {
        let old_stripped = oldpath.strip_prefix(&self.prefix).unwrap();
        let new_stripped = newpath.strip_prefix(&self.prefix).unwrap();
        let old_checked = CheckedPath::unsafe_new(Cow::Borrowed(old_stripped));
        let new_checked = CheckedPath::unsafe_new(Cow::Borrowed(new_stripped));
        self.fs.symlink_sync(&old_checked, &new_checked, file_type)
      } else if oldpath_matches {
        let old_stripped = oldpath.strip_prefix(&self.prefix).unwrap();
        let old_checked = CheckedPath::unsafe_new(Cow::Borrowed(old_stripped));
        self.fs.symlink_sync(&old_checked, newpath, file_type)
      } else {
        let new_stripped = newpath.strip_prefix(&self.prefix).unwrap();
        let new_checked = CheckedPath::unsafe_new(Cow::Borrowed(new_stripped));
        self.fs.symlink_sync(oldpath, &new_checked, file_type)
      }
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.symlink_sync(oldpath, newpath, file_type))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn symlink_async(
    &self,
    oldpath: CheckedPathBuf,
    newpath: CheckedPathBuf,
    file_type: Option<FsFileType>,
  ) -> FsResult<()> {
    let oldpath_matches = oldpath.starts_with(&self.prefix);
    let newpath_matches = newpath.starts_with(&self.prefix);
    if oldpath_matches || newpath_matches {
      self
        .fs
        .symlink_async(
          if oldpath_matches {
            let stripped = oldpath.strip_prefix(&self.prefix).unwrap();
            CheckedPathBuf::unsafe_new(stripped.to_path_buf())
          } else {
            oldpath
          },
          if newpath_matches {
            let stripped = newpath.strip_prefix(&self.prefix).unwrap();
            CheckedPathBuf::unsafe_new(stripped.to_path_buf())
          } else {
            newpath
          },
          file_type,
        )
        .await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.symlink_async(oldpath, newpath, file_type).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn read_link_sync(&self, path: &CheckedPath) -> FsResult<PathBuf> {
    self.check_sync_api_allowed("read_link_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.read_link_sync(&checked)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.read_link_sync(path))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn read_link_async(&self, path: CheckedPathBuf) -> FsResult<PathBuf> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.read_link_async(checked).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.read_link_async(path).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn truncate_sync(&self, path: &CheckedPath, len: u64) -> FsResult<()> {
    self.check_sync_api_allowed("truncate_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.truncate_sync(&checked, len)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.truncate_sync(path, len))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn truncate_async(
    &self,
    path: CheckedPathBuf,
    len: u64,
  ) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.truncate_async(checked, len).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.truncate_async(path, len).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn utime_sync(
    &self,
    path: &CheckedPath,
    atime_secs: i64,
    atime_nanos: u32,
    mtime_secs: i64,
    mtime_nanos: u32,
  ) -> FsResult<()> {
    self.check_sync_api_allowed("utime_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.utime_sync(
        &checked,
        atime_secs,
        atime_nanos,
        mtime_secs,
        mtime_nanos,
      )
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| {
          it.utime_sync(path, atime_secs, atime_nanos, mtime_secs, mtime_nanos)
        })
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn utime_async(
    &self,
    path: CheckedPathBuf,
    atime_secs: i64,
    atime_nanos: u32,
    mtime_secs: i64,
    mtime_nanos: u32,
  ) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self
        .fs
        .utime_async(checked, atime_secs, atime_nanos, mtime_secs, mtime_nanos)
        .await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.utime_async(path, atime_secs, atime_nanos, mtime_secs, mtime_nanos)
        .await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn lutime_sync(
    &self,
    path: &CheckedPath,
    atime_secs: i64,
    atime_nanos: u32,
    mtime_secs: i64,
    mtime_nanos: u32,
  ) -> FsResult<()> {
    self.check_sync_api_allowed("lutime_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.lutime_sync(
        &checked,
        atime_secs,
        atime_nanos,
        mtime_secs,
        mtime_nanos,
      )
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| {
          it.lutime_sync(path, atime_secs, atime_nanos, mtime_secs, mtime_nanos)
        })
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn lutime_async(
    &self,
    path: CheckedPathBuf,
    atime_secs: i64,
    atime_nanos: u32,
    mtime_secs: i64,
    mtime_nanos: u32,
  ) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self
        .fs
        .lutime_async(checked, atime_secs, atime_nanos, mtime_secs, mtime_nanos)
        .await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.lutime_async(path, atime_secs, atime_nanos, mtime_secs, mtime_nanos)
        .await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn write_file_sync(
    &self,
    path: &CheckedPath,
    options: OpenOptions,
    data: &[u8],
  ) -> FsResult<()> {
    self.check_sync_api_allowed("write_file_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.write_file_sync(&checked, options, data)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.write_file_sync(path, options, data))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn write_file_async<'a>(
    &'a self,
    path: CheckedPathBuf,
    options: OpenOptions,
    data: Box<[u8]>,
  ) -> FsResult<()> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.write_file_async(checked, options, data).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.write_file_async(path, options, data).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn read_file_sync(
    &self,
    path: &CheckedPath,
    options: OpenOptions,
  ) -> FsResult<Cow<'static, [u8]>> {
    self.check_sync_api_allowed("read_file_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.read_file_sync(&checked, options)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.read_file_sync(path, options))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn read_file_async<'a>(
    &'a self,
    path: CheckedPathBuf,
    options: OpenOptions,
  ) -> FsResult<Cow<'static, [u8]>> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.read_file_async(checked, options).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.read_file_async(path, options).await
    } else {
      Err(FsError::NotSupported)
    }
  }

  fn is_file_sync(&self, path: &CheckedPath) -> bool {
    if self.check_sync_api_allowed("is_file_sync").is_err() {
      return false;
    }
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.is_file_sync(&checked)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.is_file_sync(path))
        .unwrap_or_default()
    }
  }

  fn is_dir_sync(&self, path: &CheckedPath) -> bool {
    if self.check_sync_api_allowed("is_dir_sync").is_err() {
      return false;
    }
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.is_dir_sync(&checked)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.is_dir_sync(path))
        .unwrap_or_default()
    }
  }

  fn exists_sync(&self, path: &CheckedPath) -> bool {
    if self.check_sync_api_allowed("exists_sync").is_err() {
      return false;
    }
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.exists_sync(&checked)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.exists_sync(path))
        .unwrap_or_default()
    }
  }

  async fn exists_async(&self, path: CheckedPathBuf) -> FsResult<bool> {
    if path.starts_with(&self.prefix) {
      Ok(true)
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.exists_async(path).await
    } else {
      Ok(false)
    }
  }

  fn read_text_file_lossy_sync(
    &self,
    path: &CheckedPath,
  ) -> FsResult<Cow<'static, str>> {
    self.check_sync_api_allowed("read_text_file_lossy_sync")?;
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPath::unsafe_new(Cow::Borrowed(stripped));
      self.fs.read_text_file_lossy_sync(&checked)
    } else {
      self
        .base_fs
        .as_ref()
        .map(|it| it.read_text_file_lossy_sync(path))
        .unwrap_or_else(|| Err(FsError::NotSupported))
    }
  }

  async fn read_text_file_lossy_async<'a>(
    &'a self,
    path: CheckedPathBuf,
  ) -> FsResult<Cow<'static, str>> {
    if path.starts_with(&self.prefix) {
      let stripped = path.strip_prefix(&self.prefix).unwrap();
      let checked = CheckedPathBuf::unsafe_new(stripped.to_path_buf());
      self.fs.read_text_file_lossy_async(checked).await
    } else if let Some(fs) = self.base_fs.as_ref() {
      fs.read_text_file_lossy_async(path).await
    } else {
      Err(FsError::NotSupported)
    }
  }
}

impl<FileSystem> PrefixFs<FileSystem> {
  fn check_sync_api_allowed(&self, name: &'static str) -> FsResult<()> {
    if !self.check_sync_api {
      return Ok(());
    }
    let Some(state) = self.runtime_state.as_ref() else {
      return Ok(());
    };

    if state.is_init() {
      Ok(())
    } else {
      Err(FsError::Io(io::Error::other(anyhow!(format!(
        "invoking {name} is not allowed in the current context"
      )))))
    }
  }
}
