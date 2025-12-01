use std::borrow::Cow;
use std::path::Path;
use std::path::PathBuf;

use deno::deno_fs;
use deno::deno_permissions;
use deno::deno_permissions::SysDescriptor;
use deno::deno_permissions::SysDescriptorParseError;
use deno_permissions::AllowRunDescriptor;
use deno_permissions::AllowRunDescriptorParseResult;
use deno_permissions::DenyRunDescriptor;
use deno_permissions::EnvDescriptor;
use deno_permissions::FfiDescriptor;
use deno_permissions::ImportDescriptor;
use deno_permissions::NetDescriptor;
use deno_permissions::PathDescriptor;
use deno_permissions::PathQueryDescriptor;
use deno_permissions::PathResolveError;
use deno_permissions::ReadDescriptor;
use deno_permissions::RunDescriptorParseError;
use deno_permissions::RunQueryDescriptor;
use deno_permissions::SpecialFilePathQueryDescriptor;
use deno_permissions::WriteDescriptor;
use ext_node::DenoFsNodeResolverEnv;
use sys_traits::impls::RealSys;

#[derive(Debug)]
pub struct RuntimePermissionDescriptorParser {
  sys: RealSys,
  fs: deno_fs::FileSystemRc,
}

impl RuntimePermissionDescriptorParser {
  pub fn new(fs: deno_fs::FileSystemRc) -> Self {
    Self { sys: RealSys, fs }
  }

  fn resolve_cwd(&self) -> Result<PathBuf, PathResolveError> {
    sys_traits::EnvCurrentDir::env_current_dir(&self.sys)
      .map_err(PathResolveError::CwdResolve)
  }

  fn parse_path_descriptor(
    &self,
    path: Cow<'_, Path>,
  ) -> Result<PathDescriptor, PathResolveError> {
    let env = DenoFsNodeResolverEnv::new(self.fs.clone());
    PathDescriptor::new(&env, path)
  }
}

impl deno_permissions::PermissionDescriptorParser
  for RuntimePermissionDescriptorParser
{
  fn parse_read_descriptor(
    &self,
    text: &str,
  ) -> Result<ReadDescriptor, PathResolveError> {
    Ok(ReadDescriptor(
      self.parse_path_descriptor(Cow::Borrowed(Path::new(text)))?,
    ))
  }

  fn parse_write_descriptor(
    &self,
    text: &str,
  ) -> Result<WriteDescriptor, PathResolveError> {
    Ok(WriteDescriptor(
      self.parse_path_descriptor(Cow::Borrowed(Path::new(text)))?,
    ))
  }

  fn parse_net_descriptor(
    &self,
    text: &str,
  ) -> Result<NetDescriptor, deno_permissions::NetDescriptorParseError> {
    NetDescriptor::parse_for_list(text)
  }

  fn parse_import_descriptor(
    &self,
    text: &str,
  ) -> Result<ImportDescriptor, deno_permissions::NetDescriptorParseError> {
    ImportDescriptor::parse_for_list(text)
  }

  fn parse_env_descriptor(
    &self,
    text: &str,
  ) -> Result<EnvDescriptor, deno_permissions::EnvDescriptorParseError> {
    if text.is_empty() {
      Err(deno_permissions::EnvDescriptorParseError)
    } else {
      Ok(EnvDescriptor::new(Cow::Borrowed(text)))
    }
  }

  fn parse_sys_descriptor(
    &self,
    text: &str,
  ) -> Result<SysDescriptor, SysDescriptorParseError> {
    if text.is_empty() {
      Err(SysDescriptorParseError::Empty)
    } else {
      Ok(SysDescriptor::parse(text.to_string())?)
    }
  }

  fn parse_allow_run_descriptor(
    &self,
    text: &str,
  ) -> Result<AllowRunDescriptorParseResult, RunDescriptorParseError> {
    Ok(AllowRunDescriptor::parse(
      text,
      &self.resolve_cwd()?,
      &self.sys,
    )?)
  }

  fn parse_deny_run_descriptor(
    &self,
    text: &str,
  ) -> Result<DenyRunDescriptor, PathResolveError> {
    Ok(DenyRunDescriptor::parse(text, &self.resolve_cwd()?))
  }

  fn parse_ffi_descriptor(
    &self,
    text: &str,
  ) -> Result<FfiDescriptor, PathResolveError> {
    Ok(FfiDescriptor(
      self.parse_path_descriptor(Cow::Borrowed(Path::new(text)))?,
    ))
  }

  // queries

  fn parse_path_query<'a>(
    &self,
    path: Cow<'a, Path>,
  ) -> Result<PathQueryDescriptor<'a>, PathResolveError> {
    let env = DenoFsNodeResolverEnv::new(self.fs.clone());
    PathQueryDescriptor::new(&env, path)
  }

  fn parse_special_file_descriptor<'a>(
    &self,
    path: PathQueryDescriptor<'a>,
  ) -> Result<SpecialFilePathQueryDescriptor<'a>, PathResolveError> {
    let env = DenoFsNodeResolverEnv::new(self.fs.clone());
    SpecialFilePathQueryDescriptor::parse(&env, path)
  }

  fn parse_net_query(
    &self,
    text: &str,
  ) -> Result<NetDescriptor, deno_permissions::NetDescriptorParseError> {
    NetDescriptor::parse_for_query(text)
  }

  fn parse_run_query<'a>(
    &self,
    requested: &'a str,
  ) -> Result<RunQueryDescriptor<'a>, RunDescriptorParseError> {
    if requested.is_empty() {
      return Err(RunDescriptorParseError::EmptyRunQuery);
    }
    RunQueryDescriptor::parse(requested, &self.sys)
      .map_err(RunDescriptorParseError::PathResolve)
  }
}
