// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

use deno_core::ModuleSpecifier;
use deno_core::OpState;
use deno_core::op2;
use deno_error::JsErrorBox;
use deno_fs::FsPermissions;

deno_core::extension!(runtime_bootstrap,
  parameters = [P: FsPermissions],
  ops = [
    op_main_module<P>,
    op_bootstrap_color_depth,
  ],
  options = {
    main_module: Option<ModuleSpecifier>
  },
  state = |state, options| {
    if let Some(module_init) = options.main_module {
      state.put::<ModuleSpecifier>(module_init);
    }
  },
);

#[op2]
#[string]
fn op_main_module<P>(state: &mut OpState) -> Result<String, JsErrorBox>
where
  P: FsPermissions + 'static,
{
  let main = state.borrow::<ModuleSpecifier>().to_string();
  let cwd = std::env::current_dir()
    .map_err(|e| JsErrorBox::type_error(e.to_string()))?;
  let main_url = deno_core::resolve_url_or_path(&main, cwd.as_path())
    .map_err(|e| JsErrorBox::type_error(e.to_string()))?;
  if main_url.scheme() == "file" {
    let _main_path = std::env::current_dir()
      .map_err(|e| {
        JsErrorBox::type_error(format!(
          "Failed to get current working directory: {}",
          e
        ))
      })?
      .join(main_url.to_string());
    state
      .borrow_mut::<P>()
      .check_read_all("Deno.mainModule")
      .map_err(|e| JsErrorBox::generic(e.to_string()))?;
  }

  Ok(main)
}

#[op2(fast)]
pub fn op_bootstrap_color_depth(_state: &mut OpState) -> i32 {
  1
}
