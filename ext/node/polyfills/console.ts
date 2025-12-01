// Copyright 2018-2025 the Deno authors. MIT license.

import { primordials } from "ext:core/mod.js";
import { Console } from "ext:deno_node/internal/console/constructor.mjs";
import { windowOrWorkerGlobalScope } from "ext:runtime/98_global_scope_shared.js";
// Don't rely on global `console` because during bootstrapping, it is pointing
// to native `console` object provided by V8.
const console = windowOrWorkerGlobalScope.console.value;

const { ObjectDefineProperty, ObjectHasOwn } = primordials;

// Only define Console property if it doesn't already exist
// In Deno 2.5.6, the console object may already have Console defined
if (!ObjectHasOwn(console, "Console")) {
  ObjectDefineProperty(console, "Console", {
    value: Console,
    writable: true,
    enumerable: false,
    configurable: true,
  });
}

export default console;

export { Console };
export const {
  assert,
  clear,
  count,
  countReset,
  debug,
  dir,
  dirxml,
  error,
  group,
  groupCollapsed,
  groupEnd,
  info,
  log,
  profile,
  profileEnd,
  table,
  time,
  timeEnd,
  timeLog,
  timeStamp,
  trace,
  warn,
} = console;
// deno-lint-ignore no-explicit-any
export const indentLevel = (console as any)?.indentLevel;
