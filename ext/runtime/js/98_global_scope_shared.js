// Copyright 2018-2025 the Deno authors. MIT license.
// Minimal implementation providing just what ext_node/polyfills/console.ts needs
// This is a lightweight stub to avoid pulling in the full deno_runtime extension

import { core } from "ext:core/mod.js";
import * as console from "ext:deno_console/01_console.js";

// Minimal windowOrWorkerGlobalScope with just the console property
// The console.ts file only accesses windowOrWorkerGlobalScope.console.value
const windowOrWorkerGlobalScope = {
  console: core.propWritable(console),
};

export { windowOrWorkerGlobalScope };
