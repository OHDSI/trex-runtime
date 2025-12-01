// Copyright 2018-2025 the Deno authors. MIT license.
// Minimal stub for worker_threads - edge-runtime does not support Node.js worker_threads
// This module exists to provide __initWorkerThreads for bootstrap compatibility

import { internals } from "ext:core/mod.js";

// NOTE: This function MUST be registered before 02_init.js calls it
// The import statement in 02_init.js causes this file to load, which registers the function
internals.__initWorkerThreads = (
  _runningOnMainThread: boolean,
  _workerId: unknown,
  _maybeWorkerMetadata: unknown,
  _moduleSpecifier: unknown,
) => {
  // No-op: edge-runtime uses spawn_pinned for user workers, not Node.js worker_threads
};

// Minimal exports to satisfy any code that tries to import worker_threads
const isMainThread = true;
const resourceLimits = {};

export default {
  isMainThread,
  resourceLimits,
};

export {
  isMainThread,
  resourceLimits,
};
