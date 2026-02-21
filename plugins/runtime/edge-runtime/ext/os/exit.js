import { primordials } from "ext:core/mod.js";
import {
  op_exit,
  op_get_exit_code,
  op_set_exit_code,
} from "ext:core/ops";
const {
  FunctionPrototypeBind,
  NumberIsInteger,
  RangeError,
  SymbolFor,
  TypeError,
} = primordials;

import { Event, EventTarget } from "ext:deno_web/02_event.js";

const windowDispatchEvent = FunctionPrototypeBind(
  EventTarget.prototype.dispatchEvent,
  globalThis,
);

// This is an internal only method used by the test harness to override the
// behavior of exit when the exit sanitizer is enabled.
let exitHandler = null;
function setExitHandler(fn) {
  exitHandler = fn;
}

function exit(code) {
  // Set exit code first so unload event listeners can override it.
  if (typeof code === "number") {
    op_set_exit_code(code);
  } else {
    code = op_get_exit_code();
  }

  // Dispatches `unload` only when it's not dispatched yet.
  if (!globalThis[SymbolFor("Deno.isUnloadDispatched")]) {
    // Invokes the `unload` hooks before exiting
    // ref: https://github.com/denoland/deno/issues/3603
    windowDispatchEvent(new Event("unload"));
  }

  if (exitHandler) {
    exitHandler(code);
    return;
  }

  op_exit();
  throw new Error("Code not reachable");
}

function getExitCode() {
  return op_get_exit_code();
}

function setExitCode(value) {
  if (typeof value !== "number") {
    throw new TypeError(
      `Exit code must be a number, got: ${value} (${typeof value})`,
    );
  }
  if (!NumberIsInteger(value)) {
    throw new RangeError(
      `Exit code must be an integer, got: ${value}`,
    );
  }
  op_set_exit_code(value);
}

export { exit, getExitCode, setExitCode, setExitHandler };
