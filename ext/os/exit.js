import { core, internals, primordials } from "ext:core/mod.js";
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

const { Event, EventTarget } = core.loadExtScript("ext:deno_web/02_event.js");

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

// NOTE(deno-2.9.5): `ext:deno_os/30_os.js` is a `lazy_loaded_js` script now
// (upstream made theirs lazy, and the node polyfills reach it through
// `core.loadExtScript`). A script cannot `import`, so hand the helpers over on
// `internals` - the same object the captured `__bootstrap` view exposes to
// lazy scripts - which keeps a single `exitHandler` for both entry points.
internals.trexOsExit = { exit, getExitCode, setExitCode, setExitHandler };

export { exit, getExitCode, setExitCode, setExitHandler };
