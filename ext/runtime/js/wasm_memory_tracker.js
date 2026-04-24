// Tracks live WebAssembly.Memory buffer bytes so MemCheck can see WASM linear
// memory growth. v8 147 stopped surfacing WasmMemoryObject through the native
// HeapStatistics channels (external_memory, malloced_memory, and our
// ArrayBufferAllocator all stay flat while wasm grows), so we intercept every
// JS-visible source of Memory objects and sum buffer.byteLength on demand.
//
// Rust calls `globalThis.__trex_poll_wasm_bytes()` from the event-loop
// mem_check path; the result is written to a shared AtomicU64 via
// op_set_wasm_memory_bytes and read by MemCheck::check.

import { core, primordials } from "ext:core/mod.js";
import { setInterval } from "ext:deno_web/02_timers.js";

const {
  ArrayPrototypePush,
  ObjectDefineProperty,
  ObjectKeys,
  ObjectPrototypeIsPrototypeOf,
  WeakRefPrototypeDeref,
} = primordials;

const op_set_wasm_memory_bytes = core.ops.op_set_wasm_memory_bytes;

const Wasm = globalThis.WebAssembly;

// Array<WeakRef<WebAssembly.Memory>>, compacted on each poll.
const memories = [];

function trackMemory(mem) {
  if (mem !== undefined && mem !== null) {
    ArrayPrototypePush(memories, new WeakRef(mem));
  }
}

const OrigMemory = Wasm.Memory;
const OrigInstance = Wasm.Instance;
const origInstantiate = Wasm.instantiate;
const origInstantiateStreaming = Wasm.instantiateStreaming;

function trackInstanceExports(instance) {
  const exports = instance?.exports;
  if (exports === undefined || exports === null) return;
  const keys = ObjectKeys(exports);
  for (let i = 0; i < keys.length; i++) {
    const v = exports[keys[i]];
    if (ObjectPrototypeIsPrototypeOf(OrigMemory.prototype, v)) {
      trackMemory(v);
    }
  }
}

function WrapMemory(desc, ...rest) {
  const mem = new OrigMemory(desc, ...rest);
  trackMemory(mem);
  return mem;
}
WrapMemory.prototype = OrigMemory.prototype;

function WrapInstance(mod, imports) {
  const instance = new OrigInstance(mod, imports);
  trackInstanceExports(instance);
  return instance;
}
WrapInstance.prototype = OrigInstance.prototype;

ObjectDefineProperty(Wasm, "Memory", {
  configurable: true,
  writable: true,
  value: WrapMemory,
});
ObjectDefineProperty(Wasm, "Instance", {
  configurable: true,
  writable: true,
  value: WrapInstance,
});

Wasm.instantiate = function instantiate(source, imports) {
  return origInstantiate(source, imports).then((result) => {
    if (ObjectPrototypeIsPrototypeOf(OrigInstance.prototype, result)) {
      trackInstanceExports(result);
    } else if (result?.instance !== undefined) {
      trackInstanceExports(result.instance);
    }
    return result;
  });
};

if (origInstantiateStreaming !== undefined) {
  Wasm.instantiateStreaming = function instantiateStreaming(source, imports) {
    return origInstantiateStreaming(source, imports).then((result) => {
      if (result?.instance !== undefined) {
        trackInstanceExports(result.instance);
      }
      return result;
    });
  };
}

// Poll the registry and publish the current total to the shared AtomicU64.
// Includes memory grown via the wasm `memory.grow` instruction (which bypasses
// WebAssembly.Memory.prototype), because we read buffer.byteLength directly.
function pollWasmBytes() {
  let total = 0;
  let write = 0;
  for (let i = 0; i < memories.length; i++) {
    const ref = memories[i];
    const mem = WeakRefPrototypeDeref(ref);
    if (mem !== undefined) {
      total += mem.buffer.byteLength;
      memories[write++] = ref;
    }
  }
  memories.length = write;
  op_set_wasm_memory_bytes(total);
}

// 100ms keeps the feedback tight enough for the 10s memory-limit tests and is
// essentially free when the registry is empty. Invoked from bootstrap.js once
// timers and node state are wired; doing it at module-load time panics in
// op_node_new_async_id.
export function startWasmMemoryPolling() {
  setInterval(pollWasmBytes, 100);
}
