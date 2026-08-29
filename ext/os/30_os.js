// Facade for ext:deno_os/30_os.js
//
// Upstream deno_node polyfills (os.ts, process.ts, wasi.ts) reach this
// specifier through `core.loadExtScript`, so it is a `lazy_loaded_js` script
// rather than an ES module (see ext/os/lib.rs). We forward the real exit
// helpers published by `ext:os/exit.js` on `internals` and stub everything
// else so that no actual host information leaks.

(function () {
const { internals } = __bootstrap;

const { exit, getExitCode, setExitCode, setExitHandler } =
  internals.trexOsExit;

function loadavg() {
  return [0, 0, 0];
}

function hostname() {
  return "localhost";
}

function osRelease() {
  return "0.0.0-trex";
}

function osUptime() {
  return 0;
}

function systemMemoryInfo() {
  return null;
}

function networkInterfaces() {
  return [];
}

function gid() {
  return 0;
}

function uid() {
  return 0;
}

const env = {
  get(_key) {
    return undefined;
  },
  toObject() {
    return {};
  },
  set(_key, _value) {},
  has(_key) {
    return false;
  },
  delete(_key) {},
};

function execPath() {
  return "";
}

return {
  env,
  execPath,
  exit,
  getExitCode,
  gid,
  hostname,
  loadavg,
  networkInterfaces,
  osRelease,
  osUptime,
  setExitCode,
  setExitHandler,
  systemMemoryInfo,
  uid,
};
})();
