// Facade for ext:deno_os/30_os.js
//
// Upstream deno_node polyfills (os.ts, process.ts) import from this specifier.
// We re-export real exit helpers from ext:os/exit.js and stub everything else
// so that no actual host information leaks.

import { exit, getExitCode, setExitCode, setExitHandler } from "ext:os/exit.js";

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

export {
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
