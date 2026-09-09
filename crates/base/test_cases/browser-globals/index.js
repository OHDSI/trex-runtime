// Regression guard for two properties that must hold at the same time.
//
// 1. This runtime's own user code sees `window`. d2e function code reaches for
//    it directly -- `plugins/functions/demo/services/DemoService.ts` calls
//    `window.crypto.subtle.encrypt` -- and without it the "Adding demo
//    database" setup step fails with `Error while encrypting data
//    ReferenceError: window is not defined`.
//
// 2. Code loaded out of a `node_modules/` directory does NOT see it. npm
//    packages sniff their environment with bare `typeof window` checks, and
//    answering them like a browser breaks them.
//
// The `package.json` next to this file is load-bearing: it puts the worker in
// byonm mode, whose in-npm-package check is the path-based one, so the
// hand-written `node_modules/env-sniff` below counts as npm code the same way
// an installed package would.
//
// Deno 2.9.5 deleted ext/node's v8 named-property proxy (denoland/deno#33249),
// which is what keeps (2) true; the fork restores it for `window` alone.
import npm from "./node_modules/env-sniff/index.js";

Deno.serve(() =>
  Response.json({
    trex: {
      window: typeof window,
      importScripts: typeof importScripts,
      process: typeof process,
    },
    npm,
  })
);
