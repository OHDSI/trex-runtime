// Regression guard: npm packages sniff their environment with bare `typeof`
// checks against browser-only globals, and this runtime must not answer them
// like a browser. Deno 2.9.5 removed ext/node's global proxy
// (denoland/deno#33249), which had been hiding a `window` declared by
// ext/runtime/js/bootstrap.js from everything under `node_modules/`; without
// that proxy any such global reaches npm code directly. brotli@1.3.3 (pulled in
// by parquetjs) is one of the packages that then takes its browser branch and
// dies with `ReferenceError: Browser is not defined` at require time.
Deno.serve(() =>
  Response.json({
    window: typeof window,
    importScripts: typeof importScripts,
    // Counterpart: the Node-side signals brotli's `ENVIRONMENT_IS_NODE` check
    // needs must stay present, or it falls through to the browser branch anyway.
    process: typeof process,
  })
);
