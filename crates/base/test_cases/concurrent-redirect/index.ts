// NOTE(Nyannyacha): This is the same test case as described in denoland/deno_core#762, but it is a
// minimal reproducible sample of what happens in the field.
//
// Both specifiers are served as redirects, and the redirected `mod.ts` imports `types.ts` again, so
// the graph resolves two forced redirects concurrently.
//
// This used to point at `https://lib.deno.dev/x/grammy@1.x/...`, whose `@1.x` suffix made the
// registry issue the redirect. That host was a Deno Deploy Classic deployment and was sunset on
// 2026-07-20, so the test serves the redirects itself instead of depending on a live registry.

import * as A from "http://localhost:9871/mod.ts";
import * as B from "http://localhost:9871/types.ts";

console.log(A, B);

Deno.serve((_req) => new Response("meow"));
