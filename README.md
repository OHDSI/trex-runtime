# trex-runtime

[![Docker Build and Up](https://github.com/ohdsi/trex/actions/workflows/docker-build-push.yml/badge.svg)](https://github.com/ohdsi/trex/actions/workflows/docker-build-push.yml) &nbsp;&nbsp; [![NPM build package](https://github.com/ohdsi/trex/actions/workflows/npm-ci.yml/badge.svg)](https://github.com/ohdsi/trex/actions/workflows/npm-ci.yml)

`trex-runtime` is the edge function runtime used by [Trex](https://github.com/OHDSI/trex). It hosts the JavaScript / TypeScript functions that back HTTP routes, plugin APIs, and server-side workloads inside the `trex` binary, and is the execution environment that Supabase-style edge functions run in within this project.

It is a fork of the [Supabase Edge Runtime](https://github.com/supabase/edge-runtime), updated to track Deno **2.7.12**.

## Role in Trex

- Embedded as a Rust crate alongside the analytical engine and the Postgres-wire layer, so HTTP requests reaching `:8001` / `:8000` can be dispatched into user-supplied edge functions without leaving the `trex` process.
- Loads encrypted secrets from the core platform at invocation time and exposes them as plain environment variables, so functions can read them without bundling secrets into images.
- Compatible with the Supabase Edge Functions developer experience (deploy, secrets, local dev via the Supabase CLI fork) while running on a newer Deno toolchain.

## Get in contact

Please [click here](https://discord.gg/5XtHky2BZe) to join us in Discord.
