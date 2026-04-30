# trex-runtime

[![Docker Build and Up](https://github.com/ohdsi/trex/actions/workflows/docker-build-push.yml/badge.svg)](https://github.com/ohdsi/trex/actions/workflows/docker-build-push.yml) &nbsp;&nbsp; [![NPM build package](https://github.com/ohdsi/trex/actions/workflows/npm-ci.yml/badge.svg)](https://github.com/ohdsi/trex/actions/workflows/npm-ci.yml)

`trex-runtime` is the edge function runtime used by [Trex](https://github.com/OHDSI/trex). It hosts the JavaScript / TypeScript functions that back HTTP routes, plugin APIs, and server-side workloads inside the `trex` binary, and is the execution environment in which Supabase-compatible edge functions run within this project.

It is a fork of the [Supabase Edge Runtime](https://github.com/supabase/edge-runtime).

## Why a fork?

Upstream Supabase Edge Runtime is built for the Supabase cloud platform, tuned for large-scale, multi-tenant deployment. Trex is a lightweight, self-hosted backend platform that integrates an analytical-first database engine (DuckDB-based) alongside Postgres, and this fork lets us shape the edge runtime to fit that profile while staying API-compatible with Supabase Edge Functions.

The fork also lets us track our own Deno release cadence. trex-runtime currently sits on Deno 2.7.12, kept in step with the rest of the Trex stack rather than with the upstream service's release schedule.

## Role in Trex

- Integrated alongside the analytical engine and the Postgres-wire layer, so HTTP requests can be dispatched into user-supplied edge functions without leaving the `trex` process.
- Compatible with the Supabase Edge Functions developer experience (deploy, secrets, local dev via the Trex CLI, itself a fork of the Supabase CLI).

## Get in contact

Please [click here](https://discord.gg/5XtHky2BZe) to join us in Discord.
