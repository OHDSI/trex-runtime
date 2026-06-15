# trex-runtime

[![Docker Build and Up](https://github.com/ohdsi/trex-runtime/actions/workflows/docker-build-push.yml/badge.svg)](https://github.com/ohdsi/trex-runtime/actions/workflows/docker-build-push.yml) &nbsp;&nbsp; [![NPM build package](https://github.com/ohdsi/trex-runtime/actions/workflows/npm-ci.yml/badge.svg)](https://github.com/ohdsi/trex-runtime/actions/workflows/npm-ci.yml)

`trex-runtime` is a fork of the [Supabase Edge Runtime](https://github.com/supabase/edge-runtime) (MIT). It is the edge function runtime used by [Trex](https://github.com/OHDSI/trex), where it hosts JavaScript / TypeScript edge functions, plugin APIs, and server-side workloads inside the `trex` binary.

## Why a fork

Trex is a self-hosted backend platform with an analytical column-store engine and federation built in (DuckDB-based, alongside Postgres). The fork lets us:

- Shape the edge runtime to fit Trex's deployment profile.
- Track our own Deno release cadence (currently Deno 2.7.14), in step with the rest of the Trex stack rather than the upstream service's schedule.
- Stay wire-compatible with the Supabase Edge Functions developer experience (deploy, secrets, local dev via the Trex CLI).

## License

MIT — see [`LICENSE`](LICENSE).
