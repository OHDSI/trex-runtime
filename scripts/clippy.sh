#!/usr/bin/env bash
set -euo pipefail

# `base` exposes a `trex` feature that pulls in `trex_core` from the parent
# OHDSI/trexsql monorepo. This repo resolves it against an empty stub
# (see [patch.crates-io] in the root Cargo.toml), so `--features trex` won't
# compile here — only the trexsql workspace exercises that feature. Lint
# `base` with its other features explicitly, and `--all-features` everywhere else.
LINTS=(-D warnings -D clippy::unnecessary-literal-unwrap -A unknown_lints -A mismatched_lifetime_syntaxes -A clippy::needless_lifetimes)

cargo clippy --workspace --exclude base --all-targets --all-features -- "${LINTS[@]}"
cargo clippy -p base --all-targets --features "tracing termination-signal-ext" -- "${LINTS[@]}"
