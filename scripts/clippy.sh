#!/usr/bin/env bash
(cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::unnecessary-literal-unwrap -A unknown_lints -A elided_named_lifetimes -A clippy::needless_lifetimes || true) && cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::unnecessary-literal-unwrap -A unknown_lints -A elided_named_lifetimes -A clippy::needless_lifetimes
