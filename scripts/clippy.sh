#!/usr/bin/env bash
#(cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::unnecessary-literal-unwrap -A unknown_lints -A mismatched_lifetime_syntaxes -A clippy::needless_lifetimes || true) && 
cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::unnecessary-literal-unwrap -A unknown_lints -A mismatched_lifetime_syntaxes -A clippy::needless_lifetimes
