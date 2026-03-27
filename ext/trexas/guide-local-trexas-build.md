# Local Trexas Build & Hot-Swap Guide

Build the `trexas` DuckDB extension locally and hot-swap it into a running `d2e-trex` container — no full Docker image rebuild needed. This lets you iterate on Rust and JS code in the trex repo.

**What gets built:** `trexas` is a DuckDB loadable extension (Rust cdylib, ~184 MB) that includes:
- Rust code in `ext/trexas/src/` (extension entrypoint, server management, bundling)
- JS code in `ext/trex/js/` (`dbconnection.js`, `trex_lib.js`) — embedded at compile time into the `trex_core` crate
- Dependencies: `deno`, `deno_core`, `rusty_v8`, `trexsql-rs`, and ~500 other crates

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Docker | Tested with 29.3.0. Must support `--platform linux/amd64` |
| Git | For submodule init |
| ~10 GB disk | Docker image + build artifacts + cargo cache |

No Rust, clang, cmake, or other native tools needed on the host — everything builds inside Docker.

## Step 1: Initialize git submodule (~1 s)

```bash
cd repos/trex
git submodule update --init ext/trexas/extension-ci-tools
```

## Step 2: Build the Docker build environment (~1 m 30 s)

The Dockerfile at `ext/trexas/Dockerfile.trexas-builder` sets up Rust, clang, cmake, and the DuckDB linking libraries without compiling anything.

```bash
cd repos/trex
docker build --platform linux/amd64 \
  -f ext/trexas/Dockerfile.trexas-builder \
  -t trexas-builder .
```

This image is cached after the first build. Rebuild only if you need different system dependencies.

## Step 3: Run `make configure` (~22 s)

Creates a Python venv, detects the platform, and writes the extension version.

```bash
cd repos/trex
docker run --rm --platform linux/amd64 \
  -v "$(pwd)":/usr/src/trex \
  trexas-builder \
  bash -c "git config --global --add safe.directory /usr/src/trex && cd ext/trexas && make configure"
```

**Output files:**
- `ext/trexas/configure/platform.txt` → `linux_amd64`
- `ext/trexas/configure/extension_version.txt` → git short hash
- `ext/trexas/configure/venv/` → Python venv with duckdb + tools

Configure is cached — skip this step on subsequent builds unless you've deleted `ext/trexas/configure/`.

## Step 4: Run `make release` (~14–30 min)

```bash
cd repos/trex
docker run --rm --platform linux/amd64 \
  -v "$(pwd)":/usr/src/trex \
  trexas-builder \
  bash -c "git config --global --add safe.directory /usr/src/trex && cd ext/trexas && make release"
```

**First build:** ~30 min (downloads Rust 1.90.0 toolchain, git deps, 500+ crates, then compiles)
**Incremental rebuild (code change only):** ~14 min (re-downloads toolchain + git deps due to ephemeral container, but only recompiles changed crates)

**Build output:**

| File | Size | Description |
|------|------|-------------|
| `build/release/trexas.trex` | ~184 MB | Extension with DuckDB metadata — **use this one** |
| `build/release/extension/trexas/trexas.trex` | ~184 MB | Same file (Makefile copy) |
| `build/release/libtrexas.so` | ~184 MB | Raw .so without metadata — **DuckDB will reject this** |

## Step 5: Deploy the local build

Two things need to happen before restarting: enable unsigned extension loading and copy the built extension into the container.

### Enable unsigned extensions (one-time)

Local builds are unsigned (the published extensions are signed with a key we don't have). You must tell DuckDB to allow unsigned extensions.

Add to your docker-compose override (e.g., `docker-compose.local.yml`) under the trex service:

```yaml
environment:
  TREX_INIT: '{"allowUnsignedExtensions":true}'
```

**How `TREX_INIT` works:**
1. `trexsql/core.clj` reads `TREX_INIT` env var as JSON
2. `camel->kebab` converts `allowUnsignedExtensions` → `:allow-unsigned-extensions`
3. `trexsql/db.clj` sets the DuckDB config flag on connection creation
4. `trexsql/extensions.clj` runs `LOAD '<path>'` — succeeds without signature check

### Copy the extension and restart

```bash
# Copy the .trex file into the container
docker cp repos/trex/ext/trexas/build/release/trexas.trex \
  d2e-trex:/usr/src/node_modules/@trex/as/trexas.trex

# Restart the trex service (picks up compose override + reloads extension)
npm run start -- -s trex
```

**Note:** The copied file persists across restarts. It is only lost on `docker compose up --force-recreate` (which rebuilds from the image).

## Step 6: Verify your build is loaded

To confirm the container is running your local build, add a log marker to the source before building.

**Option A — JS marker** (in `ext/trex/js/dbconnection.js`):

Add a `console.log` at the top of the `TrexConnection` constructor:

```js
    constructor(
        conn,
        writeConn,
        schemaName,
        vocabSchemaName,
        resultSchemaName,
        dialect,
        translatefn,
    ) {
        console.log("=== LOCAL TREXAS BUILD ===");
        this.connection = conn
        // ... rest of constructor ...
    }
```

JS files are embedded at compile time, so this will be baked into the extension.

**Option B — Rust marker** (in `ext/trexas/src/trex_server.rs`):

Add an `eprintln!` in the `start_server_sync` function:

```rust
pub fn start_server_sync(&self, config: ServerConfig) -> Result<String> {
    eprintln!("=== LOCAL TREXAS BUILD ===");
    // ... rest of function ...
```

After hot-swapping and restarting, check the logs:

```bash
docker logs d2e-trex 2>&1 | grep "LOCAL TREXAS BUILD"
```

## Quick Reference (after first-time setup)

Once you've completed steps 1–5, the iterative cycle is:

```bash
cd repos/trex

# 1. Make your code changes
#    e.g., edit ext/trexas/src/*.rs or ext/trex/js/*.js

# 2. Build (14-30 min depending on cache state)
docker run --rm --platform linux/amd64 \
  -v "$(pwd)":/usr/src/trex \
  trexas-builder \
  bash -c "git config --global --add safe.directory /usr/src/trex && cd ext/trexas && make configure && make release"

# 3. Hot-swap into running container
docker cp ext/trexas/build/release/trexas.trex \
  d2e-trex:/usr/src/node_modules/@trex/as/trexas.trex
npm run start -- -s trex

# 4. Check logs for your marker (see Step 6)
docker logs d2e-trex 2>&1 | grep "LOCAL TREXAS BUILD"
```

## What Can You Change?

| Source | Included in trexas build? | How it's included |
|--------|--------------------------|-------------------|
| `ext/trexas/src/*.rs` | Yes | Compiled directly into the extension |
| `ext/trex/js/dbconnection.js` | Yes | Embedded at compile time via `esm` macro in `trex_core` |
| `ext/trex/js/trex_lib.js` | Yes | Embedded at compile time via `esm` macro in `trex_core` |
| `ext/trex/lib.rs` (trex_core) | Yes | Workspace dependency of trexas |
| `crates/base/`, `crates/deno_facade/`, etc. | Yes | Workspace dependencies |
| `core/server/*.ts` (D2E TypeScript) | **No** | Bundled separately as `.eszip` files |
| `trexsql.jar` (Java CLI) | **No** | Separate build, loads extensions |

To update the TypeScript service code, rebuild the eszip bundle instead:
```bash
docker exec d2e-trex npx trex bundle -e ./core/server/index.ts -o ./core/server/index.eszip
docker restart d2e-trex
```

## Cleaning Up

Build artifacts are owned by root (Docker runs as root). Use Docker to clean:

```bash
cd repos/trex

# Clean everything (configure + build + cargo cache)
docker run --rm --platform linux/amd64 \
  -v "$(pwd)":/usr/src/trex \
  trexas-builder \
  bash -c "rm -rf /usr/src/trex/ext/trexas/configure /usr/src/trex/ext/trexas/build /usr/src/trex/ext/trexas/target"

# Or just clean build output (keep cargo cache for faster rebuilds)
docker run --rm --platform linux/amd64 \
  -v "$(pwd)":/usr/src/trex \
  trexas-builder \
  bash -c "rm -rf /usr/src/trex/ext/trexas/build"
```

## Timing Summary

| Step | First Build | Subsequent Builds |
|------|-------------|-------------------|
| Init submodule | ~1 s | N/A (already done) |
| Build Docker image | ~1 m 30 s | ~0 s (cached) |
| Make configure | ~22 s | ~0 s (cached) |
| Make release | ~30 min | ~14 min |
| Hot-swap + restart | ~10 s | ~10 s |
| **Total** | **~33 min** | **~14 min** |

Note: incremental builds re-download the Rust 1.90.0 toolchain and git dependencies each time because they live inside the ephemeral container (not on the mounted volume). Only compiled crate artifacts in `target/` are cached.

## Why NOT Use `Dockerfile.trex --target builder`

The original `Dockerfile.trex` builder stage runs `cargo build --profile release --features "cli/tracing"` which compiles the ENTIRE trex workspace (CLI binary + all extensions). This would take hours under emulation and isn't needed for just the trexas extension.

`Dockerfile.trexas-builder` only installs the toolchain and dependencies (~1 m 30 s), then we mount the source and build just trexas via `make release`.

## Gotchas

1. **`libtrexsql` version in Dockerfile** — `Dockerfile.trexas-builder` downloads a prebuilt `libtrexsql` from `https://github.com/p-hoffmann/trexsql-rs/releases/download/v1.4.4-trex/libtrexsql-linux-amd64.zip`. If trexsql-rs is updated to a newer version, you'll need to update this URL in the Dockerfile to match.

2. **python3-venv required** — The `rust:1.84.0-bookworm` base image has python3 but NOT python3-venv. Already included in the custom Dockerfile.

3. **Git safe.directory** — Must run `git config --global --add safe.directory /usr/src/trex` inside the container due to UID mismatch between host and container.

4. **Root-owned artifacts** — All files created by Docker are owned by root on the host. Use `docker run ... rm -rf` for cleanup, not `rm` directly.

5. **Rust toolchain auto-upgrade** — The repo's `rust-toolchain.toml` specifies 1.90.0, overriding the Docker image's 1.84.0. Downloaded automatically on each container run (~30 s).

6. **Extension signing** — Published extensions are signed. Local builds are unsigned. Must set `TREX_INIT={"allowUnsignedExtensions":true}` env var.

7. **Use `.trex` not `.so`** — The `.trex` file has DuckDB metadata appended (platform, version, ABI type). DuckDB validates this on load and rejects the raw `.so`.

8. **V8 prebuilt binary** — Downloaded automatically via `RUSTY_V8_MIRROR` in `.cargo/config.toml`. No `V8_FROM_SOURCE=1` needed for linux_amd64.

9. **`docker restart` preserves files** — Hot-swapped files survive `docker restart` but NOT `docker compose up --force-recreate`.

10. **Cargo.toml profile warning** — The trexas `Cargo.toml` `[profile.release]` settings (lto, strip) are ignored because they must be set at the workspace root.
