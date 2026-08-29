# Post-upgrade validation — Deno 2.9.5

Branch `deno2.9.5`, validated on macOS 26.5.1 / aarch64 with rustc 1.95.0.

**There is no 2.7.14 baseline** — it was deliberately skipped. This document therefore
does **not** claim "no regressions". It records the state of the suite at 2.9.5 and,
for every failure, the evidence for why it fails.

Tests were run with plain `cargo test`, not `./scripts/test.sh`: that script hardcodes
`--features base/trex`, which cannot compile in this repo because `crates/trex_core_stub`
is an empty stub while `crates/base/build.rs` calls `trex_core::trex::init()`. Only the
parent `OHDSI/trex` repo exercises that path.

## Totals

| | passed | failed | ignored | other |
|---|---|---|---|---|
| `base` integration tests (140) | 93 | 42 | 4 | 1 hangs forever |
| all other targets + doc-tests | 78 | 1 | 24 | — |
| **total** | **171** | **43** | **28** | **1 hang** |

No single run covers all 140 integration tests: the binary aborts with a V8 fatal (R1)
and one test never terminates on macOS (R4). The table is the union of six runs; every
status was observed, none inferred.

## Checks against the spec's invariants

| # | Check | Verdict |
|---|---|---|
| 1 | Full suite | Ran. All 43 failures categorised; see below. |
| 2 | V8 snapshot **loads** | **Yes**, positively. `DENO_STARTUP_PHASES=1` shows `V8 Context::from_snapshot`, `load_data loop (get_context_data x N)` and `load module map from snapshot` for all three isolates, with `init_extension_js` at 3.9µs (nothing re-evaluated). Not verified for `--features base/trex` — unbuildable here. |
| 3 | node:http | `test_node_server`, `test_commonjs_express`, `test_commonjs_express_websocket`, `test_commonjs_hono{,_websocket}`, `test_commonjs_ws_websocket` all pass. |
| 4 | WebSocket upgrade | Both non-TLS `test_websocket_upgrade_*` pass with a full post-upgrade frame round-trip, so `conn_sync` holds the connection open. The four `*_secure` variants fail in the client on the TLS fixture (below). |
| 5 | eszip 0.109 → 0.135 | v0 / v1 / v1.1 legacy artefacts load: all 9 `deno_facade` eszip tests pass, plus `test_eszip_wasm_import`, `test_worker_boot_with_0_byte_eszip`, `test_load_corrupted_eszip_v1`. **Caveat:** `test_eszip_migration` is vacuous — its fixtures live in the private `supabase/edge-runtime-test-eszip` repo and the CI step that fetches them is commented out, so it runs 0 cases in 0.01s and writes a fresh snapshot instead of failing. It guards nothing today. |
| 6 | `require("http")` | `test_commonjs` passes. |
| 7 | Inspector (`429920c7`) | `test_inspector_devtools_ws_closes_on_worker_kill` passes. |
| 8 | Worker lifecycle (`5b8b3e2b`) | `test_user_worker_exit_reason_survives_shutdown_race` passes. |
| 9 | Cert store (`60f811b1`) | `test_tls_throw_invalid_data` passes. `DENO_TLS_CA_STORE=system\|mozilla` boot and serve; `bogus` is rejected with the expected message. `Deno.createHttpClient({caCerts})` against a real TLS server returns 200. |
| 10 | OTel | Exporter initialises **and** per-worker attributes appear: exported log records carry `edge_runtime.worker.kind: "main"` / `"event"`. Note `--enable-otel all` is not a valid value — `OtelKind` accepts `main`/`event`; `--enable-otel main,event` was used. User workers build the attribute but are not wired for export by the CLI flag (pre-existing design). |
| 11 | Cold start | Reference point only, no baseline. Process launch → first HTTP 200 from a cold user worker: **median 0.139 s** (5 runs, 0.136–0.141). TTFB once listening: **median 0.039 s**. `JsRuntime::new_inner` 4.5–5.5 ms per isolate. |

k6 was not run: `k6` is not installed, `k6/dist/` does not exist, the workflow is
`workflow_dispatch`-only, and there is no baseline to compare thresholds against.

## The 43 failures

**Environmental — 42**

- **32** ONNX Runtime absent (`libonnxruntime.dylib` not installed; CI installs it via
  `scripts/install_onnx.sh`): 31 `test_ort_*` + `test_supabase_ai_gte`, plus
  `ext_ai::onnxruntime::tensor::tests::test_ort_tensor_extract_ref`.
- **9** macOS TLS fixture: `tests/fixture/tls/localhost.pem` is valid for 7300 days and
  macOS caps server certs at 398 days, so the `reqwest`/native-tls clients fail at
  connect with `-67901` before any server code runs. Confirmed by the `*_secure`
  slowloris tests, which drive **rustls** directly and pass. Fixture unchanged since
  `eac0db16`.
- **1** `test_host_fs_access_allowed` reads `/etc/hostname`, which does not exist on macOS.

**Pre-existing — 1**

- `test_commonjs_express_forwards_request_headers` (Authorization not forwarded to the
  commonjs/express worker). Task 12b observed it failing before its own changes; the
  test comes from `86890fc8`, already on `develop`.

**Candidate regressions — 3 failures + 1 crash**

| id | Symptom | Diagnosis |
|---|---|---|
| **R1** | `Fatal error in v8::HandleScope::CreateHandle() — Cannot create a handle without a HandleScope`, always at `test_runtime_event_beforeunload_cpu`, aborting the test binary | Deterministic in a full run (4/4), at both `--test-threads=1` and `=4`; not caused by the ONNX failures; **not** reproducible in isolation or with either alphabetical half of the suite (28 or 55 tests) — only with both together (76). Points at accumulated isolate state rather than one predecessor. The beforeunload path is code this upgrade rewrote: `dispatch_beforeunload_event` now uses `v8::scope_with_context!` (`runtime/mod.rs:2190`) and the interrupt callbacks were ported to `v8::UnsafeRawIsolatePtr` (`worker/supervisor/v8_handler.rs`). |
| **R2** | `test_tmp_fs_usage`, `test_tmp_fs_should_not_be_available_in_import_stmt`: `NotFound: entity not found: open '/tmp/meow'` | deno_fs 0.167 canonicalizes paths in the permission layer before calling the `FileSystem` impl (`runtime/permissions/lib.rs:2910`), skipping only Windows device paths and, on Linux, `/proc`/`/dev`. trex mounts its tmpfs at the literal prefix `"/tmp"` (`runtime/mod.rs:727`). On macOS `/tmp` is a symlink to `/private/tmp`, so the prefix stops matching and the call falls through to `base_fs`. Latent on Linux, where `/tmp` is a real directory — but the mechanism is new in 2.9.5 and affects any symlinked mount point. |
| **R3** | `test_issue_208`: `assertion failed: reason.contains("invalid peer certificate: UnknownIssuer")` | Behaviour is intact — verified by hand against `openssl s_server`: with the root CA supplied the fetch returns **200**, without it **500**. Only the message text changed: deno_fetch at 2.9.5 no longer folds the rustls detail into the `TypeError`, which now reads just `TypeError: fetch failed`. Test assertion needs updating. |

**Platform gap, not a regression — 1 (hang)**

- `test_issue_func_280` never terminates on macOS. `CPUTimer` is Linux-only
  (`crates/cpu_timer/src/lib.rs`, `CPU timer: not enabled (need Linux)`), and the
  supervisor's fallback only enforces limits when the worker yields; the test's
  `beforeunload` handler is an infinite CPU-burn loop that never does.
  `git diff develop -- crates/cpu_timer/ crates/base/src/worker/supervisor/` is empty.
  Consequence: **`cargo test` cannot complete on macOS** without
  `--skip test_issue_func_280`.

## Not verified

1. Anything needing a before/after comparison — R1/R2/R3 are *candidates*: their
   mechanisms are traced but none is proven absent on `develop`. That would need a
   second `target/` (~12 GB).
2. `--features base/trex` (spec invariant 1, `7b1266f4`) — unbuildable in this repo.
   The `snapshot_extension_parity` test covers the source-level half and passes.
3. The private eszip fixture corpus.
4. k6 load thresholds.
5. ONNX/ORT behaviour (32 tests never reached their subject).
6. S3/MinIO (`crates/fs` integration tests: 7 ignored).
