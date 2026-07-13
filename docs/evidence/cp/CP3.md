# CP3: Resilient session — restart on protocol desync — PARTIAL

Status: **partial**. Historical local gate evidence below remains valid for its stated revision, but CP3 is not implementation-complete: later CP6 review identified remaining runtime-owner and interleaved-request work.
No live model run was needed or performed for this checkpoint; all evidence below is from deterministic,
offline gates against the `fake_app_server` fixture.

## Implementation summary

- `src/main.rs` — no longer declares duplicated private modules. It now imports the shared crate surface via
  `spark_runner::config::{Cli, Command}` and `spark_runner::orchestrator::{run_doctor, run_turn}`, so the
  binary and the integration tests exercise the same code paths.
- `src/lib.rs` — exposes `client`, `config`, `jsonl`, `orchestrator`, `process`, and `state` as public modules
  so `tests/cp3_resilience.rs` (and `tests/app_server.rs`) can drive the orchestrator directly instead of
  shelling out.
- `src/orchestrator.rs` (new) — houses `run_doctor`, `run_turn`, and the test-only
  `run_doctor_with_fake_server_args` entry point, plus the controlled-restart logic: on a detected protocol
  desync (poison-on-desync per ADR-004) the orchestrator performs exactly one restart of the app-server
  process and retries the doctor flow once; if the restart also desyncs, it fails closed rather than retrying
  further.
- `src/jsonl.rs` — hardened JSONL framing: oversized frames (bounded by `MAX_FRAME_LEN`) and malformed
  (non-JSON) lines are rejected rather than silently skipped, and responses carrying an id that was never
  requested are treated as a desync. All of these poison the session and produce sanitized errors that never
  echo raw frame content.
- `src/config.rs` — `fake_app_server_path` now also checks the parent of the current executable's directory
  for the `fake_app_server` binary, so integration tests running from `target/debug/deps` (where
  `current_exe()`'s parent is `deps/`, not `debug/`) can still locate the fixture binary alongside the
  `spark-runner` binary in `target/debug`.
- `src/bin/fake_app_server.rs` — gains `--fake-mode` and `--fail-marker` flags to deterministically simulate
  `oversized_frame`, `malformed_frame`, `unknown_response_id`, and `unknown_response_id_once` (a one-time
  desync that recovers on the next process) failure modes, appending one line to the fail-marker file per
  app-server process launched, so tests can assert exactly how many processes were spawned.
- `src/client.rs` — updated to route through the shared jsonl/state machinery used by the orchestrator's
  restart path.
- `tests/cp3_resilience.rs` (new) — five tests, all deterministic against the offline fixture, no sleeps:
  `oversized_frame_poisons_session_and_fails_closed`, `malformed_frame_poisons_session_and_fails_closed`,
  `unknown_response_id_poisons_session_and_fails_closed`, `restart_recovers_from_one_time_desync` (asserts
  exactly one restart, i.e. exactly two app-server processes total, and a completed turn), and
  `fails_closed_after_restart_also_desyncs` (asserts exactly one restart attempt and a closed failure, not
  zero or unlimited retries). All error-path assertions also confirm raw frame content never leaks into
  sanitized error messages.

## Local gates (run on `uap-build-1`)

| Gate | Command | Result |
| --- | --- | --- |
| Lint | `cargo clippy --all-targets -- -D warnings` | exit `0` |
| Tests | `cargo test --locked` | exit `0` — 8 library unit tests, 2 `app_server` integration tests, 5 `cp3_resilience` tests, 0 doc tests, all passing |

Both gates were run on `uap-build-1` at `2026-07-13T03:03:43Z`.

## Scope covered

- Poison-on-desync for oversized JSONL frames.
- Poison-on-desync for malformed (non-JSON) JSONL frames.
- Poison-on-desync for a response carrying an unknown/unrequested id.
- Exactly one controlled app-server restart when a one-time desync recovers on the next process.
- Fail-closed behavior (no further retries) when the desync persists across the restart.

## Validation checklist

- [x] `cargo clippy --all-targets -- -D warnings` run on `uap-build-1` and passing.
- [x] `cargo test --locked` run on `uap-build-1` and passing (8 unit + 2 `app_server` + 5 `cp3_resilience`
      integration tests, 0 doc tests).
- [x] No live model run required for CP3; all resilience behavior verified deterministically offline.
- [ ] Push CP3 branch and open PR #3.
- [ ] Wait for green CI on PR #3.
- [ ] Squash merge PR #3.
- [ ] Confirm `main` reflects the merge.
- [ ] Clean up branch/worktree.

## Gate decision

CP3 is **green locally**: lint and the full test suite (including the five new `cp3_resilience` tests
covering poison-on-desync, the single controlled restart, and fail-closed behavior after a persistent
desync) all passed on `uap-build-1` at `2026-07-13T03:03:43Z`. CP3 is **not yet fully closed** — pushing the
branch, opening PR #3, waiting for green CI, squash merging, confirming `main`, and cleaning up the
branch/worktree remain outstanding before this checkpoint is considered fully merged.
