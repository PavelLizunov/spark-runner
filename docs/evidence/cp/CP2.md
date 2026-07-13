# CP2: Minimal Rust runner — CI GREEN, MERGE PENDING

Status: **CI green, merge pending**. Implementation is complete on branch `cp2-minimal-runner`; local
gates, a live `doctor --live` run, and stable schema generation have all been executed on `uap-build-1` and
passed. PR #2 (https://github.com/PavelLizunov/spark-runner/pull/2) is open for commit
`7c8a9ea96087042c6365f265f59feb0f9ebc7560`, and CI was green on both the push run
(https://github.com/PavelLizunov/spark-runner/actions/runs/29218755221) and the pull_request run
(https://github.com/PavelLizunov/spark-runner/actions/runs/29218769616), with the `validate` job passing on
each. Squash merge remains pending for final closeout: a final evidence update will be pushed and CI rerun
before the squash merge.

## Implementation summary

Single binary crate `spark-runner` (no workspace), per ADR-001/ADR-003/ADR-005:

- `Cargo.toml` — two `[[bin]]` targets (`spark-runner`, `fake_app_server`), dependencies limited to the
  approved plan: `tokio`, `serde`, `serde_json`, `thiserror`, `tracing`, `tracing-subscriber`, `clap`. No
  dev-dependencies.
- `rust-toolchain.toml` — pinned to `stable` with `rustfmt`/`clippy` components.
- `codex.lock` — pinned live binary `/home/uap/.local/bin/codex`, version `0.142.0`, sha256
  `d3be844c45c4fd89392536e56e1010963f94785592596b50cd0c45bb8a341406`, transport `stdio`, required model
  `gpt-5.3-codex-spark`, schema path `protocol/0.142.0/stable.schema.json`. `schema_hash` is the real
  sha256 `efdc3e4ef848db9543c29d7f150820fe80e970720cc887e17e6f3c196bc37259` of
  `protocol/0.142.0/stable.schema.json`, generated with
  `/home/uap/.local/bin/codex app-server generate-json-schema --out protocol/0.142.0` on build-1.
- `src/process.rs` — spawns children via `tokio::process::Command` directly (no shell), `kill_on_drop`,
  stderr drained concurrently into a bounded 200-line tail, explicit async `shutdown()` (kill + wait + join
  drain task) and a `Drop` impl as a backstop (ADR-002). Process-tree fix: the spawned launcher is placed in
  its own Unix process group via `Command::process_group(0)`, and `shutdown()`/`Drop` kill the whole group
  (not just the direct child) so descendant processes spawned by the launcher are also terminated. Both the
  child `wait()` and the stderr-drain task join are bounded by `SHUTDOWN_TIMEOUT` so shutdown cannot hang
  indefinitely.
- `src/jsonl.rs` — JSONL request/response client: writes `{"id":N,"method":...,"params":...}` lines, reads
  stdout lines, matches responses by id, and tolerates unrelated notifications/malformed lines while waiting
  (ADR-004). Never logs full raw line content — only method name and whether an id was present.
- `src/state.rs` — minimal turn state machine (`Idle -> ThreadStarted -> TurnStarted -> {Completed,Failed}`);
  any other transition poisons the session (ADR-004 poison-on-desync). No reconnect/restart logic yet — that
  is CP3+ scope.
- `src/client.rs` — `CodexClient` exposing `initialize`, `account_read`, `model_list`, `rate_limits_read`,
  `thread_start`, `turn_start`, `wait_turn_completed`. `thread_start` uses the stable CP1 sandbox shape
  (`"sandbox": "read-only"` as a string, not a map), `approvalPolicy: on-request`, `ephemeral: true`, pinned
  `model: gpt-5.3-codex-spark`, and a temp `cwd`. Fails closed with `FallbackModel` if the server ever reports
  a different model.
- `src/config.rs` — CLI (clap) for `doctor [--live]` and `run --prompt <text> [--live]`, `codex.lock` loading,
  and a fresh ephemeral temp-dir helper for the thread `cwd`.
- `src/main.rs` — wires the CLI to the offline fake app-server by default, or the pinned live binary under
  `--live`. `doctor` confirms initialize/account/read/model/list/account-rateLimits-read, the exact model, an
  ephemeral read-only thread, one turn to terminal `turn/completed`, and fails if a fallback model is
  observed (checked both in `model/list` and in the `thread/start` response). `run --prompt` sends the given
  prompt but never logs or prints raw model output — only a sanitized `mode/model/turn_status` summary line.
- `src/bin/fake_app_server.rs` — deterministic offline stand-in for `codex app-server --listen stdio://`:
  answers `initialize`, `account/read`, `model/list` (includes exact `gpt-5.3-codex-spark`),
  `account/rateLimits/read`, `thread/start` (+ `thread/started` notification), `turn/start` (+ `turn/started`
  and terminal `turn/completed` notifications).
- `tests/app_server.rs` — spawns the compiled `fake_app_server` binary and drives the same
  initialize/account/model/rate-limits/thread-start/turn-start/wait-for-completion flow directly over JSONL,
  asserting the exact required model and terminal `status: completed`, while explicitly tolerating the
  interleaved `thread/started`/`turn/started` notifications. Adds a regression test,
  `shutdown_terminates_process_group_descendant`, covering the process-group fix above: it verifies that a
  descendant process spawned by the launcher is also terminated when the launcher is shut down, not just the
  direct child.
- `.github/workflows/ci.yml` already ran `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D
  warnings`, and `cargo test --locked` conditionally on `Cargo.toml` existing; no change was needed now that
  the crate exists.

## Local gates (run on `uap-build-1`)

| Gate | Command | Exit status |
| --- | --- | --- |
| Format check | `cargo fmt --all -- --check` | `0` |
| Lint | `cargo clippy --all-targets -- -D warnings` | `0` |
| Tests | `cargo test --locked` | `0` (5 unit tests, 2 integration tests, including the new `shutdown_terminates_process_group_descendant` regression test) |

## Process leak check

- Pre-doctor check: no `spark-runner`, `codex app-server`, or `app-server` processes found running.
- Post-doctor check (after the live run below completed): no matching processes found running — confirms the
  process-group shutdown fix leaves no orphaned descendants.

## Live doctor run

- First attempt, `timeout 90s cargo run --locked -- doctor --live`, failed before `doctor` itself ran because
  Cargo requires an explicit `--bin` when a crate defines more than one binary target.
- Corrected command: `timeout 90s cargo run --locked --bin spark-runner -- doctor --live`. Exited `0` with
  output `doctor: ok mode=live model=gpt-5.3-codex-spark turn_status=completed`, confirming the exact pinned
  model was used (no fallback) and the turn reached the terminal `completed` status against the pinned live
  `/home/uap/.local/bin/codex app-server --listen stdio://` binary.

## Validation checklist

- [x] `cargo fmt --all -- --check` run on build-1 and passing.
- [x] `cargo clippy --all-targets -- -D warnings` run on build-1 and passing.
- [x] `cargo test --locked` run on build-1 and passing (including `tests/app_server.rs`, 5 unit + 2
      integration tests).
- [x] `spark-runner doctor --live` executed on build-1 against the pinned
      `/home/uap/.local/bin/codex app-server --listen stdio://`, confirming exact model
      `gpt-5.3-codex-spark`, no fallback model, and terminal `turn/completed`; sanitized result recorded above.
- [x] Process leak check: no stray `spark-runner`/`codex app-server` processes before or after the live run.
- [x] Real `schema_hash` generated for `codex.lock`: sha256
      `efdc3e4ef848db9543c29d7f150820fe80e970720cc887e17e6f3c196bc37259` of
      `protocol/0.142.0/stable.schema.json`, generated on build-1.
- [x] `Cargo.lock` and `protocol/0.142.0/stable.schema.json` are present in the working tree and tracked in
      PR #2.
- [x] `Cargo.lock` consistent with `--locked` verified in CI on the pushed branch: GitHub Actions `validate`
      job green on push run https://github.com/PavelLizunov/spark-runner/actions/runs/29218755221 and
      pull_request run https://github.com/PavelLizunov/spark-runner/actions/runs/29218769616.
- [x] GitHub Actions `ubuntu-latest` run green on the pushed branch — see run ids above.
- [x] PR opened: https://github.com/PavelLizunov/spark-runner/pull/2 (commit
      `7c8a9ea96087042c6365f265f59feb0f9ebc7560`).
- [ ] Squash merge PR #2.
- [ ] Confirm `main` reflects the merge.
- [ ] Clean up branch/worktree.

## Gate decision

CP2 is **green locally and in CI**: format, lint, tests (including the process-group shutdown regression
test), and a live `doctor --live` run against the pinned Codex binary all passed on `uap-build-1`, with clean
process-leak checks before and after; PR #2 is open and GitHub Actions `validate` was green on both the push
and pull_request runs for commit `7c8a9ea96087042c6365f265f59feb0f9ebc7560`. A final evidence update will be
pushed and CI rerun before the squash merge; this evidence file's own commit has not yet had CI run on it.
CP2 is **not yet fully closed** — squash merge, confirming `main`, and branch/worktree cleanup remain
outstanding before `PROGRESS.json` records a `completed_at`.
