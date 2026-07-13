# CP6 — local HTTP/SSE adapter

Status: **cycle 11 remediated**. The evidence below is deterministic and offline: no OAuth, network request, credential value, or model turn was used.

Code SHA evidence: `1bd3736a0fbb4d158925e2b3d898916d3c81bd4c` (`fix(cp6): add runtime owner actor`). This evidence commit is intentionally separate; its tree SHA is release metadata, not a claim about the code commit it describes.

## Lifecycle root and executable coverage

- `RuntimeOwner` is the sole HTTP lifecycle authority. Authenticated handlers validate inputs then send owner commands for bootstrap/snapshot, thread and turn creation, approval decision, interrupt, controlling-SSE drop, completion, recovery, and shutdown. It owns bounded API admission, turn/approval/SSE state and the one active controlled protocol execution.
- Startup recovery completes before binding. `serve` sends a bounded bootstrap command which initializes the selected protocol launcher and checks ChatGPT auth, exact model, and quota before readiness is exposed. A failed live bootstrap remains `503 RUNTIME_NOT_READY`; the live launcher never falls back to the fake fixture.
- T01 in `tests/cp6_runtime_owner.rs` injects the canonical fake launcher into a `live=true` owner and proves bootstrap readiness, original-ID schema-valid denial, `turn/interrupt` RPC plus terminal notification, process PID exit, journal ordering, terminal-once state, and a safely admitted second turn.
- T03 proves controlling SSE drop uses the identical owner cancellation command and original-ID denial path. Existing API coverage also proves timeout, concurrent decision exclusion, and replayable terminal SSE.
- External Allow and Deny now append `ApprovalRequested` before `ApprovalDecided`; the wire-delivery acknowledgement is delayed until that durable decision is written. Test-only Allow/Deny policies append the same order from bounded internal events.
- Child-controlled approval identifiers are hashed into short opaque keys before SSE, journal, storage, or duplicate tracking. Both duplicate tracking and internal event history have count and byte caps; oversized and repeated-id coverage is executable.
- Token-file owner-only permission and API thread-capacity coverage are executable in `cp6_runtime_owner.rs`; SSE replay/retention remains covered in `cp6_local_http_sse.rs`.

## Cycle 11 gates (code SHA above)

- `cargo fmt --all -- --check` — exit 0, 0.20s.
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-11 CARGO_NET_OFFLINE=true cargo test --locked --all-targets --all-features` — exit 0, 20.41s (60 tests).
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-11 CARGO_NET_OFFLINE=true cargo clippy --locked --all-targets --all-features -- -D warnings` — exit 0, 0.10s.
- `git diff --check` — exit 0, 0.10s.

## Residual non-gating risk

The live bootstrap is intentionally not exercised against real credentials or a real model in this repository. The bounded failure path reports only sanitized classes and remains fail-closed; operational deployment still needs a separately authorized live-account smoke test. The test launcher is explicitly injected and cannot select the production live executable path.
