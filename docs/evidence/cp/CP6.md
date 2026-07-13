# CP6 — local HTTP/SSE adapter

Status: **cycle 13 remediation gate-verified offline**. All evidence below is deterministic and offline: no OAuth, network request, credential value, or model turn was used.

Code SHA evidence: `8aa051f9f2214883ad22ac7c2490090f08d69c3a` (`fix(cp6): harden auth approval and cancellation`). This evidence commit is separate metadata, not a claim that its own SHA is the code SHA.

## Lifecycle root and executable coverage

- `RuntimeOwner` is the sole HTTP lifecycle authority. Authenticated handlers validate inputs then send commands for bootstrap/snapshot, thread and turn creation, approval decision, interrupt, controlling-SSE drop, deadline, completion, and shutdown. It owns bounded admission, the single active controlled client/process execution, journal handle, turn/approval state, and SSE replay.
- Startup recovery completes before binding and the owner opens the shared writer before bootstrap. Bounded bootstrap initializes the selected launcher, verifies ChatGPT auth, the exact model, and quota, writes a sanitized durable admission snapshot, then exposes readiness. Any failure remains `503 RUNTIME_NOT_READY`; production live mode never falls back to the fake launcher.
- T01/T04 in `tests/cp6_runtime_owner.rs` inject the canonical launcher into a `live=true` owner and prove readiness, the recorded admission snapshot, original signed-ID schema-valid denial, `turn/interrupt` RPC plus terminal notification, child PID exit, one terminal journal result, and safe second-turn admission.
- T03 exercises both controller-drop and approval-timeout through the same ordered cancellation path: durable original-ID denial acknowledgement, interrupt acknowledgement, terminal notification, process-group cleanup, and terminal-once SSE/SQLite projection. Controller drop uses awaited command delivery rather than lossy `try_send`.
- External Allow/Deny/timeout decisions append `ApprovalRequested` exactly once before a durable `ApprovalDecided`; the decision is durable before it can reach the child on the wire. Generated 0.144.3 permission approvals preserve the approved request profile and send an empty fail-closed profile for denial.
- Child-controlled approval identifiers are hashed to short opaque keys before SSE, SQLite, and duplicate tracking. Duplicate tracking, internal events, API retention, and SSE replay are bounded; executable oversized/repeated-child-ID coverage verifies no raw canary reaches durable state.
- Reroute/auth/model/quota execution failures clear the owner readiness/model/quota snapshot. Token-file owner-only permissions, API capacity, replay, and retention coverage remain executable.
- Live launch now requires the explicitly configured `SPARK_RUNNER_SUBSCRIPTION_AUTH_FILE`; it rejects absent, relative, symlinked, non-regular, or non-owner-only sources before spawn. The selected opaque handle alone is copied to the fresh `CODEX_HOME/auth.json` with `0600` permissions under its `0700` home. The child inherits neither the source path nor ambient Codex configuration, MCP configuration, or credential environment. Fake-canary coverage verifies this route and unsafe-mode rejection only; it does not inspect a real auth file.
- `approval.requested` SSE events now carry a bounded, redacted, schema-aware descriptor for commands, file-change paths/types, cwd, reason, and requested permission summaries. Invalid permission profiles cannot be approved; a valid Allow returns exactly the validated in-flight profile, while Deny and Timeout return distinct fail-closed schema-valid responses.
- Controlled cancellation now records execution interruption rather than completion before initialize/admission or before an irreversible write. A tracked JSONL flush boundary turns a control race after `thread/start` or `turn/start` delivery into a `delivery_ambiguous` durable incident and Unknown owner outcome; no synthetic interrupt is sent without both real identifiers. Deterministic fixture barriers cover all four phases without sleeps.

## Cycle 13 gates (code SHA above)

- `cargo fmt --all -- --check` — exit 0.
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-13 CARGO_NET_OFFLINE=true cargo test --locked --all-targets --all-features` — exit 0 (68 tests).
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-13 CARGO_NET_OFFLINE=true cargo clippy --locked --all-targets --all-features -- -D warnings` — exit 0.
- `git diff --check` — exit 0.

## Residual non-gating risk

The live bootstrap is intentionally not exercised against real credentials or a real model in this repository. The selected-file provisioning path was tested only with fake canary data; operational deployment still needs a separately authorized live-account smoke test. The injected fixture cannot select the production live executable path. A forced process-group kill after a protocol acknowledgement/terminal timeout is deliberately treated as a conservative failed/unknown operational boundary, never as a successful model result.
