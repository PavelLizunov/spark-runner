# CP6 — local HTTP/SSE adapter

Status: **cycle 18 remediation gate-verified offline**. All evidence below is deterministic and offline: no OAuth, network request, credential value, or model turn was used.

Historical cycle-13 code SHA evidence: `8aa051f9f2214883ad22ac7c2490090f08d69c3a` (`fix(cp6): harden auth approval and cancellation`). This evidence commit is separate metadata, not a claim that it is the current code SHA.

## Lifecycle root and executable coverage

- `RuntimeOwner` is the sole HTTP lifecycle authority. Authenticated handlers validate inputs then send commands for bootstrap/snapshot, thread and turn creation, approval decision, interrupt, controlling-SSE drop, deadline, completion, and shutdown. It owns bounded admission, the single active controlled client/process execution, journal handle, turn/approval state, and SSE replay.
- Startup recovery completes before binding and the owner opens the shared writer before bootstrap. Bounded bootstrap initializes the selected launcher, verifies ChatGPT auth, the exact model, and quota, writes a sanitized durable admission snapshot, then exposes readiness. Any failure remains `503 RUNTIME_NOT_READY`; production live mode never falls back to the fake launcher.
- T01/T04 in `tests/cp6_runtime_owner.rs` inject the canonical launcher into a `live=true` owner and prove readiness, the recorded admission snapshot, original signed-ID schema-valid denial, `turn/interrupt` RPC plus terminal notification, child PID exit, one terminal journal result, and safe second-turn admission.
- T03 exercises both controller-drop and approval-timeout through the same ordered cancellation path: durable original-ID denial acknowledgement, interrupt acknowledgement, terminal notification, process-group cleanup, and terminal-once SSE/SQLite projection. Controller drop uses awaited command delivery rather than lossy `try_send`.
- External Allow/Deny/timeout decisions append `ApprovalRequested` exactly once before a durable `ApprovalDecided`; the decision is durable before it can reach the child on the wire. Generated 0.144.3 permission approvals preserve the approved request profile and send an empty fail-closed profile for denial.
- Child-controlled approval identifiers are hashed to short opaque keys before SSE, SQLite, and duplicate tracking. Duplicate tracking, internal events, API retention, and SSE replay are bounded; executable oversized/repeated-child-ID coverage verifies no raw canary reaches durable state.
- Reroute/auth/model/quota execution failures clear the owner readiness/model/quota snapshot. Token-file owner-only permissions, API capacity, replay, and retention coverage remain executable.
- Live launch now requires the explicitly configured `SPARK_RUNNER_SUBSCRIPTION_AUTH_FILE`; it rejects absent, relative, symlinked, non-regular, or non-owner-only sources before spawn. The selected opaque handle alone is copied to the fresh `CODEX_HOME/auth.json` with `0600` permissions under its `0700` home. The child inherits neither the source path nor ambient Codex configuration, MCP configuration, or credential environment. Fake-canary coverage verifies this route and unsafe-mode rejection only; it does not inspect a real auth file.
- `approval.requested` SSE events carry an explicit 0.144.3 schema-derived approval matrix. Every authority-bearing field is either copied losslessly into the authenticated owner descriptor or makes Allow impossible. This includes an exact command working directory, command environments and managed-network host/protocol, exact add/delete content, exact update `unified_diff` and `move_path`, and the exact bounded permission profile plus this owner's fixed turn/strict-review response scope. The v2 file-change callback is deny-only because its approved `FileChangeThreadItem.changes` are not present in the callback. Invalid permission profiles cannot be approved; a valid Allow returns exactly the validated in-flight profile, while Deny and Timeout return distinct fail-closed schema-valid responses.
- Controlled cancellation now records execution interruption rather than completion before initialize/admission or before an irreversible write. A tracked JSONL flush boundary turns a control race after `thread/start` or `turn/start` delivery into a `delivery_ambiguous` durable incident and Unknown owner outcome; no synthetic interrupt is sent without both real identifiers. Deterministic fixture barriers cover all four phases without sleeps.

## Cycle 13 gates (code SHA above)

- `cargo fmt --all -- --check` — exit 0.
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-13 CARGO_NET_OFFLINE=true cargo test --locked --all-targets --all-features` — exit 0 (68 tests).
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-13 CARGO_NET_OFFLINE=true cargo clippy --locked --all-targets --all-features -- -D warnings` — exit 0.
- `git diff --check` — exit 0.

## Residual non-gating risk

The live bootstrap is intentionally not exercised against real credentials or a real model in this repository. The selected-file provisioning path was tested only with fake canary data; operational deployment still needs a separately authorized live-account smoke test. The injected fixture cannot select the production live executable path. A forced process-group kill after a protocol acknowledgement/terminal timeout is deliberately treated as a conservative failed/unknown operational boundary, never as a successful model result.

## Cycle 14 offline correction timeline (superseded)

Code SHA: `a337d10450bb767a621003c61f4a7d91591c4781` (`fix(cp6): close auth approval and cancellation gaps`). This section records deterministic local verification only; no OAuth credential value, account request, network request, or model turn was performed.

- The live launcher still accepts only the explicit owner-only `SPARK_RUNNER_SUBSCRIPTION_AUTH_FILE` capability and provisions it opaquely into the fresh `0700` `CODEX_HOME` as `auth.json` with `0600` permissions. Every spawned-flow epilogue now reaps the process, explicitly unlinks that child auth copy, removes the private home, and closes an owned journal before replying. Fake-canary tests cover provisioning, unsafe-source rejection, and cleanup; they do not read a real auth file.
- The descriptors were bounded and redacted, but this cycle did not yet provide a lossless review of every allowed permission field or preserve repeated whitespace. Cycle 15 resolves those remaining approval-review gaps.
- `thread/start` and `turn/start` writes were tracked from their first attempt, but the select blocks did not prioritize a control command already queued before initialize, admission, thread start, or turn start. Cycle 15 resolves that ordering gap.
- Cancellation approval delivery was held at its wire acknowledgement, but `turn/interrupt` did not yet track its own first write attempt. Cycle 15 resolves the timeout-after-write ambiguity classification.

## Cycle 14 reported gates (timings withdrawn)

The cycle-14 duration values conflict across its records and are not relied upon as timing evidence. The exact measured replacement run is recorded below.

## Cycle 15 offline remediation and exact measurement

Code SHA: `45294513b8faca38f71dba2780c5699bf789e37c` (`fix(cp6): fail closed at approval and control boundaries`). This code-only commit was verified using fake fixtures only; no OAuth, account API, credential value, network request, or model turn was used.

- Permission approval descriptors carry the exact bounded validated profile that Allow returns, including `fileSystem.globScanMaxDepth`; an Allow is denied if text would be redacted, truncated, or whitespace-normalized. Array command arguments are kept as separate arguments rather than joined.
- Queued control has deterministic priority before initialize, admission, `thread/start`, and `turn/start`. A pre-write queued-control regression proves the initialize request is never written.
- `turn/interrupt` is tracked from its first write attempt. The timeout-after-write fixture proves it journals `delivery_ambiguous` and leaves the execution unresolved rather than recording `TurnDeadlineExceeded`/failed completion.
- The JSONL unterminated-frame limit remains fail-closed even if the writer closes at the limit boundary, and the existing owner command queue now boxes only its pending-approval payload to retain warning-denied Clippy compatibility.

## Cycle 15 serial gates (code SHA above)

These durations are outer measurements from this exact serial run, rounded to milliseconds except the near-zero diff check.

- `cargo fmt --all -- --check` — exit 0; duration 0.043s.
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-15 CARGO_NET_OFFLINE=true cargo test --locked --all-targets --all-features` — exit 0; duration 26.703s; 73 tests.
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-15 CARGO_NET_OFFLINE=true cargo clippy --locked --all-targets --all-features -- -D warnings` — exit 0; duration 0.149s.
- `git diff --check` — exit 0; duration 0.000013s.

## Cycle 16 offline approval remediation and exact measurement

This record is limited to the local offline run described below; it does not
assert live app-server, account, credential, network, or model verification.

- `applyPatchApproval` descriptors exposed an update's exact `move_path`.
  A destination that would be redacted, truncated, or whitespace-normalized
  was deny-only. This cycle did **not** yet expose required add/delete content
  or update `unified_diff`; cycle 17 corrects that incomplete approval review.
- The runtime owner computes the complete `approval.requested` SSE event
  before adding its decision sender to the actionable approval map. If that
  exact event exceeds the replay-event limit, it sends an original-ID Deny
  and creates no invisible approvable record. The deterministic multibyte
  argv regression covers the whole-event boundary.
- Existing deterministic queued-control and after-write interrupt regressions
  also passed in this run; they retain pre-write control priority and durable
  `delivery_ambiguous` classification respectively.

## Cycle 16 serial gate measurements

Durations are command-runner wall measurements from this exact serial run,
rounded to one decimal second. The final diff check is reported in the cycle
handoff JSON after it completes.

- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-16 cargo fmt --all -- --check` — exit 0; duration 0.2s.
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-16 CARGO_NET_OFFLINE=true cargo test --locked --all-targets --all-features` — exit 0; duration 27.4s; 74 tests.
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-16 CARGO_NET_OFFLINE=true cargo clippy --locked --all-targets --all-features -- -D warnings` — exit 0; duration 2.7s.

## Cycle 17 schema-wide approval remediation

This cycle is limited to deterministic local/offline code and fake fixtures;
it does not claim live app-server, account, credential, OAuth, network, or
model verification.

This remediation remained incomplete: `item/commandExecution/requestApproval`
could still Allow when its working directory was absent, and
`item/fileChange/requestApproval` could still Allow a separately delivered
file-change payload that the descriptor did not contain. Cycle 18 corrects
both gaps; the historical cycle-17 gate counts below do not establish that
the original claim was complete.

- `src/client.rs` now declares a 0.144.3-derived matrix for every supported
  request variant: command execution, legacy exec, apply-patch, file-change,
  and permissions. The matrix identifies descriptor fields, persistent
  proposals that this owner rejects rather than silently broadening, and
  correlation/best-effort-only fields.
- Add/delete `content` and update `unified_diff` are copied byte-for-byte into
  the authenticated descriptor. Every accepted `FileChange` uses only its
  schema variant's keys, so an extension cannot be silently omitted. The
  aggregate patch-copy budget is 8 KiB and the pre-registration final SSE
  event admission remains the authoritative size check; either limit makes
  Allow impossible before a pending approval is actionable.
- The table-driven regression covers all five variants, command environment
  and network context, argv separation, patch add/delete/update/move fields,
  permission special paths and `globScanMaxDepth`, fixed permission grant
  scope, persistent proposals, malformed/oversized patches, and two distinct
  update payloads that must serialize to distinct allowable descriptors.
- Existing move-path, queued-control, event-size admission, and interrupt
  delivery-ambiguity corrections remain covered by the full suite.

## Cycle 17 serial gate measurements

- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-17 cargo fmt --all -- --check` — exit 0; duration 0.6s.
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-17 CARGO_NET_OFFLINE=true cargo test --locked --all-targets --all-features` — exit 0; duration 28.7s; 75 tests.
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-17 CARGO_NET_OFFLINE=true cargo clippy --locked --all-targets --all-features -- -D warnings` — exit 0; duration 8.7s.
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-17 git diff --check` — exit 0; duration 0.1s.

## Cycle 18 exhaustive approval-matrix remediation

This cycle is limited to deterministic local/offline code and fake fixtures;
it does not claim live app-server, account, credential, OAuth, network, or
model verification.

- The matrix now enumerates every declared top-level 0.144.3 field for all
  five supported variants. An unclassified top-level extension is deny-only,
  even when a generated schema is forward-compatible. The table-driven
  regression asserts that field list and whether an exact Allow review is
  supported for each variant.
- Command-execution approvals require an exact `cwd`, matching legacy exec
  and permissions approval. `item/fileChange/requestApproval` is permanently
  deny-only in this owner because the accepted response grants correlated
  `FileChangeThreadItem.changes` that the request does not provide; the owner
  does not attempt to reconstruct or infer those bytes.
- `applyPatchApproval` keeps add/delete `content` and update `unified_diff`
  byte-for-byte in the descriptor, including the update move destination. Two
  distinct update payloads serialize to distinct allowable descriptors. A
  malformed, extended, source-budget-exceeded, or final-event-over-budget
  patch is denied before an actionable approval record exists. The latter two
  checks use the existing 8 KiB patch-copy and final 16 KiB event-budget
  paths; no digest stands in for reviewable patch bytes.
- Existing move-path, queued-control priority, event-size admission, and
  interrupt delivery-ambiguity regressions remain in the offline suite.

## Cycle 18 serial gate measurements

- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-18 cargo fmt --all -- --check` — exit 0; duration 0.19s.
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-18 CARGO_NET_OFFLINE=true cargo test --locked --all-targets --all-features` — exit 0; duration 29.55s; 76 tests.
- `CARGO_TARGET_DIR=/home/uap/swarm-out/spark-runner-cp6-multiagent-20260713T123358Z/target-author-cycle-18 CARGO_NET_OFFLINE=true cargo clippy --locked --all-targets --all-features -- -D warnings` — exit 0; duration 3.61s.
- `git diff --check` — exit 0; duration 0.00s.
