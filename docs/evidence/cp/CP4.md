# CP4 - Stage 3 lifecycle and approvals

Status: **partial**. Follow-up CP6 work is required for externally brokered approval and interrupt authority.

Completed at: `2026-07-13T04:10:18Z`

## Scope

Implemented only Stage 3 lifecycle and approvals:

- `WorkerState` and expanded `TurnState` are owned by the single `CodexClient` task.
- Invalid, reverse, and terminal state transitions poison the session and fail closed.
- Server-initiated approval requests are distinguished from stray responses in the JSONL layer.
- Only known app-server approval request methods are handled.
- Default approval policy denies; owner-origin allow exists only for deterministic tests.
- Model-origin self-approval is rejected by state.
- Duplicate approval ids poison the session.
- A desync after an approval boundary blocks restart and fails closed.
- Internal lifecycle/approval events carry monotonic sequence ids.
- Active-turn denial interrupts the turn and yields terminal failed status in the fake-server path.

## Deterministic fake-server tests

No live model run was performed.

Commands run separately on build-1:

```text
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test
```

Observed test result summary:

```text
state/jsonl unit tests: 10 passed
app_server integration tests: 2 passed
CP3 resilience tests: 5 passed
CP4 approvals tests: 5 passed
Doc-tests: 0 passed
```

CP4 coverage:

- allow: `owner_allow_approval_completes_turn`
- deny: `default_deny_approval_interrupts_and_fails_turn_closed`
- timeout/disconnect: `approval_disconnect_fails_closed_without_hanging`
- duplicate decision: `duplicate_approval_request_fails_closed`
- restart with unresolved approval boundary: `approval_boundary_blocks_restart_after_unresolved_desync`

## Sanitization

Evidence records only command classes, pass/fail counts, lifecycle decisions, and sanitized status strings. It does not include OAuth data, prompts, raw model output, private paths beyond repository-relative files, or live app-server output.
