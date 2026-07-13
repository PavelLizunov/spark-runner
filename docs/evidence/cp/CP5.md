# CP5 - Journal and restart recovery

Status: **partial**. Startup persistence and legacy-capture migration are validated by the current CP6 remediation gates.

Completed at: `2026-07-13T05:34:19Z`

## Scope

Implemented deterministic append-only SQLite journaling and restart projection for the single-worker runner:

- Typed lifecycle events for executions, turns, approvals, incidents, and rate-limit snapshots.
- One owned journal writer task with WAL-backed SQLite persistence.
- Explicit opt-in TTL storage for terminal output and raw captures.
- Payload redaction before persistence, while preserving internal approval `request_key` identifiers required for recovery projection.
- Restart projection marks unterminated executions as `UnknownAfterRestart` and unresolved approvals as `DeniedOnRestart`.
- Recovery projection never replays turns or approvals.

## Deterministic fake-server tests

No live model run was performed.

Commands run on build-1:

```text
cargo test --locked --test cp5_journal_recovery
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --locked
```

Observed test result summary:

```text
CP5 journal recovery integration tests: 4 passed
library unit tests: 11 passed
app_server integration tests: 2 passed
CP3 resilience tests: 5 passed
CP4 approvals tests: 5 passed
Doc-tests: 0 passed
```

Focused CP5 coverage:

- `restart_projection_marks_unknown_and_denied_without_replay`
- `terminal_states_survive_projection`
- `redacts_before_persistence_and_raw_capture_requires_ttl_opt_in`
- `opt_in_capture_is_redacted_and_pruned_by_ttl`
- `journal::tests::redacts_sensitive_payload_before_serialization`, including regression coverage that `request_key` is preserved while real API key fields are redacted.

## Sanitization

Evidence records only command classes, pass/fail counts, journal state classes, and sanitized status strings. It does not include OAuth data, prompts, raw model output, raw captures, terminal transcripts, private tokens, or live app-server output.
