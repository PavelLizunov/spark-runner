# CP6 — local HTTP/SSE adapter

Status: **partial/active**. All evidence below is deterministic and offline: no OAuth, network request, credential value, or model turn was used.

Cycle-seven code/tree evidence is the commit created from this tree; it is intentionally not stated here because a markdown file cannot contain its own final commit SHA. Reviewer/release SHA remains external verification.

## Executable coverage

- T01: interrupt path sends `turn/interrupt` with accepted identifiers, checks delivery, cleans up before acknowledgement, and has a terminal-once HTTP regression.
- T02/T03: genuine approval decisions and timeout remain fail-closed; T03 does not yet exercise a dropped controlling SSE lease.
- T04: unadmitted live mode rejects before the offline launcher can run.
- T05/T06: post-delivery ambiguity is not replayed; oversized newline-free frames fail closed.
- T07: executable long-line and many-line stderr fixture proves byte retention is capped and diagnostics disclose only counts.
- T08: executable ambient-secret canary proves curated child environment and 0700 private `CODEX_HOME`.
- T09: executable native/schema/platform/version/symlink mismatch checks fail before live spawn.
- T10: executable exhausted quota and post-admission model reroute fixtures fail closed.
- T11: strict `initialize` then `initialized`, signed-i64 interleaved approval delegation, and unknown string-ID `-32601` response are covered. ChatGPT token refresh absence returns a deterministic bounded error without reading or persisting secret values.
- T12: durable, idempotent startup recovery remains covered.

## Cycle-seven gates

- `CARGO_NET_OFFLINE=true cargo test --locked --all-targets --all-features` — exit 0, 22.102s (52 tests).
- `CARGO_NET_OFFLINE=true cargo clippy --locked --all-targets --all-features -- -D warnings` — exit 0, 1.343s.
- `cargo fmt --all -- --check` and `git diff --check` are recorded with the final tree verification.

## Remaining risk

The HTTP adapter still has local projection state rather than a fully persistent actor command loop, live admission is fail-closed until a real authenticated session is supplied, and controlling-SSE disconnect ownership lacks the required executable lease/drop test. No claim of live OAuth or production model execution is made.
