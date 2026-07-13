# CP6 — local HTTP/SSE adapter

Status: **partial/active**. All evidence below is deterministic and offline: no OAuth, network request, credential value, or model turn was used.

Code SHA evidence: `2812d89f6682275a5d94e2effedb1647c6a1e9f1` (`fix: acknowledge CP6 approval delivery`). Tree evidence: this markdown is committed separately so it cannot self-report its final tree SHA; reviewer/release SHA remains external verification.

## Executable coverage

- T01: interrupt path sends `turn/interrupt` with accepted identifiers, checks delivery, cleans up before acknowledgement, and has a terminal-once HTTP regression.
- T02/T03: genuine approval decisions and timeout remain fail-closed. The authenticated decision is recorded by the adapter only after the owner reports that it flushed the original JSON-RPC response. T03 does not yet exercise a dropped controlling SSE lease.
- T04: unadmitted live mode rejects before the offline launcher can run.
- T05/T06: post-delivery ambiguity is not replayed; oversized newline-free frames fail closed.
- T07: executable long-line and many-line stderr fixture proves byte retention is capped and diagnostics disclose only counts.
- T08: executable ambient-secret canary proves curated child environment and 0700 private `CODEX_HOME`.
- T09: executable native/schema/platform/version/symlink mismatch checks fail before live spawn.
- T10: executable exhausted quota and post-admission model reroute fixtures fail closed.
- T11: strict `initialize` then `initialized`, signed-i64 interleaved approval delegation, and unknown string-ID `-32601` response are covered. Interleaved and terminal request dispatch share duplicate tracking. The generated ChatGPT token-refresh shape has an explicit bounded owner provider; its default absence returns a deterministic error without reading, logging, or persisting secret values.
- T12: durable, idempotent startup recovery remains covered.

## Cycle-eight author gates

- `cargo fmt --all -- --check` — exit 0, 0.10s.
- `CARGO_NET_OFFLINE=true cargo test --locked --all-targets --all-features` — exit 0, 20.51s (52 tests).
- `CARGO_NET_OFFLINE=true cargo clippy --locked --all-targets --all-features -- -D warnings` — exit 0, 0.14s.
- `git diff --check` — exit 0, 0.00s.

## Remaining risk

The HTTP adapter still has local projection state rather than a fully persistent actor command loop, live admission is fail-closed until a real authenticated session is supplied, and controlling-SSE disconnect ownership lacks the required executable lease/drop test. No claim of live OAuth or production model execution is made.
