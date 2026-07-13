# CP6 — local HTTP/SSE adapter

Status: **partial/active**. The locked offline test and clippy gates are required before this checkpoint can be marked green. Evidence records only local deterministic verification; no live model turn is performed by this checkpoint.

Current controls include loopback-only binding, mandatory bearer authentication, bounded request bodies and SSE replay, a compact API rejection type, initialized handshake notification, byte-bounded JSONL and stderr retention, curated child environment, live-turn admission checks, and append-only journal events with independently expiring captures.

Cycle-two remediation additionally verifies the actual executable/schema bytes and platform before live spawn, rejects stale terminal transitions, requires an authenticated API approval before the offline fixture proceeds, cancels the owned task on interrupt, migrates legacy expiring captures, and projects recovery before work is admitted. CP6 remains partial until a live pinned runtime is available and the HTTP adapter is fully backed by its runtime owner.

Cycle-five pre-commit evidence (commit SHA: `PENDING_COMMIT_SHA`):

- `cargo fmt --all -- --check` — exit 0.
- `CARGO_NET_OFFLINE=true cargo test --locked --all-targets --all-features` — exit 0; 43 deterministic offline tests passed.
- `CARGO_NET_OFFLINE=true cargo clippy --locked --all-targets --all-features -- -D warnings` — exit 0.
- `git diff --check` — exit 0.

This cycle parses the generated 0.144.3 account (`account.type`), rate-limit (`rateLimits` / `rateLimitsByLimitId` with `usedPercent`), and turn (`turn.id`) envelopes; updates the offline fixture to those shapes; rejects runtime/schema symlink indirection; dispatches interleaved JSON-RPC server requests with string or signed-i64 IDs; and verifies global/per-turn SSE byte accounting under eviction. The interleaved-request test verifies `-32601` for an unknown request and a schema-valid deny for a known approval request.

Remaining risks are intentionally not marked closed: the HTTP adapter still needs a command-channel owner that retains a live `CodexClient` across API commands, and interrupt/disconnect paths need end-to-end protocol-delivery, journal, and process-exit assertions. No live OAuth, network access, credentials, or model turn was used for this evidence.
