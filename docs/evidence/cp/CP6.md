# CP6 — local HTTP/SSE adapter

Status: **partial/active**. This evidence covers deterministic offline proof only; no live OAuth, network request, credential, or model turn was performed.

Current controls include loopback-only binding, mandatory bearer authentication, bounded request bodies and SSE replay, a compact API rejection type, initialized handshake notification, byte-bounded JSONL and stderr retention, curated child environment, live-turn admission checks, and append-only journal events with independently expiring captures.

Cycle-two remediation additionally verifies the actual executable/schema bytes and platform before live spawn, rejects stale terminal transitions, requires an authenticated API approval before the offline fixture proceeds, cancels the owned task on interrupt, migrates legacy expiring captures, and projects recovery before work is admitted. CP6 remains partial until a live pinned runtime is available and the HTTP adapter is fully backed by its runtime owner.

Cycle-six executable remediation commit: `f12798dab310f6faf099a51b84695b0f4484322c`.

- `cargo fmt --all -- --check` — exit 0.
- `CARGO_NET_OFFLINE=true cargo test --locked --all-targets --all-features` — exit 0; deterministic offline suite passed.
- `CARGO_NET_OFFLINE=true cargo clippy --locked --all-targets --all-features -- -D warnings` — exit 0.
- `git diff --check` — exit 0.

This cycle parses the generated 0.144.3 account (`account.type`), rate-limit (`rateLimits` / `rateLimitsByLimitId`, `rateLimitReachedType`, and `usedPercent`), and turn (`turn.id`) envelopes. The offline fixture emits those canonical shapes. Quota admission now rejects exhausted primary windows, reached types, depleted credits, and malformed snapshots. Interleaved unknown server requests preserve string/signed-i64 IDs, receive `-32601`, and force controlled degradation instead of a transport-level approval decision.

The HTTP adapter sends interrupt through the runtime execution command channel. The execution owns the real `CodexClient`, sends generated-schema-valid `turn/interrupt` with accepted `threadId` and `turnId`, appends an interrupted terminal journal record, reaps the process group, then acknowledges the API so admission is released only after cleanup. Timeout closure emits one fail-closed approval decision and one terminal turn event; SSE aggregate eviction updates global and per-turn retained byte counts together.

Remaining risks: live startup admission is intentionally fail-closed until an authenticated pinned runtime session is supplied; this offline suite cannot prove live OAuth behavior. SSE controller-disconnect ownership is not separately exercised; terminal, explicit deny, timeout, and interrupt closure are covered. No live OAuth, network access, credentials, or model turn was used.
