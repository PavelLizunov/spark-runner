# CP6 — local HTTP/SSE adapter

Status: **partial/active**. The locked offline test and clippy gates are required before this checkpoint can be marked green. Evidence records only local deterministic verification; no live model turn is performed by this checkpoint.

Current controls include loopback-only binding, mandatory bearer authentication, bounded request bodies and SSE replay, a compact API rejection type, initialized handshake notification, byte-bounded JSONL and stderr retention, curated child environment, live-turn admission checks, and append-only journal events with independently expiring captures.

Remaining CP6 work is tracked by the remediation plan: a single runtime owner for HTTP approval/interrupt authority and durable startup recovery must be complete before CP6 is green.
