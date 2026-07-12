# CP1: Phase 0 bootstrap evidence — GREEN

Status: **green**. Proceed to CP2. No fallback model was used at any point.

## Environment

- Codex CLI version: `0.142.0`

## Raw `codex app-server --listen stdio://` evidence

- `initialize` ok: `userAgent` `spark-runner-cp1/0.142.0 (Ubuntu 22.4.0; x86_64) unknown (spark-runner-cp1; 0.0.0)`,
  `platformFamily` `unix`, `platformOs` `linux`.
- `account/read` ok: `planType` `pro`, account type `chatgpt`, `requiresOpenaiAuth` `true`.
- `model/list` ok, exact model listed: `gpt-5.3-codex-spark`.
- `account/rateLimits/read` ok, Spark rate limit present; rate limit ids are redacted, count `2`.
- `thread/start` ok: `approvalPolicy` `on-request`, `approvalsReviewer` `user`, `model` `gpt-5.3-codex-spark`,
  `modelProvider` `openai`, status `idle`.
- `turn/start` ok; terminal event `turn/completed` observed with status `completed`.

### Raw app-server run summary

| Field | Value |
| --- | --- |
| duration_s | 8.02 |
| event_count | 30 |
| stderr_hashes | [`c70ce0c2a7ec`] |
| stderr_hash_count | 1 |
| ok | true |

### Incident and recovery

The first raw live turn attempt failed with JSON-RPC `-32600` because the sandbox field was encoded as a map
where the stable schema expected a single-key map/string shape. The strategy was changed to use the stable
thread-level `read-only` sandbox shape, and the next attempt passed cleanly.

## Official Python SDK oracle evidence

- Package: `openai-codex`.
- Public APIs used: `CodexConfig`, `Codex`, `account`, `models`, generated rate-limit read, `thread_start`,
  `Thread.run`.

### SDK oracle green summary

| Field | Value |
| --- | --- |
| account_plan | pro |
| account_type | chatgpt |
| duration_ms | 2331 |
| exact_model_listed | true |
| final_response_sha256 | d1227f1a79010d8247c552cdc4a9691be976a53e130bc6f6fb6b13db59fa2383 |
| ok | true |
| requires_openai_auth | true |
| spark_rate_limit_present | true |
| turn_status | completed |
| usage_present | true |
| wall_duration_s | 3.713 |

### Incident and recovery

The initial SDK summary attempt failed only because enum objects returned by the SDK were not JSON
serializable. The strategy was changed to normalize enum values (using their string value) before
serialization, and the rerun passed.

## Gate decision

CP1 is **green**. Proceed to CP2. No fallback model was used.
