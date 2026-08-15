# spark-runner

`spark-runner` is a Linux Rust service that exposes the pinned Codex Spark app-server through a small authenticated HTTP/SSE API. The production path accepts only `gpt-5.3-codex-spark`; a missing model, reroute, quota failure, ambiguous delivery, or approval failure closes the run without choosing a fallback.

The canonical application contract is [`/api/v1/spark`](openapi/spark-runner-v1.json). The older `/v1` routes remain available for existing local clients but are not the ninitux product surface.

## Status

This worktree contains a pre-delivery API-product candidate based on `main` at `397aacd2cd4b350f59cbdffefae178eb4fd096bf`. Deterministic offline checks can be run here. Live UAT, CI, merge, post-merge verification, deployment, and soak evidence remain delivery-flow work and are not claimed by this candidate.

## Build and run offline

The offline mode uses only the checked-in deterministic fake app-server. It performs no OAuth, account, network, or model request.

```sh
cargo build --locked --release
export SPARK_RUNNER_BEARER_TOKEN='local-development-token'
export SPARK_RUNNER_WORKSPACES="repo=$(pwd)"
./target/release/spark-runner serve
```

Liveness is public; every other endpoint requires the configured internal bearer token.

```sh
curl http://127.0.0.1:8787/api/v1/spark/health
curl -H 'Authorization: Bearer local-development-token' \
  http://127.0.0.1:8787/api/v1/spark/ready
```

Print the package version without starting app-server:

```sh
spark-runner --version
```

## Configuration

| Variable | Required | Behavior |
| --- | --- | --- |
| `SPARK_RUNNER_BIND` | No | Loopback address; defaults to `127.0.0.1:8787`. Non-loopback binds fail closed. |
| `SPARK_RUNNER_BEARER_TOKEN` | One token source | Internal bearer value. Prefer the file option outside development. |
| `SPARK_RUNNER_BEARER_TOKEN_FILE` | One token source | Owner-only regular file containing the internal bearer. Symlinks and group/world-readable files are rejected on Unix. |
| `SPARK_RUNNER_WORKSPACES` | Recommended | Comma-separated `alias=/absolute/directory` mappings. Requests accept aliases, never paths. With no value, `default` and `repo` map to the startup directory. |
| `SPARK_RUNNER_SUBSCRIPTION_AUTH_FILE` | Live only | Explicit owner-only subscription auth file. Ambient `HOME` and `CODEX_HOME` are never used as fallback credential sources. |
| `SPARK_RUNNER_EGRESS_PROXY` | No | Explicit credential-free `http://host:port` or `https://host:port` proxy passed only as child HTTP(S) proxy variables. Userinfo, paths, query strings, fragments, whitespace, and invalid ports are rejected. |
| `SPARK_RUNNER_JOURNAL_PATH` | No | SQLite lifecycle journal. If unset, journaling is disabled. |
| `SPARK_RUNNER_JOURNAL_MAX_EVENTS` | No | Rolling lifecycle-event bound; defaults to `100000`, allowed range `1..=1000000`. |
| `SPARK_RUNNER_TERMINAL_OUTPUT_TTL_SECS` | No | Opt-in TTL for redacted terminal captures. Core lifecycle records follow the event bound. |
| `SPARK_RUNNER_RAW_CAPTURE_TTL_SECS` | No | Opt-in TTL for redacted raw captures. |
| `RUST_LOG` | No | Rust tracing filter; defaults to `info`. |

`codex.lock` pins the native Codex version, native SHA-256, generated schema SHA-256, platform, transport, and exact model. Live startup re-verifies the binary inode and schema before every spawn.

## API lifecycle

Create an ephemeral session, submit one run, stream ordered events, inspect terminal status, and release the session:

```sh
base=http://127.0.0.1:8787/api/v1/spark
auth='Authorization: Bearer local-development-token'

session=$(curl -fsS -H "$auth" -H 'Content-Type: application/json' \
  -d '{"workspace_alias":"repo"}' "$base/sessions")
session_id=$(printf '%s' "$session" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

run=$(curl -fsS -H "$auth" -H 'Content-Type: application/json' \
  -d '{"input":"inspect the project","timeout_seconds":180}' \
  "$base/sessions/$session_id/runs")
run_id=$(printf '%s' "$run" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

curl -N -H "$auth" -H 'X-Spark-Runner-Observer: 1' "$base/runs/$run_id/events"
curl -fsS -H "$auth" "$base/runs/$run_id"
curl -fsS -X DELETE -H "$auth" "$base/sessions/$session_id"
```

Keep returned identifiers opaque; do not construct them. Reconnect SSE with `Last-Event-ID`. The first non-observer SSE connection is a controller and its disconnect cancels the run. Monitoring clients must send `X-Spark-Runner-Observer: 1`.

Cancel explicitly with `POST /runs/{run_id}/cancel`. Resolve an `approval.requested` event with `POST /approvals/{approval_id}/approve` or `/deny`. An approval can be allowed only when its exact bounded action is reviewable; all other approval paths deny.

Network timeouts around run submission or cancellation can have an unknown delivery outcome. Query `GET /runs/{run_id}` before deciding whether another action is safe. Never retry a run submission blindly.

### Errors

Contract handlers return a deterministic JSON envelope:

```json
{"error":{"code":"RUNTIME_NOT_READY","message":"runtime admission has not completed","retryable":true}}
```

Stable code families include `UNAUTHORIZED`; `INVALID_JSON`, `PAYLOAD_TOO_LARGE`, `INVALID_WORKSPACE`, `UNKNOWN_WORKSPACE`, `WORKSPACE_MISMATCH`, `CONTEXT_LIMIT`, and `TIMEOUT_LIMIT`; `RUNTIME_NOT_READY` and `JOURNAL_UNAVAILABLE`; `SATURATED`, `THREAD_CAPACITY`, `TURN_CAPACITY`, and `APPROVAL_CAPACITY`; `NOT_FOUND`, `SESSION_ACTIVE`, `TURN_TERMINAL`, and `TURN_CLOSED`; plus approval-delivery and interrupt-timeout errors. Use the HTTP status and `retryable` together. A retryable flag never makes non-idempotent run submission safe to repeat without first reconciling run state.

### Minimal application example

This dependency-free server-side JavaScript example creates a session and run from within the trusted upstream boundary. Browser and other public clients must authenticate to ninitux instead of receiving the internal token. Production applications should add their own durable ID storage and SSE parser.

```js
const base = "https://ninitux.com/api/v1/spark";
const headers = {
  Authorization: `Bearer ${process.env.SPARK_RUNNER_INTERNAL_TOKEN}`,
  "Content-Type": "application/json",
};

const session = await fetch(`${base}/sessions`, {
  method: "POST",
  headers,
  body: JSON.stringify({ workspace_alias: "repo" }),
}).then((response) => response.json());

const run = await fetch(`${base}/sessions/${session.id}/runs`, {
  method: "POST",
  headers,
  body: JSON.stringify({ input: "summarize the current change", timeout_seconds: 120 }),
}).then((response) => response.json());

console.log({ sessionId: session.id, runId: run.id, status: run.status });
```

## ninitux reverse proxy and authentication boundary

The runner is intentionally loopback-only and is not a public authentication service. The upstream ninitux gateway must authenticate and authorize the external caller, apply tenant/rate limits, remove the caller's `Authorization` header, and inject a separate internal runner bearer from a secret store. Public credentials must never be reused as the runner token.

For an nginx-style proxy, preserve the `/api/v1/spark/...` path and disable buffering for SSE. `${SPARK_RUNNER_INTERNAL_TOKEN}` below must be rendered from the runtime secret store; do not check a value into configuration.

```nginx
location /api/v1/spark/ {
    auth_request /_ninitux_auth;
    proxy_set_header Authorization "Bearer ${SPARK_RUNNER_INTERNAL_TOKEN}";
    proxy_set_header X-Forwarded-For "";
    proxy_pass http://127.0.0.1:8787;
    proxy_http_version 1.1;
    proxy_buffering off;
    proxy_cache off;
    proxy_read_timeout 360s;
}
```

Do not log request bodies, `Authorization`, SSE data, prompts, approval descriptors, or model content at the proxy. The runner itself emits bounded status classes and child stderr byte counts, not raw child diagnostics or model output.

## Container operation

The included `Dockerfile` builds only the runner. The pinned Codex native executable is deliberately not stored in this repository or image; mount the already verified artifact at the exact `native_path` recorded in `codex.lock`, read-only. Also mount separate token/auth secrets, workspace directories, and a journal directory.

Secret files must be readable by container uid `10001` and remain owner-only (`0600`); the runner rejects broader Unix permissions.

Because the service rejects non-loopback binds, the runner and reverse proxy must share a network namespace (for example, one Kubernetes pod or Docker `--network=container:<proxy-container>`). A normal bridged port publication cannot reach the runner's loopback socket.

Example shape, with deployment-specific source paths omitted:

```sh
docker build -t spark-runner:local .
docker run --rm --network=container:ninitux-proxy \
  --read-only --tmpfs /tmp:rw,noexec,nosuid,size=128m \
  -e SPARK_RUNNER_BEARER_TOKEN_FILE=/run/secrets/runner-token \
  -e SPARK_RUNNER_SUBSCRIPTION_AUTH_FILE=/run/secrets/codex-auth.json \
  -e SPARK_RUNNER_WORKSPACES=repo=/workspaces/repo \
  -e SPARK_RUNNER_JOURNAL_PATH=/var/lib/spark-runner/journal.sqlite3 \
  -v /host/runner-token:/run/secrets/runner-token:ro \
  -v /host/codex-auth.json:/run/secrets/codex-auth.json:ro \
  -v /host/repo:/workspaces/repo:ro \
  -v /host/journal:/var/lib/spark-runner:rw \
  -v /host/pinned-codex:/home/uap/.local/lib/node_modules/@openai/codex/node_modules/@openai/codex-linux-x64/vendor/x86_64-unknown-linux-musl/bin/codex:ro \
  spark-runner:local
```

`SIGTERM` and Ctrl-C stop admission, cancel the one active run through the bounded owner path, close the journal writer, reap the child process group, and then stop the listener.

## Bounds and cleanup

- One active run at a time.
- At most 128 retained sessions; delete idle sessions to release capacity.
- At most 256 retained runs and approvals; terminal run records are pruned before capacity is refused.
- At most 256 replay events per run, 16 KiB per event, and 1 MiB total replay memory.
- Request bodies are at most 16 KiB; input is at most 8192 Unicode scalar values; run timeout is `1..=300` seconds.
- The JSONL wait budget is derived from the accepted run timeout plus bounded cancellation grace.
- The SQLite lifecycle journal defaults to at most 100000 events; capture rows have separate opt-in TTLs.

## Verification

Deterministic offline gates use fake fixtures and contain no OAuth material:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
cargo build --locked --release
```

Live `doctor --live` and HTTP/SSE UAT must run only through the configured `uap-build-1` delivery path with authorized subscription auth and egress. A missing credential or egress path is a blocked live result, not permission to add a model fallback or put credentials in CI.

## Mission records

- [Mission](MISSION.md)
- [Progress](PROGRESS.json)
- [Evidence index](docs/evidence/run.json)
- [CP6 evidence](docs/evidence/cp/CP6.md)
