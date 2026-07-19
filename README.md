# spark-runner

A Rust runner for the pinned Codex Spark app-server protocol, with a loopback HTTP/SSE adapter and fail-closed runtime controls.

## Status

CP6 offline remediation was accepted at `ad2952cdf3e0ad1a4921c2d6fd64925e10eb7c7e` and squash-merged by PR [#6](https://github.com/PavelLizunov/spark-runner/pull/6) as `072b777b290a2dddc7c38009de438c4173db99b2`. GitHub Actions runs [29281701984](https://github.com/PavelLizunov/spark-runner/actions/runs/29281701984) and [29281705056](https://github.com/PavelLizunov/spark-runner/actions/runs/29281705056) passed with 76 offline tests. The current tree passes 79 offline tests locally; CI validation is pending. Controlled live UAT remains pending; CP7 has not started.

## Safe offline checks

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
cargo build --locked --release
```

## Service integration

Linux only. Configure opaque aliases for each service workspace; requests never accept filesystem paths:

```sh
export SPARK_RUNNER_BEARER_TOKEN_FILE=/run/secrets/spark-runner-token
export SPARK_RUNNER_WORKSPACES='billing=/srv/billing,search=/srv/search'
export SPARK_RUNNER_BIND=127.0.0.1:8787
./spark-runner serve --live
```

Minimal lifecycle for one service:

```sh
base=http://127.0.0.1:8787
token=$(cat "$SPARK_RUNNER_BEARER_TOKEN_FILE")
auth="Authorization: Bearer $token"

curl -H "$auth" "$base/ready"
curl -H "$auth" -H 'Content-Type: application/json' \
  -d '{"workspace_alias":"billing"}' "$base/v1/threads"
curl -H "$auth" -H 'Content-Type: application/json' \
  -d '{"workspace_alias":"billing","input":"inspect the current service","timeout_seconds":180}' \
  "$base/v1/threads/thread_1/turns"
curl -N -H "$auth" "$base/v1/turns/turn_1/events"
curl -X DELETE -H "$auth" "$base/v1/threads/thread_1"
```

Keep the returned thread/turn IDs; do not construct them. Reconnect SSE with `Last-Event-ID`. A non-observer SSE connection owns cancellation on disconnect; monitoring clients must send `X-Spark-Runner-Observer: 1`. Delete an idle thread when the integrating service no longer needs it so bounded capacity is released.

Approval events are resolved with `POST /v1/approvals/{id}/approve` or `/deny`. Treat network timeouts as unknown outcomes and query `GET /v1/turns/{id}` before retrying a turn.

## Live boundary

Do not run live paths, real app-server/model/account turns, or credential-dependent commands without separate authorization on `uap-build-1`. CI and the offline checks use deterministic fake fixtures only.

## Mission records

- [Mission](MISSION.md)
- [Progress](PROGRESS.json)
- [Evidence index](docs/evidence/run.json)
- [CP6 evidence](docs/evidence/cp/CP6.md)
