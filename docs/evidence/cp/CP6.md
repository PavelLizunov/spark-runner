# CP6 — local HTTP/SSE adapter

Status: **blocked**. The integration-ready candidate passed offline gates, but the pinned app-server returns an internal error from its rate-limit read before a live thread can start.

## Accepted remediation

- Accepted code SHA: `ad2952cdf3e0ad1a4921c2d6fd64925e10eb7c7e`.
- PR: [#6](https://github.com/PavelLizunov/spark-runner/pull/6), squash-merged as `072b777b290a2dddc7c38009de438c4173db99b2`.
- GitHub Actions: [29281701984](https://github.com/PavelLizunov/spark-runner/actions/runs/29281701984) and [29281705056](https://github.com/PavelLizunov/spark-runner/actions/runs/29281705056) completed successfully.

## Offline evidence

The accepted remediation's deterministic fake-fixture suite passed 76 tests. It exercised fail-closed approval and cancellation handling, journal recovery, bounded SSE/event retention, and child-process cleanup. These checks used no OAuth credential value, account request, network request, live app-server, or model turn.

## Integration-ready candidate

- Candidate commit: `30efa5c441e7e015992db39a8ad42c989a00f397`.
- Build host: 8 logical CPUs, 11 GiB RAM with about 10 GiB available, and 53 GiB free disk; resource capacity is sufficient.
- Native build-host gates: formatting, clippy with warnings denied, 80 offline tests, and locked release build passed.
- The pinned Codex `0.144.3` native binary and checked-in schema matched their locked SHA-256 values.
- Subscription OAuth was selected explicitly from an owner-only regular file. No credential value was copied into the repository, output, evidence, or CI.

## Controlled live UAT

The initial conclusion that Spark entitlement was absent was incorrect: preview models are not reliably represented by the app-server catalog, and the owner confirmed both the model and its unused separate quota in the product UI. The runner now uses `model/list` only as a protocol-health read; the subsequent `thread/start` remains the authoritative exact-model/no-fallback gate.

Live diagnosis then exposed the actual blocker: pinned Codex `0.144.3` returns JSON-RPC internal error `-32603` from `account/rateLimits/read`, including after the request was corrected to the schema's parameterless shape. The current published patch is `0.144.6`. No `thread/start`, model turn, fallback model, or approval occurred.

Per `MISSION.md`, another model must not be substituted and the production quota gate must not be silently weakened. The next action requires an owner decision: update and repin Codex `0.144.6`, or authorize one diagnostic-only turn that bypasses the failing quota read.

## Residual live risk

Authenticated bootstrap, account read, and model-catalog RPC were exercised, but a real thread/turn and operational service behavior could not be tested because the rate-limit RPC failed before `thread/start`. CP6 is not complete, and CP7 remains pending.
