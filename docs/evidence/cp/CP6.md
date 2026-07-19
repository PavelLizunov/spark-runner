# CP6 — local HTTP/SSE adapter

Status: **ready for CI**. The integration-ready candidate passed the complete offline gate and one controlled live Spark turn on `uap-build-1`; CP6 remains open until the candidate PR is green and merged.

## Accepted remediation

- Accepted code SHA: `ad2952cdf3e0ad1a4921c2d6fd64925e10eb7c7e`.
- PR: [#6](https://github.com/PavelLizunov/spark-runner/pull/6), squash-merged as `072b777b290a2dddc7c38009de438c4173db99b2`.
- GitHub Actions: [29281701984](https://github.com/PavelLizunov/spark-runner/actions/runs/29281701984) and [29281705056](https://github.com/PavelLizunov/spark-runner/actions/runs/29281705056) completed successfully.

## Offline evidence

The accepted remediation's deterministic fake-fixture suite passed 76 tests. It exercised fail-closed approval and cancellation handling, journal recovery, bounded SSE/event retention, and child-process cleanup. These checks used no OAuth credential value, account request, network request, live app-server, or model turn.

## Integration-ready candidate

- Live-tested code commit: `3e54de89acdd01dcffc5140da41360d5f0bf6281`.
- Build host: 8 logical CPUs, 11 GiB RAM with about 10 GiB available, and 53 GiB free disk; resource capacity is sufficient.
- Native build-host gates: formatting, clippy with warnings denied, 80 offline tests, and locked release build passed.
- The pinned Codex `0.144.6` native binary and regenerated checked-in schema matched their locked SHA-256 values.
- Subscription OAuth was selected explicitly from an owner-only regular file. No credential value was copied into the repository, output, evidence, or CI.

## Controlled live UAT

The initial conclusion that Spark entitlement was absent was incorrect: preview models are not reliably represented by the app-server catalog, and the owner confirmed both the model and its unused separate quota in the product UI. The runner uses `model/list` only as a protocol-health read; `thread/start` is the authoritative exact-model/no-fallback gate.

The `-32603` rate-limit failure was caused by the runner's intentionally cleared child environment also removing the required RU egress settings. The host shell already used the cluster VLESS gateway, but `codex app-server` did not inherit it. The runner now accepts only an explicit `SPARK_RUNNER_EGRESS_PROXY`, rejects credential-bearing or malformed URLs, and passes the validated endpoint to the child as standard HTTP(S) proxy variables. No UAP, VPNRouter, k3s, or routing configuration was changed.

The next admission failure, `quota_unavailable`, was a local interpretation bug: `credits.hasCredits=false` means there are no separately purchased credits, not that subscription usage is exhausted. Admission now relies on the explicit reached type and every advertised usage window; a reached type or `usedPercent >= 100` still fails closed. The observed snapshot had available General and separate Spark windows.

With `SPARK_RUNNER_EGRESS_PROXY=http://192.168.0.202:30880`, the release binary completed `doctor --live` using exact model `gpt-5.3-codex-spark`. Terminal status was `completed`, no fallback or approval occurred, and pre/post process checks were clean.

## Residual live risk

The one-process doctor path is now proven online. GitHub Actions for the candidate, merge, service-level HTTP/SSE integration against each real consumer, and the CP7 service/soak/release gates remain pending.
