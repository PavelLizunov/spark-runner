# CP6 — local HTTP/SSE adapter

Status: **blocked**. The integration-ready candidate passed offline gates, but controlled live admission showed that the required model is unavailable to the authenticated build-host account.

## Accepted remediation

- Accepted code SHA: `ad2952cdf3e0ad1a4921c2d6fd64925e10eb7c7e`.
- PR: [#6](https://github.com/PavelLizunov/spark-runner/pull/6), squash-merged as `072b777b290a2dddc7c38009de438c4173db99b2`.
- GitHub Actions: [29281701984](https://github.com/PavelLizunov/spark-runner/actions/runs/29281701984) and [29281705056](https://github.com/PavelLizunov/spark-runner/actions/runs/29281705056) completed successfully.

## Offline evidence

The accepted remediation's deterministic fake-fixture suite passed 76 tests. It exercised fail-closed approval and cancellation handling, journal recovery, bounded SSE/event retention, and child-process cleanup. These checks used no OAuth credential value, account request, network request, live app-server, or model turn.

## Integration-ready candidate

- Candidate commit: `0d32fbd0ef2100a8cae05d7f310370bc8c01e218`.
- Build host: 8 logical CPUs, 11 GiB RAM with about 10 GiB available, and 53 GiB free disk; resource capacity is sufficient.
- Native build-host gates: formatting, clippy with warnings denied, 80 offline tests, and locked release build passed.
- The pinned Codex `0.144.3` native binary and checked-in schema matched their locked SHA-256 values.
- Subscription OAuth was selected explicitly from an owner-only regular file. No credential value was copied into the repository, output, evidence, or CI.

## Controlled live UAT

The live doctor reached authenticated model admission on `uap-build-1`, then failed closed with `required_model_unavailable`: `gpt-5.3-codex-spark` was absent from `model/list`. The same admission blocker was confirmed three times after correcting the initially omitted explicit auth-file selector. No `thread/start`, model turn, fallback model, or approval occurred.

Per `MISSION.md`, another model must not be substituted. The next action is an owner decision or restoration of the exact Spark model entitlement, followed by one controlled rerun.

## Residual live risk

Authenticated bootstrap and model admission were exercised, but a real thread/turn and operational service behavior could not be tested because admission failed before `thread/start`. CP6 is not complete, and CP7 remains pending.
