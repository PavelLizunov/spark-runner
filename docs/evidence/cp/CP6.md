# CP6 — local HTTP/SSE adapter

Status: **partial**. The offline remediation was accepted, but controlled live UAT remains pending.

## Accepted remediation

- Accepted code SHA: `ad2952cdf3e0ad1a4921c2d6fd64925e10eb7c7e`.
- PR: [#6](https://github.com/PavelLizunov/spark-runner/pull/6), squash-merged as `072b777b290a2dddc7c38009de438c4173db99b2`.
- GitHub Actions: [29281701984](https://github.com/PavelLizunov/spark-runner/actions/runs/29281701984) and [29281705056](https://github.com/PavelLizunov/spark-runner/actions/runs/29281705056) completed successfully.

## Offline evidence

The accepted remediation's deterministic fake-fixture suite passed 76 tests. It exercised fail-closed approval and cancellation handling, journal recovery, bounded SSE/event retention, and child-process cleanup. These checks used no OAuth credential value, account request, network request, live app-server, or model turn.

## Residual live risk

Controlled UAT of the real live bootstrap, authenticated account/model admission, and operational process behavior has not been performed. The selected subscription-auth-file path has only fake-canary coverage. CP6 is therefore not complete, and CP7 remains pending.
