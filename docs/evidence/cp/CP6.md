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

## API-product pre-delivery candidate

At `2026-08-15T18:47:49Z`, a new candidate was prepared from `main` SHA `397aacd2cd4b350f59cbdffefae178eb4fd096bf` under the corrected API-product mission. It adds the canonical `/api/v1/spark` sessions/runs/SSE contract and OpenAPI JSON, explicit session cleanup, bounded journal history, workspace-to-directory mapping, timeout propagation, runtime re-admission, safe explicit egress, deterministic JSON request errors, graceful signal shutdown, a Linux-only compilation boundary, container instructions, and ninitux reverse-proxy/authentication guidance.

PR #8 was used as an audit input for the blockers that remain applicable to current code. Draft PR #9 diverges from current `main`; its relevant runtime ideas were selectively reimplemented, while its obsolete protocol pin and delivery-state claims were not imported.

The local deterministic gate passed formatting, locked clippy with warnings denied, 81 locked offline tests across all targets/features, and locked release build. No OAuth material, account request, network request, live app-server, or model turn was used. These are pre-delivery local results only: authoritative `uap-build-1` checks, independent review, PR, required CI, merge, fresh-main verification, cleanup, live UAT, and soak remain outstanding.
