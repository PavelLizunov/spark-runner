# Hermes mission: Spark Runner full-cycle foundation

## Owner decisions required before START

- Repository: `PavelLizunov/spark-runner`
- Visibility: `public`
- License: `Apache-2.0`
- Build host: `uap-build-1`
- Independent deterministic test host: GitHub Actions `ubuntu-latest`
- Live OAuth/model tests: `uap-build-1` only; never copy auth into CI

Do not start repository creation until `codex login status`
on build-1 is authenticated. Never silently substitute another model for `gpt-5.3-codex-spark`.

## Objective

Execute `IMPLEMENTATION-PLAN.md` from Phase 0 through CP7 and leave a release-ready Rust repository.
One owner command starts the mission, but this is a durable multi-session run, not one LLM turn. Use
repository checkpoints and Hermes Kanban so daily reset, compaction, worker crash, or CI wait cannot lose state.

## Sources of truth

Read the entire intake bundle before implementation:

1. `IMPLEMENTATION-PLAN.md`
2. `README.md`
3. `01-executive-summary.md` through `09-bake-off-checklist.md`
4. every file under `adrs/` and `reference/`
5. `08-source-license-matrix.csv`

The plan and accepted ADRs are closed. If live Phase 0 evidence contradicts them, stop at CP1, record the
evidence, and request an owner decision. Do not redesign silently.

## Execution contract

> **Note:** this repository currently uses the GitHub default branch `main`, not `master`. Branch protection
> requirements below apply to whichever branch GitHub reports as the default branch — currently `main` — even
> though the imported mission text below refers to it as `master`.

1. Work only on `uap-build-1` under `~/projects/spark-runner` and disposable GitHub Actions runners.
2. Do not modify unified-agent-platform, k3s, Proxmox, VPN routing, Windows/Qwen, Mac/Ornith, or their secrets.
3. Use subscription OAuth only. Never introduce paid API keys or copy Codex auth into git, CI, artifacts,
   prompts, traces, or test fixtures.
4. Create one Rust binary crate first. Follow Ponytail/YAGNI from the implementation plan: no speculative
   workspace, adapters, database, or HTTP layer before its gate requires it.
5. Every coding worker uses its own git worktree. One checkpoint/PR at a time unless file ownership is
   disjoint and explicitly recorded.
6. Protected `master`: PR plus required green CI; no direct push, bypass, force-push, or disabled checks.
7. Each CP must finish in a terminal state: checks green, squash-merged, master confirmed, branch/worktree
   removed, and handoff files updated.
8. On gate failure, attempt bounded diagnosis and one root-cause fix. If the same blocker repeats three
   times or needs owner credentials/architecture, stop safely and report it. Never weaken a gate.

## Phase 0 bootstrap

Before Rust implementation:

- install/verify official Codex CLI, rustfmt and clippy on build-1;
- pin Codex version and SHA-256;
- verify `codex app-server --listen stdio://`, stable schema generation, account/model/rate-limit reads;
- run the official Python SDK oracle with an ephemeral read-only prompt;
- prove the exact `gpt-5.3-codex-spark` model, auth route, and terminal event;
- store only normalized/redacted protocol evidence.

If exact Spark is absent, auth is unavailable, schema generation fails, or the server substitutes a model,
CP1 fails and implementation stops. Produce a factual blocker report; do not fall back to Luna, Qwen, Claude,
another Codex model, `codex exec --json`, or an OpenAI-compatible proxy.

## Checkpoint loop

For CP1 through CP7:

1. Read current `PROGRESS.json` and the relevant plan section.
2. Create/update Kanban tasks with dependencies and acceptance commands.
3. Implement the smallest change that can pass the current gate.
4. Run local checks on build-1.
5. Push a branch and run deterministic tests on GitHub Actions (`ubuntu-latest`).
6. Run live OAuth/Spark tests only on build-1 and redact their evidence.
7. Record results, failures, retries, durations and resource measurements.
8. Merge only after all current gates are green; clean branch/worktree; advance `PROGRESS.json` atomically.

For CP7, install the release candidate as a build-1 user service and run the required 24h soak, then 72h
release-candidate soak. Monitoring must survive logout and Hermes session reset. A failed soak opens an incident,
returns the phase to active, and does not publish a release.

## Required repository evidence

Create these early and keep them current:

```text
MISSION.md                 # this contract, copied into the new repository
PROGRESS.json              # machine-readable current CP, state, blocker and next command
docs/evidence/run.json     # run id, timestamps, Hermes/build/CI versions and final outcome
docs/evidence/events.jsonl # sanitized phase/gate/command-result timeline
docs/evidence/cp/CP1.md ... CP7.md
docs/decisions/            # project ADRs and any owner-approved amendments
```

Each event records: timestamp, run/session/worker id, phase/checkpoint, action class, duration, exit status,
retry number, git commit/PR/CI URL, test counts, and failure classification. Never record auth values, prompts,
personal paths, raw environment, private command output, or model response content. Store aggregate token/turn
counts when Hermes exposes them, not message bodies.

Track at minimum:

- wall time and active agent time per CP;
- model/provider used by orchestrator and coding worker;
- tool calls, delegated workers, retries, timeouts and compactions;
- commits, PRs, CI runs, review/fix loops and changed LOC;
- test counts/durations, clippy/fmt/audit/deny results;
- startup/handshake latency, RSS, FD count, descendants, queue high-water and rate-limit snapshots;
- every gate failure, root cause, recovery action and whether human input was required.

## CI and security

- CI uses fake app-server fixtures only and has no Codex OAuth or other secrets.
- Run fmt, clippy `-D warnings`, tests, locked release build, dependency/license checks and secret scan.
- Pin actions by full commit SHA before release readiness.
- Preserve provenance/notices. BSL/AGPL and unknown-license sources are reference-only unless the owner approves
  a compatible reuse decision. Do not copy code merely because it is public.
- Default API bind is loopback; approvals fail closed; process and queue limits remain bounded.

## Terminal outcome

Success requires CP1..CP7 green, 24h and 72h soaks green, a tagged release with checksums/SBOM/notices,
clean protected master, no disposable branches/worktrees/processes, and a final report linking every evidence
file, PR, CI run and release artifact.

Failure is also terminal when a hard gate is genuinely blocked: leave no running/disposable state, preserve
sanitized evidence, set `PROGRESS.json` to `blocked`, and send the owner one concise Telegram report with the
exact blocker and minimal required action.
