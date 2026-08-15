# Spark Runner API product mission

## Current objective

Finish Spark Runner as a ready-to-use project so exact Codex Spark capability can be embedded into applications through `https://ninitux.com/api`.

This contract supersedes the earlier rust-foundation intake. The old references to `IMPLEMENTATION-PLAN.md`, documents `01` through `09`, `adrs/`, `reference/`, and `08-source-license-matrix.csv` are obsolete; those files are not required and their absence is not a blocker. The current repository, its open pull requests, this file, `README.md`, `PROGRESS.json`, `docs/decisions/`, and verified runtime evidence are the sources of truth.

## Delivery workflow

Repository work goes through the registered Central delivery flow on `uap-build-1`: isolated worktree, target checks, independent review, pull request, required CI, exact-head merge, post-verify, and cleanup. Do not bypass branch protection or place credentials in repository files, prompts, logs, fixtures, CI, or artifacts.

Before implementation:

1. Fetch the current `main` branch and enumerate all open pull requests.
2. Evaluate PR #8 (independent audit), draft PR #9 (integration-ready), and any newer work against this product goal.
3. Merge valid work through normal gates or supersede it with evidence-backed replacement work. A draft PR must be evaluated, not ignored merely because it is draft.
4. Reconcile `README.md`, `MISSION.md`, `PROGRESS.json`, and evidence with the actual resulting state.

## Product outcome

Provide a small, documented, stable API contract suitable for application developers:

- versioned ninitux-compatible routes, preferably under `/api/v1/spark` unless an existing ninitux convention requires another prefix;
- health and readiness endpoints;
- thread or session creation;
- message or run submission;
- run status and cancellation;
- SSE or streaming events where supported by the existing runner;
- deterministic JSON schemas and safe error codes;
- cancellation, timeout, startup, shutdown, and bounded lifecycle behavior.

Preserve exact-model enforcement for `gpt-5.3-codex-spark` and fail closed. Do not add fallback models.

Add the minimum operator and developer assets needed for a ready project:

- documented configuration and environment variables;
- a Dockerfile or clear container deployment instructions if missing;
- reverse-proxy notes for mounting below `ninitux.com/api`;
- safe logging and redaction guidance;
- an explicit integration boundary for upstream authentication;
- an OpenAPI specification or equivalent endpoint contract;
- curl examples and one minimal application integration example;
- a clear distinction between deterministic offline checks and live Spark/model checks.

Address still-relevant audit blockers that prevent UAT or soak readiness, including permanent 503 after a failed live turn, unbounded thread/session or journal growth, timeout inconsistency, graceful shutdown, and Linux-only compilation hazards. Reuse existing Rust code and standard patterns. Do not add speculative dependencies or abstractions.

## Verification

Run on `uap-build-1`:

```text
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
cargo build --locked --release
```

Add deterministic fake-app-server API tests for the ninitux-compatible surface. CI remains offline and contains no OAuth material. Run live doctor/UAT only through the configured build-1 path when credentials and egress are available; otherwise record a precise blocked result instead of claiming success.

Completion requires green target checks and CI, independent review, merged PRs, fresh `main` verification at the exact merge SHA, current documentation/evidence, and removal of disposable branches and worktrees.
