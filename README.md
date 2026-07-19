# spark-runner

A Rust runner for the pinned Codex Spark app-server protocol, with a loopback HTTP/SSE adapter and fail-closed runtime controls.

Print the package version without starting app-server:

```sh
$ spark-runner --version
spark-runner 0.1.0
```

## Status

CP6 offline remediation was accepted at `ad2952cdf3e0ad1a4921c2d6fd64925e10eb7c7e` and squash-merged by PR [#6](https://github.com/PavelLizunov/spark-runner/pull/6) as `072b777b290a2dddc7c38009de438c4173db99b2`. GitHub Actions runs [29281701984](https://github.com/PavelLizunov/spark-runner/actions/runs/29281701984) and [29281705056](https://github.com/PavelLizunov/spark-runner/actions/runs/29281705056) passed with 76 offline tests. Controlled live UAT remains pending; CP7 has not started.

## Safe offline checks

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
```

## Live boundary

Do not run live paths, real app-server/model/account turns, or credential-dependent commands without separate authorization on `uap-build-1`. CI and the offline checks use deterministic fake fixtures only.

## Mission records

- [Mission](MISSION.md)
- [Progress](PROGRESS.json)
- [Evidence index](docs/evidence/run.json)
- [CP6 evidence](docs/evidence/cp/CP6.md)
