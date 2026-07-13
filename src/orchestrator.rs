//! Doctor/run orchestration: spawns the app-server, drives one full flow, and
//! restarts the app-server exactly once on a recoverable protocol desync
//! before failing closed (ADR-004: poison-on-desync, CP3 controlled restart).
//!
//! A "recoverable desync" is narrowly scoped to [`ClientError::is_recoverable_desync`]
//! — an oversized/malformed JSONL frame or an unexpected response id. Other
//! failures (fallback model, invalid state transitions, timeouts, spawn/config
//! errors) are never retried; they fail closed on the first attempt.

use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::client::{ClientError, CodexClient, REQUIRED_MODEL};
use crate::config::{self, CodexLock, ConfigError, DEFAULT_CODEX_LOCK};
use crate::process::{ChildProcess, ProcessError};

/// Pinned live app-server binary; the exact path/version/sha256 also live in `codex.lock`.
const LIVE_ARGS: &[&str] = &["app-server", "--listen", "stdio://"];

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Process(#[from] ProcessError),
    #[error(transparent)]
    Client(#[from] ClientError),
}

impl AppError {
    fn is_recoverable_desync(&self) -> bool {
        matches!(self, AppError::Client(error) if error.is_recoverable_desync())
    }
}

enum Flow {
    Doctor,
    Run(String),
}

fn launch_spec(live: bool, fake_server_args: &[String]) -> Result<(String, Vec<String>), AppError> {
    if live {
        let lock = CodexLock::load(Path::new(DEFAULT_CODEX_LOCK))?;
        lock.validate()?;
        Ok((
            lock.binary_path,
            LIVE_ARGS.iter().map(|arg| arg.to_string()).collect(),
        ))
    } else {
        let path = config::fake_app_server_path()?;
        Ok((
            path.to_string_lossy().to_string(),
            fake_server_args.to_vec(),
        ))
    }
}

async fn spawn_client(
    live: bool,
    fake_server_args: &[String],
) -> Result<(CodexClient, PathBuf), AppError> {
    let (program, args) = launch_spec(live, fake_server_args)?;
    let cwd = config::ephemeral_cwd()?;
    let spawned = ChildProcess::spawn(&program, &args, None)?;
    let client = CodexClient::new(spawned.process, spawned.stdin, spawned.stdout);
    Ok((client, cwd))
}

fn model_list_has_required_model(model_list: &Value) -> bool {
    model_list
        .get("data")
        .or_else(|| model_list.get("models"))
        .and_then(Value::as_array)
        .map(|models| {
            models
                .iter()
                .any(|model| model.get("id").and_then(Value::as_str) == Some(REQUIRED_MODEL))
        })
        .unwrap_or(false)
}

fn mode_label(live: bool) -> &'static str {
    if live {
        "live"
    } else {
        "offline"
    }
}

async fn run_flow_body(
    client: &mut CodexClient,
    cwd: &Path,
    flow: &Flow,
    live: bool,
) -> Result<String, AppError> {
    match flow {
        Flow::Doctor => {
            client.initialize().await?;
            client.account_read().await?;
            let model_list = client.model_list().await?;
            if !model_list_has_required_model(&model_list) {
                return Err(AppError::Client(ClientError::FallbackModel {
                    observed: "missing-from-model-list".to_string(),
                    required: REQUIRED_MODEL,
                }));
            }
            client.rate_limits_read().await?;

            let thread = client.thread_start(cwd).await?;
            client
                .turn_start(&thread.thread_id, "spark-runner doctor readiness check")
                .await?;
            let turn = client.wait_turn_completed().await?;
            if client.is_poisoned() {
                return Err(AppError::Client(ClientError::SessionPoisoned));
            }
            tracing::debug!(stderr_tail = %client.stderr_tail().await, "app-server stderr tail (diagnostic only)");

            Ok(format!(
                "doctor: ok mode={} model={} turn_status={}",
                mode_label(live),
                thread.model,
                turn.status
            ))
        }
        Flow::Run(prompt) => {
            client.initialize().await?;
            let thread = client.thread_start(cwd).await?;
            client.turn_start(&thread.thread_id, prompt).await?;
            let turn = client.wait_turn_completed().await?;
            if client.is_poisoned() {
                return Err(AppError::Client(ClientError::SessionPoisoned));
            }
            tracing::debug!(stderr_tail = %client.stderr_tail().await, "app-server stderr tail (diagnostic only)");

            Ok(format!(
                "run: mode={} model={} turn_status={}",
                mode_label(live),
                thread.model,
                turn.status
            ))
        }
    }
}

/// Spawn a fresh app-server process, run `flow` once to completion or
/// failure, and always shut down the child and clean up its ephemeral `cwd`
/// before returning — regardless of whether `flow` succeeded.
async fn execute_flow_once(
    flow: &Flow,
    live: bool,
    fake_server_args: &[String],
) -> Result<String, AppError> {
    let (mut client, cwd) = spawn_client(live, fake_server_args).await?;
    let outcome = run_flow_body(&mut client, &cwd, flow, live).await;
    let _ = client.shutdown().await;
    let _ = std::fs::remove_dir_all(&cwd);
    outcome
}

/// Run `flow` once; on a recoverable protocol desync, start one fresh
/// app-server process and retry the whole flow exactly once, then fail
/// closed if the second attempt also fails.
async fn run_with_restart(
    flow: Flow,
    live: bool,
    fake_server_args: &[String],
) -> Result<String, AppError> {
    match execute_flow_once(&flow, live, fake_server_args).await {
        Ok(summary) => Ok(summary),
        Err(error) if error.is_recoverable_desync() => {
            tracing::warn!(
                error = %error,
                "recoverable protocol desync on first attempt; restarting app-server once"
            );
            execute_flow_once(&flow, live, fake_server_args).await
        }
        Err(error) => Err(error),
    }
}

pub async fn run_doctor(live: bool) -> Result<String, AppError> {
    run_with_restart(Flow::Doctor, live, &[]).await
}

pub async fn run_turn(prompt: String, live: bool) -> Result<String, AppError> {
    run_with_restart(Flow::Run(prompt), live, &[]).await
}

/// Test-support entry point for the offline fake app-server only: same as
/// [`run_doctor`], but passes `fake_server_args` through to the fake server
/// process so CP3 regression tests can select a deterministic fault mode
/// (see `src/bin/fake_app_server.rs`).
pub async fn run_doctor_with_fake_server_args(
    fake_server_args: &[String],
) -> Result<String, AppError> {
    run_with_restart(Flow::Doctor, false, fake_server_args).await
}
