//! Doctor/run orchestration: spawns the app-server, drives one full flow, and
//! restarts the app-server exactly once on a recoverable protocol desync
//! before failing closed (ADR-004: poison-on-desync, CP3 controlled restart).
//!
//! A "recoverable desync" is narrowly scoped to [`ClientError::is_recoverable_desync`]
//! — an oversized/malformed JSONL frame or an unexpected response id. Other
//! failures (fallback model, invalid state transitions, timeouts, spawn/config
//! errors) are never retried; they fail closed on the first attempt.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::client::{ApprovalPolicy, ClientError, CodexClient, REQUIRED_MODEL};
use crate::config::{self, CodexLock, ConfigError, DEFAULT_CODEX_LOCK};
use crate::journal::{
    ApprovalTerminalDecision, ExecutionTerminalStatus, JournalConfig, JournalEvent, JournalWriter,
    TurnTerminalStatus,
};
use crate::process::{ChildProcess, ProcessError};
use crate::state::{ApprovalDecision, InternalEventKind};

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
    #[error(transparent)]
    Journal(#[from] crate::journal::JournalError),
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
    approval_policy: ApprovalPolicy,
) -> Result<(CodexClient, PathBuf), AppError> {
    let (program, args) = launch_spec(live, fake_server_args)?;
    let cwd = config::ephemeral_cwd()?;
    let spawned = ChildProcess::spawn(&program, &args, None)?;
    let client = CodexClient::with_approval_policy(
        spawned.process,
        spawned.stdin,
        spawned.stdout,
        approval_policy,
    );
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
    journal: Option<&JournalWriter>,
    execution_id: &str,
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
            let rate_limits = client.rate_limits_read().await?;
            append_journal(
                journal,
                JournalEvent::RateLimitSnapshot {
                    execution_id: execution_id.to_string(),
                    snapshot: rate_limits,
                },
            )
            .await?;

            let thread = client.thread_start(cwd).await?;
            let turn_id = client
                .turn_start(&thread.thread_id, "spark-runner doctor readiness check")
                .await?;
            append_journal(
                journal,
                JournalEvent::TurnStarted {
                    execution_id: execution_id.to_string(),
                    turn_id: turn_id.clone(),
                },
            )
            .await?;
            let turn = client.wait_turn_completed().await?;
            append_journal(
                journal,
                JournalEvent::TurnCompleted {
                    execution_id: execution_id.to_string(),
                    turn_id,
                    status: turn_status(&turn.status),
                },
            )
            .await?;
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
            let turn_id = client.turn_start(&thread.thread_id, prompt).await?;
            append_journal(
                journal,
                JournalEvent::TurnStarted {
                    execution_id: execution_id.to_string(),
                    turn_id: turn_id.clone(),
                },
            )
            .await?;
            let turn = client.wait_turn_completed().await?;
            append_journal(
                journal,
                JournalEvent::TurnCompleted {
                    execution_id: execution_id.to_string(),
                    turn_id,
                    status: turn_status(&turn.status),
                },
            )
            .await?;
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
    approval_policy: ApprovalPolicy,
    journal: Option<&JournalWriter>,
) -> Result<String, AppError> {
    let execution_id = next_execution_id();
    append_journal(
        journal,
        JournalEvent::ExecutionStarted {
            execution_id: execution_id.clone(),
        },
    )
    .await?;
    let (mut client, cwd) = spawn_client(live, fake_server_args, approval_policy).await?;
    let outcome = run_flow_body(&mut client, &cwd, flow, live, journal, &execution_id).await;
    append_internal_events(journal, &execution_id, client.internal_events()).await?;
    let terminal_status = if outcome.is_ok() {
        ExecutionTerminalStatus::Completed
    } else {
        ExecutionTerminalStatus::Failed
    };
    append_journal(
        journal,
        JournalEvent::ExecutionCompleted {
            execution_id: execution_id.clone(),
            status: terminal_status,
        },
    )
    .await?;
    if let Err(error) = &outcome {
        append_journal(
            journal,
            JournalEvent::Incident {
                execution_id: Some(execution_id.clone()),
                class: "flow_error".to_string(),
                message: error.to_string(),
            },
        )
        .await?;
    }
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
    approval_policy: ApprovalPolicy,
) -> Result<String, AppError> {
    let journal = match JournalConfig::from_env() {
        Some(config) => Some(JournalWriter::open(config)?),
        None => None,
    };
    let result = match execute_flow_once(
        &flow,
        live,
        fake_server_args,
        approval_policy,
        journal.as_ref(),
    )
    .await
    {
        Ok(summary) => Ok(summary),
        Err(error) if error.is_recoverable_desync() => {
            tracing::warn!(
                error = %error,
                "recoverable protocol desync on first attempt; restarting app-server once"
            );
            execute_flow_once(
                &flow,
                live,
                fake_server_args,
                approval_policy,
                journal.as_ref(),
            )
            .await
        }
        Err(error) => Err(error),
    };
    if let Some(journal) = journal {
        journal.shutdown().await?;
    }
    result
}

async fn append_journal(
    journal: Option<&JournalWriter>,
    event: JournalEvent,
) -> Result<(), AppError> {
    if let Some(journal) = journal {
        journal.append(event).await?;
    }
    Ok(())
}

async fn append_internal_events(
    journal: Option<&JournalWriter>,
    execution_id: &str,
    events: &[crate::state::InternalEvent],
) -> Result<(), AppError> {
    for event in events {
        match &event.kind {
            InternalEventKind::ApprovalRequested {
                request_key,
                method,
            } => {
                append_journal(
                    journal,
                    JournalEvent::ApprovalRequested {
                        execution_id: execution_id.to_string(),
                        request_key: request_key.clone(),
                        method: method.clone(),
                    },
                )
                .await?;
            }
            InternalEventKind::ApprovalDecided {
                request_key,
                method,
                decision,
            } => {
                append_journal(
                    journal,
                    JournalEvent::ApprovalDecided {
                        execution_id: execution_id.to_string(),
                        request_key: request_key.clone(),
                        method: method.clone(),
                        decision: approval_decision(*decision),
                    },
                )
                .await?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn approval_decision(decision: ApprovalDecision) -> ApprovalTerminalDecision {
    match decision {
        ApprovalDecision::Allow => ApprovalTerminalDecision::Allowed,
        ApprovalDecision::Deny => ApprovalTerminalDecision::Denied,
        ApprovalDecision::Timeout => ApprovalTerminalDecision::TimedOut,
    }
}

fn turn_status(status: &str) -> TurnTerminalStatus {
    if status == "completed" {
        TurnTerminalStatus::Completed
    } else {
        TurnTerminalStatus::Failed
    }
}

fn next_execution_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("exec-{}-{nanos}", std::process::id())
}

pub async fn run_doctor(live: bool) -> Result<String, AppError> {
    run_with_restart(Flow::Doctor, live, &[], ApprovalPolicy::Deny).await
}

pub async fn run_turn(prompt: String, live: bool) -> Result<String, AppError> {
    run_with_restart(Flow::Run(prompt), live, &[], ApprovalPolicy::Deny).await
}

/// Test-support entry point for the offline fake app-server only: same as
/// [`run_doctor`], but passes `fake_server_args` through to the fake server
/// process so CP3 regression tests can select a deterministic fault mode
/// (see `src/bin/fake_app_server.rs`).
pub async fn run_doctor_with_fake_server_args(
    fake_server_args: &[String],
) -> Result<String, AppError> {
    run_with_restart(Flow::Doctor, false, fake_server_args, ApprovalPolicy::Deny).await
}

pub async fn run_doctor_with_fake_server_args_and_approval_policy(
    fake_server_args: &[String],
    approval_policy: ApprovalPolicy,
) -> Result<String, AppError> {
    run_with_restart(Flow::Doctor, false, fake_server_args, approval_policy).await
}
