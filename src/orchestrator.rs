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

use tokio::sync::{mpsc, oneshot};

use crate::client::{ApprovalPolicy, ClientError, CodexClient, REQUIRED_MODEL};
use crate::config::{self, CodexLock, ConfigError, DEFAULT_CODEX_LOCK};
use crate::journal::{
    project_recovery, ApprovalTerminalDecision, ExecutionTerminalStatus, JournalConfig,
    JournalEvent, JournalWriter, TurnTerminalStatus,
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
    #[error(transparent)]
    Api(#[from] crate::api::ApiError),
}

/// Commands accepted by the one active runtime execution.  The HTTP adapter
/// never aborts a task: it asks this owner to deliver protocol cancellation,
/// journal the terminal transition, and reap the process group first.
pub enum RuntimeControl {
    Interrupt(oneshot::Sender<()>),
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
        let verified = lock.verify_for_spawn()?;
        Ok((
            verified.to_string_lossy().to_string(),
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
    let spawned = ChildProcess::spawn(&program, &args, Some(&cwd))?;
    let client = CodexClient::with_approval_policy(
        spawned.process,
        spawned.stdin,
        spawned.stdout,
        approval_policy,
    );
    Ok((client, cwd))
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
            let rate_limits = client.admit_live_turn().await?;
            append_journal(
                journal,
                JournalEvent::RateLimitSnapshot {
                    execution_id: execution_id.to_string(),
                    snapshot: rate_limits,
                },
            )
            .await?;

            let thread = client
                .thread_start(cwd)
                .await
                .map_err(non_idempotent("thread/start"))?;
            let turn_id = client
                .turn_start(&thread.thread_id, "spark-runner doctor readiness check")
                .await
                .map_err(non_idempotent("turn/start"))?;
            append_journal(
                journal,
                JournalEvent::TurnStarted {
                    execution_id: execution_id.to_string(),
                    turn_id: turn_id.clone(),
                },
            )
            .await?;
            // Once turn/start acknowledged, every later protocol ambiguity is
            // after an irreversible request.  Never restart the flow and risk
            // replaying it.
            let turn = client
                .wait_turn_completed()
                .await
                .map_err(non_idempotent("turn/completed"))?;
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
            let stderr_tail = client.stderr_tail().await;
            tracing::debug!(stderr_tail = %stderr_tail, "app-server stderr tail (diagnostic only)");

            Ok(format!(
                "doctor: ok mode={} model={} turn_status={}",
                mode_label(live),
                thread.model,
                turn.status
            ))
        }
        Flow::Run(prompt) => {
            client.initialize().await?;
            let rate_limits = client.admit_live_turn().await?;
            append_journal(
                journal,
                JournalEvent::RateLimitSnapshot {
                    execution_id: execution_id.to_string(),
                    snapshot: rate_limits,
                },
            )
            .await?;
            let thread = client
                .thread_start(cwd)
                .await
                .map_err(non_idempotent("thread/start"))?;
            let turn_id = client
                .turn_start(&thread.thread_id, prompt)
                .await
                .map_err(non_idempotent("turn/start"))?;
            append_journal(
                journal,
                JournalEvent::TurnStarted {
                    execution_id: execution_id.to_string(),
                    turn_id: turn_id.clone(),
                },
            )
            .await?;
            // See the doctor flow above: terminal observation is after the
            // non-idempotent turn/start boundary.
            let turn = client
                .wait_turn_completed()
                .await
                .map_err(non_idempotent("turn/completed"))?;
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
            let stderr_tail = client.stderr_tail().await;
            tracing::debug!(stderr_tail = %stderr_tail, "app-server stderr tail (diagnostic only)");

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
    if outcome.is_err() {
        append_journal(
            journal,
            JournalEvent::Incident {
                execution_id: Some(execution_id.clone()),
                class: "flow_error".to_string(),
                // Remote protocol errors are intentionally not persisted:
                // their JSON may contain prompts, paths, or credentials.
                message: "flow failed; diagnostic payload suppressed".to_string(),
            },
        )
        .await?;
    }
    let _ = client.shutdown().await;
    let _ = std::fs::remove_dir_all(&cwd);
    outcome
}

fn non_idempotent(method: &'static str) -> impl FnOnce(ClientError) -> AppError {
    move |error| match error {
        // After delivery, all transport ambiguity classes (including timeout
        // and stream close) are durable unknown execution, not a retry cue.
        ClientError::Jsonl(_) | ClientError::UnexpectedResponseWhileWaiting => {
            AppError::Client(ClientError::AmbiguousNonIdempotent { method })
        }
        other => AppError::Client(other),
    }
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
        Some(config) => {
            let projection = project_recovery(&config.path)?;
            let writer = JournalWriter::open(config)?;
            persist_recovery(&writer, projection).await?;
            Some(writer)
        }
        None => None,
    };
    let result = match execute_flow_once(
        &flow,
        live,
        fake_server_args,
        approval_policy.clone(),
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

async fn persist_recovery(
    journal: &JournalWriter,
    projection: crate::journal::RecoveryProjection,
) -> Result<(), AppError> {
    for execution_id in projection.unresolved_executions {
        journal
            .append(JournalEvent::RecoveryExecutionUnknown { execution_id })
            .await?;
    }
    for request_key in projection.unresolved_approvals {
        journal
            .append(JournalEvent::RecoveryApprovalDenied {
                execution_id: "recovery".to_string(),
                request_key,
                method: "unknown".to_string(),
            })
            .await?;
    }
    Ok(())
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

/// Runtime-owner entry point.  HTTP adapters use this rather than selecting a
/// launcher or inventing an approval path of their own.
pub async fn run_turn_with_approval_policy(
    prompt: String,
    live: bool,
    approval_policy: ApprovalPolicy,
) -> Result<String, AppError> {
    run_with_restart(Flow::Run(prompt), live, &[], approval_policy).await
}

/// The controlled runtime-owner path used by HTTP.  It intentionally has no
/// retry after `thread/start`/`turn/start`: delivery ambiguity is an operator
/// recovery condition, never permission to replay a model turn.
pub async fn run_turn_with_approval_policy_controlled(
    prompt: String,
    live: bool,
    approval_policy: ApprovalPolicy,
    turn_timeout: std::time::Duration,
    controls: &mut mpsc::Receiver<RuntimeControl>,
) -> Result<String, AppError> {
    let journal = match JournalConfig::from_env() {
        Some(config) => {
            let projection = project_recovery(&config.path)?;
            let writer = JournalWriter::open(config)?;
            persist_recovery(&writer, projection).await?;
            Some(writer)
        }
        None => None,
    };
    let execution_id = next_execution_id();
    append_journal(
        journal.as_ref(),
        JournalEvent::ExecutionStarted {
            execution_id: execution_id.clone(),
        },
    )
    .await?;
    // Offline is an injected deterministic runtime, but it traverses the
    // exact same owner/client/protocol path as live execution.
    let fake_args = (!live).then(|| vec!["--approval-mode".to_string(), "command".to_string()]);
    let (mut client, cwd) =
        spawn_client(live, fake_args.as_deref().unwrap_or(&[]), approval_policy).await?;
    let result = async {
        client.initialize().await?;
        let rate_limits = client.admit_live_turn().await?;
        append_journal(
            journal.as_ref(),
            JournalEvent::RateLimitSnapshot {
                execution_id: execution_id.clone(),
                snapshot: rate_limits,
            },
        )
        .await?;
        let thread = client
            .thread_start(&cwd)
            .await
            .map_err(non_idempotent("thread/start"))?;
        let turn_id = client
            .turn_start(&thread.thread_id, &prompt)
            .await
            .map_err(non_idempotent("turn/start"))?;
        append_journal(
            journal.as_ref(),
            JournalEvent::TurnStarted {
                execution_id: execution_id.clone(),
                turn_id: turn_id.clone(),
            },
        )
        .await?;

        let deadline = tokio::time::sleep(turn_timeout);
        tokio::pin!(deadline);
        tokio::select! {
            biased;
            control = controls.recv() => match control {
                Some(RuntimeControl::Interrupt(ack)) => {
                    // Both ids come from accepted 0.144.3 responses; do not
                    // synthesize an interrupt for an unknown execution.
                    let _ = client.turn_interrupt(&thread.thread_id, &turn_id).await;
                    append_journal(
                        journal.as_ref(),
                        JournalEvent::TurnCompleted {
                            execution_id: execution_id.clone(),
                            turn_id,
                            status: TurnTerminalStatus::Interrupted,
                        },
                    ).await?;
                    Ok((Some(ack), "interrupted".to_string()))
                }
                None => Err(AppError::Client(ClientError::TurnDeadlineExceeded)),
            },
            completed = client.wait_turn_completed() => {
                let turn = completed.map_err(non_idempotent("turn/completed"))?;
                append_journal(
                    journal.as_ref(),
                    JournalEvent::TurnCompleted {
                        execution_id: execution_id.clone(),
                        turn_id,
                        status: turn_status(&turn.status),
                    },
                ).await?;
                Ok((None, turn.status))
            },
            _ = &mut deadline => Err(AppError::Client(ClientError::TurnDeadlineExceeded)),
        }
    }
    .await;
    append_internal_events(journal.as_ref(), &execution_id, client.internal_events()).await?;
    append_journal(
        journal.as_ref(),
        JournalEvent::ExecutionCompleted {
            execution_id: execution_id.clone(),
            status: if result.is_ok() {
                ExecutionTerminalStatus::Completed
            } else {
                ExecutionTerminalStatus::Failed
            },
        },
    )
    .await?;
    let interrupt_ack = result.as_ref().ok().and_then(|(ack, _)| ack.as_ref());
    client.shutdown().await?;
    let _ = std::fs::remove_dir_all(&cwd);
    if let Some(ack) = interrupt_ack {
        // `ack` is borrowed only to show the ordering; it is sent below from
        // the owned result after process-group cleanup has completed.
        let _ = ack;
    }
    if let Some(journal) = journal {
        journal.shutdown().await?;
    }
    match result {
        Ok((Some(ack), status)) => {
            let _ = ack.send(());
            Ok(format!(
                "run: mode={} model={} turn_status={status}",
                mode_label(live),
                REQUIRED_MODEL
            ))
        }
        Ok((None, status)) => Ok(format!(
            "run: mode={} model={} turn_status={status}",
            mode_label(live),
            REQUIRED_MODEL
        )),
        Err(error) => Err(error),
    }
}

/// Perform durable restart projection before an HTTP listener becomes ready.
/// This never starts a process and never replays a prior turn.
pub async fn recover_before_readiness() -> Result<(), AppError> {
    if let Some(config) = JournalConfig::from_env() {
        recover_journal_before_readiness(&config.path).await?;
    }
    Ok(())
}

/// Durable, idempotent startup recovery used by the HTTP owner and by the
/// deterministic recovery test. A second invocation projects the terminal
/// recovery records written by the first one and therefore appends nothing.
pub async fn recover_journal_before_readiness(path: &Path) -> Result<(), AppError> {
    let projection = project_recovery(path)?;
    let writer = JournalWriter::open(JournalConfig::new(path))?;
    persist_recovery(&writer, projection).await?;
    writer.shutdown().await?;
    Ok(())
}

/// Test-support/API entry point for the offline fake app-server only: same as
/// [`run_turn`], but passes deterministic fake-server args and approval policy
/// through the existing runtime/client path.
pub async fn run_turn_with_fake_server_args_and_approval_policy(
    prompt: String,
    fake_server_args: &[String],
    approval_policy: ApprovalPolicy,
) -> Result<String, AppError> {
    run_with_restart(Flow::Run(prompt), false, fake_server_args, approval_policy).await
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
