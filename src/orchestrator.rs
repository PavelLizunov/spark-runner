//! Doctor/run orchestration: spawns the app-server, drives one full flow, and
//! restarts the app-server exactly once on a recoverable protocol desync
//! before failing closed (ADR-004: poison-on-desync, CP3 controlled restart).
//!
//! A "recoverable desync" is narrowly scoped to [`ClientError::is_recoverable_desync`]
//! — an oversized/malformed JSONL frame or an unexpected response id. Other
//! failures (fallback model, invalid state transitions, timeouts, spawn/config
//! errors) are never retried; they fail closed on the first attempt.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, oneshot};

use crate::client::{
    journal_rate_limit_snapshot, ApprovalPolicy, ApprovalReceipt, ClientError, CodexClient,
    REQUIRED_MODEL,
};
use crate::config::{self, CodexLock, ConfigError, DEFAULT_CODEX_LOCK};
use crate::journal::{
    project_recovery, ApprovalTerminalDecision, ExecutionTerminalStatus, JournalConfig,
    JournalEvent, JournalWriter, TurnTerminalStatus,
};
use crate::jsonl::RequestDelivery;
use crate::process::{ChildProcess, ProcessError};
use crate::state::{ApprovalDecision, InternalEventKind};

/// Pinned live app-server binary; the exact path/version/sha256 also live in `codex.lock`.
const LIVE_ARGS: &[&str] = &["app-server", "--listen", "stdio://"];
/// The owner HTTP deadline includes both this protocol phase and the
/// subsequent process-group cleanup. A stuck interrupt/terminal read must
/// therefore return to the cleanup path rather than retain the child for the
/// JSONL client's general 120-second read timeout.
const CONTROLLED_CANCEL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

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
    #[error("failed to remove ephemeral runtime directory: {0}")]
    EphemeralCleanup(std::io::Error),
}

/// Commands accepted by the one active runtime execution.  The HTTP adapter
/// never aborts a task: it asks this owner to deliver protocol cancellation,
/// journal the terminal transition, and reap the process group first.
pub enum RuntimeControl {
    Interrupt(oneshot::Sender<ControlOutcome>),
}

/// What the process owner learned after requested cancellation and bounded
/// cleanup. An ambiguous delivery cannot be recast as an ordinary interrupt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlOutcome {
    Interrupted,
    Unknown,
}

struct ControlledCompletion {
    status: String,
    execution_status: ExecutionTerminalStatus,
}

struct ControlledFlowContext<'a> {
    journal: Option<&'a JournalWriter>,
    execution_id: &'a str,
}

fn control_received(
    control: Option<RuntimeControl>,
    acknowledgement: &mut Option<oneshot::Sender<ControlOutcome>>,
) -> Result<(), AppError> {
    match control {
        Some(RuntimeControl::Interrupt(ack)) => {
            *acknowledgement = Some(ack);
            Ok(())
        }
        // Losing the command authority is fail-closed. The final cleanup
        // below still owns the child process and releases admission once.
        None => Err(AppError::Client(ClientError::TurnDeadlineExceeded)),
    }
}

fn interrupted_completion() -> ControlledCompletion {
    ControlledCompletion {
        status: "interrupted".to_string(),
        execution_status: ExecutionTerminalStatus::Interrupted,
    }
}

/// The auth copy lives only below this directory. Remove it explicitly before
/// the directory tree so a failed recursive removal never retains an auth
/// value merely because a parent cleanup raced or was interrupted.
fn remove_ephemeral_cwd(cwd: &Path) -> Result<(), AppError> {
    let auth = cwd.join("codex-home").join("auth.json");
    match std::fs::remove_file(&auth) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            // Continue with recursive removal; a successful tree removal is
            // sufficient even if the explicit unlink observed a transient.
        }
    }
    match std::fs::remove_dir_all(cwd) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(AppError::EphemeralCleanup(error)),
    }
}

fn retain_first_error<T>(result: &mut Result<T, AppError>, candidate: Result<(), AppError>) {
    if result.is_ok() {
        if let Err(error) = candidate {
            *result = Err(error);
        }
    }
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

/// The runtime-owner launcher capability. `Live` is the only production
/// choice and always re-verifies the pinned executable. `Fake` is an explicit
/// deterministic fixture injection used by offline tests; live mode never
/// falls back to it.
#[derive(Clone, Debug)]
pub enum RuntimeLauncher {
    Live,
    Fake { args: Vec<String> },
}

impl RuntimeLauncher {
    pub fn for_mode(live: bool) -> Self {
        if live {
            Self::Live
        } else {
            Self::Fake {
                args: vec!["--approval-mode".to_string(), "command".to_string()],
            }
        }
    }

    pub fn is_live(&self) -> bool {
        matches!(self, Self::Live)
    }
}

enum LaunchSpec {
    Live {
        executable: crate::config::VerifiedExecutable,
        subscription_auth: crate::config::VerifiedSubscriptionAuth,
        args: Vec<String>,
    },
    Fake {
        program: String,
        args: Vec<String>,
    },
}

impl LaunchSpec {
    fn program(&self) -> String {
        match self {
            Self::Live { executable, .. } => executable.program(),
            Self::Fake { program, .. } => program.clone(),
        }
    }

    fn args(&self) -> &[String] {
        match self {
            Self::Live { args, .. } | Self::Fake { args, .. } => args,
        }
    }

    fn subscription_auth_mut(&mut self) -> Option<&mut crate::config::VerifiedSubscriptionAuth> {
        match self {
            Self::Live {
                subscription_auth, ..
            } => Some(subscription_auth),
            Self::Fake { .. } => None,
        }
    }
}

fn launch_spec(launcher: &RuntimeLauncher) -> Result<LaunchSpec, AppError> {
    match launcher {
        RuntimeLauncher::Live => {
            let lock = CodexLock::load(Path::new(DEFAULT_CODEX_LOCK))?;
            Ok(LaunchSpec::Live {
                executable: lock.verified_for_spawn()?,
                subscription_auth: config::selected_subscription_auth()?,
                args: LIVE_ARGS.iter().map(|arg| arg.to_string()).collect(),
            })
        }
        RuntimeLauncher::Fake { args } => {
            let path = config::fake_app_server_path()?;
            Ok(LaunchSpec::Fake {
                program: path.to_string_lossy().to_string(),
                args: args.clone(),
            })
        }
    }
}

async fn spawn_client(
    live: bool,
    fake_server_args: &[String],
    approval_policy: ApprovalPolicy,
) -> Result<(CodexClient, PathBuf), AppError> {
    let launcher = if live {
        RuntimeLauncher::Live
    } else {
        RuntimeLauncher::Fake {
            args: fake_server_args.to_vec(),
        }
    };
    spawn_client_with_launcher(&launcher, approval_policy).await
}

async fn spawn_client_with_launcher(
    launcher: &RuntimeLauncher,
    approval_policy: ApprovalPolicy,
) -> Result<(CodexClient, PathBuf), AppError> {
    spawn_client_with_launcher_timeout(
        launcher,
        approval_policy,
        crate::jsonl::DEFAULT_WAIT_TIMEOUT,
    )
    .await
}

async fn spawn_client_with_launcher_timeout(
    launcher: &RuntimeLauncher,
    approval_policy: ApprovalPolicy,
    wait_timeout: Duration,
) -> Result<(CodexClient, PathBuf), AppError> {
    let mut launch = launch_spec(launcher)?;
    let cwd = config::ephemeral_cwd()?;
    // `launch` stays alive across spawn. On Linux its program is an inherited
    // `/proc/self/fd/N` handle to the inode whose bytes were just verified.
    let program = launch.program();
    let args = launch.args().to_vec();
    let spawned = match ChildProcess::spawn_with_subscription_auth(
        &program,
        &args,
        Some(&cwd),
        launch.subscription_auth_mut(),
    ) {
        Ok(spawned) => spawned,
        Err(error) => {
            remove_ephemeral_cwd(&cwd)?;
            return Err(error.into());
        }
    };
    let client = CodexClient::with_approval_policy_and_timeout(
        spawned.process,
        spawned.stdin,
        spawned.stdout,
        approval_policy,
        wait_timeout,
    );
    Ok((client, cwd))
}

/// Bounded owner bootstrap. It validates the launcher path by spawning only
/// the selected capability, performs the protocol initialize/auth/model/quota
/// admission sequence, returns the admitted quota snapshot for the owner's
/// durable record, and always reaps that process before reporting.
pub async fn bootstrap_runtime(launcher: RuntimeLauncher) -> Result<serde_json::Value, AppError> {
    let (mut client, cwd) = spawn_client_with_launcher(&launcher, ApprovalPolicy::Deny).await?;
    let mut result = async {
        client.initialize().await?;
        let rate_limits = client.admit_live_turn().await?;
        Ok::<serde_json::Value, AppError>(rate_limits)
    }
    .await;
    retain_first_error(&mut result, client.shutdown().await.map_err(AppError::from));
    retain_first_error(&mut result, remove_ephemeral_cwd(&cwd));
    result
}

/// Install the durable receipt at the only point that knows both the active
/// execution id and its single journal writer.  The client appends it before
/// the pending request can become visible to an HTTP/SSE authority.
fn with_approval_receipt(
    policy: ApprovalPolicy,
    journal: Option<&JournalWriter>,
    execution_id: &str,
) -> ApprovalPolicy {
    match policy {
        ApprovalPolicy::External {
            pending, timeout, ..
        } => ApprovalPolicy::External {
            pending,
            timeout,
            receipt: journal.map(|journal| ApprovalReceipt {
                journal: journal.clone(),
                execution_id: execution_id.to_string(),
            }),
        },
        policy => policy,
    }
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
                    snapshot: journal_rate_limit_snapshot(&rate_limits),
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
                    snapshot: journal_rate_limit_snapshot(&rate_limits),
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
    let approval_events_are_synchronous =
        matches!(&approval_policy, ApprovalPolicy::External { .. });
    let approval_policy = with_approval_receipt(approval_policy, journal, &execution_id);
    let (mut client, cwd) = spawn_client(live, fake_server_args, approval_policy).await?;
    let mut result = run_flow_body(&mut client, &cwd, flow, live, journal, &execution_id).await;
    let flow_failed = result.is_err();
    retain_first_error(
        &mut result,
        append_internal_events(
            journal,
            &execution_id,
            client.internal_events(),
            approval_events_are_synchronous,
        )
        .await,
    );
    let terminal_status = if result.is_ok() {
        ExecutionTerminalStatus::Completed
    } else {
        ExecutionTerminalStatus::Failed
    };
    retain_first_error(
        &mut result,
        append_journal(
            journal,
            JournalEvent::ExecutionCompleted {
                execution_id: execution_id.clone(),
                status: terminal_status,
            },
        )
        .await,
    );
    if flow_failed {
        retain_first_error(
            &mut result,
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
            .await,
        );
    }
    retain_first_error(&mut result, client.shutdown().await.map_err(AppError::from));
    retain_first_error(&mut result, remove_ephemeral_cwd(&cwd));
    result
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
    approval_events_are_synchronous: bool,
) -> Result<(), AppError> {
    for event in events {
        match &event.kind {
            InternalEventKind::ApprovalRequested {
                request_key,
                method,
            } if !approval_events_are_synchronous => {
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
            InternalEventKind::ApprovalRequested { .. } => {}
            InternalEventKind::ApprovalDecided {
                request_key,
                method,
                decision,
            } if !approval_events_are_synchronous => {
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
            InternalEventKind::ApprovalDecided { .. } => {}
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

/// All fallible controlled protocol work lives in this inner future so `?`
/// returns to the caller's single cleanup epilogue, never directly from the
/// runtime owner. A cancellation before real thread/turn identifiers exist
/// can only be an interruption; after a non-idempotent write attempt it is
/// deliberately Unknown and no synthetic interrupt is sent.
async fn run_controlled_flow(
    client: &mut CodexClient,
    cwd: &Path,
    prompt: &str,
    turn_timeout: std::time::Duration,
    controls: &mut mpsc::Receiver<RuntimeControl>,
    context: ControlledFlowContext<'_>,
    control_acknowledgement: &mut Option<oneshot::Sender<ControlOutcome>>,
) -> Result<ControlledCompletion, AppError> {
    let initialized = tokio::select! {
        biased;
        control = controls.recv() => {
            control_received(control, control_acknowledgement)?;
            return Ok(interrupted_completion());
        },
        result = client.initialize() => result,
    };
    initialized?;
    let rate_limits = tokio::select! {
        biased;
        control = controls.recv() => {
            control_received(control, control_acknowledgement)?;
            return Ok(interrupted_completion());
        },
        result = client.admit_live_turn() => result,
    }?;
    append_journal(
        context.journal,
        JournalEvent::RateLimitSnapshot {
            execution_id: context.execution_id.to_string(),
            snapshot: journal_rate_limit_snapshot(&rate_limits),
        },
    )
    .await?;
    let thread_delivery = RequestDelivery::new();
    let thread = tokio::select! {
        biased;
        control = controls.recv() => {
            control_received(control, control_acknowledgement)?;
            if thread_delivery.may_have_been_written() {
                return Err(AppError::Client(ClientError::AmbiguousNonIdempotent { method: "thread/start" }));
            }
            return Ok(interrupted_completion());
        },
        result = client.thread_start_with_delivery(cwd, Some(&thread_delivery)) => result.map_err(non_idempotent("thread/start")),
    }?;
    let turn_delivery = RequestDelivery::new();
    let turn_id = tokio::select! {
        biased;
        control = controls.recv() => {
            control_received(control, control_acknowledgement)?;
            if turn_delivery.may_have_been_written() {
                return Err(AppError::Client(ClientError::AmbiguousNonIdempotent { method: "turn/start" }));
            }
            return Ok(interrupted_completion());
        },
        result = client.turn_start_with_delivery(&thread.thread_id, prompt, Some(&turn_delivery)) => result.map_err(non_idempotent("turn/start")),
    }?;
    append_journal(
        context.journal,
        JournalEvent::TurnStarted {
            execution_id: context.execution_id.to_string(),
            turn_id: turn_id.clone(),
        },
    )
    .await?;

    let deadline = tokio::time::sleep(turn_timeout);
    tokio::pin!(deadline);
    tokio::select! {
        biased;
        control = controls.recv() => {
            control_received(control, control_acknowledgement)?;
            // Both ids come from accepted 0.144.3 responses; do not
            // synthesize a terminal state if delivery failed or its result
            // was rejected. That boundary is ambiguous and is deliberately
            // left recoverable as Unknown on restart.
            let interrupt_delivery = RequestDelivery::new();
            match tokio::time::timeout(
                CONTROLLED_CANCEL_TIMEOUT,
                client.turn_interrupt_with_delivery(
                    &thread.thread_id,
                    &turn_id,
                    Some(&interrupt_delivery),
                ),
            )
            .await
            {
                Ok(result) => result.map_err(non_idempotent("turn/interrupt"))?,
                Err(_) if interrupt_delivery.may_have_been_written() => {
                    return Err(AppError::Client(ClientError::AmbiguousNonIdempotent {
                        method: "turn/interrupt",
                    }));
                }
                Err(_) => return Err(AppError::Client(ClientError::TurnDeadlineExceeded)),
            }
            // The interrupt response only acknowledges the RPC. A terminal
            // state is authoritative only after the matching notification.
            let terminal = tokio::time::timeout(
                CONTROLLED_CANCEL_TIMEOUT,
                client.wait_turn_completed(),
            )
            .await
            .map_err(|_| AppError::Client(ClientError::TurnDeadlineExceeded))?
            .map_err(non_idempotent("turn/completed"))?;
            append_journal(
                context.journal,
                JournalEvent::TurnCompleted {
                    execution_id: context.execution_id.to_string(),
                    turn_id,
                    status: TurnTerminalStatus::Interrupted,
                },
            )
            .await?;
            Ok(ControlledCompletion {
                status: terminal.status,
                execution_status: ExecutionTerminalStatus::Interrupted,
            })
        },
        completed = client.wait_turn_completed() => {
            let turn = completed.map_err(non_idempotent("turn/completed"))?;
            append_journal(
                context.journal,
                JournalEvent::TurnCompleted {
                    execution_id: context.execution_id.to_string(),
                    turn_id,
                    status: turn_status(&turn.status),
                },
            ).await?;
            Ok(ControlledCompletion {
                execution_status: if turn.status == "completed" {
                    ExecutionTerminalStatus::Completed
                } else {
                    ExecutionTerminalStatus::Failed
                },
                status: turn.status,
            })
        },
        _ = &mut deadline => Err(AppError::Client(ClientError::TurnDeadlineExceeded)),
    }
}

/// Owner-only variant of the controlled path.  `shared_journal` is opened
/// during owner bootstrap and cloned into the active protocol execution, so
/// startup recovery, approval receipts, terminal records, and shutdown all
/// share one lifecycle root instead of opening per-turn writers.
pub async fn run_turn_with_launcher_controlled_with_journal(
    prompt: String,
    launcher: RuntimeLauncher,
    approval_policy: ApprovalPolicy,
    turn_timeout: std::time::Duration,
    controls: &mut mpsc::Receiver<RuntimeControl>,
    shared_journal: Option<JournalWriter>,
    workspace: PathBuf,
) -> Result<String, AppError> {
    let live = launcher.is_live();
    let (journal, owns_journal) = match shared_journal {
        Some(writer) => (Some(writer), false),
        None => match JournalConfig::from_env() {
            Some(config) => {
                let projection = project_recovery(&config.path)?;
                let writer = JournalWriter::open(config)?;
                if let Err(error) = persist_recovery(&writer, projection).await {
                    let _ = writer.shutdown().await;
                    return Err(error);
                }
                (Some(writer), true)
            }
            None => (None, false),
        },
    };
    let execution_id = next_execution_id();
    if let Err(error) = append_journal(
        journal.as_ref(),
        JournalEvent::ExecutionStarted {
            execution_id: execution_id.clone(),
        },
    )
    .await
    {
        if owns_journal {
            if let Some(journal) = journal {
                let _ = journal.shutdown().await;
            }
        }
        return Err(error);
    }
    let approval_events_are_synchronous =
        matches!(&approval_policy, ApprovalPolicy::External { .. });
    let approval_policy = with_approval_receipt(approval_policy, journal.as_ref(), &execution_id);
    let wait_timeout = turn_timeout
        .saturating_add(CONTROLLED_CANCEL_TIMEOUT)
        .saturating_add(Duration::from_secs(1));
    let (mut client, cwd) =
        match spawn_client_with_launcher_timeout(&launcher, approval_policy, wait_timeout).await {
            Ok(client) => client,
            Err(error) => {
                let completion = append_journal(
                    journal.as_ref(),
                    JournalEvent::ExecutionCompleted {
                        execution_id,
                        status: ExecutionTerminalStatus::Failed,
                    },
                )
                .await;
                let mut result = completion.and(Err(error));
                if owns_journal {
                    if let Some(journal) = journal {
                        retain_first_error(
                            &mut result,
                            journal.shutdown().await.map_err(AppError::from),
                        );
                    }
                }
                return result;
            }
        };
    let mut control_acknowledgement = None;
    let mut result = run_controlled_flow(
        &mut client,
        &workspace,
        &prompt,
        turn_timeout,
        controls,
        ControlledFlowContext {
            journal: journal.as_ref(),
            execution_id: &execution_id,
        },
        &mut control_acknowledgement,
    )
    .await;
    retain_first_error(
        &mut result,
        append_internal_events(
            journal.as_ref(),
            &execution_id,
            client.internal_events(),
            approval_events_are_synchronous,
        )
        .await,
    );
    let ambiguous_delivery = matches!(
        &result,
        Err(AppError::Client(ClientError::AmbiguousNonIdempotent { .. }))
    );
    let cancellation_timeout = matches!(
        &result,
        Err(AppError::Client(ClientError::TurnDeadlineExceeded))
    );
    if ambiguous_delivery {
        retain_first_error(
            &mut result,
            append_journal(
                journal.as_ref(),
                JournalEvent::Incident {
                    execution_id: Some(execution_id.clone()),
                    class: "delivery_ambiguous".to_string(),
                    message: "non-idempotent delivery outcome is unknown; recovery required"
                        .to_string(),
                },
            )
            .await,
        );
    } else {
        if cancellation_timeout {
            retain_first_error(
                &mut result,
                append_journal(
                    journal.as_ref(),
                    JournalEvent::Incident {
                        execution_id: Some(execution_id.clone()),
                        class: "cancellation_timeout".to_string(),
                        message: "interrupt acknowledgement or terminal notification timed out; process group reaped"
                            .to_string(),
                    },
                )
                .await,
            );
        }
        let execution_status = result
            .as_ref()
            .map(|completion| completion.execution_status)
            .unwrap_or(ExecutionTerminalStatus::Failed);
        retain_first_error(
            &mut result,
            append_journal(
                journal.as_ref(),
                JournalEvent::ExecutionCompleted {
                    execution_id: execution_id.clone(),
                    status: execution_status,
                },
            )
            .await,
        );
    }
    retain_first_error(&mut result, client.shutdown().await.map_err(AppError::from));
    retain_first_error(&mut result, remove_ephemeral_cwd(&cwd));
    if owns_journal {
        if let Some(journal) = journal {
            retain_first_error(
                &mut result,
                journal.shutdown().await.map_err(AppError::from),
            );
        }
    }
    match result {
        Ok(completion) => {
            if let Some(acknowledgement) = control_acknowledgement {
                let _ = acknowledgement.send(ControlOutcome::Interrupted);
            }
            Ok(format!(
                "run: mode={} model={} turn_status={status}",
                mode_label(live),
                REQUIRED_MODEL,
                status = completion.status,
            ))
        }
        Err(error) => {
            if let Some(acknowledgement) = control_acknowledgement {
                let _ = acknowledgement.send(ControlOutcome::Unknown);
            }
            Err(error)
        }
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
