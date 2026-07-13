//! CP6 local loopback HTTP/SSE API.
//!
//! HTTP is intentionally only an authenticated command adapter.  The small
//! [`RuntimeOwner`] actor is the one lifecycle root: it owns admission,
//! turn/approval state, SSE replay, and the command channel into the one
//! active protocol/process execution.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::env;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{sse::Event as SseEvent, IntoResponse, Response, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::stream::BoxStream;
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::client::{
    journal_rate_limit_snapshot, ApprovalCommand, ApprovalPolicy, PendingApproval, REQUIRED_MODEL,
};
use crate::journal::{JournalConfig, JournalEvent, JournalWriter};
use crate::orchestrator::{
    bootstrap_runtime, recover_before_readiness, run_turn_with_launcher_controlled_with_journal,
    ControlOutcome, RuntimeControl, RuntimeLauncher,
};
use crate::state::ApprovalDecision;

const DEFAULT_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787);
const MAX_BODY_BYTES: usize = 16 * 1024;
const MAX_INPUT_CHARS: usize = 8 * 1024;
const MAX_EVENTS_PER_TURN: usize = 256;
const MAX_EVENT_BYTES: usize = 16 * 1024;
const MAX_SSE_REPLAY_BYTES: usize = 1024 * 1024;
const EVENT_CHANNEL_CAPACITY: usize = 256;
const MAX_CONCURRENT_TURNS: usize = 1;
const MAX_THREADS: usize = 128;
const MAX_TURNS: usize = 256;
const MAX_APPROVALS: usize = 256;
const OWNER_COMMAND_CAPACITY: usize = 128;
const OWNER_DEADLINE: Duration = Duration::from_secs(6);
const DEFAULT_TURN_TIMEOUT_SECONDS: u64 = 120;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("CP6 bind address must stay on 127.0.0.1, got {0}")]
    NonLoopbackBind(SocketAddr),
    #[error("SPARK_RUNNER_BIND must be host:port: {0}")]
    BindParse(String),
    #[error("failed to read SPARK_RUNNER_BEARER_TOKEN_FILE: {0}")]
    TokenFile(std::io::Error),
    #[error("SPARK_RUNNER_BEARER_TOKEN_FILE must not be group/world-readable")]
    InsecureTokenFile,
    #[error("set only one of SPARK_RUNNER_BEARER_TOKEN or SPARK_RUNNER_BEARER_TOKEN_FILE")]
    DuplicateTokenSources,
    #[error("configure a non-empty SPARK_RUNNER_BEARER_TOKEN or SPARK_RUNNER_BEARER_TOKEN_FILE")]
    MissingBearerToken,
    #[error("HTTP server error: {0}")]
    Serve(std::io::Error),
}

#[derive(Clone, Debug)]
pub struct ApiConfig {
    pub bind: SocketAddr,
    pub bearer_token: String,
    pub workspace_aliases: HashSet<String>,
    pub live: bool,
}

impl ApiConfig {
    pub fn from_env(live: bool) -> Result<Self, ApiError> {
        let bind = match env::var("SPARK_RUNNER_BIND") {
            Ok(raw) if !raw.is_empty() => raw.parse().map_err(|_| ApiError::BindParse(raw))?,
            _ => DEFAULT_BIND,
        };
        if bind.ip() != IpAddr::V4(Ipv4Addr::LOCALHOST) {
            return Err(ApiError::NonLoopbackBind(bind));
        }
        Ok(Self {
            bind,
            bearer_token: load_token()?,
            workspace_aliases: workspace_aliases(),
            live,
        })
    }
}

fn load_token() -> Result<String, ApiError> {
    let from_env = env::var("SPARK_RUNNER_BEARER_TOKEN")
        .ok()
        .filter(|value| !value.is_empty());
    let from_file = match env::var("SPARK_RUNNER_BEARER_TOKEN_FILE") {
        Ok(path) if !path.is_empty() => Some({
            validate_token_file_permissions(&path)?;
            fs::read_to_string(path)
                .map_err(ApiError::TokenFile)?
                .trim()
                .to_string()
        }),
        _ => None,
    };
    match (from_env, from_file) {
        (Some(_), Some(_)) => Err(ApiError::DuplicateTokenSources),
        (Some(token), None) | (None, Some(token)) if !token.is_empty() => Ok(token),
        _ => Err(ApiError::MissingBearerToken),
    }
}

#[cfg(unix)]
fn validate_token_file_permissions(path: &str) -> Result<(), ApiError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = fs::symlink_metadata(path).map_err(ApiError::TokenFile)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(ApiError::InsecureTokenFile);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_token_file_permissions(_path: &str) -> Result<(), ApiError> {
    Ok(())
}

fn workspace_aliases() -> HashSet<String> {
    env::var("SPARK_RUNNER_WORKSPACES")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|alias| !alias.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_else(|| HashSet::from(["default".to_string(), "repo".to_string()]))
}

#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    config: ApiConfig,
    owner: RuntimeOwner,
}

/// A persistent command handle. Its task is the only owner of turn state,
/// approval senders, admission snapshot, and cancellation sequencing.
#[derive(Clone)]
struct RuntimeOwner {
    tx: mpsc::Sender<OwnerCommand>,
    live: bool,
}

#[derive(Clone)]
struct RuntimeSnapshot {
    ready: bool,
    model: Option<&'static str>,
    quota_available: bool,
}

#[derive(Clone)]
struct ThreadRecord {
    workspace_alias: String,
}

struct TurnRecord {
    id: String,
    thread_id: String,
    workspace_alias: String,
    status: TurnStatus,
    events: Vec<TurnEvent>,
    event_bytes: usize,
    sender: broadcast::Sender<TurnEvent>,
    control: Option<mpsc::Sender<RuntimeControl>>,
    controlling_sse_active: bool,
}

struct ApprovalRecord {
    id: String,
    turn_id: String,
    status: ApprovalStatus,
    allow_permitted: bool,
    decision: Option<oneshot::Sender<ApprovalCommand>>,
}

struct OwnerState {
    snapshot: RuntimeSnapshot,
    // Opened once by the owner and cloned only into its active execution.
    // HTTP adapters never open, write, or shut down a journal.
    journal: Option<JournalWriter>,
    next_thread: u64,
    next_turn: u64,
    next_event: u64,
    next_approval: u64,
    active_turns: usize,
    replay_bytes: usize,
    threads: HashMap<String, ThreadRecord>,
    turns: HashMap<String, TurnRecord>,
    approvals: HashMap<String, ApprovalRecord>,
}

impl OwnerState {
    fn new(live: bool) -> Self {
        Self {
            // Offline remains immediately usable for deterministic API tests,
            // while live is fail-closed until the startup bootstrap command.
            snapshot: RuntimeSnapshot {
                ready: !live,
                model: (!live).then_some(REQUIRED_MODEL),
                quota_available: !live,
            },
            journal: None,
            next_thread: 1,
            next_turn: 1,
            next_event: 1,
            next_approval: 1,
            active_turns: 0,
            replay_bytes: 0,
            threads: HashMap::new(),
            turns: HashMap::new(),
            approvals: HashMap::new(),
        }
    }
}

enum OwnerCommand {
    Bootstrap(oneshot::Sender<bool>),
    Snapshot(oneshot::Sender<RuntimeSnapshot>),
    CreateThread {
        workspace_alias: String,
        reply: oneshot::Sender<Result<ThreadResponse, ApiRejection>>,
    },
    CreateTurn {
        thread_id: String,
        input: String,
        timeout: Duration,
        reply: oneshot::Sender<Result<TurnResponse, ApiRejection>>,
    },
    GetTurn {
        id: String,
        reply: oneshot::Sender<Result<TurnResponse, ApiRejection>>,
    },
    Subscribe {
        id: String,
        last_seen: u64,
        observer: bool,
        reply: oneshot::Sender<Result<Subscription, ApiRejection>>,
    },
    Decide {
        id: String,
        decision: ApprovalStatus,
        reply: oneshot::Sender<Result<ApprovalResponse, ApiRejection>>,
    },
    Interrupt {
        turn_id: String,
        authority: &'static str,
        reply: Option<oneshot::Sender<Result<TurnResponse, ApiRejection>>>,
    },
    ControllerDrop {
        turn_id: String,
    },
    Pending {
        turn_id: String,
        pending: Box<PendingApproval>,
    },
    ApprovalDeadline {
        turn_id: String,
        approval_id: String,
    },
    TurnDeadline {
        turn_id: String,
    },
    Finished {
        turn_id: String,
        result: Result<String, crate::orchestrator::AppError>,
    },
    #[allow(dead_code)]
    Shutdown(oneshot::Sender<()>),
}

struct Subscription {
    replay: Vec<TurnEvent>,
    receiver: broadcast::Receiver<TurnEvent>,
    terminal: bool,
    controller: bool,
}

impl RuntimeOwner {
    fn spawn(live: bool, launcher: RuntimeLauncher) -> Self {
        let (tx, rx) = mpsc::channel(OWNER_COMMAND_CAPACITY);
        let owner = Self {
            tx: tx.clone(),
            live,
        };
        tokio::spawn(owner_loop(rx, tx, live, launcher));
        owner
    }

    fn mode(&self) -> &'static str {
        if self.live {
            "live"
        } else {
            "offline-fake-app-server"
        }
    }

    async fn snapshot(&self) -> Option<RuntimeSnapshot> {
        let (reply, response) = oneshot::channel();
        self.tx.send(OwnerCommand::Snapshot(reply)).await.ok()?;
        response.await.ok()
    }

    async fn bootstrap(&self) -> bool {
        let (reply, response) = oneshot::channel();
        if self.tx.send(OwnerCommand::Bootstrap(reply)).await.is_err() {
            return false;
        }
        tokio::time::timeout(OWNER_DEADLINE, response)
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(false)
    }

    async fn command<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, ApiRejection>>) -> OwnerCommand,
    ) -> Result<T, ApiRejection> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(build(reply))
            .await
            .map_err(|_| owner_closed())?;
        tokio::time::timeout(OWNER_DEADLINE, response)
            .await
            .map_err(|_| {
                rejection(
                    StatusCode::GATEWAY_TIMEOUT,
                    "OWNER_TIMEOUT",
                    "runtime owner timed out",
                    true,
                )
            })?
            .map_err(|_| owner_closed())?
    }

    fn controller_dropped(&self, turn_id: String) {
        // `Drop` cannot await, but a full best-effort queue is not a valid
        // reason to keep a controller-owned turn running.  Schedule the
        // exact same ordered owner command and let backpressure delay, never
        // discard, cancellation.
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(OwnerCommand::ControllerDrop { turn_id }).await;
        });
    }
}

async fn owner_loop(
    mut rx: mpsc::Receiver<OwnerCommand>,
    tx: mpsc::Sender<OwnerCommand>,
    live: bool,
    launcher: RuntimeLauncher,
) {
    let mut state = OwnerState::new(live);
    while let Some(command) = rx.recv().await {
        match command {
            OwnerCommand::Bootstrap(reply) => {
                // Only `serve` and explicitly injected test owners invoke
                // this. `app(live=true)` remains inert/fail-closed, avoiding
                // accidental real authentication attempts in unit tests.
                let admitted = if ensure_owner_journal(&mut state).await.is_ok() {
                    tokio::time::timeout(OWNER_DEADLINE, bootstrap_runtime(launcher.clone()))
                        .await
                        .ok()
                        .and_then(Result::ok)
                } else {
                    None
                };
                let ready = match admitted {
                    Some(rate_limits) => match state.journal.as_ref() {
                        Some(journal) => journal
                            .append(JournalEvent::RateLimitSnapshot {
                                execution_id: "bootstrap".to_string(),
                                snapshot: journal_rate_limit_snapshot(&rate_limits),
                            })
                            .await
                            .is_ok(),
                        None => true,
                    },
                    None => false,
                };
                state.snapshot.ready = ready;
                state.snapshot.model = ready.then_some(REQUIRED_MODEL);
                state.snapshot.quota_available = ready;
                let _ = reply.send(ready);
            }
            OwnerCommand::Snapshot(reply) => {
                let _ = reply.send(state.snapshot.clone());
            }
            OwnerCommand::CreateThread {
                workspace_alias,
                reply,
            } => {
                let _ = reply.send(owner_create_thread(&mut state, workspace_alias));
            }
            OwnerCommand::CreateTurn {
                thread_id,
                input,
                timeout,
                reply,
            } => {
                let result = if ensure_owner_journal(&mut state).await.is_ok() {
                    owner_create_turn(&mut state, &tx, launcher.clone(), thread_id, input, timeout)
                } else {
                    state.snapshot.ready = false;
                    state.snapshot.model = None;
                    state.snapshot.quota_available = false;
                    Err(rejection(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "JOURNAL_UNAVAILABLE",
                        "runtime journal is unavailable",
                        true,
                    ))
                };
                let _ = reply.send(result);
            }
            OwnerCommand::GetTurn { id, reply } => {
                let _ = reply.send(turn_response(&state, &id));
            }
            OwnerCommand::Subscribe {
                id,
                last_seen,
                observer,
                reply,
            } => {
                let result = owner_subscribe(&mut state, &id, last_seen, observer);
                let _ = reply.send(result);
            }
            OwnerCommand::Decide {
                id,
                decision,
                reply,
            } => {
                let result = owner_decide(&mut state, &id, decision).await;
                let _ = reply.send(result);
            }
            OwnerCommand::Interrupt {
                turn_id,
                authority,
                reply,
            } => {
                let result = owner_interrupt(&mut state, &turn_id, authority).await;
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
            }
            OwnerCommand::ControllerDrop { turn_id } => {
                let _ = owner_interrupt(&mut state, &turn_id, "controlling_sse_drop").await;
                if let Some(turn) = state.turns.get_mut(&turn_id) {
                    turn.controlling_sse_active = false;
                }
            }
            OwnerCommand::Pending { turn_id, pending } => {
                let _ = owner_register_pending(&mut state, &tx, &turn_id, *pending).await;
            }
            OwnerCommand::ApprovalDeadline {
                turn_id,
                approval_id,
            } => {
                let pending = state.approvals.get(&approval_id).is_some_and(|approval| {
                    approval.turn_id == turn_id && approval.status == ApprovalStatus::Pending
                });
                if pending {
                    let _ = owner_interrupt(&mut state, &turn_id, "approval_timeout").await;
                }
            }
            OwnerCommand::TurnDeadline { turn_id } => {
                let _ = owner_interrupt(&mut state, &turn_id, "turn_deadline").await;
            }
            OwnerCommand::Finished { turn_id, result } => {
                owner_finished(&mut state, &turn_id, result);
            }
            OwnerCommand::Shutdown(reply) => {
                let ids: Vec<String> = state.turns.keys().cloned().collect();
                for id in ids {
                    let _ = owner_interrupt(&mut state, &id, "shutdown").await;
                }
                if let Some(journal) = state.journal.take() {
                    let _ = journal.shutdown().await;
                }
                let _ = reply.send(());
                break;
            }
        }
    }
}

/// The journal writer belongs to the owner lifecycle. Recovery is completed
/// before this writer is exposed to either bootstrap or a turn, and every
/// active client receives only a clone of this single serialized writer.
async fn ensure_owner_journal(state: &mut OwnerState) -> Result<(), crate::orchestrator::AppError> {
    if state.journal.is_none() {
        if let Some(config) = JournalConfig::from_env() {
            recover_before_readiness().await?;
            state.journal = Some(JournalWriter::open(config)?);
        }
    }
    Ok(())
}

fn owner_create_thread(
    state: &mut OwnerState,
    workspace_alias: String,
) -> Result<ThreadResponse, ApiRejection> {
    if state.threads.len() >= MAX_THREADS {
        prune_terminal_records(state);
    }
    if state.threads.len() >= MAX_THREADS {
        return Err(rejection(
            StatusCode::TOO_MANY_REQUESTS,
            "THREAD_CAPACITY",
            "thread capacity is full",
            true,
        ));
    }
    let id = format!("thread_{}", state.next_thread);
    state.next_thread += 1;
    state.threads.insert(
        id.clone(),
        ThreadRecord {
            workspace_alias: workspace_alias.clone(),
        },
    );
    Ok(ThreadResponse {
        id,
        workspace_alias,
        model: REQUIRED_MODEL,
        sandbox: "read_only",
        ephemeral: true,
    })
}

fn owner_create_turn(
    state: &mut OwnerState,
    owner_tx: &mpsc::Sender<OwnerCommand>,
    launcher: RuntimeLauncher,
    thread_id: String,
    input: String,
    timeout: Duration,
) -> Result<TurnResponse, ApiRejection> {
    if !state.snapshot.ready {
        return Err(rejection(
            StatusCode::SERVICE_UNAVAILABLE,
            "RUNTIME_NOT_READY",
            "live runtime admission has not completed",
            true,
        ));
    }
    if state.active_turns >= MAX_CONCURRENT_TURNS {
        return Err(rejection(
            StatusCode::TOO_MANY_REQUESTS,
            "SATURATED",
            "turn queue is full",
            true,
        ));
    }
    if state.turns.len() >= MAX_TURNS {
        prune_terminal_records(state);
    }
    if state.turns.len() >= MAX_TURNS {
        return Err(rejection(
            StatusCode::TOO_MANY_REQUESTS,
            "TURN_CAPACITY",
            "terminal turn retention is full",
            true,
        ));
    }
    let workspace_alias = state
        .threads
        .get(&thread_id)
        .map(|thread| thread.workspace_alias.clone())
        .ok_or_else(|| {
            rejection(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                "thread not found",
                false,
            )
        })?;
    let turn_id = format!("turn_{}", state.next_turn);
    state.next_turn += 1;
    state.active_turns += 1;
    let (sender, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
    let (control_tx, mut control_rx) = mpsc::channel(1);
    state.turns.insert(
        turn_id.clone(),
        TurnRecord {
            id: turn_id.clone(),
            thread_id: thread_id.clone(),
            workspace_alias: workspace_alias.clone(),
            status: TurnStatus::Running,
            events: Vec::new(),
            event_bytes: 0,
            sender,
            control: Some(control_tx),
            controlling_sse_active: false,
        },
    );
    push_event(
        state,
        &turn_id,
        "turn.started",
        serde_json::json!({}),
        false,
    );

    let (pending_tx, mut pending_rx) = mpsc::channel(1);
    let journal = state.journal.clone();
    let pending_owner_tx = owner_tx.clone();
    let pending_turn_id = turn_id.clone();
    tokio::spawn(async move {
        while let Some(pending) = pending_rx.recv().await {
            if pending_owner_tx
                .send(OwnerCommand::Pending {
                    turn_id: pending_turn_id.clone(),
                    pending: Box::new(pending),
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let deadline_owner_tx = owner_tx.clone();
    let deadline_turn_id = turn_id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(timeout).await;
        let _ = deadline_owner_tx
            .send(OwnerCommand::TurnDeadline {
                turn_id: deadline_turn_id,
            })
            .await;
    });
    let finished_owner_tx = owner_tx.clone();
    let finished_turn_id = turn_id.clone();
    tokio::spawn(async move {
        let result = run_turn_with_launcher_controlled_with_journal(
            input,
            launcher,
            ApprovalPolicy::External {
                pending: pending_tx,
                timeout,
                receipt: None,
            },
            // The owner timer is authoritative.  The client keeps only a
            // small grace period so its fallback cannot race ahead of the
            // durable deny -> interrupt cancellation sequence.
            timeout.saturating_add(OWNER_DEADLINE),
            &mut control_rx,
            journal,
        )
        .await;
        let _ = finished_owner_tx
            .send(OwnerCommand::Finished {
                turn_id: finished_turn_id,
                result,
            })
            .await;
    });

    Ok(TurnResponse {
        id: turn_id,
        thread_id,
        workspace_alias,
        status: TurnStatus::Running,
    })
}

fn owner_subscribe(
    state: &mut OwnerState,
    id: &str,
    last_seen: u64,
    observer: bool,
) -> Result<Subscription, ApiRejection> {
    let turn = state
        .turns
        .get_mut(id)
        .ok_or_else(|| rejection(StatusCode::NOT_FOUND, "NOT_FOUND", "turn not found", false))?;
    let replay = turn
        .events
        .iter()
        .filter(|event| event.id > last_seen)
        .cloned()
        .collect();
    let terminal = is_terminal(turn.status);
    let controller = !observer && !terminal && !turn.controlling_sse_active;
    if controller {
        turn.controlling_sse_active = true;
    }
    Ok(Subscription {
        replay,
        receiver: turn.sender.subscribe(),
        terminal,
        controller,
    })
}

async fn owner_register_pending(
    state: &mut OwnerState,
    owner_tx: &mpsc::Sender<OwnerCommand>,
    turn_id: &str,
    pending: PendingApproval,
) -> Result<(), ApiRejection> {
    let PendingApproval {
        request_key,
        method,
        descriptor,
        allow_permitted,
        deadline,
        decision,
    } = pending;
    let terminal = state
        .turns
        .get(turn_id)
        .map(|turn| is_terminal(turn.status))
        .unwrap_or(true);
    if state.approvals.len() >= MAX_APPROVALS || terminal {
        deny_pending_approval(decision).await;
        return Err(rejection(
            StatusCode::TOO_MANY_REQUESTS,
            "APPROVAL_CAPACITY",
            "approval capacity is full",
            true,
        ));
    }
    let id = format!("approval_{}", state.next_approval);
    let payload = approval_requested_payload(&id, request_key, method, descriptor);
    // `push_event` intentionally drops arbitrary oversized events to keep
    // replay bounded. An approval cannot use that generic behavior: if its
    // exact descriptor would not reach the authority, retaining its sender
    // would create a grantable invisible approval. Deny on the original
    // request before creating any local approval record instead.
    if !event_fits(state, turn_id, "approval.requested", &payload, false) {
        deny_pending_approval(decision).await;
        return Ok(());
    }
    state.next_approval += 1;
    state.approvals.insert(
        id.clone(),
        ApprovalRecord {
            id: id.clone(),
            turn_id: turn_id.to_string(),
            status: ApprovalStatus::Pending,
            allow_permitted,
            decision: Some(decision),
        },
    );
    if let Some(turn) = state.turns.get_mut(turn_id) {
        turn.status = TurnStatus::WaitingApproval;
    }
    push_event(state, turn_id, "approval.requested", payload, false);
    let timeout_tx = owner_tx.clone();
    let timeout_turn_id = turn_id.to_string();
    let timeout_approval_id = id;
    tokio::spawn(async move {
        tokio::time::sleep(deadline).await;
        let _ = timeout_tx
            .send(OwnerCommand::ApprovalDeadline {
                turn_id: timeout_turn_id,
                approval_id: timeout_approval_id,
            })
            .await;
    });
    Ok(())
}

async fn deny_pending_approval(decision: oneshot::Sender<ApprovalCommand>) {
    let (delivered, response) = oneshot::channel();
    if decision
        .send(ApprovalCommand {
            decision: ApprovalDecision::Deny,
            delivered,
            resume: None,
        })
        .is_ok()
    {
        let _ = tokio::time::timeout(OWNER_DEADLINE, response).await;
    }
}

fn approval_requested_payload(
    id: &str,
    request_key: String,
    method: String,
    descriptor: crate::client::ApprovalDescriptor,
) -> serde_json::Value {
    serde_json::json!({
        "approval_id": id,
        "request_key": request_key,
        "method": method,
        "descriptor": descriptor,
    })
}

async fn owner_decide(
    state: &mut OwnerState,
    id: &str,
    decision: ApprovalStatus,
) -> Result<ApprovalResponse, ApiRejection> {
    let (approval_id, turn_id, allow_permitted, sender) = claim_approval(state, id)?;
    let delivered_status = if decision == ApprovalStatus::Approved && !allow_permitted {
        ApprovalStatus::Denied
    } else {
        decision
    };
    let protocol_decision = match delivered_status {
        ApprovalStatus::Approved => ApprovalDecision::Allow,
        ApprovalStatus::Denied | ApprovalStatus::Pending | ApprovalStatus::Delivering => {
            ApprovalDecision::Deny
        }
    };
    if !deliver_approval(sender, protocol_decision).await {
        set_approval_status(state, id, ApprovalStatus::Denied);
        return Err(rejection(
            StatusCode::CONFLICT,
            "APPROVAL_DELIVERY_FAILED",
            "runtime could not deliver the approval decision",
            true,
        ));
    }
    set_approval_status(state, id, delivered_status);
    push_event(
        state,
        &turn_id,
        "approval.decided",
        serde_json::json!({ "approval_id": approval_id, "decision": delivered_status }),
        false,
    );
    Ok(ApprovalResponse {
        id: id.to_string(),
        turn_id,
        status: delivered_status,
    })
}

async fn owner_interrupt(
    state: &mut OwnerState,
    turn_id: &str,
    authority: &'static str,
) -> Result<TurnResponse, ApiRejection> {
    let Some(turn) = state.turns.get(turn_id) else {
        return Err(rejection(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "turn not found",
            false,
        ));
    };
    if is_terminal(turn.status) {
        return Err(rejection(
            StatusCode::BAD_REQUEST,
            "TURN_TERMINAL",
            "turn is already terminal",
            false,
        ));
    }

    // Claim every still-pending sender before the asynchronous delivery
    // boundary. A concurrent approve sees Delivering and can never overwrite
    // this fail-closed decision.
    let approval_decision = match authority {
        "approval_timeout" | "turn_deadline" => ApprovalDecision::Timeout,
        _ => ApprovalDecision::Deny,
    };
    let control = state
        .turns
        .get(turn_id)
        .and_then(|turn| turn.control.clone());
    let mut claimed = Vec::new();
    for approval in state.approvals.values_mut() {
        if approval.turn_id == turn_id && approval.status == ApprovalStatus::Pending {
            approval.status = ApprovalStatus::Delivering;
            if let Some(sender) = approval.decision.take() {
                claimed.push((approval.id.clone(), sender));
            }
        }
    }
    // Keep a cancellation approval response on the wire boundary until the
    // owner has queued its interrupt. Otherwise a fast child can terminalize
    // between a valid denial and the control command, leaving a closed channel
    // that cannot distinguish interruption from an unknown protocol outcome.
    let mut resume_after_interrupt = Vec::new();
    for (approval_id, sender) in claimed {
        let (resume_sender, resume_receiver) = if control.is_some() {
            let (sender, receiver) = oneshot::channel();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        let delivered =
            deliver_approval_with_resume(sender, approval_decision, resume_receiver).await;
        set_approval_status(state, &approval_id, ApprovalStatus::Denied);
        push_event(
            state,
            turn_id,
            "approval.decided",
            serde_json::json!({ "approval_id": approval_id, "decision": "denied", "authority": authority }),
            false,
        );
        if !delivered {
            // The runtime task owns the process and will reap it when its
            // protocol side observes the failed hand-off. Do not lie with a
            // terminal HTTP status before that task has reported completion.
            return Err(rejection(
                StatusCode::CONFLICT,
                "APPROVAL_DELIVERY_FAILED",
                "runtime could not deliver the approval decision",
                true,
            ));
        }
        if let Some(sender) = resume_sender {
            resume_after_interrupt.push(sender);
        }
    }
    let (terminal_status, terminal_kind) = cancellation_terminal(authority);
    let control = control.ok_or_else(|| {
        rejection(
            StatusCode::CONFLICT,
            "TURN_CLOSED",
            "turn is no longer owned by a running runtime",
            false,
        )
    })?;
    let (ack, response) = oneshot::channel();
    let sent = control.send(RuntimeControl::Interrupt(ack)).await.is_ok();
    for resume in resume_after_interrupt {
        let _ = resume.send(());
    }
    if !sent {
        // A closed command channel provides no proof of an interrupt. Report
        // the conservative Unknown boundary rather than manufacturing an
        // interrupted terminal state from the caller's intent.
        terminalize(
            state,
            turn_id,
            TurnStatus::Failed,
            "turn.failed",
            serde_json::json!({ "status": "failed", "error": { "class": "delivery_ambiguous" } }),
        );
        return turn_response(state, turn_id);
    }
    match tokio::time::timeout(OWNER_DEADLINE, response).await {
        Ok(Ok(ControlOutcome::Interrupted)) => {}
        Ok(Ok(ControlOutcome::Unknown)) => {
            terminalize(
                state,
                turn_id,
                TurnStatus::Failed,
                "turn.failed",
                serde_json::json!({ "status": "failed", "error": { "class": "delivery_ambiguous" } }),
            );
            return turn_response(state, turn_id);
        }
        Ok(Err(_)) => {
            // The protocol owner dropped the acknowledgement without a
            // confirmed cancellation outcome. Keep Unknown distinct from a
            // requested human interrupt.
            terminalize(
                state,
                turn_id,
                TurnStatus::Failed,
                "turn.failed",
                serde_json::json!({ "status": "failed", "error": { "class": "delivery_ambiguous" } }),
            );
            return turn_response(state, turn_id);
        }
        Err(_) => {
            return Err(rejection(
                StatusCode::GATEWAY_TIMEOUT,
                "INTERRUPT_TIMEOUT",
                "runtime cleanup timed out",
                true,
            ));
        }
    }
    // The protocol task sends this acknowledgement only after the interrupt
    // RPC, terminal notification, journal record, and process-group cleanup.
    // A deadline is still a fail-closed runtime failure for API/SSE purposes;
    // a human/API interrupt remains explicitly interrupted.
    terminalize(
        state,
        turn_id,
        terminal_status,
        terminal_kind,
        serde_json::json!({ "status": terminal_status }),
    );
    turn_response(state, turn_id)
}

fn cancellation_terminal(authority: &str) -> (TurnStatus, &'static str) {
    match authority {
        "approval_timeout" | "turn_deadline" => (TurnStatus::Failed, "turn.failed"),
        _ => (TurnStatus::Interrupted, "turn.interrupted"),
    }
}

fn claim_approval(
    state: &mut OwnerState,
    id: &str,
) -> Result<(String, String, bool, oneshot::Sender<ApprovalCommand>), ApiRejection> {
    let approval = state.approvals.get_mut(id).ok_or_else(|| {
        rejection(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "approval not found",
            false,
        )
    })?;
    if approval.status != ApprovalStatus::Pending {
        return Err(rejection(
            StatusCode::BAD_REQUEST,
            "APPROVAL_DECIDED",
            "approval already decided",
            false,
        ));
    }
    approval.status = ApprovalStatus::Delivering;
    let sender = approval.decision.take().ok_or_else(|| {
        rejection(
            StatusCode::CONFLICT,
            "APPROVAL_CLOSED",
            "approval is no longer owned by a running turn",
            false,
        )
    })?;
    Ok((
        approval.id.clone(),
        approval.turn_id.clone(),
        approval.allow_permitted,
        sender,
    ))
}

fn set_approval_status(state: &mut OwnerState, id: &str, status: ApprovalStatus) {
    if let Some(approval) = state.approvals.get_mut(id) {
        approval.status = status;
    }
}

async fn deliver_approval(
    sender: oneshot::Sender<ApprovalCommand>,
    decision: ApprovalDecision,
) -> bool {
    deliver_approval_with_resume(sender, decision, None).await
}

async fn deliver_approval_with_resume(
    sender: oneshot::Sender<ApprovalCommand>,
    decision: ApprovalDecision,
    resume: Option<oneshot::Receiver<()>>,
) -> bool {
    let (delivered, response) = oneshot::channel();
    if sender
        .send(ApprovalCommand {
            decision,
            delivered,
            resume,
        })
        .is_err()
    {
        return false;
    }
    tokio::time::timeout(OWNER_DEADLINE, response)
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or(false)
}

fn owner_finished(
    state: &mut OwnerState,
    turn_id: &str,
    result: Result<String, crate::orchestrator::AppError>,
) {
    // Admission is an owner snapshot, not an optimistic startup bit.  Any
    // live execution failure can no longer truthfully advertise an admitted
    // model/quota until a later bounded bootstrap succeeds.
    if result.is_err() {
        state.snapshot.ready = false;
        state.snapshot.model = None;
        state.snapshot.quota_available = false;
    }
    let (status, kind, payload) = match result {
        Ok(summary) if summary.contains("turn_status=completed") => (
            TurnStatus::Completed,
            "turn.completed",
            serde_json::json!({ "status": "completed", "summary": summary }),
        ),
        Ok(summary) if summary.contains("turn_status=interrupted") => (
            TurnStatus::Interrupted,
            "turn.interrupted",
            serde_json::json!({ "status": "interrupted" }),
        ),
        Ok(_) => (
            TurnStatus::Failed,
            "turn.failed",
            serde_json::json!({ "status": "failed", "error": { "class": "turn_rejected" } }),
        ),
        Err(_) => (
            TurnStatus::Failed,
            "turn.failed",
            serde_json::json!({ "status": "failed", "error": { "class": "runtime_failure" } }),
        ),
    };
    terminalize(state, turn_id, status, kind, payload);
}

fn terminalize(
    state: &mut OwnerState,
    turn_id: &str,
    status: TurnStatus,
    kind: &str,
    payload: serde_json::Value,
) {
    let transitioned = if let Some(turn) = state.turns.get_mut(turn_id) {
        if is_terminal(turn.status) {
            false
        } else {
            turn.status = status;
            true
        }
    } else {
        false
    };
    if !transitioned {
        return;
    }
    state.active_turns = state.active_turns.saturating_sub(1);
    let pending: Vec<String> = state
        .approvals
        .values_mut()
        .filter_map(|approval| {
            (approval.turn_id == turn_id && approval.status == ApprovalStatus::Pending).then(|| {
                approval.status = ApprovalStatus::Denied;
                let _ = approval.decision.take();
                approval.id.clone()
            })
        })
        .collect();
    for approval_id in pending {
        push_event(
            state,
            turn_id,
            "approval.decided",
            serde_json::json!({ "approval_id": approval_id, "decision": "denied", "authority": "terminal" }),
            false,
        );
    }
    push_event(state, turn_id, kind, payload, true);
}

#[derive(Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TurnStatus {
    Running,
    WaitingApproval,
    Interrupted,
    Completed,
    Failed,
}

#[derive(Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ApprovalStatus {
    Pending,
    Delivering,
    Approved,
    Denied,
}

#[derive(Clone, Serialize)]
struct TurnEvent {
    id: u64,
    #[serde(rename = "type")]
    kind: String,
    turn_id: String,
    payload: serde_json::Value,
    terminal: bool,
}

/// Unlike ordinary diagnostic events, approval requests must be visible in
/// their entirety before they can become actionable. Keep the size check at
/// the owner boundary, where the final SSE envelope and exact event id are
/// both known.
fn event_fits(
    state: &OwnerState,
    turn_id: &str,
    kind: &str,
    payload: &serde_json::Value,
    terminal: bool,
) -> bool {
    serde_json::to_vec(&TurnEvent {
        id: state.next_event,
        kind: kind.to_string(),
        turn_id: turn_id.to_string(),
        payload: payload.clone(),
        terminal,
    })
    .is_ok_and(|encoded| encoded.len() <= MAX_EVENT_BYTES)
}

#[derive(Serialize)]
struct HealthResponse<'a> {
    status: &'a str,
}

#[derive(Serialize)]
struct RuntimeResponse {
    runtime: &'static str,
    mode: &'static str,
    public_access: bool,
    full_access: bool,
    chat_completions: bool,
    bind_default: &'static str,
    max_body_bytes: usize,
    max_input_chars: usize,
    workspace_aliases: Vec<String>,
}

#[derive(Serialize)]
struct ModelsResponse<'a> {
    object: &'a str,
    data: Vec<ModelInfo<'a>>,
}

#[derive(Serialize)]
struct ModelInfo<'a> {
    id: &'a str,
    object: &'a str,
    owned_by: &'a str,
}

#[derive(Serialize)]
struct RateLimitsResponse {
    quota_available: bool,
    concurrent_turns: usize,
    max_body_bytes: usize,
    max_input_chars: usize,
}

#[derive(Deserialize)]
struct CreateThreadRequest {
    workspace_alias: String,
    model: Option<String>,
    sandbox: Option<String>,
    ephemeral: Option<bool>,
    bearer_token: Option<String>,
}

#[derive(Serialize)]
struct ThreadResponse {
    id: String,
    workspace_alias: String,
    model: &'static str,
    sandbox: &'static str,
    ephemeral: bool,
}

#[derive(Deserialize)]
struct CreateTurnRequest {
    workspace_alias: Option<String>,
    input: String,
    timeout_seconds: Option<u64>,
    bearer_token: Option<String>,
}

#[derive(Clone, Serialize)]
struct TurnResponse {
    id: String,
    thread_id: String,
    workspace_alias: String,
    status: TurnStatus,
}

#[derive(Serialize)]
struct ApprovalResponse {
    id: String,
    turn_id: String,
    status: ApprovalStatus,
}

#[derive(Serialize)]
struct ErrorResponse<'a> {
    error: ErrorBody<'a>,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'a str,
    message: &'a str,
    retryable: bool,
}

#[derive(Clone, Copy)]
struct ApiRejection {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    retryable: bool,
}

impl IntoResponse for ApiRejection {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                    retryable: self.retryable,
                },
            }),
        )
            .into_response()
    }
}

/// Normal construction uses the pinned live launcher or the canonical fake
/// fixture. It deliberately does not bootstrap live mode; `serve` performs
/// that bounded startup action before accepting a connection.
pub fn app(config: ApiConfig) -> Router {
    let launcher = RuntimeLauncher::for_mode(config.live);
    app_with_owner(config, RuntimeOwner::spawn(launcher.is_live(), launcher))
}

/// Test-only-friendly construction for an explicitly injected launcher. The
/// injected launcher still traverses the same owner bootstrap and turn path;
/// it never changes the production `RuntimeLauncher::Live` selection.
pub fn app_with_launcher(config: ApiConfig, launcher: RuntimeLauncher) -> Router {
    let owner = RuntimeOwner::spawn(config.live, launcher);
    let bootstrap_owner = owner.clone();
    tokio::spawn(async move {
        let _ = bootstrap_owner.bootstrap().await;
    });
    app_with_owner(config, owner)
}

fn app_with_owner(config: ApiConfig, owner: RuntimeOwner) -> Router {
    let state = AppState {
        inner: Arc::new(Inner { config, owner }),
    };
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/runtime", get(runtime))
        .route("/v1/models", get(models))
        .route("/v1/rate-limits", get(rate_limits))
        .route("/v1/threads", post(create_thread))
        .route("/v1/threads/:id/turns", post(create_turn))
        .route("/v1/turns/:id", get(get_turn))
        .route("/v1/turns/:id/events", get(turn_events))
        .route("/v1/turns/:id/interrupt", post(interrupt_turn))
        .route("/v1/approvals/:id/approve", post(approve))
        .route("/v1/approvals/:id/deny", post(deny))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(middleware::from_fn_with_state(state.clone(), auth))
        .with_state(state)
}

async fn auth(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Request<Body>,
    next: Next,
) -> Response {
    if request.uri().path() == "/health" {
        return next.run(request).await;
    }
    let expected = &state.inner.config.bearer_token;
    let authorized = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| constant_time_eq(token.as_bytes(), expected.as_bytes()));
    if authorized {
        next.run(request).await
    } else {
        rejection(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "missing or invalid bearer token",
            false,
        )
        .into_response()
    }
}

async fn health() -> Json<HealthResponse<'static>> {
    Json(HealthResponse { status: "ok" })
}

async fn ready(
    State(state): State<AppState>,
) -> Result<Json<HealthResponse<'static>>, ApiRejection> {
    if state
        .inner
        .owner
        .snapshot()
        .await
        .is_some_and(|snapshot| snapshot.ready)
    {
        Ok(Json(HealthResponse { status: "ready" }))
    } else {
        Err(rejection(
            StatusCode::SERVICE_UNAVAILABLE,
            "RUNTIME_NOT_READY",
            "live runtime admission has not completed",
            true,
        ))
    }
}

async fn runtime(State(state): State<AppState>) -> Json<RuntimeResponse> {
    let mut aliases: Vec<String> = state
        .inner
        .config
        .workspace_aliases
        .iter()
        .cloned()
        .collect();
    aliases.sort();
    Json(RuntimeResponse {
        runtime: "spark-runner",
        mode: state.inner.owner.mode(),
        public_access: false,
        full_access: false,
        chat_completions: false,
        bind_default: "127.0.0.1:8787",
        max_body_bytes: MAX_BODY_BYTES,
        max_input_chars: MAX_INPUT_CHARS,
        workspace_aliases: aliases,
    })
}

async fn models(State(state): State<AppState>) -> Json<ModelsResponse<'static>> {
    let snapshot = state
        .inner
        .owner
        .snapshot()
        .await
        .unwrap_or(RuntimeSnapshot {
            ready: false,
            model: None,
            quota_available: false,
        });
    Json(ModelsResponse {
        object: "list",
        data: snapshot
            .model
            .map(|id| ModelInfo {
                id,
                object: "model",
                owned_by: "openai",
            })
            .into_iter()
            .collect(),
    })
}

async fn rate_limits(State(state): State<AppState>) -> Json<RateLimitsResponse> {
    let snapshot = state
        .inner
        .owner
        .snapshot()
        .await
        .unwrap_or(RuntimeSnapshot {
            ready: false,
            model: None,
            quota_available: false,
        });
    Json(RateLimitsResponse {
        quota_available: snapshot.quota_available,
        concurrent_turns: MAX_CONCURRENT_TURNS,
        max_body_bytes: MAX_BODY_BYTES,
        max_input_chars: MAX_INPUT_CHARS,
    })
}

async fn create_thread(
    State(state): State<AppState>,
    Json(req): Json<CreateThreadRequest>,
) -> Result<Json<ThreadResponse>, ApiRejection> {
    reject_payload_token(req.bearer_token.as_deref())?;
    validate_workspace(&state, &req.workspace_alias)?;
    if req
        .model
        .as_deref()
        .is_some_and(|model| model != REQUIRED_MODEL)
    {
        return Err(rejection(
            StatusCode::BAD_REQUEST,
            "MODEL_UNAVAILABLE",
            "only the pinned Spark model is accepted",
            false,
        ));
    }
    if req
        .sandbox
        .as_deref()
        .is_some_and(|sandbox| sandbox != "read_only")
    {
        return Err(rejection(
            StatusCode::BAD_REQUEST,
            "INVALID_SANDBOX",
            "only read_only sandbox is accepted",
            false,
        ));
    }
    if req.ephemeral == Some(false) {
        return Err(rejection(
            StatusCode::BAD_REQUEST,
            "INVALID_THREAD",
            "threads must be ephemeral",
            false,
        ));
    }
    state
        .inner
        .owner
        .command(|reply| OwnerCommand::CreateThread {
            workspace_alias: req.workspace_alias,
            reply,
        })
        .await
        .map(Json)
}

async fn create_turn(
    State(state): State<AppState>,
    Path(thread_id): Path<String>,
    Json(req): Json<CreateTurnRequest>,
) -> Result<Json<TurnResponse>, ApiRejection> {
    reject_payload_token(req.bearer_token.as_deref())?;
    if req.input.is_empty() || req.input.chars().count() > MAX_INPUT_CHARS {
        return Err(rejection(
            StatusCode::BAD_REQUEST,
            "CONTEXT_LIMIT",
            "input length outside allowed bounds",
            false,
        ));
    }
    if req
        .timeout_seconds
        .is_some_and(|seconds| seconds == 0 || seconds > 300)
    {
        return Err(rejection(
            StatusCode::BAD_REQUEST,
            "TIMEOUT_LIMIT",
            "timeout_seconds must be 1..=300",
            false,
        ));
    }
    if let Some(alias) = req.workspace_alias.as_deref() {
        validate_workspace(&state, alias)?;
    }
    let timeout = Duration::from_secs(req.timeout_seconds.unwrap_or(DEFAULT_TURN_TIMEOUT_SECONDS));
    state
        .inner
        .owner
        .command(|reply| OwnerCommand::CreateTurn {
            thread_id,
            input: req.input,
            timeout,
            reply,
        })
        .await
        .map(Json)
}

async fn get_turn(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TurnResponse>, ApiRejection> {
    state
        .inner
        .owner
        .command(|reply| OwnerCommand::GetTurn { id, reply })
        .await
        .map(Json)
}

async fn turn_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Sse<ControllingSseStream>, ApiRejection> {
    let last_seen = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let observer = headers
        .get("x-spark-runner-observer")
        .and_then(|value| value.to_str().ok())
        == Some("1");
    let subscription = state
        .inner
        .owner
        .command(|reply| OwnerCommand::Subscribe {
            id: id.clone(),
            last_seen,
            observer,
            reply,
        })
        .await?;
    let replay = tokio_stream::iter(subscription.replay);
    let live = BroadcastStream::new(subscription.receiver).filter_map(move |event| match event {
        Ok(event) if event.id > last_seen => Some(event),
        Ok(_) | Err(_) => None,
    });
    let stream: BoxStream<'static, TurnEvent> = if subscription.terminal {
        Box::pin(replay)
    } else {
        Box::pin(replay.chain(live))
    };
    let guard = subscription.controller.then(|| ControllingSseDropGuard {
        owner: state.inner.owner.clone(),
        turn_id: id,
    });
    Ok(Sse::new(ControllingSseStream {
        stream,
        terminal_seen: false,
        _guard: guard,
    })
    .keep_alive(axum::response::sse::KeepAlive::new()))
}

struct ControllingSseDropGuard {
    owner: RuntimeOwner,
    turn_id: String,
}
impl Drop for ControllingSseDropGuard {
    fn drop(&mut self) {
        self.owner.controller_dropped(self.turn_id.clone());
    }
}
struct ControllingSseStream {
    stream: BoxStream<'static, TurnEvent>,
    terminal_seen: bool,
    _guard: Option<ControllingSseDropGuard>,
}
impl Stream for ControllingSseStream {
    type Item = Result<SseEvent, Infallible>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.terminal_seen {
            return std::task::Poll::Ready(None);
        }
        match self.stream.as_mut().poll_next(context) {
            std::task::Poll::Ready(Some(event)) => {
                self.terminal_seen = event.terminal;
                std::task::Poll::Ready(Some(event_to_sse(event)))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

async fn interrupt_turn(
    State(state): State<AppState>,
    Path(turn_id): Path<String>,
) -> Result<Json<TurnResponse>, ApiRejection> {
    state
        .inner
        .owner
        .command(|reply| OwnerCommand::Interrupt {
            turn_id,
            authority: "interrupt",
            reply: Some(reply),
        })
        .await
        .map(Json)
}

async fn approve(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApprovalResponse>, ApiRejection> {
    decide_approval(&state, id, ApprovalStatus::Approved).await
}
async fn deny(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApprovalResponse>, ApiRejection> {
    decide_approval(&state, id, ApprovalStatus::Denied).await
}
async fn decide_approval(
    state: &AppState,
    id: String,
    decision: ApprovalStatus,
) -> Result<Json<ApprovalResponse>, ApiRejection> {
    state
        .inner
        .owner
        .command(|reply| OwnerCommand::Decide {
            id,
            decision,
            reply,
        })
        .await
        .map(Json)
}

fn turn_response(state: &OwnerState, id: &str) -> Result<TurnResponse, ApiRejection> {
    let turn = state
        .turns
        .get(id)
        .ok_or_else(|| rejection(StatusCode::NOT_FOUND, "NOT_FOUND", "turn not found", false))?;
    Ok(TurnResponse {
        id: turn.id.clone(),
        thread_id: turn.thread_id.clone(),
        workspace_alias: turn.workspace_alias.clone(),
        status: turn.status,
    })
}

fn reject_payload_token(value: Option<&str>) -> Result<(), ApiRejection> {
    if value.is_some() {
        Err(rejection(
            StatusCode::BAD_REQUEST,
            "TOKEN_IN_PAYLOAD",
            "bearer token is accepted only in the Authorization header",
            false,
        ))
    } else {
        Ok(())
    }
}
fn validate_workspace(state: &AppState, alias: &str) -> Result<(), ApiRejection> {
    if alias.contains('/') || alias.contains('\\') || alias.contains("..") || alias.is_empty() {
        return Err(rejection(
            StatusCode::BAD_REQUEST,
            "INVALID_WORKSPACE",
            "workspace must be an alias, not a path",
            false,
        ));
    }
    if !state.inner.config.workspace_aliases.contains(alias) {
        return Err(rejection(
            StatusCode::BAD_REQUEST,
            "UNKNOWN_WORKSPACE",
            "unknown workspace alias",
            false,
        ));
    }
    Ok(())
}

fn push_event(
    state: &mut OwnerState,
    turn_id: &str,
    kind: &str,
    payload: serde_json::Value,
    terminal: bool,
) {
    let id = state.next_event;
    state.next_event += 1;
    let event = TurnEvent {
        id,
        kind: kind.to_string(),
        turn_id: turn_id.to_string(),
        payload,
        terminal,
    };
    let encoded_len = serde_json::to_vec(&event).map_or(usize::MAX, |encoded| encoded.len());
    if encoded_len > MAX_EVENT_BYTES {
        return;
    }
    while state.replay_bytes.saturating_add(encoded_len) > MAX_SSE_REPLAY_BYTES {
        let Some(oldest_turn) = state
            .turns
            .iter()
            .filter_map(|(id, record)| record.events.first().map(|event| (id.clone(), event.id)))
            .min_by_key(|(_, event_id)| *event_id)
            .map(|(id, _)| id)
        else {
            return;
        };
        let removed = state
            .turns
            .get_mut(&oldest_turn)
            .and_then(|record| (!record.events.is_empty()).then(|| record.events.remove(0)));
        if let Some(removed) = removed {
            let bytes = serde_json::to_vec(&removed).map_or(0, |encoded| encoded.len());
            state.replay_bytes = state.replay_bytes.saturating_sub(bytes);
            if let Some(turn) = state.turns.get_mut(&oldest_turn) {
                turn.event_bytes = turn.event_bytes.saturating_sub(bytes);
            }
        }
    }
    let Some(turn) = state.turns.get_mut(turn_id) else {
        return;
    };
    turn.events.push(event.clone());
    turn.event_bytes = turn.event_bytes.saturating_add(encoded_len);
    let removed_len = if turn.events.len() > MAX_EVENTS_PER_TURN {
        turn.events
            .first()
            .cloned()
            .map(|removed| {
                turn.events.remove(0);
                let bytes = serde_json::to_vec(&removed).map_or(0, |encoded| encoded.len());
                turn.event_bytes = turn.event_bytes.saturating_sub(bytes);
                bytes
            })
            .unwrap_or(0)
    } else {
        0
    };
    state.replay_bytes = state
        .replay_bytes
        .saturating_add(encoded_len)
        .saturating_sub(removed_len);
    let _ = turn.sender.send(event);
}

fn prune_terminal_records(state: &mut OwnerState) {
    let terminal: Vec<String> = state
        .turns
        .iter()
        .filter(|(_, turn)| is_terminal(turn.status))
        .map(|(id, _)| id.clone())
        .collect();
    for id in terminal {
        if let Some(turn) = state.turns.remove(&id) {
            state.replay_bytes = state.replay_bytes.saturating_sub(turn.event_bytes);
        }
    }
    state.approvals.retain(|_, approval| {
        approval.status == ApprovalStatus::Pending || state.turns.contains_key(&approval.turn_id)
    });
    // Threads are independently capacity-accounted. Dropping idle threads
    // here would make the public thread cap bypassable by repeated creates.
}

fn is_terminal(status: TurnStatus) -> bool {
    matches!(
        status,
        TurnStatus::Interrupted | TurnStatus::Completed | TurnStatus::Failed
    )
}
fn event_to_sse(event: TurnEvent) -> Result<SseEvent, Infallible> {
    let data = serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string());
    Ok(SseEvent::default()
        .id(event.id.to_string())
        .event(event.kind)
        .data(data))
}
fn rejection(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    retryable: bool,
) -> ApiRejection {
    ApiRejection {
        status,
        code,
        message,
        retryable,
    }
}
fn owner_closed() -> ApiRejection {
    rejection(
        StatusCode::CONFLICT,
        "TURN_CLOSED",
        "runtime owner is not available",
        false,
    )
}
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        diff |= usize::from(*left.get(index).unwrap_or(&0) ^ *right.get(index).unwrap_or(&0));
    }
    diff == 0
}

pub async fn serve(config: ApiConfig) -> Result<SocketAddr, ApiError> {
    recover_before_readiness()
        .await
        .map_err(|error| ApiError::Serve(std::io::Error::other(error.to_string())))?;
    let launcher = RuntimeLauncher::for_mode(config.live);
    let owner = RuntimeOwner::spawn(config.live, launcher);
    // A failed bootstrap is deliberately not fatal to loopback diagnostics,
    // but it leaves /ready and create-turn fail-closed. Successful admission
    // is the only transition which exposes the pinned model and quota.
    let _ = owner.bootstrap().await;
    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .map_err(ApiError::Serve)?;
    let addr = listener.local_addr().map_err(ApiError::Serve)?;
    axum::serve(listener, app_with_owner(config, owner))
        .await
        .map_err(ApiError::Serve)?;
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ApprovalDescriptor;

    #[tokio::test]
    async fn oversized_approval_descriptor_is_denied_before_becoming_actionable() {
        let mut state = OwnerState::new(false);
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        state.turns.insert(
            "turn_1".to_string(),
            TurnRecord {
                id: "turn_1".to_string(),
                thread_id: "thread_1".to_string(),
                workspace_alias: "repo".to_string(),
                status: TurnStatus::Running,
                events: Vec::new(),
                event_bytes: 0,
                sender: events,
                control: None,
                controlling_sse_active: false,
            },
        );
        let descriptor = ApprovalDescriptor {
            kind: "command",
            reviewable: true,
            command: None,
            // The existing schema allows this many arguments, and each is
            // reviewable text. Their UTF-8 bytes nevertheless exceed the
            // complete SSE event ceiling, which must make the request
            // deny-only rather than silently invisible.
            command_arguments: Some(vec!["界".repeat(512); 16]),
            cwd: None,
            environment_id: None,
            network_approval: None,
            reason: None,
            file_changes: Vec::new(),
            requested_permissions: None,
            requested_permission_profile: None,
            permission_grant_scope: None,
            strict_auto_review: None,
        };
        let payload = approval_requested_payload(
            "approval_1",
            "approval:test".to_string(),
            "execCommandApproval".to_string(),
            descriptor.clone(),
        );
        assert!(
            !event_fits(&state, "turn_1", "approval.requested", &payload, false),
            "fixture must exercise the whole-event, not per-field, limit"
        );

        let (decision, received) = oneshot::channel::<ApprovalCommand>();
        let acknowledgement = tokio::spawn(async move {
            let command = received.await.expect("owner must make a decision");
            assert_eq!(command.decision, ApprovalDecision::Deny);
            command
                .delivered
                .send(true)
                .expect("owner is still waiting for delivery");
        });
        let (owner_tx, _owner_rx) = mpsc::channel(1);
        let registered = owner_register_pending(
            &mut state,
            &owner_tx,
            "turn_1",
            PendingApproval {
                request_key: "approval:test".to_string(),
                method: "execCommandApproval".to_string(),
                descriptor,
                allow_permitted: true,
                deadline: Duration::from_secs(1),
                decision,
            },
        )
        .await;
        assert!(
            registered.is_ok(),
            "an invisible approval is denied, not registered as an owner failure"
        );
        acknowledgement
            .await
            .expect("decision acknowledgement task");

        assert!(state.approvals.is_empty());
        assert!(state.turns["turn_1"].events.is_empty());
        assert!(matches!(state.turns["turn_1"].status, TurnStatus::Running));
    }
}
