//! CP6 local loopback HTTP/SSE API.
//!
//! The API is deliberately local-only and narrow. It exposes the CP6 routes on
//! top of the existing runner/fake app-server path, keeps bearer tokens out of
//! request payloads, accepts only configured workspace aliases, and stores a
//! bounded replay buffer for SSE `Last-Event-ID` resume.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::env;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

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

use crate::client::{ApprovalCommand, ApprovalPolicy, PendingApproval, REQUIRED_MODEL};
use crate::orchestrator::{
    recover_before_readiness, run_turn_with_approval_policy_controlled, RuntimeControl,
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
    let mode = fs::metadata(path)
        .map_err(ApiError::TokenFile)?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        Err(ApiError::InsecureTokenFile)
    } else {
        Ok(())
    }
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

/// The one runtime authority used by the HTTP adapter.  It decides the launch
/// mode at construction, validates live bytes before reporting readiness, and
/// is the only route from a turn to the orchestrator.
struct RuntimeOwner {
    live: bool,
    snapshot: Mutex<RuntimeSnapshot>,
    /// The owner, rather than an HTTP projection, is the sole holder of turn,
    /// approval, event, and task lifecycle state. Each active execution has
    /// one command channel into the process/protocol owner.
    data: Mutex<StateData>,
}

#[derive(Clone)]
struct RuntimeSnapshot {
    ready: bool,
    model: Option<&'static str>,
    quota_available: bool,
}

impl RuntimeOwner {
    fn new(live: bool) -> Self {
        // Byte pinning is necessary but not an authenticated admission. A
        // live owner remains fail-closed until its own protocol session has
        // observed ChatGPT auth, exact model, and usable quota.
        let ready = !live;
        Self {
            live,
            snapshot: Mutex::new(RuntimeSnapshot {
                ready,
                // The offline fixture is an explicit deterministic runtime;
                // live values stay absent until a live admission has observed
                // them, rather than being fabricated by an HTTP handler.
                model: (!live).then_some(REQUIRED_MODEL),
                quota_available: !live,
            }),
            data: Mutex::new(StateData {
                next_thread: 1,
                next_turn: 1,
                next_event: 1,
                next_approval: 1,
                active_turns: 0,
                replay_bytes: 0,
                threads: HashMap::new(),
                turns: HashMap::new(),
                approvals: HashMap::new(),
            }),
        }
    }

    fn mode(&self) -> &'static str {
        if self.live {
            "live"
        } else {
            "offline-fake-app-server"
        }
    }

    fn snapshot(&self) -> RuntimeSnapshot {
        self.snapshot
            .lock()
            .expect("owner snapshot mutex poisoned")
            .clone()
    }
}

struct StateData {
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
    task: Option<tokio::task::JoinHandle<()>>,
    control: Option<mpsc::Sender<RuntimeControl>>,
    controlling_sse_active: bool,
}

struct ApprovalRecord {
    id: String,
    turn_id: String,
    status: ApprovalStatus,
    decision: Option<oneshot::Sender<ApprovalCommand>>,
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
    /// Exactly one authority has removed the one-shot sender and is awaiting
    /// the write acknowledgement from the process/protocol owner.
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
    requests_per_minute: u32,
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

#[derive(Serialize)]
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

/// A small internal rejection keeps handler results compact while preserving
/// the documented JSON response body at the HTTP boundary.
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

pub fn app(config: ApiConfig) -> Router {
    let state = AppState {
        inner: Arc::new(Inner {
            owner: RuntimeOwner::new(config.live),
            config,
        }),
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
    if !state.inner.owner.snapshot().ready {
        return Err(rejection(
            StatusCode::SERVICE_UNAVAILABLE,
            "RUNTIME_NOT_READY",
            "live runtime admission has not completed",
            true,
        ));
    }
    Ok(Json(HealthResponse { status: "ready" }))
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
    let snapshot = state.inner.owner.snapshot();
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
    let snapshot = state.inner.owner.snapshot();
    Json(RateLimitsResponse {
        requests_per_minute: u32::from(snapshot.quota_available) * 60,
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

    let mut data = state.inner.owner.data.lock().expect("owner mutex poisoned");
    if data.threads.len() >= MAX_THREADS {
        prune_terminal_records(&mut data);
    }
    if data.threads.len() >= MAX_THREADS {
        return Err(rejection(
            StatusCode::TOO_MANY_REQUESTS,
            "THREAD_CAPACITY",
            "thread capacity is full",
            true,
        ));
    }
    let id = format!("thread_{}", data.next_thread);
    data.next_thread += 1;
    data.threads.insert(
        id.clone(),
        ThreadRecord {
            workspace_alias: req.workspace_alias.clone(),
        },
    );
    Ok(Json(ThreadResponse {
        id,
        workspace_alias: req.workspace_alias,
        model: REQUIRED_MODEL,
        sandbox: "read_only",
        ephemeral: true,
    }))
}

async fn create_turn(
    State(state): State<AppState>,
    Path(thread_id): Path<String>,
    Json(req): Json<CreateTurnRequest>,
) -> Result<Json<TurnResponse>, ApiRejection> {
    reject_payload_token(req.bearer_token.as_deref())?;
    if !state.inner.owner.snapshot().ready {
        return Err(rejection(
            StatusCode::SERVICE_UNAVAILABLE,
            "RUNTIME_NOT_READY",
            "live runtime admission has not completed",
            true,
        ));
    }
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

    let (turn_id, workspace_alias) = {
        let mut data = state.inner.owner.data.lock().expect("owner mutex poisoned");
        if data.active_turns >= MAX_CONCURRENT_TURNS {
            return Err(rejection(
                StatusCode::TOO_MANY_REQUESTS,
                "SATURATED",
                "turn queue is full",
                true,
            ));
        }
        if data.turns.len() >= MAX_TURNS {
            prune_terminal_records(&mut data);
        }
        if data.turns.len() >= MAX_TURNS {
            return Err(rejection(
                StatusCode::TOO_MANY_REQUESTS,
                "TURN_CAPACITY",
                "terminal turn retention is full",
                true,
            ));
        }
        let thread = data.threads.get(&thread_id).ok_or_else(|| {
            rejection(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                "thread not found",
                false,
            )
        })?;
        if let Some(alias) = req.workspace_alias.as_deref() {
            validate_workspace(&state, alias)?;
            if alias != thread.workspace_alias {
                return Err(rejection(
                    StatusCode::BAD_REQUEST,
                    "WORKSPACE_MISMATCH",
                    "workspace alias does not match thread",
                    false,
                ));
            }
        }
        let workspace_alias = thread.workspace_alias.clone();
        let turn_id = format!("turn_{}", data.next_turn);
        data.next_turn += 1;
        data.active_turns += 1;
        let (sender, _receiver) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        data.turns.insert(
            turn_id.clone(),
            TurnRecord {
                id: turn_id.clone(),
                thread_id: thread_id.clone(),
                workspace_alias: workspace_alias.clone(),
                status: TurnStatus::Running,
                events: Vec::new(),
                event_bytes: 0,
                sender,
                task: None,
                control: None,
                controlling_sse_active: false,
            },
        );
        (turn_id, workspace_alias)
    };

    let child_state = state.clone();
    let child_turn_id = turn_id.clone();
    let approval_timeout =
        std::time::Duration::from_secs(req.timeout_seconds.unwrap_or(DEFAULT_TURN_TIMEOUT_SECONDS));
    let (control_tx, control_rx) = mpsc::channel(1);
    let task = tokio::spawn(async move {
        run_runtime_owner_turn(
            child_state,
            child_turn_id,
            req.input,
            approval_timeout,
            control_rx,
        )
        .await;
    });
    if let Some(turn) = state
        .inner
        .owner
        .data
        .lock()
        .expect("owner mutex poisoned")
        .turns
        .get_mut(&turn_id)
    {
        turn.task = Some(task);
        turn.control = Some(control_tx);
    }

    Ok(Json(TurnResponse {
        id: turn_id,
        thread_id,
        workspace_alias,
        status: TurnStatus::Running,
    }))
}

/// Offline construction is an explicit test fixture.  The HTTP layer never
/// creates approvals itself: it only receives real app-server requests from
/// this owner and returns exactly one caller decision to the original RPC id.
async fn run_runtime_owner_turn(
    state: AppState,
    turn_id: String,
    prompt: String,
    timeout: std::time::Duration,
    mut controls: mpsc::Receiver<RuntimeControl>,
) {
    push_event(
        &state,
        &turn_id,
        "turn.started",
        serde_json::json!({}),
        false,
    );
    let (pending_tx, mut pending_rx) = mpsc::channel(1);
    let runtime_state = state.clone();
    let mut run = Box::pin(async move {
        let policy = ApprovalPolicy::External {
            pending: pending_tx,
            timeout,
        };
        run_turn_with_approval_policy_controlled(
            prompt,
            runtime_state.inner.owner.live,
            policy,
            timeout,
            &mut controls,
        )
        .await
    });

    let result = loop {
        tokio::select! {
            result = &mut run => break result,
            pending = pending_rx.recv() => if let Some(pending) = pending {
                if register_runtime_approval(&state, &turn_id, pending).is_err() {
                    break Err(crate::orchestrator::AppError::Client(
                        crate::client::ClientError::SessionPoisoned,
                    ));
                }
            }
        }
    };
    if state.inner.owner.live {
        let mut snapshot = state
            .inner
            .owner
            .snapshot
            .lock()
            .expect("owner snapshot mutex poisoned");
        snapshot.ready = result.is_ok();
        snapshot.model = result.is_ok().then_some(REQUIRED_MODEL);
        snapshot.quota_available = result.is_ok();
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
        Err(_err) => (
            TurnStatus::Failed,
            "turn.failed",
            // AppError Display may contain local paths or child-controlled
            // text.  HTTP/SSE exports a bounded allowlisted class only.
            serde_json::json!({ "status": "failed", "error": { "class": "runtime_failure" } }),
        ),
    };
    let _ = update_turn_terminal(&state, &turn_id, status, kind, payload);
}

async fn get_turn(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TurnResponse>, ApiRejection> {
    let data = state.inner.owner.data.lock().expect("owner mutex poisoned");
    let turn = data
        .turns
        .get(&id)
        .ok_or_else(|| rejection(StatusCode::NOT_FOUND, "NOT_FOUND", "turn not found", false))?;
    Ok(Json(TurnResponse {
        id: turn.id.clone(),
        thread_id: turn.thread_id.clone(),
        workspace_alias: turn.workspace_alias.clone(),
        status: turn.status,
    }))
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
    let (replay, receiver, terminal, guard) = {
        let mut data = state.inner.owner.data.lock().expect("owner mutex poisoned");
        let turn = data.turns.get_mut(&id).ok_or_else(|| {
            rejection(StatusCode::NOT_FOUND, "NOT_FOUND", "turn not found", false)
        })?;
        let replay: Vec<TurnEvent> = turn
            .events
            .iter()
            .filter(|event| event.id > last_seen)
            .cloned()
            .collect();
        let terminal = is_terminal(turn.status);
        // The first authenticated non-terminal stream is the controlling
        // authority. Later streams are observers and must never cancel work.
        let guard = if !observer && !terminal && !turn.controlling_sse_active {
            turn.controlling_sse_active = true;
            Some(ControllingSseDropGuard {
                state: state.clone(),
                turn_id: id.clone(),
                control: turn.control.clone(),
            })
        } else {
            None
        };
        (replay, turn.sender.subscribe(), terminal, guard)
    };
    let replay_stream = tokio_stream::iter(replay.into_iter().map(event_to_sse));
    let live_stream = BroadcastStream::new(receiver).filter_map(move |event| match event {
        Ok(event) if event.id > last_seen => Some(event_to_sse(event)),
        Ok(_) | Err(_) => None,
    });
    let stream: BoxStream<'static, Result<SseEvent, Infallible>> = if terminal {
        Box::pin(replay_stream)
    } else {
        Box::pin(replay_stream.chain(live_stream))
    };
    Ok(Sse::new(ControllingSseStream {
        stream,
        _guard: guard,
    })
    .keep_alive(axum::response::sse::KeepAlive::new()))
}

/// Owns the controller lease for the lifetime of one SSE body. Axum drops the
/// stream when the peer disconnects; Drop is deliberately synchronous and
/// only submits bounded local commands. The process/protocol owner performs
/// the actual wire denial, cleanup, journal terminal transition, and release.
struct ControllingSseDropGuard {
    state: AppState,
    turn_id: String,
    control: Option<mpsc::Sender<RuntimeControl>>,
}

impl Drop for ControllingSseDropGuard {
    fn drop(&mut self) {
        close_pending_approvals(&self.state, &self.turn_id, "controlling_sse_drop");
        if let Some(control) = &self.control {
            let (ack, _receiver) = oneshot::channel();
            let _ = control.try_send(RuntimeControl::Interrupt(ack));
        }
        if let Some(turn) = self
            .state
            .inner
            .owner
            .data
            .lock()
            .expect("owner mutex poisoned")
            .turns
            .get_mut(&self.turn_id)
        {
            turn.controlling_sse_active = false;
        }
    }
}

struct ControllingSseStream {
    stream: BoxStream<'static, Result<SseEvent, Infallible>>,
    _guard: Option<ControllingSseDropGuard>,
}

impl Stream for ControllingSseStream {
    type Item = Result<SseEvent, Infallible>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(context)
    }
}

async fn interrupt_turn(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TurnResponse>, ApiRejection> {
    let control = {
        let mut data = state.inner.owner.data.lock().expect("owner mutex poisoned");
        let turn = data.turns.get_mut(&id).ok_or_else(|| {
            rejection(StatusCode::NOT_FOUND, "NOT_FOUND", "turn not found", false)
        })?;
        if is_terminal(turn.status) {
            return Err(rejection(
                StatusCode::BAD_REQUEST,
                "TURN_TERMINAL",
                "turn is already terminal",
                false,
            ));
        }
        turn.control.clone()
    };
    let control = control.ok_or_else(|| {
        rejection(
            StatusCode::CONFLICT,
            "TURN_CLOSED",
            "turn is no longer owned by a running runtime",
            false,
        )
    })?;
    let (ack_tx, ack_rx) = oneshot::channel();
    control
        .send(RuntimeControl::Interrupt(ack_tx))
        .await
        .map_err(|_| {
            rejection(
                StatusCode::CONFLICT,
                "TURN_CLOSED",
                "turn is no longer owned by a running runtime",
                false,
            )
        })?;
    close_pending_approvals(&state, &id, "interrupt");
    tokio::time::timeout(std::time::Duration::from_secs(6), ack_rx)
        .await
        .map_err(|_| {
            rejection(
                StatusCode::GATEWAY_TIMEOUT,
                "INTERRUPT_TIMEOUT",
                "runtime cleanup timed out",
                true,
            )
        })?
        .map_err(|_| {
            rejection(
                StatusCode::CONFLICT,
                "TURN_CLOSED",
                "runtime ended before interrupt cleanup",
                false,
            )
        })?;
    let response = update_turn_terminal(
        &state,
        &id,
        TurnStatus::Interrupted,
        "turn.interrupted",
        serde_json::json!({ "status": "interrupted" }),
    )?;
    Ok(Json(response))
}

async fn approve(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApprovalResponse>, ApiRejection> {
    decide_approval(&state, &id, ApprovalStatus::Approved).await
}

async fn deny(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApprovalResponse>, ApiRejection> {
    decide_approval(&state, &id, ApprovalStatus::Denied).await
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

fn register_runtime_approval(
    state: &AppState,
    turn_id: &str,
    pending: PendingApproval,
) -> Result<(), ApiRejection> {
    let mut data = state.inner.owner.data.lock().expect("owner mutex poisoned");
    if data.approvals.len() >= MAX_APPROVALS {
        prune_terminal_records(&mut data);
    }
    if data.approvals.len() >= MAX_APPROVALS {
        let (delivered, _ack) = oneshot::channel();
        let _ = pending.decision.send(ApprovalCommand {
            decision: ApprovalDecision::Deny,
            delivered,
        });
        return Err(rejection(
            StatusCode::TOO_MANY_REQUESTS,
            "APPROVAL_CAPACITY",
            "approval capacity is full",
            true,
        ));
    }
    let turn = data
        .turns
        .get(turn_id)
        .ok_or_else(|| rejection(StatusCode::NOT_FOUND, "NOT_FOUND", "turn not found", false))?;
    if is_terminal(turn.status) {
        let (delivered, _ack) = oneshot::channel();
        let _ = pending.decision.send(ApprovalCommand {
            decision: ApprovalDecision::Deny,
            delivered,
        });
        return Err(rejection(
            StatusCode::BAD_REQUEST,
            "TURN_TERMINAL",
            "turn is already terminal",
            false,
        ));
    }
    let id = format!("approval_{}", data.next_approval);
    data.next_approval += 1;
    data.approvals.insert(
        id.clone(),
        ApprovalRecord {
            id: id.clone(),
            turn_id: turn_id.to_string(),
            status: ApprovalStatus::Pending,
            decision: Some(pending.decision),
        },
    );
    drop(data);
    set_turn_status(state, turn_id, TurnStatus::WaitingApproval);
    push_event(
        state,
        turn_id,
        "approval.requested",
        serde_json::json!({
            "approval_id": id,
            "request_key": pending.request_key,
            "method": pending.method,
        }),
        false,
    );
    Ok(())
}

async fn decide_approval(
    state: &AppState,
    id: &str,
    decision: ApprovalStatus,
) -> Result<Json<ApprovalResponse>, ApiRejection> {
    let (approval_id, turn_id, sender) = {
        let mut data = state.inner.owner.data.lock().expect("owner mutex poisoned");
        let approval = data.approvals.get_mut(id).ok_or_else(|| {
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
        (
            approval.id.clone(),
            approval.turn_id.clone(),
            approval.decision.take().ok_or_else(|| {
                rejection(
                    StatusCode::CONFLICT,
                    "APPROVAL_CLOSED",
                    "approval is no longer owned by a running turn",
                    false,
                )
            })?,
        )
    };
    let protocol_decision = match decision {
        ApprovalStatus::Approved => ApprovalDecision::Allow,
        ApprovalStatus::Denied | ApprovalStatus::Pending | ApprovalStatus::Delivering => {
            ApprovalDecision::Deny
        }
    };
    let (delivered_tx, delivered_rx) = oneshot::channel();
    if sender
        .send(ApprovalCommand {
            decision: protocol_decision,
            delivered: delivered_tx,
        })
        .is_err()
    {
        if let Some(approval) = state
            .inner
            .owner
            .data
            .lock()
            .expect("owner mutex poisoned")
            .approvals
            .get_mut(id)
        {
            approval.status = ApprovalStatus::Denied;
        }
        return Err(rejection(
            StatusCode::CONFLICT,
            "APPROVAL_CLOSED",
            "approval is no longer owned by a running turn",
            false,
        ));
    }
    let delivered = tokio::time::timeout(std::time::Duration::from_secs(6), delivered_rx)
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or(false);
    if !delivered {
        if let Some(approval) = state
            .inner
            .owner
            .data
            .lock()
            .expect("owner mutex poisoned")
            .approvals
            .get_mut(id)
        {
            // The API never acknowledges an undelivered allow.  The owner
            // will clean up this turn on the same bounded failure path.
            approval.status = ApprovalStatus::Denied;
        }
        return Err(rejection(
            StatusCode::CONFLICT,
            "APPROVAL_DELIVERY_FAILED",
            "runtime could not deliver the approval decision",
            true,
        ));
    }
    let status = {
        let mut data = state.inner.owner.data.lock().expect("owner mutex poisoned");
        let approval = data.approvals.get_mut(id).ok_or_else(|| {
            rejection(
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                "approval not found",
                false,
            )
        })?;
        approval.status = decision;
        approval.status
    };
    push_event(
        state,
        &turn_id,
        "approval.decided",
        serde_json::json!({ "approval_id": approval_id, "decision": status }),
        false,
    );
    Ok(Json(ApprovalResponse {
        id: id.to_string(),
        turn_id,
        status,
    }))
}

fn update_turn_terminal(
    state: &AppState,
    id: &str,
    status: TurnStatus,
    kind: &str,
    payload: serde_json::Value,
) -> Result<TurnResponse, ApiRejection> {
    let (response, transitioned) = {
        let mut data = state.inner.owner.data.lock().expect("owner mutex poisoned");
        let mut finished_now = false;
        let response = {
            let turn = data.turns.get_mut(id).ok_or_else(|| {
                rejection(StatusCode::NOT_FOUND, "NOT_FOUND", "turn not found", false)
            })?;
            if !is_terminal(turn.status) {
                turn.status = status;
                finished_now = true;
            }
            TurnResponse {
                id: turn.id.clone(),
                thread_id: turn.thread_id.clone(),
                workspace_alias: turn.workspace_alias.clone(),
                status: turn.status,
            }
        };
        if finished_now {
            data.active_turns = data.active_turns.saturating_sub(1);
        }
        (response, finished_now)
    };
    if transitioned {
        close_pending_approvals(state, id, "terminal");
        push_event(state, id, kind, payload, true);
    }
    Ok(response)
}

/// Terminalisation owns approval closure.  This makes a stale URL unable to
/// approve after interrupt, timeout, shutdown, or another terminal outcome.
fn close_pending_approvals(state: &AppState, turn_id: &str, authority: &'static str) {
    let mut closed = Vec::new();
    {
        let mut data = state.inner.owner.data.lock().expect("owner mutex poisoned");
        for approval in data.approvals.values_mut() {
            if approval.turn_id == turn_id
                && matches!(
                    approval.status,
                    ApprovalStatus::Pending | ApprovalStatus::Delivering
                )
            {
                approval.status = ApprovalStatus::Denied;
                if let Some(sender) = approval.decision.take() {
                    let (delivered, _ack) = oneshot::channel();
                    let _ = sender.send(ApprovalCommand {
                        decision: ApprovalDecision::Deny,
                        delivered,
                    });
                }
                closed.push(approval.id.clone());
            }
        }
    }
    for approval_id in closed {
        push_event(
            state,
            turn_id,
            "approval.decided",
            serde_json::json!({ "approval_id": approval_id, "decision": "denied", "authority": authority }),
            false,
        );
    }
}

fn set_turn_status(state: &AppState, id: &str, status: TurnStatus) {
    if let Some(turn) = state
        .inner
        .owner
        .data
        .lock()
        .expect("owner mutex poisoned")
        .turns
        .get_mut(id)
    {
        if !is_terminal(turn.status) {
            turn.status = status;
        }
    }
}

fn push_event(
    state: &AppState,
    turn_id: &str,
    kind: &str,
    payload: serde_json::Value,
    terminal: bool,
) {
    let mut data = state.inner.owner.data.lock().expect("owner mutex poisoned");
    let id = data.next_event;
    data.next_event += 1;
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
    while data.replay_bytes.saturating_add(encoded_len) > MAX_SSE_REPLAY_BYTES {
        let Some(oldest_turn) = data
            .turns
            .iter()
            .filter_map(|(id, record)| record.events.first().map(|event| (id.clone(), event.id)))
            .min_by_key(|(_, event_id)| *event_id)
            .map(|(id, _)| id)
        else {
            return;
        };
        let removed = data
            .turns
            .get_mut(&oldest_turn)
            .and_then(|record| (!record.events.is_empty()).then(|| record.events.remove(0)));
        if let Some(removed) = removed {
            let removed_len = serde_json::to_vec(&removed).map_or(0, |encoded| encoded.len());
            data.replay_bytes = data.replay_bytes.saturating_sub(removed_len);
            if let Some(turn) = data.turns.get_mut(&oldest_turn) {
                turn.event_bytes = turn.event_bytes.saturating_sub(removed_len);
            }
        }
    }
    let (sender, removed_len) = {
        let Some(turn) = data.turns.get_mut(turn_id) else {
            return;
        };
        turn.events.push(event.clone());
        turn.event_bytes = turn.event_bytes.saturating_add(encoded_len);
        let removed_len = if turn.events.len() > MAX_EVENTS_PER_TURN {
            if let Some(removed) = (!turn.events.is_empty()).then(|| turn.events.remove(0)) {
                let removed_len = serde_json::to_vec(&removed).map_or(0, |encoded| encoded.len());
                turn.event_bytes = turn.event_bytes.saturating_sub(removed_len);
                removed_len
            } else {
                0
            }
        } else {
            0
        };
        (turn.sender.clone(), removed_len)
    };
    data.replay_bytes = data
        .replay_bytes
        .saturating_add(encoded_len)
        .saturating_sub(removed_len);
    let _ = sender.send(event);
}

/// Only terminal state is evicted. Running turns and pending approvals remain
/// authoritative until their owner has closed them, while completed records
/// cannot permanently saturate the local adapter.
fn prune_terminal_records(data: &mut StateData) {
    let terminal_turns: Vec<String> = data
        .turns
        .iter()
        .filter(|(_, turn)| is_terminal(turn.status))
        .map(|(id, _)| id.clone())
        .collect();
    for turn_id in terminal_turns {
        if let Some(turn) = data.turns.remove(&turn_id) {
            data.replay_bytes = data.replay_bytes.saturating_sub(turn.event_bytes);
        }
    }
    data.approvals.retain(|_, approval| {
        approval.status == ApprovalStatus::Pending || data.turns.contains_key(&approval.turn_id)
    });
    data.threads
        .retain(|thread_id, _| data.turns.values().any(|turn| &turn.thread_id == thread_id));
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

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    let max_len = left.len().max(right.len());
    for index in 0..max_len {
        diff |= usize::from(*left.get(index).unwrap_or(&0) ^ *right.get(index).unwrap_or(&0));
    }
    diff == 0
}

pub async fn serve(config: ApiConfig) -> Result<SocketAddr, ApiError> {
    // Recovery is complete (or startup fails) before the listener can expose
    // readiness or accept a turn.  Its detailed errors never cross HTTP.
    recover_before_readiness()
        .await
        .map_err(|error| ApiError::Serve(std::io::Error::other(error.to_string())))?;
    let bind = config.bind;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(ApiError::Serve)?;
    let addr = listener.local_addr().map_err(ApiError::Serve)?;
    axum::serve(listener, app(config))
        .await
        .map_err(ApiError::Serve)?;
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T07: global eviction and terminal pruning each account for a retained
    /// event exactly once. The old global-only subtraction left a stale
    /// per-turn total, so terminal eviction subtracted it a second time.
    #[test]
    fn global_sse_eviction_keeps_per_turn_and_global_bytes_in_sync() {
        let owner = RuntimeOwner::new(false);
        let (sender, _receiver) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        owner.data.lock().expect("owner mutex").turns.insert(
            "turn-test".to_string(),
            TurnRecord {
                id: "turn-test".to_string(),
                thread_id: "thread-test".to_string(),
                workspace_alias: "default".to_string(),
                status: TurnStatus::Completed,
                events: Vec::new(),
                event_bytes: 0,
                sender,
                task: None,
                control: None,
                controlling_sse_active: false,
            },
        );
        let state = AppState {
            inner: Arc::new(Inner {
                config: ApiConfig {
                    bind: DEFAULT_BIND,
                    bearer_token: "test".to_string(),
                    workspace_aliases: HashSet::from(["default".to_string()]),
                    live: false,
                },
                owner,
            }),
        };
        for _ in 0..80 {
            push_event(
                &state,
                "turn-test",
                "diagnostic",
                serde_json::json!({ "padding": "x".repeat(MAX_EVENT_BYTES - 256) }),
                false,
            );
        }
        let before = state
            .inner
            .owner
            .data
            .lock()
            .expect("owner mutex")
            .replay_bytes;
        assert!(before <= MAX_SSE_REPLAY_BYTES);
        prune_terminal_records(&mut state.inner.owner.data.lock().expect("owner mutex"));
        assert_eq!(
            state
                .inner
                .owner
                .data
                .lock()
                .expect("owner mutex")
                .replay_bytes,
            0,
            "pruning must not underflow a stale per-turn byte total"
        );
    }
}
