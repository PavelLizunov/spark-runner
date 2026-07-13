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
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::client::{ApprovalPolicy, REQUIRED_MODEL};
use crate::orchestrator::run_turn_with_fake_server_args_and_approval_policy;

const DEFAULT_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787);
const MAX_BODY_BYTES: usize = 16 * 1024;
const MAX_INPUT_CHARS: usize = 8 * 1024;
const MAX_EVENTS_PER_TURN: usize = 256;
const EVENT_CHANNEL_CAPACITY: usize = 256;
const MAX_CONCURRENT_TURNS: usize = 1;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("CP6 bind address must stay on 127.0.0.1, got {0}")]
    NonLoopbackBind(SocketAddr),
    #[error("SPARK_RUNNER_BIND must be host:port: {0}")]
    BindParse(String),
    #[error("failed to read SPARK_RUNNER_BEARER_TOKEN_FILE: {0}")]
    TokenFile(std::io::Error),
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
        Ok(path) if !path.is_empty() => Some(
            fs::read_to_string(path)
                .map_err(ApiError::TokenFile)?
                .trim()
                .to_string(),
        ),
        _ => None,
    };
    match (from_env, from_file) {
        (Some(_), Some(_)) => Err(ApiError::DuplicateTokenSources),
        (Some(token), None) | (None, Some(token)) if !token.is_empty() => Ok(token),
        _ => Err(ApiError::MissingBearerToken),
    }
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
    data: Mutex<StateData>,
}

struct StateData {
    next_thread: u64,
    next_turn: u64,
    next_event: u64,
    next_approval: u64,
    active_turns: usize,
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
    sender: broadcast::Sender<TurnEvent>,
}

#[derive(Clone)]
struct ApprovalRecord {
    id: String,
    turn_id: String,
    status: ApprovalStatus,
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
            config,
            data: Mutex::new(StateData {
                next_thread: 1,
                next_turn: 1,
                next_event: 1,
                next_approval: 1,
                active_turns: 0,
                threads: HashMap::new(),
                turns: HashMap::new(),
                approvals: HashMap::new(),
            }),
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
    if state.inner.config.live {
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
        mode: if state.inner.config.live {
            "live"
        } else {
            "offline-fake-app-server"
        },
        public_access: false,
        full_access: false,
        chat_completions: false,
        bind_default: "127.0.0.1:8787",
        max_body_bytes: MAX_BODY_BYTES,
        max_input_chars: MAX_INPUT_CHARS,
        workspace_aliases: aliases,
    })
}

async fn models() -> Json<ModelsResponse<'static>> {
    Json(ModelsResponse {
        object: "list",
        data: vec![ModelInfo {
            id: REQUIRED_MODEL,
            object: "model",
            owned_by: "openai",
        }],
    })
}

async fn rate_limits() -> Json<RateLimitsResponse> {
    Json(RateLimitsResponse {
        requests_per_minute: 60,
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

    let mut data = state.inner.data.lock().expect("state mutex poisoned");
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
    if state.inner.config.live {
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
        let mut data = state.inner.data.lock().expect("state mutex poisoned");
        if data.active_turns >= MAX_CONCURRENT_TURNS {
            return Err(rejection(
                StatusCode::TOO_MANY_REQUESTS,
                "SATURATED",
                "turn queue is full",
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
                sender,
            },
        );
        (turn_id, workspace_alias)
    };

    let child_state = state.clone();
    let child_turn_id = turn_id.clone();
    tokio::spawn(async move {
        run_fake_child_turn(child_state, child_turn_id, req.input).await;
    });

    Ok(Json(TurnResponse {
        id: turn_id,
        thread_id,
        workspace_alias,
        status: TurnStatus::Running,
    }))
}

async fn run_fake_child_turn(state: AppState, turn_id: String, prompt: String) {
    push_event(
        &state,
        &turn_id,
        "turn.started",
        serde_json::json!({}),
        false,
    );
    let approval_id = create_approval(&state, &turn_id);
    set_turn_status(&state, &turn_id, TurnStatus::WaitingApproval);
    push_event(
        &state,
        &turn_id,
        "approval.requested",
        serde_json::json!({ "approval_id": approval_id }),
        false,
    );
    let result = run_turn_with_fake_server_args_and_approval_policy(
        prompt,
        &["--approval-mode".to_string(), "command".to_string()],
        ApprovalPolicy::AllowForTests,
    )
    .await;
    mark_approval(&state, &approval_id, ApprovalStatus::Approved);
    push_event(
        &state,
        &turn_id,
        "approval.decided",
        serde_json::json!({ "approval_id": approval_id, "decision": "approved" }),
        false,
    );
    let (status, kind, payload) = match result {
        Ok(summary) => (
            TurnStatus::Completed,
            "turn.completed",
            serde_json::json!({ "status": "completed", "summary": summary }),
        ),
        Err(err) => (
            TurnStatus::Failed,
            "turn.failed",
            serde_json::json!({ "status": "failed", "error": err.to_string() }),
        ),
    };
    let _ = update_turn_terminal(&state, &turn_id, status, kind, payload);
}

async fn get_turn(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TurnResponse>, ApiRejection> {
    let data = state.inner.data.lock().expect("state mutex poisoned");
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
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<SseEvent, Infallible>>>, ApiRejection> {
    let last_seen = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let (replay, receiver, terminal) = {
        let data = state.inner.data.lock().expect("state mutex poisoned");
        let turn = data.turns.get(&id).ok_or_else(|| {
            rejection(StatusCode::NOT_FOUND, "NOT_FOUND", "turn not found", false)
        })?;
        let replay: Vec<TurnEvent> = turn
            .events
            .iter()
            .filter(|event| event.id > last_seen)
            .cloned()
            .collect();
        let terminal = is_terminal(turn.status);
        (replay, turn.sender.subscribe(), terminal)
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
    Ok(Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::new()))
}

async fn interrupt_turn(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TurnResponse>, ApiRejection> {
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
    decide_approval(&state, &id, ApprovalStatus::Approved)
}

async fn deny(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApprovalResponse>, ApiRejection> {
    decide_approval(&state, &id, ApprovalStatus::Denied)
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

fn create_approval(state: &AppState, turn_id: &str) -> String {
    let mut data = state.inner.data.lock().expect("state mutex poisoned");
    let id = format!("approval_{}", data.next_approval);
    data.next_approval += 1;
    data.approvals.insert(
        id.clone(),
        ApprovalRecord {
            id: id.clone(),
            turn_id: turn_id.to_string(),
            status: ApprovalStatus::Pending,
        },
    );
    id
}

fn mark_approval(state: &AppState, id: &str, status: ApprovalStatus) {
    if let Some(approval) = state
        .inner
        .data
        .lock()
        .expect("state mutex poisoned")
        .approvals
        .get_mut(id)
    {
        approval.status = status;
    }
}

fn decide_approval(
    state: &AppState,
    id: &str,
    decision: ApprovalStatus,
) -> Result<Json<ApprovalResponse>, ApiRejection> {
    let (approval_id, turn_id, status) = {
        let mut data = state.inner.data.lock().expect("state mutex poisoned");
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
        approval.status = decision;
        (
            approval.id.clone(),
            approval.turn_id.clone(),
            approval.status,
        )
    };
    push_event(
        state,
        &turn_id,
        "approval.decided",
        serde_json::json!({ "approval_id": approval_id, "decision": status }),
        false,
    );
    let (terminal_kind, terminal_status) = match decision {
        ApprovalStatus::Approved => ("turn.completed", TurnStatus::Completed),
        ApprovalStatus::Denied => ("turn.denied", TurnStatus::Interrupted),
        ApprovalStatus::Pending => unreachable!(),
    };
    let _ = update_turn_terminal(
        state,
        &turn_id,
        terminal_status,
        terminal_kind,
        serde_json::json!({ "status": terminal_kind }),
    )?;
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
    let response = {
        let mut data = state.inner.data.lock().expect("state mutex poisoned");
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
        response
    };
    push_event(state, id, kind, payload, true);
    Ok(response)
}

fn set_turn_status(state: &AppState, id: &str, status: TurnStatus) {
    if let Some(turn) = state
        .inner
        .data
        .lock()
        .expect("state mutex poisoned")
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
    let mut data = state.inner.data.lock().expect("state mutex poisoned");
    let id = data.next_event;
    data.next_event += 1;
    let Some(turn) = data.turns.get_mut(turn_id) else {
        return;
    };
    let event = TurnEvent {
        id,
        kind: kind.to_string(),
        turn_id: turn_id.to_string(),
        payload,
        terminal,
    };
    turn.events.push(event.clone());
    if turn.events.len() > MAX_EVENTS_PER_TURN {
        let overflow = turn.events.len() - MAX_EVENTS_PER_TURN;
        turn.events.drain(0..overflow);
    }
    let _ = turn.sender.send(event);
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
