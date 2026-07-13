//! High-level Codex app-server client: initialize/account/model/rate-limit
//! reads plus one ephemeral read-only thread and turn, using the stable
//! sandbox shape confirmed in CP1 (`sandbox: "read-only"`, not a map).

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::jsonl::{JsonlClient, JsonlError};
use crate::process::ChildProcess;
use crate::state::{ApprovalDecision, ApprovalSource, InternalEvent, SessionState, StateError};

pub const REQUIRED_MODEL: &str = "gpt-5.3-codex-spark";

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(transparent)]
    Jsonl(#[from] JsonlError),
    #[error(transparent)]
    State(#[from] StateError),
    #[error(
        "app-server substituted model (class={class}, hash={hash}) instead of required {required}"
    )]
    FallbackModel {
        /// Remote model names are never retained.  They can contain arbitrary
        /// child-controlled text, so diagnostics carry only a stable class and
        /// a bounded fingerprint.
        class: &'static str,
        hash: String,
        required: &'static str,
    },
    #[error("thread/start response missing thread.id")]
    MissingThreadId,
    #[error("turn/completed notification missing turn.status field")]
    MissingTurnStatus,
    #[error("turn/start response missing turn.id")]
    MissingTurnId,
    #[error("session state was poisoned by a protocol desync")]
    SessionPoisoned,
    #[error("server approval request missing a string or signed-integer id")]
    MissingServerRequestId,
    #[error("unknown server request class; session poisoned")]
    UnknownServerRequest,
    #[error("duplicate approval request {request_key}; session poisoned")]
    DuplicateApproval { request_key: String },
    #[error("unexpected response while waiting for terminal turn notification")]
    UnexpectedResponseWhileWaiting,
    #[error("protocol desync after an approval boundary; restart denied fail-closed")]
    UnresolvedApprovalRestart,
    #[error("protocol state is ambiguous after non-idempotent {method}; automatic replay denied")]
    AmbiguousNonIdempotent { method: &'static str },
    #[error("app-server account is not authenticated through the ChatGPT route")]
    ChatGptAuthRequired,
    #[error("app-server rate-limit response has no remaining quota")]
    QuotaUnavailable,
    #[error("runtime turn deadline elapsed; execution cancelled")]
    TurnDeadlineExceeded,
    #[error("app-server requested ChatGPT token refresh but the runtime owner has no bounded refresh response")]
    AuthTokensRefreshUnavailable,
}

impl ClientError {
    /// Whether this error is a JSONL protocol desync (oversized/malformed
    /// frame or an unexpected response id) that poisoned the session and may
    /// be worth a single controlled app-server restart. Other failures
    /// (fallback model, invalid state transitions, timeouts) are not
    /// automatically retried.
    pub fn is_recoverable_desync(&self) -> bool {
        matches!(self, ClientError::Jsonl(error) if error.is_desync())
    }
}

fn remote_value_hash(value: &str) -> String {
    // This is a diagnostic correlation token, not a security primitive.  It
    // deliberately hashes the complete remote value while retaining only a
    // fixed-width hexadecimal representation at every error boundary.
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn fallback_model(class: &'static str, observed: &str) -> ClientError {
    ClientError::FallbackModel {
        class,
        hash: remote_value_hash(observed),
        required: REQUIRED_MODEL,
    }
}

#[derive(Debug, Clone)]
pub enum ApprovalPolicy {
    Deny,
    AllowForTests,
    /// The sole runtime owner relays a genuine server request to an
    /// authenticated adapter.  The original JSON-RPC id never leaves this
    /// client; the adapter receives only an opaque pending handle.
    External {
        pending: mpsc::Sender<PendingApproval>,
        timeout: Duration,
    },
}

/// The protocol owner may relay a refresh request to an authenticated
/// authority, but it never obtains credentials from the environment or logs
/// them.  The default is deliberately unavailable and therefore fail-closed.
#[derive(Debug, Clone)]
pub enum AuthRefreshPolicy {
    Unavailable,
    External {
        pending: mpsc::Sender<PendingAuthRefresh>,
        timeout: Duration,
    },
}

/// A decision channel for one real app-server request.  It is deliberately
/// one-shot: duplicate HTTP decisions, disconnects and owner shutdown cannot
/// result in two JSON-RPC responses for the same request id.
#[derive(Debug)]
pub struct PendingApproval {
    pub request_key: String,
    pub method: String,
    pub decision: oneshot::Sender<ApprovalCommand>,
}

/// An approval is not considered decided by the HTTP adapter until the owner
/// has flushed the original JSON-RPC response.  This acknowledgement crosses
/// only the local owner boundary; no response payload is retained.
#[derive(Debug)]
pub struct ApprovalCommand {
    pub decision: ApprovalDecision,
    pub delivered: oneshot::Sender<bool>,
}

/// Schema-shaped token-refresh hand-off.  Values are intentionally opaque to
/// every layer except the authenticated provider and JSON-RPC writer.
#[derive(Debug)]
pub struct PendingAuthRefresh {
    pub reason: String,
    pub previous_account_id: Option<String>,
    pub response: oneshot::Sender<AuthRefreshResponse>,
}

#[derive(Debug)]
pub struct AuthRefreshResponse {
    pub access_token: String,
    pub chatgpt_account_id: String,
    pub chatgpt_plan_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ThreadStarted {
    pub thread_id: String,
    pub model: String,
}

#[derive(Debug, Clone)]
pub struct TurnCompleted {
    pub status: String,
}

pub struct CodexClient {
    rpc: JsonlClient,
    process: ChildProcess,
    state: SessionState,
    approval_policy: ApprovalPolicy,
    auth_refresh_policy: AuthRefreshPolicy,
    /// Requests can arrive while any ordinary RPC is awaited.  Keep this
    /// shared with that dispatch path so an id can never receive two answers.
    seen_approvals: Arc<Mutex<HashSet<String>>>,
}

impl CodexClient {
    pub fn new(
        process: ChildProcess,
        stdin: tokio::process::ChildStdin,
        stdout: tokio::process::ChildStdout,
    ) -> Self {
        Self::with_approval_policy(process, stdin, stdout, ApprovalPolicy::Deny)
    }

    pub fn with_approval_policy(
        process: ChildProcess,
        stdin: tokio::process::ChildStdin,
        stdout: tokio::process::ChildStdout,
        approval_policy: ApprovalPolicy,
    ) -> Self {
        Self {
            rpc: JsonlClient::new(stdin, stdout),
            process,
            state: SessionState::new(),
            approval_policy,
            auth_refresh_policy: AuthRefreshPolicy::Unavailable,
            seen_approvals: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn with_auth_refresh_policy(mut self, policy: AuthRefreshPolicy) -> Self {
        self.auth_refresh_policy = policy;
        self
    }

    /// Send an RPC call and poison the session if the app-server response
    /// desyncs (oversized/malformed frame or an unexpected response id),
    /// so every direct `CodexClient` caller observes the poison, not just
    /// the higher-level doctor/run orchestration.
    async fn rpc_call(&mut self, method: &str, params: Value) -> Result<Value, ClientError> {
        // JsonlClient delegates server requests back to this owner even while
        // an ordinary response is awaited. The cloned policy is an owner
        // capability (the external channel leads to the authenticated API),
        // never transport-side approval authority.
        let rpc = &self.rpc;
        let approval_policy = self.approval_policy.clone();
        let auth_refresh_policy = self.auth_refresh_policy.clone();
        let seen_approvals = Arc::clone(&self.seen_approvals);
        match rpc
            .call_with_server_request_handler(method, params, move |message| {
                let approval_policy = approval_policy.clone();
                let auth_refresh_policy = auth_refresh_policy.clone();
                let seen_approvals = Arc::clone(&seen_approvals);
                async move {
                    Self::dispatch_server_request_during_call(
                        rpc,
                        &approval_policy,
                        &auth_refresh_policy,
                        &seen_approvals,
                        message,
                    )
                    .await
                    .map_err(|_| JsonlError::ServerRequestDuringCall)
                }
            })
            .await
        {
            Ok(value) => Ok(value),
            Err(error) => {
                if error.is_desync() {
                    self.state.poison();
                    // A server request can be interleaved with any ordinary
                    // admission RPC.  Once its response has been attempted,
                    // restarting that whole flow would replay an irreversible
                    // approval boundary, so classify the later desync here
                    // rather than only in wait_turn_completed.
                    if !self
                        .seen_approvals
                        .lock()
                        .expect("approval id mutex poisoned")
                        .is_empty()
                    {
                        return Err(ClientError::UnresolvedApprovalRestart);
                    }
                }
                Err(error.into())
            }
        }
    }

    pub async fn initialize(&mut self) -> Result<Value, ClientError> {
        let initialized = self
            .rpc_call(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "spark-runner",
                        "version": env!("CARGO_PKG_VERSION"),
                    }
                }),
            )
            .await?;
        self.rpc.notify("initialized", json!({})).await?;
        Ok(initialized)
    }

    pub async fn account_read(&mut self) -> Result<Value, ClientError> {
        self.rpc_call("account/read", json!({})).await
    }

    pub async fn model_list(&mut self) -> Result<Value, ClientError> {
        self.rpc_call("model/list", json!({})).await
    }

    pub async fn rate_limits_read(&mut self) -> Result<Value, ClientError> {
        self.rpc_call("account/rateLimits/read", json!({})).await
    }

    /// Admission checks shared by every live turn. They run before the first
    /// non-idempotent request, so a bad account/model/quota state consumes no
    /// turn or approval capacity.
    pub async fn admit_live_turn(&mut self) -> Result<Value, ClientError> {
        let account = self.account_read().await?;
        if account.pointer("/account/type").and_then(Value::as_str) != Some("chatgpt") {
            return Err(ClientError::ChatGptAuthRequired);
        }
        let models = self.model_list().await?;
        let has_required_model = models
            .get("data")
            .or_else(|| models.get("models"))
            .and_then(Value::as_array)
            .is_some_and(|models| {
                models
                    .iter()
                    .any(|model| model.get("id").and_then(Value::as_str) == Some(REQUIRED_MODEL))
            });
        if !has_required_model {
            return Err(fallback_model(
                "missing_from_model_list",
                "missing-from-model-list",
            ));
        }
        let rate_limits = self.rate_limits_read().await?;
        // A secondary window being available does not override an exhausted
        // primary bucket (nor a workspace-credit exhaustion).  The native
        // 0.144.3 response deliberately carries both the legacy single view
        // and the metered-by-limit view; every advertised bucket must be
        // usable before we spend a non-idempotent turn request.
        let has_quota = quota_available(&rate_limits);
        if !has_quota {
            return Err(ClientError::QuotaUnavailable);
        }
        Ok(rate_limits)
    }

    /// Start an ephemeral, read-only, on-request-approval thread pinned to
    /// `REQUIRED_MODEL`. Fails closed if the server reports a different model.
    pub async fn thread_start(&mut self, cwd: &Path) -> Result<ThreadStarted, ClientError> {
        let params = json!({
            "sandbox": "read-only",
            "approvalPolicy": "on-request",
            "ephemeral": true,
            "model": REQUIRED_MODEL,
            "cwd": cwd.to_string_lossy(),
        });
        let result = self.rpc_call("thread/start", params).await?;

        let thread_id = result
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
            .ok_or(ClientError::MissingThreadId)?
            .to_string();
        let model = result
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        if model != REQUIRED_MODEL {
            self.state.poison();
            return Err(fallback_model("thread_start_model", &model));
        }

        self.state.on_thread_started()?;
        Ok(ThreadStarted { thread_id, model })
    }

    pub async fn turn_start(
        &mut self,
        thread_id: &str,
        prompt: &str,
    ) -> Result<String, ClientError> {
        let params = json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": prompt }],
        });
        let result = self.rpc_call("turn/start", params).await?;
        self.state.on_turn_started()?;
        result
            .get("turn")
            .and_then(|turn| turn.get("id"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or(ClientError::MissingTurnId)
    }

    /// Wait for the terminal `turn/completed` notification while the same
    /// owner task handles server-initiated approval requests. Raw model output
    /// is intentionally not extracted or logged here — only the status field.
    pub async fn wait_turn_completed(&mut self) -> Result<TurnCompleted, ClientError> {
        loop {
            let message = match self.rpc.next_message().await {
                Ok(message) => message,
                Err(error) => {
                    if error.is_desync() {
                        self.state.poison();
                        if !self
                            .seen_approvals
                            .lock()
                            .expect("approval id mutex poisoned")
                            .is_empty()
                        {
                            return Err(ClientError::UnresolvedApprovalRestart);
                        }
                    }
                    return Err(error.into());
                }
            };

            if let Some(method) = message.get("method").and_then(Value::as_str) {
                if message.get("id").is_some() {
                    self.handle_server_request(&message).await?;
                    continue;
                }
                if method == "turn/completed" {
                    return self
                        .handle_turn_completed(message.get("params").unwrap_or(&Value::Null));
                }
                if method == "model/rerouted" {
                    self.state.poison();
                    let observed = message
                        .get("params")
                        .and_then(|params| {
                            params
                                .get("model")
                                .or_else(|| params.get("toModel"))
                                .or_else(|| params.get("newModel"))
                        })
                        .and_then(Value::as_str)
                        .unwrap_or("rerouted")
                        .to_string();
                    return Err(fallback_model("model_rerouted", &observed));
                }
                // Notification names originate at the child.  Preserve only a
                // bounded class in diagnostics, never child-controlled text.
                tracing::debug!(
                    class = "non_terminal_notification",
                    "ignoring app-server notification"
                );
                continue;
            }

            if message.get("id").is_some() {
                self.state.poison();
                return Err(ClientError::UnexpectedResponseWhileWaiting);
            }
        }
    }

    fn handle_turn_completed(&mut self, params: &Value) -> Result<TurnCompleted, ClientError> {
        let status = params
            .get("turn")
            .and_then(|turn| turn.get("status"))
            .and_then(Value::as_str)
            .or_else(|| params.get("status").and_then(Value::as_str))
            .ok_or(ClientError::MissingTurnStatus)?
            .to_string();

        if status == "completed" {
            self.state.on_turn_completed()?;
        } else {
            self.state.on_turn_failed()?;
        }
        Ok(TurnCompleted { status })
    }

    async fn handle_server_request(&mut self, message: &Value) -> Result<(), ClientError> {
        Self::handle_server_request_parts(
            &self.rpc,
            &mut self.state,
            &self.approval_policy,
            &self.auth_refresh_policy,
            &self.seen_approvals,
            message,
        )
        .await
    }

    async fn handle_server_request_parts(
        rpc: &JsonlClient,
        state: &mut SessionState,
        approval_policy: &ApprovalPolicy,
        auth_refresh_policy: &AuthRefreshPolicy,
        seen_approvals: &Arc<Mutex<HashSet<String>>>,
        message: &Value,
    ) -> Result<(), ClientError> {
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let id = message
            .get("id")
            .filter(is_request_id)
            .cloned()
            .ok_or(ClientError::MissingServerRequestId)?;
        if method == "account/chatgptAuthTokens/refresh" {
            return Self::handle_auth_refresh(rpc, state, auth_refresh_policy, id, message).await;
        }
        if !is_known_approval_method(method) {
            rpc.respond_error(id, -32601, "method not found").await?;
            state.poison();
            return Err(ClientError::UnknownServerRequest);
        }

        let params = message.get("params").unwrap_or(&Value::Null);
        let request_key = approval_request_key(method, &id, params);
        if !seen_approvals
            .lock()
            .expect("approval id mutex poisoned")
            .insert(request_key.clone())
        {
            rpc.respond(
                id.clone(),
                approval_response(method, ApprovalDecision::Deny),
            )
            .await?;
            state.poison();
            return Err(ClientError::DuplicateApproval { request_key });
        }

        state.on_approval_requested(request_key.clone(), method.to_string())?;
        let (decision, delivered_by_command) = match approval_policy {
            ApprovalPolicy::AllowForTests => (ApprovalDecision::Allow, false),
            ApprovalPolicy::Deny => (ApprovalDecision::Deny, false),
            ApprovalPolicy::External { pending, timeout } => {
                let (decision_tx, decision_rx) = oneshot::channel();
                let pending_approval = PendingApproval {
                    request_key: request_key.clone(),
                    method: method.to_string(),
                    decision: decision_tx,
                };
                if pending.send(pending_approval).await.is_err() {
                    (ApprovalDecision::Deny, false)
                } else {
                    match tokio::time::timeout(*timeout, decision_rx).await {
                        Ok(Ok(command)) => {
                            let delivered = rpc
                                .respond(id.clone(), approval_response(method, command.decision))
                                .await
                                .is_ok();
                            let _ = command.delivered.send(delivered);
                            if !delivered {
                                return Err(ClientError::SessionPoisoned);
                            }
                            (command.decision, true)
                        }
                        // A closed local authority or deadline must still
                        // receive one schema-valid fail-closed response on the
                        // original JSON-RPC request id.
                        Ok(Err(_)) | Err(_) => (ApprovalDecision::Timeout, false),
                    }
                }
            }
        };
        // An external command writes above and explicitly acknowledges the
        // flush. All other policies write here. State/journal projection is
        // only advanced after that delivery boundary succeeds.
        if !delivered_by_command {
            rpc.respond(id, approval_response(method, decision)).await?;
        }
        state.on_approval_decided(
            request_key,
            method.to_string(),
            decision,
            ApprovalSource::Owner,
        )?;
        Ok(())
    }

    async fn dispatch_server_request_during_call(
        rpc: &JsonlClient,
        approval_policy: &ApprovalPolicy,
        auth_refresh_policy: &AuthRefreshPolicy,
        seen_approvals: &Arc<Mutex<HashSet<String>>>,
        message: Value,
    ) -> Result<(), ClientError> {
        let id = message
            .get("id")
            .filter(is_request_id)
            .cloned()
            .ok_or(ClientError::MissingServerRequestId)?;
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if method == "account/chatgptAuthTokens/refresh" {
            let mut state = SessionState::new();
            return Self::handle_auth_refresh(rpc, &mut state, auth_refresh_policy, id, &message)
                .await;
        }
        if !is_known_approval_method(method) {
            rpc.respond_error(id, -32601, "method not found").await?;
            return Err(ClientError::UnknownServerRequest);
        }
        let params = message.get("params").unwrap_or(&Value::Null);
        let request_key = approval_request_key(method, &id, params);
        if !seen_approvals
            .lock()
            .expect("approval id mutex poisoned")
            .insert(request_key.clone())
        {
            rpc.respond(id, approval_response(method, ApprovalDecision::Deny))
                .await?;
            return Err(ClientError::DuplicateApproval { request_key });
        }
        let (decision, delivered_by_command) = match approval_policy {
            ApprovalPolicy::AllowForTests => (ApprovalDecision::Allow, false),
            ApprovalPolicy::Deny => (ApprovalDecision::Deny, false),
            ApprovalPolicy::External { pending, timeout } => {
                let (decision_tx, decision_rx) = oneshot::channel();
                pending
                    .send(PendingApproval {
                        request_key: request_key.clone(),
                        method: method.to_string(),
                        decision: decision_tx,
                    })
                    .await
                    .map_err(|_| ClientError::AuthTokensRefreshUnavailable)?;
                match tokio::time::timeout(*timeout, decision_rx).await {
                    Ok(Ok(command)) => {
                        let delivered = rpc
                            .respond(id.clone(), approval_response(method, command.decision))
                            .await
                            .is_ok();
                        let _ = command.delivered.send(delivered);
                        if !delivered {
                            return Err(ClientError::SessionPoisoned);
                        }
                        (command.decision, true)
                    }
                    Ok(Err(_)) | Err(_) => (ApprovalDecision::Timeout, false),
                }
            }
        };
        if !delivered_by_command {
            rpc.respond(id, approval_response(method, decision)).await?;
        }
        Ok(())
    }

    async fn handle_auth_refresh(
        rpc: &JsonlClient,
        state: &mut SessionState,
        policy: &AuthRefreshPolicy,
        id: Value,
        message: &Value,
    ) -> Result<(), ClientError> {
        let params = message.get("params").unwrap_or(&Value::Null);
        let reason = params
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if reason != "unauthorized" {
            rpc.respond_error(id, -32602, "invalid refresh request")
                .await?;
            state.poison();
            return Err(ClientError::AuthTokensRefreshUnavailable);
        }
        let response = match policy {
            AuthRefreshPolicy::Unavailable => None,
            AuthRefreshPolicy::External { pending, timeout } => {
                let (tx, rx) = oneshot::channel();
                let request = PendingAuthRefresh {
                    reason: reason.to_string(),
                    previous_account_id: params
                        .get("previousAccountId")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    response: tx,
                };
                if pending.send(request).await.is_err() {
                    None
                } else {
                    tokio::time::timeout(*timeout, rx)
                        .await
                        .ok()
                        .and_then(Result::ok)
                }
            }
        };
        if let Some(response) = response {
            rpc.respond(
                id,
                json!({
                    "accessToken": response.access_token,
                    "chatgptAccountId": response.chatgpt_account_id,
                    "chatgptPlanType": response.chatgpt_plan_type,
                }),
            )
            .await?;
            Ok(())
        } else {
            rpc.respond_error(id, -32000, "authentication refresh unavailable")
                .await?;
            state.poison();
            Err(ClientError::AuthTokensRefreshUnavailable)
        }
    }

    /// Bounded, drained stderr tail from the child app-server process — for
    /// local diagnostics only; never written into evidence files.
    pub async fn stderr_tail(&self) -> String {
        self.process.stderr_tail().await
    }

    pub fn is_poisoned(&self) -> bool {
        self.state.is_poisoned()
    }

    pub fn internal_events(&self) -> &[InternalEvent] {
        self.state.events()
    }

    pub async fn shutdown(mut self) -> Result<(), ClientError> {
        let _ = self.state.on_shutdown();
        self.process.shutdown().await;
        Ok(())
    }

    /// Interrupt the exact live turn before process cleanup.  The generated
    /// 0.144.3 shape requires both identifiers; callers must not fabricate a
    /// terminal state without making this protocol attempt.
    pub async fn turn_interrupt(
        &mut self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<(), ClientError> {
        self.rpc_call(
            "turn/interrupt",
            json!({ "threadId": thread_id, "turnId": turn_id }),
        )
        .await?;
        Ok(())
    }
}

fn rate_limit_windows(rate_limits: &Value) -> Vec<&Value> {
    fn collect<'a>(windows: &mut Vec<&'a Value>, snapshot: &'a Value) {
        for key in ["primary", "secondary"] {
            if let Some(window) = snapshot.get(key).filter(|window| !window.is_null()) {
                windows.push(window);
            }
        }
    }
    let mut windows = Vec::new();
    if let Some(snapshot) = rate_limits.get("rateLimits") {
        collect(&mut windows, snapshot);
    }
    if let Some(by_id) = rate_limits
        .get("rateLimitsByLimitId")
        .and_then(Value::as_object)
    {
        for snapshot in by_id.values() {
            collect(&mut windows, snapshot);
        }
    }
    windows
}

fn credits_available(rate_limits: &Value) -> bool {
    fn snapshot_credits_available(snapshot: &Value) -> bool {
        snapshot.get("credits").is_none_or(|credits| {
            credits.is_null()
                || credits
                    .get("unlimited")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                || credits
                    .get("hasCredits")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
        })
    }
    snapshot_credits_available(rate_limits.get("rateLimits").unwrap_or(&Value::Null))
        && rate_limits
            .get("rateLimitsByLimitId")
            .and_then(Value::as_object)
            .is_none_or(|by_id| by_id.values().all(snapshot_credits_available))
}

fn quota_available(rate_limits: &Value) -> bool {
    let windows = rate_limit_windows(rate_limits);
    rate_limits
        .pointer("/rateLimits/rateLimitReachedType")
        .is_none_or(Value::is_null)
        && rate_limits
            .get("rateLimitsByLimitId")
            .and_then(Value::as_object)
            .is_none_or(|by_id| {
                by_id.values().all(|snapshot| {
                    snapshot
                        .get("rateLimitReachedType")
                        .is_none_or(Value::is_null)
                })
            })
        && !windows.is_empty()
        && windows.into_iter().all(|window| {
            window
                .get("usedPercent")
                .and_then(Value::as_i64)
                .is_some_and(|used| (0..100).contains(&used))
        })
        && credits_available(rate_limits)
}

fn is_known_approval_method(method: &str) -> bool {
    matches!(
        method,
        "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval"
            | "execCommandApproval"
            | "applyPatchApproval"
    )
}

fn is_request_id(value: &&Value) -> bool {
    value.as_str().is_some() || value.as_i64().is_some()
}

fn approval_request_key(method: &str, id: &Value, params: &Value) -> String {
    let stable = params
        .get("approvalId")
        .and_then(Value::as_str)
        .or_else(|| params.get("itemId").and_then(Value::as_str))
        .or_else(|| params.get("callId").and_then(Value::as_str))
        .unwrap_or("");
    if stable.is_empty() {
        format!("{method}:{id}")
    } else {
        format!("{method}:{stable}")
    }
}

fn approval_response(method: &str, decision: ApprovalDecision) -> Value {
    match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            let value = match decision {
                ApprovalDecision::Allow => "accept",
                ApprovalDecision::Deny => "cancel",
                ApprovalDecision::Timeout => "cancel",
            };
            json!({ "decision": value })
        }
        "execCommandApproval" | "applyPatchApproval" => {
            let value = match decision {
                ApprovalDecision::Allow => "approved",
                ApprovalDecision::Deny => "abort",
                ApprovalDecision::Timeout => "timed_out",
            };
            json!({ "decision": value })
        }
        "item/permissions/requestApproval" => json!({
            "permissions": {
                "fileSystem": { "entries": [] },
                "network": { "enabled": false }
            },
            "scope": "turn",
            "strictAutoReview": true
        }),
        _ => json!({ "decision": "cancel" }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T10: canonical 0.144.3 quota admission is fail-closed.  An available
    /// secondary window never overrides an exhausted primary or an explicit
    /// reached type, and malformed snapshots do not become capacity.
    #[test]
    fn canonical_rate_limit_admission_rejects_partial_or_malformed_capacity() {
        let exhausted_primary = json!({
            "rateLimits": {
                "primary": { "usedPercent": 100 },
                "secondary": { "usedPercent": 0 },
                "rateLimitReachedType": null,
                "credits": null
            },
            "rateLimitsByLimitId": null
        });
        assert!(!quota_available(&exhausted_primary));
        assert!(!credits_available(
            &json!({ "rateLimits": { "credits": { "hasCredits": false, "unlimited": false } } })
        ));

        let reached = json!({
            "rateLimits": { "primary": { "usedPercent": 0 }, "rateLimitReachedType": "workspace_owner_credits_depleted", "credits": null },
            "rateLimitsByLimitId": null
        });
        assert!(!quota_available(&reached));
        assert!(!quota_available(&json!({ "rateLimits": {} })));
    }

    /// Remote model text is never retained in an error. The canary proves a
    /// reroute value cannot cross a Display/log boundary verbatim.
    #[test]
    fn reroute_diagnostic_retains_only_bounded_hash_metadata() {
        let canary = "MODEL_CANARY_please_do_not_render";
        let error = fallback_model("model_rerouted", canary);
        let rendered = error.to_string();
        assert!(!rendered.contains(canary));
        assert!(rendered.contains("class=model_rerouted"));
        assert!(rendered.contains("hash="));
    }
}
