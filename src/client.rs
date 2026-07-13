//! High-level Codex app-server client: initialize/account/model/rate-limit
//! reads plus one ephemeral read-only thread and turn, using the stable
//! sandbox shape confirmed in CP1 (`sandbox: "read-only"`, not a map).

use std::collections::HashSet;
use std::path::Path;

use serde_json::{json, Value};

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
    #[error("app-server substituted model {observed:?} instead of required {required}")]
    FallbackModel {
        observed: String,
        required: &'static str,
    },
    #[error("thread/start response missing thread.id")]
    MissingThreadId,
    #[error("turn/completed notification missing turn.status field")]
    MissingTurnStatus,
    #[error("session state was poisoned by a protocol desync")]
    SessionPoisoned,
    #[error("server approval request missing a string or signed-integer id")]
    MissingServerRequestId,
    #[error("unknown server request method {method:?}; session poisoned")]
    UnknownServerRequest { method: String },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalPolicy {
    Deny,
    AllowForTests,
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
    seen_approvals: HashSet<String>,
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
            seen_approvals: HashSet::new(),
        }
    }

    /// Send an RPC call and poison the session if the app-server response
    /// desyncs (oversized/malformed frame or an unexpected response id),
    /// so every direct `CodexClient` caller observes the poison, not just
    /// the higher-level doctor/run orchestration.
    async fn rpc_call(&mut self, method: &str, params: Value) -> Result<Value, ClientError> {
        match self.rpc.call(method, params).await {
            Ok(value) => Ok(value),
            Err(error) => {
                if error.is_desync() {
                    self.state.poison();
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
        if account.get("accountType").and_then(Value::as_str) != Some("chatgpt") {
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
            return Err(ClientError::FallbackModel {
                observed: "missing-from-model-list".to_string(),
                required: REQUIRED_MODEL,
            });
        }
        let rate_limits = self.rate_limits_read().await?;
        let has_quota = rate_limits
            .pointer("/limits")
            .and_then(Value::as_array)
            .is_some_and(|limits| {
                limits.iter().any(|limit| {
                    limit
                        .get("remaining")
                        .and_then(Value::as_i64)
                        .unwrap_or_default()
                        > 0
                })
            });
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
            .or_else(|| result.get("threadId").and_then(Value::as_str))
            .ok_or(ClientError::MissingThreadId)?
            .to_string();
        let model = result
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        if model != REQUIRED_MODEL {
            self.state.poison();
            return Err(ClientError::FallbackModel {
                observed: model,
                required: REQUIRED_MODEL,
            });
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
        Ok(result
            .get("turnId")
            .and_then(Value::as_str)
            .unwrap_or("unknown-turn")
            .to_string())
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
                        if !self.seen_approvals.is_empty() {
                            return Err(ClientError::UnresolvedApprovalRestart);
                        }
                    }
                    return Err(error.into());
                }
            };

            if let Some(method) = message.get("method").and_then(Value::as_str) {
                if message.get("id").is_some() {
                    self.handle_server_request(&message, method).await?;
                    continue;
                }
                if method == "turn/completed" {
                    return self
                        .handle_turn_completed(message.get("params").unwrap_or(&Value::Null));
                }
                tracing::debug!(method, "ignoring non-terminal app-server notification");
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

    async fn handle_server_request(
        &mut self,
        message: &Value,
        method: &str,
    ) -> Result<(), ClientError> {
        let id = message
            .get("id")
            .filter(is_request_id)
            .cloned()
            .ok_or(ClientError::MissingServerRequestId)?;
        if !is_known_approval_method(method) {
            let _ = self.rpc.respond_error(id, -32601, "method not found").await;
            self.state.poison();
            return Err(ClientError::UnknownServerRequest {
                method: method.to_string(),
            });
        }

        let params = message.get("params").unwrap_or(&Value::Null);
        let request_key = approval_request_key(method, &id, params);
        if !self.seen_approvals.insert(request_key.clone()) {
            let _ = self
                .rpc
                .respond(
                    id.clone(),
                    approval_response(method, ApprovalDecision::Deny),
                )
                .await;
            self.state.poison();
            return Err(ClientError::DuplicateApproval { request_key });
        }

        self.state
            .on_approval_requested(request_key.clone(), method.to_string())?;
        let decision = match self.approval_policy {
            ApprovalPolicy::AllowForTests => ApprovalDecision::Allow,
            ApprovalPolicy::Deny => ApprovalDecision::Deny,
        };
        self.state.on_approval_decided(
            request_key,
            method.to_string(),
            decision,
            ApprovalSource::Owner,
        )?;
        self.rpc
            .respond(id, approval_response(method, decision))
            .await?;
        Ok(())
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
