//! High-level Codex app-server client: initialize/account/model/rate-limit
//! reads plus one ephemeral read-only thread and turn, using the stable
//! sandbox shape confirmed in CP1 (`sandbox: "read-only"`, not a map).

use std::path::Path;

use serde_json::{json, Value};

use crate::jsonl::{JsonlClient, JsonlError};
use crate::process::ChildProcess;
use crate::state::{SessionState, StateError};

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
}

impl CodexClient {
    pub fn new(
        process: ChildProcess,
        stdin: tokio::process::ChildStdin,
        stdout: tokio::process::ChildStdout,
    ) -> Self {
        Self {
            rpc: JsonlClient::new(stdin, stdout),
            process,
            state: SessionState::new(),
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

    /// Same as [`Self::rpc_call`] but for notification waits.
    async fn rpc_wait_for_notification(&mut self, method: &str) -> Result<Value, ClientError> {
        match self.rpc.wait_for_notification(method).await {
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
        self.rpc_call(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "spark-runner",
                    "version": env!("CARGO_PKG_VERSION"),
                }
            }),
        )
        .await
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

    pub async fn turn_start(&mut self, thread_id: &str, prompt: &str) -> Result<(), ClientError> {
        let params = json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": prompt }],
        });
        self.rpc_call("turn/start", params).await?;
        self.state.on_turn_started()?;
        Ok(())
    }

    /// Wait for the terminal `turn/completed` notification. Raw model output
    /// is intentionally not extracted or logged here — only the status field.
    pub async fn wait_turn_completed(&mut self) -> Result<TurnCompleted, ClientError> {
        let params = self.rpc_wait_for_notification("turn/completed").await?;
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

    /// Bounded, drained stderr tail from the child app-server process — for
    /// local diagnostics only; never written into evidence files.
    pub async fn stderr_tail(&self) -> String {
        self.process.stderr_tail().await
    }

    pub fn is_poisoned(&self) -> bool {
        self.state.is_poisoned()
    }

    pub async fn shutdown(mut self) -> Result<(), ClientError> {
        self.process.shutdown().await;
        Ok(())
    }
}
