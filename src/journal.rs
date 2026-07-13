//! Append-only SQLite journal and restart projection for CP5.
//!
//! The journal owns exactly one writer task. Callers send typed events; the
//! writer redacts the payload before it reaches SQLite. Raw capture and
//! terminal output are persisted only when explicitly supplied and the matching
//! TTL is configured.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub path: PathBuf,
    pub terminal_output_ttl: Option<Duration>,
    pub raw_capture_ttl: Option<Duration>,
}

impl JournalConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            terminal_output_ttl: None,
            raw_capture_ttl: None,
        }
    }

    pub fn from_env() -> Option<Self> {
        let path = std::env::var_os("SPARK_RUNNER_JOURNAL_PATH")?;
        let mut config = Self::new(PathBuf::from(path));
        config.terminal_output_ttl = ttl_from_env("SPARK_RUNNER_TERMINAL_OUTPUT_TTL_SECS");
        config.raw_capture_ttl = ttl_from_env("SPARK_RUNNER_RAW_CAPTURE_TTL_SECS");
        Some(config)
    }
}

fn ttl_from_env(name: &str) -> Option<Duration> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JournalEvent {
    ExecutionStarted {
        execution_id: String,
    },
    ExecutionCompleted {
        execution_id: String,
        status: ExecutionTerminalStatus,
    },
    TurnStarted {
        execution_id: String,
        turn_id: String,
    },
    TurnCompleted {
        execution_id: String,
        turn_id: String,
        status: TurnTerminalStatus,
    },
    ApprovalRequested {
        execution_id: String,
        request_key: String,
        method: String,
    },
    ApprovalDecided {
        execution_id: String,
        request_key: String,
        method: String,
        decision: ApprovalTerminalDecision,
    },
    RecoveryExecutionUnknown {
        execution_id: String,
    },
    RecoveryApprovalDenied {
        execution_id: String,
        request_key: String,
        method: String,
    },
    Incident {
        execution_id: Option<String>,
        class: String,
        message: String,
    },
    RateLimitSnapshot {
        execution_id: String,
        snapshot: Value,
    },
}

impl JournalEvent {
    fn event_type(&self) -> &'static str {
        match self {
            Self::ExecutionStarted { .. }
            | Self::ExecutionCompleted { .. }
            | Self::RecoveryExecutionUnknown { .. } => "execution",
            Self::TurnStarted { .. } | Self::TurnCompleted { .. } => "turn",
            Self::ApprovalRequested { .. }
            | Self::ApprovalDecided { .. }
            | Self::RecoveryApprovalDenied { .. } => "approval",
            Self::Incident { .. } => "incident",
            Self::RateLimitSnapshot { .. } => "rate_limit_snapshot",
        }
    }

    fn execution_id(&self) -> Option<&str> {
        match self {
            Self::ExecutionStarted { execution_id }
            | Self::ExecutionCompleted { execution_id, .. }
            | Self::TurnStarted { execution_id, .. }
            | Self::TurnCompleted { execution_id, .. }
            | Self::ApprovalRequested { execution_id, .. }
            | Self::ApprovalDecided { execution_id, .. }
            | Self::RateLimitSnapshot { execution_id, .. } => Some(execution_id),
            Self::RecoveryExecutionUnknown { execution_id }
            | Self::RecoveryApprovalDenied { execution_id, .. } => Some(execution_id),
            Self::Incident { execution_id, .. } => execution_id.as_deref(),
        }
    }

    fn turn_id(&self) -> Option<&str> {
        match self {
            Self::TurnStarted { turn_id, .. } | Self::TurnCompleted { turn_id, .. } => {
                Some(turn_id)
            }
            _ => None,
        }
    }

    fn approval_key(&self) -> Option<&str> {
        match self {
            Self::ApprovalRequested { request_key, .. }
            | Self::ApprovalDecided { request_key, .. }
            | Self::RecoveryApprovalDenied { request_key, .. } => Some(request_key),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTerminalStatus {
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnTerminalStatus {
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalTerminalDecision {
    Allowed,
    Denied,
    TimedOut,
}

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("sqlite journal error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("journal io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to serialize journal payload: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("journal writer task stopped")]
    WriterStopped,
    #[error("journal writer task panicked")]
    WriterPanicked,
}

type JournalResult<T> = Result<T, JournalError>;

#[derive(Debug)]
struct AppendRecord {
    event: JournalEvent,
    terminal_output: Option<String>,
    raw_capture: Option<Value>,
}

enum WriterCommand {
    Append(AppendRecord, oneshot::Sender<JournalResult<()>>),
    Prune(oneshot::Sender<JournalResult<usize>>),
    Shutdown(oneshot::Sender<JournalResult<()>>),
}

#[derive(Debug)]
pub struct JournalWriter {
    tx: mpsc::Sender<WriterCommand>,
    join: Option<JoinHandle<()>>,
}

impl Clone for JournalWriter {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            join: None,
        }
    }
}

impl JournalWriter {
    pub fn open(config: JournalConfig) -> JournalResult<Self> {
        init_db(&config.path)?;
        let (tx, mut rx) = mpsc::channel::<WriterCommand>(128);
        let join = thread::spawn(move || {
            let connection = match Connection::open(&config.path).and_then(|connection| {
                prepare_connection(&connection)?;
                Ok(connection)
            }) {
                Ok(connection) => connection,
                Err(error) => {
                    if let Some(command) = rx.blocking_recv() {
                        respond_error(command, JournalError::Sqlite(error));
                    }
                    return;
                }
            };

            while let Some(command) = rx.blocking_recv() {
                match command {
                    WriterCommand::Append(record, reply) => {
                        let _ = reply.send(append_record(&connection, &config, record));
                    }
                    WriterCommand::Prune(reply) => {
                        let _ = reply.send(prune_expired(&connection));
                    }
                    WriterCommand::Shutdown(reply) => {
                        let _ = reply.send(Ok(()));
                        break;
                    }
                }
            }
        });
        Ok(Self {
            tx,
            join: Some(join),
        })
    }

    pub async fn append(&self, event: JournalEvent) -> JournalResult<()> {
        self.append_record(AppendRecord {
            event,
            terminal_output: None,
            raw_capture: None,
        })
        .await
    }

    pub async fn append_with_capture(
        &self,
        event: JournalEvent,
        terminal_output: Option<String>,
        raw_capture: Option<Value>,
    ) -> JournalResult<()> {
        self.append_record(AppendRecord {
            event,
            terminal_output,
            raw_capture,
        })
        .await
    }

    async fn append_record(&self, record: AppendRecord) -> JournalResult<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WriterCommand::Append(record, reply_tx))
            .await
            .map_err(|_| JournalError::WriterStopped)?;
        reply_rx.await.map_err(|_| JournalError::WriterStopped)?
    }

    pub async fn prune_expired(&self) -> JournalResult<usize> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WriterCommand::Prune(reply_tx))
            .await
            .map_err(|_| JournalError::WriterStopped)?;
        reply_rx.await.map_err(|_| JournalError::WriterStopped)?
    }

    pub async fn shutdown(mut self) -> JournalResult<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WriterCommand::Shutdown(reply_tx))
            .await
            .map_err(|_| JournalError::WriterStopped)?;
        reply_rx.await.map_err(|_| JournalError::WriterStopped)??;
        if let Some(join) = self.join.take() {
            join.join().map_err(|_| JournalError::WriterPanicked)?;
        }
        Ok(())
    }
}

fn respond_error(command: WriterCommand, error: JournalError) {
    match command {
        WriterCommand::Append(_, reply) => {
            let _ = reply.send(Err(error));
        }
        WriterCommand::Prune(reply) => {
            let _ = reply.send(Err(error));
        }
        WriterCommand::Shutdown(reply) => {
            let _ = reply.send(Err(error));
        }
    }
}

fn init_db(path: &Path) -> JournalResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(path)?;
    prepare_connection(&connection)?;
    Ok(())
}

fn prepare_connection(connection: &Connection) -> rusqlite::Result<()> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS journal_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at_ms INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            execution_id TEXT,
            turn_id TEXT,
            approval_key TEXT,
            payload_json TEXT NOT NULL,
            terminal_output TEXT,
            raw_capture_json TEXT,
            expires_at_ms INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_journal_events_execution ON journal_events(execution_id, id);
        CREATE INDEX IF NOT EXISTS idx_journal_events_approval ON journal_events(approval_key, id);
        CREATE TABLE IF NOT EXISTS journal_captures (
            event_id INTEGER NOT NULL REFERENCES journal_events(id),
            kind TEXT NOT NULL,
            data TEXT NOT NULL,
            expires_at_ms INTEGER NOT NULL,
            PRIMARY KEY(event_id, kind)
        );
        CREATE INDEX IF NOT EXISTS idx_journal_captures_expiry ON journal_captures(expires_at_ms);",
    )?;
    migrate_legacy_captures(connection)
}

/// CP5 originally kept expiring captures on the append-only event row. Move
/// those values once into the side table and clear the legacy cells, so the
/// normal expiry job can never delete lifecycle history.
fn migrate_legacy_captures(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute(
        "INSERT OR IGNORE INTO journal_captures (event_id, kind, data, expires_at_ms)
         SELECT id, 'terminal_output', terminal_output, expires_at_ms
         FROM journal_events
         WHERE terminal_output IS NOT NULL AND expires_at_ms IS NOT NULL",
        [],
    )?;
    connection.execute(
        "INSERT OR IGNORE INTO journal_captures (event_id, kind, data, expires_at_ms)
         SELECT id, 'raw_capture', raw_capture_json, expires_at_ms
         FROM journal_events
         WHERE raw_capture_json IS NOT NULL AND expires_at_ms IS NOT NULL",
        [],
    )?;
    connection.execute(
        "UPDATE journal_events
         SET terminal_output = NULL, raw_capture_json = NULL, expires_at_ms = NULL
         WHERE expires_at_ms IS NOT NULL",
        [],
    )?;
    Ok(())
}

fn append_record(
    connection: &Connection,
    config: &JournalConfig,
    record: AppendRecord,
) -> JournalResult<()> {
    let now = now_ms();
    let mut payload = serde_json::to_value(&record.event)?;
    redact_value(&mut payload);
    let payload_json = serde_json::to_string(&payload)?;
    connection.execute(
        "INSERT INTO journal_events (created_at_ms, event_type, execution_id, turn_id, approval_key, payload_json, terminal_output, raw_capture_json, expires_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![now, record.event.event_type(), record.event.execution_id(), record.event.turn_id(), record.event.approval_key(), payload_json, Option::<String>::None, Option::<String>::None, Option::<i64>::None],
    )?;
    let event_id = connection.last_insert_rowid();
    if let (Some(output), Some(ttl)) = (record.terminal_output, config.terminal_output_ttl) {
        connection.execute(
            "INSERT INTO journal_captures (event_id, kind, data, expires_at_ms) VALUES (?1, 'terminal_output', ?2, ?3)",
            params![event_id, redact_string(&output), now.saturating_add(duration_ms(ttl))],
        )?;
    }
    if let (Some(mut raw), Some(ttl)) = (record.raw_capture, config.raw_capture_ttl) {
        redact_value(&mut raw);
        connection.execute(
            "INSERT INTO journal_captures (event_id, kind, data, expires_at_ms) VALUES (?1, 'raw_capture', ?2, ?3)",
            params![event_id, serde_json::to_string(&raw)?, now.saturating_add(duration_ms(ttl))],
        )?;
    }
    Ok(())
}

fn prune_expired(connection: &Connection) -> JournalResult<usize> {
    Ok(connection.execute(
        "DELETE FROM journal_captures WHERE expires_at_ms <= ?1",
        params![now_ms()],
    )?)
}

fn duration_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn redact_value(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                if is_sensitive_key(key) {
                    *value = Value::String("[REDACTED]".to_string());
                } else {
                    redact_value(value);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                redact_value(value);
            }
        }
        Value::String(text) => {
            *text = redact_string(text);
        }
        _ => {}
    }
}

fn redact_string(text: &str) -> String {
    let mut out = text.to_string();
    for marker in ["sk-", "ghp_", "github_pat_", "xoxb-", "Bearer "] {
        while let Some(start) = out.find(marker) {
            let end = out[start..]
                .find(char::is_whitespace)
                .map(|offset| start + offset)
                .unwrap_or(out.len());
            out.replace_range(start..end, "[REDACTED]");
        }
    }
    out
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("token")
        || key.contains("secret")
        || key.contains("password")
        || key.contains("authorization")
        || matches!(key.as_str(), "key" | "api_key" | "apikey" | "access_key")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryProjection {
    pub executions: BTreeMap<String, ExecutionRecoveryState>,
    pub approvals: BTreeMap<String, ApprovalRecoveryState>,
    /// Entries which were unresolved in the durable input, as opposed to
    /// entries already terminalised by a previous startup recovery.
    pub unresolved_executions: Vec<String>,
    pub unresolved_approvals: Vec<String>,
    pub replayed_turns: usize,
    pub replayed_approvals: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionRecoveryState {
    Completed,
    Failed,
    UnknownAfterRestart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalRecoveryState {
    Allowed,
    Denied,
    TimedOut,
    DeniedOnRestart,
}

pub fn project_recovery(path: &Path) -> JournalResult<RecoveryProjection> {
    let connection = Connection::open(path)?;
    prepare_connection(&connection)?;
    let mut statement =
        connection.prepare("SELECT payload_json FROM journal_events ORDER BY id ASC")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let mut executions: BTreeMap<String, Option<ExecutionRecoveryState>> = BTreeMap::new();
    let mut approvals: BTreeMap<String, Option<ApprovalRecoveryState>> = BTreeMap::new();
    let mut unresolved_executions = BTreeSet::new();
    let mut unresolved_approvals = BTreeSet::new();

    for row in rows {
        let payload = row?;
        let event: JournalEvent = serde_json::from_str(&payload)?;
        match event {
            JournalEvent::ExecutionStarted { execution_id } => {
                unresolved_executions.insert(execution_id.clone());
                executions.entry(execution_id).or_insert(None);
            }
            JournalEvent::ExecutionCompleted {
                execution_id,
                status,
            } => {
                unresolved_executions.remove(&execution_id);
                executions.insert(
                    execution_id,
                    Some(match status {
                        ExecutionTerminalStatus::Completed => ExecutionRecoveryState::Completed,
                        ExecutionTerminalStatus::Failed => ExecutionRecoveryState::Failed,
                    }),
                );
            }
            JournalEvent::RecoveryExecutionUnknown { execution_id } => {
                unresolved_executions.remove(&execution_id);
                executions.insert(
                    execution_id,
                    Some(ExecutionRecoveryState::UnknownAfterRestart),
                );
            }
            JournalEvent::ApprovalRequested { request_key, .. } => {
                unresolved_approvals.insert(request_key.clone());
                approvals.entry(request_key).or_insert(None);
            }
            JournalEvent::ApprovalDecided {
                request_key,
                decision,
                ..
            } => {
                unresolved_approvals.remove(&request_key);
                approvals.insert(
                    request_key,
                    Some(match decision {
                        ApprovalTerminalDecision::Allowed => ApprovalRecoveryState::Allowed,
                        ApprovalTerminalDecision::Denied => ApprovalRecoveryState::Denied,
                        ApprovalTerminalDecision::TimedOut => ApprovalRecoveryState::TimedOut,
                    }),
                );
            }
            JournalEvent::RecoveryApprovalDenied { request_key, .. } => {
                unresolved_approvals.remove(&request_key);
                approvals.insert(request_key, Some(ApprovalRecoveryState::DeniedOnRestart));
            }
            JournalEvent::TurnStarted { .. }
            | JournalEvent::TurnCompleted { .. }
            | JournalEvent::Incident { .. }
            | JournalEvent::RateLimitSnapshot { .. } => {}
        }
    }

    Ok(RecoveryProjection {
        executions: executions
            .into_iter()
            .map(|(id, state)| {
                (
                    id,
                    state.unwrap_or(ExecutionRecoveryState::UnknownAfterRestart),
                )
            })
            .collect(),
        approvals: approvals
            .into_iter()
            .map(|(id, state)| (id, state.unwrap_or(ApprovalRecoveryState::DeniedOnRestart)))
            .collect(),
        unresolved_executions: unresolved_executions.into_iter().collect(),
        unresolved_approvals: unresolved_approvals.into_iter().collect(),
        replayed_turns: 0,
        replayed_approvals: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_sensitive_payload_before_serialization() {
        let mut value = serde_json::json!({
            "token": "sk-live-secret",
            "nested": { "authorization": "Bearer abc123", "safe": "ok" },
            "line": "prefix ghp_deadbeef suffix",
            "request_key": "approval-original-id",
            "api_key": "real-api-key"
        });
        redact_value(&mut value);
        let rendered = serde_json::to_string(&value).unwrap();
        assert!(!rendered.contains("sk-live-secret"));
        assert!(!rendered.contains("abc123"));
        assert!(!rendered.contains("ghp_deadbeef"));
        assert!(!rendered.contains("real-api-key"));
        assert!(rendered.contains("approval-original-id"));
        assert!(rendered.contains("[REDACTED]"));
    }
}
