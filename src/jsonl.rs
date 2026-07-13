//! JSON-RPC-ish JSONL client over a child process's stdin/stdout.
//!
//! Writer sends `{"id":N,"method":"...","params":...}` lines. Reader tolerates
//! unknown notifications while waiting for a specific response id or
//! notification method, but poisons the session on protocol desync: an
//! oversized frame, a malformed frame, or a response whose id does not match
//! the one being awaited (ADR-004: tolerant reader, strict writer,
//! poison-on-desync).
//!
//! Unmatched notifications read while waiting are retained in a small pending
//! buffer (checked before reading more stdout) rather than discarded, so a
//! terminal notification (e.g. `turn/completed`) that arrives while a
//! different wait is in flight is not lost. Every wait is additionally bounded
//! by a timeout, so a stalled app-server fails fast with a sanitized error
//! instead of hanging forever.

use std::time::Duration;

use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

/// Default bound for a single response/notification wait. Generous enough for
/// a live model turn, but short enough that a protocol desync fails loudly
/// instead of hanging the `doctor`/`run` commands indefinitely.
const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(120);

/// Upper bound on a single JSONL frame (line), measured via
/// [`tokio::io::AsyncBufReadExt::read_line`]'s returned byte count. Protects
/// against an unbounded or corrupted frame instead of buffering it
/// indefinitely; the frame content itself is never logged.
pub const MAX_FRAME_LEN: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum JsonlError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("app-server stdout closed while waiting for a response or notification")]
    StreamClosed,
    #[error("app-server returned an error for id {id}: {message}")]
    Remote { id: u64, message: String },
    #[error("timed out after {0:?} waiting for an app-server response or notification")]
    Timeout(Duration),
    #[error(
        "app-server sent an oversized protocol frame (limit: {limit} bytes); session poisoned"
    )]
    OversizedFrame { limit: usize },
    #[error("app-server sent a malformed protocol frame; session poisoned")]
    MalformedFrame,
    #[error(
        "app-server response id {actual} did not match the expected id {expected} \
         (protocol desync); session poisoned"
    )]
    UnexpectedResponseId { expected: u64, actual: u64 },
}

impl JsonlError {
    /// Whether this error represents a protocol desync that has poisoned the
    /// session and may be worth a single controlled app-server restart.
    pub fn is_desync(&self) -> bool {
        matches!(
            self,
            JsonlError::OversizedFrame { .. }
                | JsonlError::MalformedFrame
                | JsonlError::UnexpectedResponseId { .. }
        )
    }
}

/// Stdout reader plus the buffer of valid-but-unmatched messages seen so far.
/// Kept behind one lock so a message is never "in flight" between the two.
struct ReaderState {
    stdout: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    pending: Vec<Value>,
}

pub struct JsonlClient {
    stdin: Mutex<Box<dyn AsyncWrite + Unpin + Send>>,
    reader: Mutex<ReaderState>,
    next_id: AtomicU64,
    wait_timeout: Duration,
}

impl JsonlClient {
    pub fn new(
        stdin: impl AsyncWrite + Unpin + Send + 'static,
        stdout: impl AsyncRead + Unpin + Send + 'static,
    ) -> Self {
        Self::with_timeout(stdin, stdout, DEFAULT_WAIT_TIMEOUT)
    }

    pub fn with_timeout(
        stdin: impl AsyncWrite + Unpin + Send + 'static,
        stdout: impl AsyncRead + Unpin + Send + 'static,
        wait_timeout: Duration,
    ) -> Self {
        Self {
            stdin: Mutex::new(Box::new(stdin)),
            reader: Mutex::new(ReaderState {
                stdout: BufReader::new(Box::new(stdout)),
                pending: Vec::new(),
            }),
            next_id: AtomicU64::new(1),
            wait_timeout,
        }
    }

    /// Send a request and wait for the matching response id. A valid
    /// response for a different id is a protocol desync, not something to
    /// buffer forever (ADR-004: poison-on-desync); unrelated notifications
    /// are still tolerated.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, JsonlError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let request = serde_json::json!({
            "id": id,
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&request)?;

        {
            let mut stdin = self.stdin.lock().await;
            stdin.write_all(line.as_bytes()).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await?;
        }

        let response = self.wait_for(WaitTarget::ResponseId(id)).await?;

        if let Some(error) = response.get("error") {
            return Err(JsonlError::Remote {
                id,
                message: error.to_string(),
            });
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }

    /// Wait for the next notification (no `id`) matching `method`, tolerating
    /// unrelated notifications and stray responses in between. Checks the
    /// pending buffer first, so a matching notification already observed
    /// while waiting on something else (e.g. a terminal `turn/completed`
    /// that arrived before its `turn/start` response was consumed) is not
    /// lost.
    pub async fn wait_for_notification(&self, method: &str) -> Result<Value, JsonlError> {
        let notification = self.wait_for(WaitTarget::Notification(method)).await?;
        Ok(notification.get("params").cloned().unwrap_or(Value::Null))
    }

    async fn wait_for(&self, target: WaitTarget<'_>) -> Result<Value, JsonlError> {
        tokio::time::timeout(self.wait_timeout, self.read_until_match(&target))
            .await
            .map_err(|_| JsonlError::Timeout(self.wait_timeout))?
    }

    async fn read_until_match(&self, target: &WaitTarget<'_>) -> Result<Value, JsonlError> {
        let mut reader = self.reader.lock().await;

        if let Some(pos) = reader
            .pending
            .iter()
            .position(|value| target.matches(value))
        {
            return Ok(reader.pending.remove(pos));
        }

        loop {
            let mut line = String::new();
            let bytes_read = reader.stdout.read_line(&mut line).await?;
            if bytes_read == 0 {
                return Err(JsonlError::StreamClosed);
            }
            if bytes_read > MAX_FRAME_LEN {
                return Err(JsonlError::OversizedFrame {
                    limit: MAX_FRAME_LEN,
                });
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value: Value =
                serde_json::from_str(trimmed).map_err(|_| JsonlError::MalformedFrame)?;

            if target.matches(&value) {
                return Ok(value);
            }

            // Waiting for a specific response id: any other response-shaped
            // message (one carrying an `id`) is a desync, not something to
            // tolerate and buffer — it means the app-server answered a
            // request we did not make (or answered one twice).
            if let WaitTarget::ResponseId(expected) = target {
                if let Some(actual) = value.get("id").and_then(Value::as_u64) {
                    return Err(JsonlError::UnexpectedResponseId {
                        expected: *expected,
                        actual,
                    });
                }
            }

            tracing::debug!(
                method = ?value.get("method"),
                has_id = value.get("id").is_some(),
                "buffering unrelated message while waiting"
            );
            reader.pending.push(value);
        }
    }
}

/// What a given wait is looking for: a response with a specific id, or a
/// notification with a specific method.
enum WaitTarget<'a> {
    ResponseId(u64),
    Notification(&'a str),
}

impl WaitTarget<'_> {
    fn matches(&self, value: &Value) -> bool {
        match self {
            WaitTarget::ResponseId(id) => value.get("id").and_then(Value::as_u64) == Some(*id),
            WaitTarget::Notification(method) => {
                value.get("id").is_none()
                    && value.get("method").and_then(Value::as_str) == Some(*method)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn client_with_canned_stdout(lines: &[Value], wait_timeout: Duration) -> JsonlClient {
        let (mut server_write, client_read) = tokio::io::duplex(64 * 1024);
        let mut payload = String::new();
        for line in lines {
            payload.push_str(&serde_json::to_string(line).unwrap());
            payload.push('\n');
        }
        tokio::spawn(async move {
            let _ = server_write.write_all(payload.as_bytes()).await;
            let _ = server_write.flush().await;
            // Keep the write end alive so EOF is not observed until the test
            // is done making assertions.
            std::future::pending::<()>().await;
        });
        JsonlClient::with_timeout(tokio::io::sink(), client_read, wait_timeout)
    }

    /// The terminal `turn/completed` notification is emitted (and thus read)
    /// before the `turn/start` response is consumed. The old implementation
    /// discarded unmatched messages while waiting for the response id, which
    /// permanently lost the notification and caused `wait_for_notification`
    /// to hang forever.
    #[tokio::test]
    async fn buffers_terminal_notification_seen_before_matching_response_is_consumed() {
        let client = client_with_canned_stdout(
            &[
                json!({ "method": "turn/started", "params": { "threadId": "t1" } }),
                json!({ "method": "turn/completed", "params": { "threadId": "t1", "status": "completed" } }),
                json!({ "id": 1, "result": { "status": "started" } }),
            ],
            Duration::from_secs(5),
        );

        let call_result = client.call("turn/start", json!({})).await.unwrap();
        assert_eq!(call_result["status"], "started");

        // Must resolve from the pending buffer without reading more stdout
        // (there is nothing left to read; the write end never sends more).
        let notification = tokio::time::timeout(
            Duration::from_secs(1),
            client.wait_for_notification("turn/completed"),
        )
        .await
        .expect("wait_for_notification must not hang")
        .unwrap();
        assert_eq!(notification["status"], "completed");
    }

    /// The terminal notification arrives (and is read as part of consuming
    /// the response) before the caller even invokes `wait_for_notification`.
    #[tokio::test]
    async fn buffers_terminal_notification_seen_before_wait_for_notification_is_called() {
        let client = client_with_canned_stdout(
            &[
                json!({ "id": 1, "result": { "status": "started" } }),
                json!({ "method": "turn/started", "params": { "threadId": "t1" } }),
                json!({ "method": "turn/completed", "params": { "threadId": "t1", "status": "completed" } }),
            ],
            Duration::from_secs(5),
        );

        client.call("turn/start", json!({})).await.unwrap();

        let notification = tokio::time::timeout(
            Duration::from_secs(1),
            client.wait_for_notification("turn/completed"),
        )
        .await
        .expect("wait_for_notification must not hang")
        .unwrap();
        assert_eq!(notification["status"], "completed");
    }

    /// A wait that will never be satisfied fails fast with a sanitized
    /// timeout error rather than hanging forever.
    #[tokio::test]
    async fn wait_for_notification_times_out_with_sanitized_error() {
        let (server_write, client_read) = tokio::io::duplex(64 * 1024);
        // Hold the write end open (no EOF) but never send anything.
        tokio::spawn(async move {
            let _keep_alive = server_write;
            std::future::pending::<()>().await;
        });
        let client =
            JsonlClient::with_timeout(tokio::io::sink(), client_read, Duration::from_millis(50));

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            client.wait_for_notification("turn/completed"),
        )
        .await
        .expect("the internal timeout must fire well before the test's own bound");

        match result {
            Err(JsonlError::Timeout(duration)) => {
                assert_eq!(duration, Duration::from_millis(50));
                let message = duration_error_message(duration);
                assert!(!message.contains("turn/completed"));
                assert!(!message.contains('{'));
            }
            other => panic!("expected a sanitized Timeout error, got {other:?}"),
        }
    }

    fn duration_error_message(duration: Duration) -> String {
        JsonlError::Timeout(duration).to_string()
    }

    /// A response for a different id than the one being awaited is a
    /// protocol desync, not something to buffer and keep waiting past.
    #[tokio::test]
    async fn call_fails_on_unexpected_response_id() {
        let client = client_with_canned_stdout(
            &[json!({ "id": 999, "result": { "status": "started" } })],
            Duration::from_secs(5),
        );

        let result = client.call("turn/start", json!({})).await;
        match result {
            Err(
                err @ JsonlError::UnexpectedResponseId {
                    expected: 1,
                    actual: 999,
                },
            ) => {
                assert!(err.is_desync());
            }
            other => panic!("expected UnexpectedResponseId, got {other:?}"),
        }
    }

    /// A malformed (non-JSON) line poisons the session with a sanitized
    /// error instead of being silently skipped.
    #[tokio::test]
    async fn call_fails_on_malformed_frame() {
        let (mut server_write, client_read) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let _ = server_write.write_all(b"not-json\n").await;
            let _ = server_write.flush().await;
            std::future::pending::<()>().await;
        });
        let client =
            JsonlClient::with_timeout(tokio::io::sink(), client_read, Duration::from_secs(5));

        let result = client.call("turn/start", json!({})).await;
        match result {
            Err(err @ JsonlError::MalformedFrame) => {
                assert!(!err.to_string().contains("not-json"));
            }
            other => panic!("expected MalformedFrame, got {other:?}"),
        }
    }

    /// A line longer than [`MAX_FRAME_LEN`] poisons the session with a
    /// sanitized error instead of being buffered/parsed.
    #[tokio::test]
    async fn call_fails_on_oversized_frame() {
        let (mut server_write, client_read) = tokio::io::duplex(2 * 1024 * 1024);
        tokio::spawn(async move {
            let oversized = "x".repeat(MAX_FRAME_LEN + 1);
            let _ = server_write.write_all(oversized.as_bytes()).await;
            let _ = server_write.write_all(b"\n").await;
            let _ = server_write.flush().await;
            std::future::pending::<()>().await;
        });
        let client =
            JsonlClient::with_timeout(tokio::io::sink(), client_read, Duration::from_secs(5));

        let result = client.call("turn/start", json!({})).await;
        match result {
            Err(err @ JsonlError::OversizedFrame { limit }) => {
                assert_eq!(limit, MAX_FRAME_LEN);
                assert!(!err.to_string().contains('x'));
            }
            other => panic!("expected OversizedFrame, got {other:?}"),
        }
    }
}
