//! JSON-RPC-ish JSONL client over a child process's stdin/stdout.
//!
//! Writer sends `{"id":N,"method":"...","params":...}` lines. Reader tolerates
//! malformed lines and unrelated notifications while waiting for a specific
//! response id or notification method (ADR-004: tolerant reader, strict writer).
//!
//! Unmatched-but-valid messages read while waiting are retained in a small
//! pending buffer (checked before reading more stdout) rather than discarded,
//! so a terminal notification (e.g. `turn/completed`) that arrives while a
//! different wait is in flight is not lost. Every wait is additionally bounded
//! by a timeout, so a stalled or desynced app-server fails fast with a
//! sanitized error instead of hanging forever.

use std::time::Duration;

use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

/// Default bound for a single response/notification wait. Generous enough for
/// a live model turn, but short enough that a protocol desync fails loudly
/// instead of hanging the `doctor`/`run` commands indefinitely.
const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(120);

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

    /// Send a request and wait for the matching response id, tolerating
    /// unrelated notifications in between.
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

        let response = self
            .wait_for(|value| value.get("id").and_then(Value::as_u64) == Some(id))
            .await?;

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
        let notification = self
            .wait_for(|value| {
                value.get("id").is_none()
                    && value.get("method").and_then(Value::as_str) == Some(method)
            })
            .await?;
        Ok(notification.get("params").cloned().unwrap_or(Value::Null))
    }

    async fn wait_for<F>(&self, mut matches: F) -> Result<Value, JsonlError>
    where
        F: FnMut(&Value) -> bool,
    {
        tokio::time::timeout(self.wait_timeout, self.read_until_match(&mut matches))
            .await
            .map_err(|_| JsonlError::Timeout(self.wait_timeout))?
    }

    async fn read_until_match<F>(&self, matches: &mut F) -> Result<Value, JsonlError>
    where
        F: FnMut(&Value) -> bool,
    {
        let mut reader = self.reader.lock().await;

        if let Some(pos) = reader.pending.iter().position(&mut *matches) {
            return Ok(reader.pending.remove(pos));
        }

        loop {
            let mut line = String::new();
            let bytes_read = reader.stdout.read_line(&mut line).await?;
            if bytes_read == 0 {
                return Err(JsonlError::StreamClosed);
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value: Value = match serde_json::from_str(trimmed) {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(error = %error, "ignoring malformed line from app-server");
                    continue;
                }
            };
            if matches(&value) {
                return Ok(value);
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
}
