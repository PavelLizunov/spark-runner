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

use std::future::Future;
use std::time::Duration;

use serde_json::Value;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
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
const MAX_PENDING_MESSAGES: usize = 128;
const MAX_PENDING_BYTES: usize = 2 * MAX_FRAME_LEN;

/// Shared with the runtime owner around a non-idempotent request. Once a
/// flushed JSONL line may have reached the child, cancellation cannot safely
/// assume the request was not delivered even if its response was not seen.
#[derive(Clone, Default)]
pub struct RequestDelivery {
    written: Arc<AtomicBool>,
}

impl RequestDelivery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn was_written(&self) -> bool {
        self.written.load(Ordering::SeqCst)
    }

    fn mark_written(&self) {
        self.written.store(true, Ordering::SeqCst);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JsonlError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("app-server stdout closed while waiting for a response or notification")]
    StreamClosed,
    #[error(
        "app-server returned a remote error for request id {id} (diagnostic payload suppressed)"
    )]
    Remote { id: u64 },
    #[error("timed out after {0:?} waiting for an app-server response or notification")]
    Timeout(Duration),
    #[error(
        "app-server sent an oversized protocol frame (limit: {limit} bytes); session poisoned"
    )]
    OversizedFrame { limit: usize },
    #[error("app-server sent a malformed protocol frame; session poisoned")]
    MalformedFrame,
    #[error("app-server sent a server request while an RPC response was awaited; request was rejected and session poisoned")]
    ServerRequestDuringCall,
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
                | JsonlError::ServerRequestDuringCall
                | JsonlError::UnexpectedResponseId { .. }
        )
    }
}

/// Stdout reader plus the buffer of valid-but-unmatched messages seen so far.
/// Kept behind one lock so a message is never "in flight" between the two.
struct ReaderState {
    stdout: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    // A read may be cancelled when the runtime owner receives a higher
    // priority cancellation command.  Keeping an incomplete frame with the
    // reader, rather than in the cancelled future's stack, means the next
    // protocol operation resumes at the exact byte boundary.
    frame: Vec<u8>,
    pending: Vec<Value>,
    pending_bytes: usize,
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
                frame: Vec::with_capacity(1024),
                pending: Vec::new(),
                pending_bytes: 0,
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
        self.call_with_server_request_handler(method, params, |request| async move {
            // This convenience entry point has no owner to delegate to.  It
            // still gives the peer a schema-valid JSON-RPC error before
            // failing closed; production callers use the handler variant.
            let id = request
                .get("id")
                .cloned()
                .ok_or(JsonlError::MalformedFrame)?;
            if id.as_str().is_none() && id.as_i64().is_none() {
                return Err(JsonlError::MalformedFrame);
            }
            self.respond_error(id, -32601, "method not found").await
        })
        .await
    }

    /// Like [`Self::call`], but gives the single protocol owner each
    /// server-initiated request while an ordinary RPC is outstanding.  This
    /// avoids a request/response deadlock without assigning approval or auth
    /// authority to the transport layer.
    pub async fn call_with_server_request_handler<F, Fut>(
        &self,
        method: &str,
        params: Value,
        handler: F,
    ) -> Result<Value, JsonlError>
    where
        F: FnMut(Value) -> Fut,
        Fut: Future<Output = Result<(), JsonlError>>,
    {
        self.call_with_server_request_handler_and_delivery(method, params, None, handler)
            .await
    }

    /// As [`Self::call_with_server_request_handler`], while exposing the
    /// irreversible write boundary to the runtime owner for non-idempotent
    /// cancellation classification.
    pub async fn call_with_server_request_handler_and_delivery<F, Fut>(
        &self,
        method: &str,
        params: Value,
        delivery: Option<&RequestDelivery>,
        mut handler: F,
    ) -> Result<Value, JsonlError>
    where
        F: FnMut(Value) -> Fut,
        Fut: Future<Output = Result<(), JsonlError>>,
    {
        // Server requests are dispatched while this ordinary request is
        // pending. A conforming app-server may wait for that response before
        // returning ours; buffering it would deadlock both peers.
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
        if let Some(delivery) = delivery {
            delivery.mark_written();
        }

        let response = tokio::time::timeout(
            self.wait_timeout,
            self.read_response_dispatching_requests(id, &mut handler),
        )
        .await
        .map_err(|_| JsonlError::Timeout(self.wait_timeout))??;

        if let Some(error) = response.get("error") {
            let _ = error;
            return Err(JsonlError::Remote { id });
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }

    async fn read_response_dispatching_requests<F, Fut>(
        &self,
        expected: u64,
        handler: &mut F,
    ) -> Result<Value, JsonlError>
    where
        F: FnMut(Value) -> Fut,
        Fut: Future<Output = Result<(), JsonlError>>,
    {
        loop {
            let next = {
                let mut reader = self.reader.lock().await;
                if let Some(pos) = reader
                    .pending
                    .iter()
                    .position(|value| WaitTarget::ResponseId(expected).matches(value))
                {
                    Some(Ok::<Value, JsonlError>(remove_pending(&mut reader, pos)))
                } else {
                    reader
                        .pending
                        .iter()
                        .position(is_server_request)
                        .map(|pos| Ok::<Value, JsonlError>(remove_pending(&mut reader, pos)))
                }
            };

            let value = match next {
                Some(value) => value?,
                None => self.read_raw_message().await?,
            };
            if WaitTarget::ResponseId(expected).matches(&value) {
                return Ok(value);
            }
            if is_server_request(&value) {
                handler(value).await?;
                continue;
            }
            if value.get("method").is_none() {
                if let Some(actual) = value.get("id").and_then(Value::as_u64) {
                    return Err(JsonlError::UnexpectedResponseId { expected, actual });
                }
            }
            let mut reader = self.reader.lock().await;
            push_pending(&mut reader, value)?;
        }
    }

    async fn read_raw_message(&self) -> Result<Value, JsonlError> {
        let mut reader = self.reader.lock().await;
        loop {
            let Some(line) = read_bounded_frame(&mut reader).await? else {
                return Err(JsonlError::StreamClosed);
            };
            let trimmed = trim_ascii(&line);
            if trimmed.is_empty() {
                continue;
            }
            return serde_json::from_slice(trimmed).map_err(|_| JsonlError::MalformedFrame);
        }
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

    /// Send a JSON-RPC response to a request initiated by the app-server.
    pub async fn respond(&self, id: Value, result: Value) -> Result<(), JsonlError> {
        let response = serde_json::json!({ "id": id, "result": result });
        let line = serde_json::to_string(&response)?;
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(line.as_bytes()).await?;
        stdin
            .write_all(
                b"
",
            )
            .await?;
        stdin.flush().await?;
        Ok(())
    }

    /// Reply to a server-initiated request before failing the session. The
    /// request id is echoed unchanged because the stable protocol accepts
    /// both strings and signed integers.
    pub async fn respond_error(
        &self,
        id: Value,
        code: i64,
        message: &str,
    ) -> Result<(), JsonlError> {
        let response =
            serde_json::json!({ "id": id, "error": { "code": code, "message": message } });
        let line = serde_json::to_string(&response)?;
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(line.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
    }

    /// Send a JSON-RPC notification. The stable protocol requires this
    /// immediately after a successful initialize response.
    pub async fn notify(&self, method: &str, params: Value) -> Result<(), JsonlError> {
        let line = serde_json::to_string(&serde_json::json!({
            "method": method,
            "params": params,
        }))?;
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(line.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
    }

    /// Read the next protocol message, first draining messages buffered while
    /// another wait was in progress. This is used by the single owner task
    /// that must handle app-server approval requests interleaved with turn
    /// notifications.
    pub async fn next_message(&self) -> Result<Value, JsonlError> {
        tokio::time::timeout(self.wait_timeout, self.read_next_message())
            .await
            .map_err(|_| JsonlError::Timeout(self.wait_timeout))?
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
            return Ok(remove_pending(&mut reader, pos));
        }

        loop {
            let Some(line) = read_bounded_frame(&mut reader).await? else {
                return Err(JsonlError::StreamClosed);
            };
            let trimmed = trim_ascii(&line);
            if trimmed.is_empty() {
                continue;
            }
            let value: Value =
                serde_json::from_slice(trimmed).map_err(|_| JsonlError::MalformedFrame)?;

            if target.matches(&value) {
                return Ok(value);
            }

            // Waiting for a specific response id: any other response-shaped
            // message (one carrying an `id`) is a desync, not something to
            // tolerate and buffer — it means the app-server answered a
            // request we did not make (or answered one twice). Server-initiated
            // requests also carry an `id`, but they additionally carry a
            // `method`; those belong to the client owner task and are buffered.
            if let WaitTarget::ResponseId(expected) = target {
                if value.get("method").is_none() {
                    if let Some(actual) = value.get("id").and_then(Value::as_u64) {
                        return Err(JsonlError::UnexpectedResponseId {
                            expected: *expected,
                            actual,
                        });
                    }
                }
            }

            // Both method and id are child-controlled.  Diagnostic output
            // exposes only a bounded message class.
            tracing::debug!(
                class = "unrelated_protocol_message",
                has_id = value.get("id").is_some(),
                "buffering protocol message while waiting"
            );
            push_pending(&mut reader, value)?;
        }
    }

    async fn read_next_message(&self) -> Result<Value, JsonlError> {
        let mut reader = self.reader.lock().await;
        if !reader.pending.is_empty() {
            return Ok(remove_pending(&mut reader, 0));
        }

        loop {
            let Some(line) = read_bounded_frame(&mut reader).await? else {
                return Err(JsonlError::StreamClosed);
            };
            let trimmed = trim_ascii(&line);
            if trimmed.is_empty() {
                continue;
            }
            return serde_json::from_slice(trimmed).map_err(|_| JsonlError::MalformedFrame);
        }
    }
}

fn is_server_request(value: &Value) -> bool {
    value.get("id").is_some() && value.get("method").is_some()
}

fn remove_pending(reader: &mut ReaderState, index: usize) -> Value {
    let value = reader.pending.remove(index);
    // Serialize exactly as `push_pending` did. This makes the accounting
    // represent retained bytes rather than cumulative traffic.
    let bytes = serde_json::to_vec(&value).map_or(0, |bytes| bytes.len());
    reader.pending_bytes = reader.pending_bytes.saturating_sub(bytes);
    value
}

async fn read_bounded_frame(reader: &mut ReaderState) -> Result<Option<Vec<u8>>, JsonlError> {
    loop {
        let mut byte = [0_u8; 1];
        let read = reader.stdout.read(&mut byte).await?;
        if read == 0 {
            return if reader.frame.is_empty() {
                Ok(None)
            } else {
                Ok(Some(std::mem::take(&mut reader.frame)))
            };
        }
        if reader.frame.len() == MAX_FRAME_LEN {
            reader.frame.clear();
            return Err(JsonlError::OversizedFrame {
                limit: MAX_FRAME_LEN,
            });
        }
        reader.frame.push(byte[0]);
        if byte[0] == b'\n' {
            return Ok(Some(std::mem::take(&mut reader.frame)));
        }
    }
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(start, |index| index + 1);
    &bytes[start..end]
}

fn push_pending(reader: &mut ReaderState, value: Value) -> Result<(), JsonlError> {
    let bytes = serde_json::to_vec(&value)?.len();
    if reader.pending.len() == MAX_PENDING_MESSAGES
        || reader.pending_bytes.saturating_add(bytes) > MAX_PENDING_BYTES
    {
        return Err(JsonlError::OversizedFrame {
            limit: MAX_PENDING_BYTES,
        });
    }
    reader.pending_bytes += bytes;
    reader.pending.push(value);
    Ok(())
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
            WaitTarget::ResponseId(id) => {
                value.get("method").is_none()
                    && value.get("id").and_then(Value::as_u64) == Some(*id)
            }
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
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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

    /// T11: a server request can precede the response to an ordinary RPC.
    /// The no-owner convenience entry point returns `-32601` with the
    /// original string id and then fails closed rather than inventing
    /// approval authority.
    #[tokio::test]
    async fn dispatches_server_requests_while_an_rpc_response_is_awaited() {
        let (client_write, server_read) = tokio::io::duplex(16 * 1024);
        let (server_write, client_read) = tokio::io::duplex(16 * 1024);
        let server = tokio::spawn(async move {
            let mut reader = BufReader::new(server_read);
            let mut writer = server_write;
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&line).unwrap()["method"],
                "account/read"
            );

            writer
                .write_all(
                    serde_json::to_string(&json!({
                        "id": "server-unknown",
                        "method": "extension/unknown",
                        "params": {}
                    }))
                    .unwrap()
                    .as_bytes(),
                )
                .await
                .unwrap();
            writer.write_all(b"\n").await.unwrap();
            writer.flush().await.unwrap();
            line.clear();
            reader.read_line(&mut line).await.unwrap();
            let unknown = serde_json::from_str::<Value>(&line).unwrap();
            assert_eq!(unknown["id"], "server-unknown");
            assert_eq!(unknown["error"]["code"], -32601);
        });
        let client = JsonlClient::with_timeout(client_write, client_read, Duration::from_secs(1));
        assert!(client.call("account/read", json!({})).await.is_err());
        server.await.unwrap();
    }
}
