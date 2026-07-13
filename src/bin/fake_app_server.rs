//! Deterministic offline stand-in for `codex app-server --listen stdio://`.
//! Reads JSONL requests on stdin, writes JSONL responses/notifications on
//! stdout: enough of the happy path for offline `doctor`/`run` and tests.
//!
//! For CP3 regression tests it also supports a small set of deterministic
//! fault modes selected via `--fake-mode <mode>` (applied once, to the first
//! request received): `oversized_frame`, `malformed_frame`,
//! `unknown_response_id`, `unknown_response_id_once`. An optional
//! `--fail-marker <path>` records one line per process invocation (used by
//! `unknown_response_id_once` to behave normally only from the second
//! invocation onward, simulating "fixed after a restart", and by tests to
//! assert exactly how many app-server processes were spawned).

use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use spark_runner::jsonl::MAX_FRAME_LEN;

const REQUIRED_MODEL: &str = "gpt-5.3-codex-spark";

/// Append one record to `marker` and return this invocation's 1-based attempt
/// number (the number of times this marker has now been recorded, including
/// this call).
fn record_attempt(marker: &Path) -> io::Result<usize> {
    let previous = std::fs::read_to_string(marker).unwrap_or_default();
    let attempt = previous.lines().count() + 1;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(marker)?;
    writeln!(file, "attempt")?;
    Ok(attempt)
}

/// If a fault mode is active for this (the first) request, write the fault
/// response/frame and return `true`; otherwise return `false` and let the
/// caller fall through to normal handling.
fn maybe_apply_fault(
    fake_mode: Option<&str>,
    fail_marker: Option<&PathBuf>,
    stdout: &mut impl Write,
    id: Option<u64>,
) -> io::Result<bool> {
    let mode = match fake_mode {
        Some(mode) => mode,
        None => return Ok(false),
    };

    let attempt = match fail_marker {
        Some(marker) => record_attempt(marker)?,
        None => 1,
    };

    match mode {
        "oversized_frame" => {
            // Deliberately exceeds MAX_FRAME_LEN so the client's bounded
            // JSONL reader must reject it rather than buffering it.
            let padding = "x".repeat(MAX_FRAME_LEN + 1024);
            let line = format!(
                "{}\n",
                json!({ "id": id, "result": { "padding": padding } })
            );
            stdout.write_all(line.as_bytes())?;
            stdout.flush()?;
            Ok(true)
        }
        "malformed_frame" => {
            stdout.write_all(b"not-a-valid-jsonl-frame\n")?;
            stdout.flush()?;
            Ok(true)
        }
        "unknown_response_id" => {
            let wrong_id = id.map(|value| value + 1000).unwrap_or(9_999);
            send(
                stdout,
                &json!({ "id": wrong_id, "result": { "serverInfo": { "name": "fake-codex-app-server" } } }),
            )?;
            Ok(true)
        }
        "unknown_response_id_once" => {
            if attempt == 1 {
                let wrong_id = id.map(|value| value + 1000).unwrap_or(9_999);
                send(
                    stdout,
                    &json!({ "id": wrong_id, "result": { "serverInfo": { "name": "fake-codex-app-server" } } }),
                )?;
                Ok(true)
            } else {
                Ok(false)
            }
        }
        _ => Ok(false),
    }
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == flag)
        .and_then(|index| args.get(index + 1))
        .cloned()
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let fake_mode = arg_value(&args, "--fake-mode");
    let fail_marker = arg_value(&args, "--fail-marker").map(PathBuf::from);

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut thread_counter: u64 = 0;
    let mut turn_counter: u64 = 0;
    let mut fault_pending = fake_mode.is_some();

    for line in stdin.lock().lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let request: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let id = request.get("id").and_then(Value::as_u64);
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let params = request.get("params").cloned().unwrap_or(Value::Null);

        if fault_pending {
            fault_pending = false;
            if maybe_apply_fault(fake_mode.as_deref(), fail_marker.as_ref(), &mut stdout, id)? {
                continue;
            }
        }

        match method {
            "initialize" => send(
                &mut stdout,
                &json!({
                    "id": id,
                    "result": {
                        "serverInfo": { "name": "fake-codex-app-server", "version": "0.142.0" }
                    }
                }),
            )?,
            "account/read" => send(
                &mut stdout,
                &json!({
                    "id": id,
                    "result": {
                        "planType": "pro",
                        "accountType": "chatgpt",
                        "requiresOpenaiAuth": true
                    }
                }),
            )?,
            "model/list" => send(
                &mut stdout,
                &json!({
                    "id": id,
                    "result": {
                        "data": [
                            { "id": REQUIRED_MODEL, "provider": "openai" }
                        ]
                    }
                }),
            )?,
            "account/rateLimits/read" => send(
                &mut stdout,
                &json!({
                    "id": id,
                    "result": {
                        "limits": [
                            { "id": "spark-primary", "remaining": 100 }
                        ]
                    }
                }),
            )?,
            "thread/start" => {
                thread_counter += 1;
                let thread_id = format!("fake-thread-{thread_counter}");
                let model = params
                    .get("model")
                    .and_then(Value::as_str)
                    .unwrap_or(REQUIRED_MODEL)
                    .to_string();
                send(
                    &mut stdout,
                    &json!({
                        "id": id,
                        "result": {
                            "thread": { "id": thread_id },
                            "threadId": thread_id,
                            "model": model,
                            "modelProvider": "openai",
                            "approvalPolicy": "on-request",
                            "status": "idle"
                        }
                    }),
                )?;
                send(
                    &mut stdout,
                    &json!({
                        "method": "thread/started",
                        "params": {
                            "thread": { "id": thread_id },
                            "threadId": thread_id
                        }
                    }),
                )?;
            }
            "turn/start" => {
                turn_counter += 1;
                let turn_id = format!("fake-turn-{turn_counter}");
                let thread_id = params
                    .get("threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                send(
                    &mut stdout,
                    &json!({
                        "id": id,
                        "result": { "turnId": turn_id, "status": "started" }
                    }),
                )?;
                send(
                    &mut stdout,
                    &json!({
                        "method": "turn/started",
                        "params": { "threadId": thread_id, "turnId": turn_id }
                    }),
                )?;
                send(
                    &mut stdout,
                    &json!({
                        "method": "turn/completed",
                        "params": { "threadId": thread_id, "turnId": turn_id, "status": "completed" }
                    }),
                )?;
            }
            _ => {
                if let Some(id) = id {
                    send(
                        &mut stdout,
                        &json!({
                            "id": id,
                            "error": { "code": -32601, "message": format!("method not found: {method}") }
                        }),
                    )?;
                }
            }
        }
    }

    Ok(())
}

fn send(stdout: &mut impl Write, value: &Value) -> io::Result<()> {
    let line = serde_json::to_string(value)?;
    stdout.write_all(line.as_bytes())?;
    stdout.write_all(b"\n")?;
    stdout.flush()
}
