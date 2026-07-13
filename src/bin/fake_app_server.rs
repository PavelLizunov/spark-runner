//! Deterministic offline stand-in for `codex app-server --listen stdio://`.
//! Reads JSONL requests on stdin, writes JSONL responses/notifications on
//! stdout: enough of the happy path for offline `doctor`/`run` and tests.

use std::io::{self, BufRead, Write};

use serde_json::{json, Value};

const REQUIRED_MODEL: &str = "gpt-5.3-codex-spark";

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut thread_counter: u64 = 0;
    let mut turn_counter: u64 = 0;

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
