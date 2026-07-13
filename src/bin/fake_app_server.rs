//! Deterministic offline stand-in for `codex app-server --listen stdio://`.
//! Reads JSONL requests on stdin, writes JSONL responses/notifications on
//! stdout: enough of the happy path for offline `doctor`/`run` and tests.

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
            let padding = "x".repeat(MAX_FRAME_LEN + 1024);
            let line = format!(
                "{}\n",
                json!({ "id": id, "result": { "padding": padding } })
            );
            stdout.write_all(line.as_bytes())?;
            stdout.flush()?;
            Ok(true)
        }
        "oversized_no_newline" => {
            // Exercise the pre-allocation frame limit: keep stdout open after
            // writing an unterminated payload.
            stdout.write_all(&vec![b'x'; MAX_FRAME_LEN + 1])?;
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
    let approval_mode = arg_value(&args, "--approval-mode");
    if approval_mode.is_some() {
        if let Some(marker) = fail_marker.as_ref() {
            let _ = record_attempt(marker)?;
        }
    }

    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    let mut stdout = io::stdout();
    let mut thread_counter: u64 = 0;
    let mut turn_counter: u64 = 0;
    let mut fault_pending = fake_mode.is_some();

    while let Some(line) = lines.next() {
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
                        "serverInfo": { "name": "fake-codex-app-server", "version": "0.144.3" }
                    }
                }),
            )?,
            "account/read" => send(
                &mut stdout,
                &json!({
                    "id": id,
                    "result": {
                        "account": {
                            "type": "chatgpt",
                            "email": "offline@example.invalid",
                            "planType": "pro"
                        },
                        "requiresOpenaiAuth": false
                    }
                }),
            )?,
            "model/list" => send(
                &mut stdout,
                &json!({
                    "id": id,
                    "result": {
                        "data": [
                            {
                                "id": REQUIRED_MODEL,
                                "model": REQUIRED_MODEL,
                                "displayName": "Spark",
                                "description": "deterministic offline fixture",
                                "hidden": false,
                                "isDefault": true,
                                "defaultReasoningEffort": "medium",
                                "supportedReasoningEfforts": []
                            }
                        ]
                    }
                }),
            )?,
            "account/rateLimits/read" => send(
                &mut stdout,
                &json!({
                    "id": id,
                    "result": {
                        "rateLimits": {
                            "limitId": "codex",
                            "limitName": "Codex",
                            "planType": "pro",
                            "primary": { "usedPercent": 0, "resetsAt": null, "windowDurationMins": null },
                            "secondary": null,
                            "credits": null,
                            "individualLimit": null,
                            "rateLimitReachedType": null
                        },
                        "rateLimitsByLimitId": {
                            "codex": {
                                "limitId": "codex",
                                "limitName": "Codex",
                                "planType": "pro",
                                "primary": { "usedPercent": 0, "resetsAt": null, "windowDurationMins": null },
                                "secondary": null,
                                "credits": null,
                                "individualLimit": null,
                                "rateLimitReachedType": null
                            }
                        }
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
                            "thread": {
                                "id": thread_id,
                                "cliVersion": "0.144.3",
                                "createdAt": 1,
                                "updatedAt": 1,
                                "cwd": "/tmp",
                                "ephemeral": true,
                                "modelProvider": "openai",
                                "preview": "",
                                "sessionId": "offline-session",
                                "source": "appServer",
                                "status": { "type": "idle" },
                                "turns": []
                            },
                            "model": model,
                            "modelProvider": "openai",
                            "approvalPolicy": "on-request",
                            "approvalsReviewer": "user",
                            "cwd": "/tmp",
                            "sandbox": "read-only"
                        }
                    }),
                )?;
                send(
                    &mut stdout,
                    &json!({
                        "method": "thread/started",
                        "params": {
                            "thread": {
                                "id": thread_id,
                                "cliVersion": "0.144.3",
                                "createdAt": 1,
                                "updatedAt": 1,
                                "cwd": "/tmp",
                                "ephemeral": true,
                                "modelProvider": "openai",
                                "preview": "",
                                "sessionId": "offline-session",
                                "source": "appServer",
                                "status": { "type": "idle" },
                                "turns": []
                            }
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
                        "result": { "turn": { "id": turn_id, "status": "inProgress", "items": [] } }
                    }),
                )?;
                if fake_mode.as_deref() == Some("desync_after_turn_start") {
                    stdout.write_all(b"not-a-valid-jsonl-frame\n")?;
                    stdout.flush()?;
                    continue;
                }
                send(
                    &mut stdout,
                    &json!({
                                "method": "turn/started",
                    "params": { "threadId": thread_id, "turn": { "id": turn_id, "status": "inProgress", "items": [] } }
                            }),
                )?;
                if let Some(mode) = approval_mode.as_deref() {
                    handle_approval_mode(&mut lines, &mut stdout, mode, &thread_id, &turn_id)?;
                } else {
                    send_turn_completed(&mut stdout, &thread_id, &turn_id, "completed")?;
                }
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

fn handle_approval_mode(
    lines: &mut impl Iterator<Item = io::Result<String>>,
    stdout: &mut impl Write,
    mode: &str,
    thread_id: &str,
    turn_id: &str,
) -> io::Result<()> {
    let approval_id = 9001;
    send_approval_request(stdout, approval_id, thread_id, turn_id)?;

    if mode == "timeout" {
        std::process::exit(0);
    }

    let first_decision = read_decision(lines)?.unwrap_or_else(|| "missing".to_string());
    match mode {
        "duplicate" => {
            send_approval_request(stdout, approval_id, thread_id, turn_id)?;
            let _ = read_decision(lines)?;
        }
        "restart_unresolved" => {
            stdout.write_all(b"not-a-valid-jsonl-frame\n")?;
            stdout.flush()?;
        }
        _ => {
            let status = if first_decision == "accept" {
                "completed"
            } else {
                "failed"
            };
            send_turn_completed(stdout, thread_id, turn_id, status)?;
        }
    }
    Ok(())
}

fn send_approval_request(
    stdout: &mut impl Write,
    id: u64,
    thread_id: &str,
    turn_id: &str,
) -> io::Result<()> {
    send(
        stdout,
        &json!({
            "id": id,
            "method": "item/commandExecution/requestApproval",
            "params": {
                "approvalId": "approval-1",
                "command": "echo deterministic-fake-approval",
                "itemId": "item-1",
                "startedAtMs": 1,
                "threadId": thread_id,
                "turnId": turn_id
            }
        }),
    )
}

fn read_decision(
    lines: &mut impl Iterator<Item = io::Result<String>>,
) -> io::Result<Option<String>> {
    let Some(line) = lines.next() else {
        return Ok(None);
    };
    let line = line?;
    let value: Value = match serde_json::from_str(line.trim()) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    Ok(value
        .get("result")
        .and_then(|result| result.get("decision"))
        .and_then(Value::as_str)
        .map(str::to_string))
}

fn send_turn_completed(
    stdout: &mut impl Write,
    thread_id: &str,
    turn_id: &str,
    status: &str,
) -> io::Result<()> {
    send(
        stdout,
        &json!({
            "method": "turn/completed",
            "params": { "threadId": thread_id, "turn": { "id": turn_id, "status": status, "items": [] } }
        }),
    )
}

fn send(stdout: &mut impl Write, value: &Value) -> io::Result<()> {
    let line = serde_json::to_string(value)?;
    stdout.write_all(line.as_bytes())?;
    stdout.write_all(b"\n")?;
    stdout.flush()
}
