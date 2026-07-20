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

struct ApprovalFixture<'a> {
    mode: &'a str,
    approval_key: &'a str,
    approval_method: &'a str,
    wire_marker: Option<&'a Path>,
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let fake_mode = arg_value(&args, "--fake-mode");
    let fail_marker = arg_value(&args, "--fail-marker").map(PathBuf::from);
    let approval_mode = arg_value(&args, "--approval-mode");
    let approval_key =
        arg_value(&args, "--approval-id").unwrap_or_else(|| "approval-1".to_string());
    let approval_method = arg_value(&args, "--approval-method")
        .unwrap_or_else(|| "item/commandExecution/requestApproval".to_string());
    let barrier_phase = arg_value(&args, "--barrier-phase");
    let barrier_marker = arg_value(&args, "--barrier-marker").map(PathBuf::from);
    let codex_home_marker = arg_value(&args, "--codex-home-marker").map(PathBuf::from);
    let thread_cwd_marker = arg_value(&args, "--thread-cwd-marker").map(PathBuf::from);
    if let Some(marker) = arg_value(&args, "--pid-marker") {
        std::fs::write(marker, std::process::id().to_string())?;
    }
    if let Some(marker) = codex_home_marker {
        let home = std::env::var_os("CODEX_HOME").ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "fixture CODEX_HOME is unavailable")
        })?;
        std::fs::write(marker, home.to_string_lossy().as_bytes())?;
    }
    let wire_marker = arg_value(&args, "--wire-marker").map(PathBuf::from);
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
    let mut awaiting_initialized = false;

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

        // Deterministic cancellation split point: the parent has already
        // flushed this request, but this fixture deliberately withholds the
        // response until the parent either cancels or releases stdin. Tests
        // synchronize on the marker rather than sleeping.
        if barrier_phase.as_deref() == Some(method) {
            if let Some(marker) = barrier_marker.as_ref() {
                std::fs::write(marker, method)?;
            }
            let _ = read_server_message(&mut lines)?;
            continue;
        }

        if awaiting_initialized {
            if method != "initialized" || request.get("id").is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "strict fixture expected initialized notification",
                ));
            }
            awaiting_initialized = false;
            continue;
        }

        if fault_pending {
            fault_pending = false;
            if maybe_apply_fault(fake_mode.as_deref(), fail_marker.as_ref(), &mut stdout, id)? {
                continue;
            }
        }

        match method {
            "initialize" => {
                send(
                    &mut stdout,
                    &json!({
                    "id": id,
                    "result": {
                        "serverInfo": { "name": "fake-codex-app-server", "version": "0.144.6" }
                    }
                    }),
                )?;
                awaiting_initialized = fake_mode.as_deref() == Some("strict_initialize");
            }
            "account/read" => {
                // T11 fixture: a conforming server may need an owner answer
                // before releasing an unrelated RPC response.
                if fake_mode.as_deref() == Some("approval_during_account") {
                    send_approval_request(
                        &mut stdout,
                        -9001,
                        "bootstrap-thread",
                        "bootstrap-turn",
                        "bootstrap-approval",
                        "item/commandExecution/requestApproval",
                    )?;
                    let response = read_server_message(&mut lines)?;
                    let decision = response
                        .as_ref()
                        .and_then(|value| value.pointer("/result/decision"))
                        .and_then(Value::as_str);
                    if decision != Some("accept") {
                        return Ok(());
                    }
                }
                send(
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
                )?
            }
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
            "account/rateLimits/read" if request.get("params").is_some() => send(
                &mut stdout,
                &json!({
                    "id": id,
                    "error": { "code": -32602, "message": "params must be null" }
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
                            "primary": { "usedPercent": if fake_mode.as_deref() == Some("quota_exhausted") { 100 } else { 0 }, "resetsAt": null, "windowDurationMins": null },
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
                                "primary": { "usedPercent": if fake_mode.as_deref() == Some("quota_exhausted") { 100 } else { 0 }, "resetsAt": null, "windowDurationMins": null },
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
                if let Some(marker) = thread_cwd_marker.as_ref() {
                    let cwd = params
                        .get("cwd")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    std::fs::write(marker, cwd.as_bytes())?;
                }
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
                                "cliVersion": "0.144.6",
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
                                "cliVersion": "0.144.6",
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
                if fake_mode.as_deref() == Some("model_rerouted") {
                    send(
                        &mut stdout,
                        &json!({
                            "method": "model/rerouted",
                            "params": { "model": "gpt-unpinned-reroute" }
                        }),
                    )?;
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
                    handle_approval_mode(
                        &mut lines,
                        &mut stdout,
                        &thread_id,
                        &turn_id,
                        ApprovalFixture {
                            mode,
                            approval_key: &approval_key,
                            approval_method: &approval_method,
                            wire_marker: wire_marker.as_deref(),
                        },
                    )?;
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
    thread_id: &str,
    turn_id: &str,
    fixture: ApprovalFixture<'_>,
) -> io::Result<()> {
    let approval_id = 9001;
    send_approval_request(
        stdout,
        approval_id,
        thread_id,
        turn_id,
        fixture.approval_key,
        fixture.approval_method,
    )?;

    if fixture.mode == "timeout" {
        std::process::exit(0);
    }

    let first = read_server_message(lines)?;
    record_wire(fixture.wire_marker, first.as_ref())?;
    if first
        .as_ref()
        .and_then(|value| value.get("method"))
        .and_then(Value::as_str)
        == Some("turn/interrupt")
    {
        let id = first.as_ref().and_then(|value| value.get("id")).cloned();
        send(stdout, &json!({ "id": id, "result": {} }))?;
        send_turn_completed(stdout, thread_id, turn_id, "failed")?;
        return Ok(());
    }
    let first_decision = first
        .as_ref()
        .and_then(|value| value.get("result"))
        .and_then(|result| result.get("decision"))
        .and_then(Value::as_str)
        .unwrap_or("missing");
    match fixture.mode {
        "wait_interrupt" => {
            let second = read_server_message(lines)?;
            record_wire(fixture.wire_marker, second.as_ref())?;
            if second
                .as_ref()
                .and_then(|value| value.get("method"))
                .and_then(Value::as_str)
                == Some("turn/interrupt")
            {
                let id = second.as_ref().and_then(|value| value.get("id")).cloned();
                send(stdout, &json!({ "id": id, "result": {} }))?;
                send_turn_completed(stdout, thread_id, turn_id, "failed")?;
            }
        }
        "interrupt_timeout" => {
            let second = read_server_message(lines)?;
            record_wire(fixture.wire_marker, second.as_ref())?;
            if second
                .as_ref()
                .and_then(|value| value.get("method"))
                .and_then(Value::as_str)
                == Some("turn/interrupt")
            {
                // The marker above proves the owner completed the interrupt
                // write. Keep the protocol peer alive without a response so
                // the owner's bounded wait crosses the after-write timeout
                // boundary deterministically.
                std::thread::park();
            }
        }
        "duplicate" => {
            send_approval_request(
                stdout,
                approval_id,
                thread_id,
                turn_id,
                fixture.approval_key,
                fixture.approval_method,
            )?;
            let duplicate = read_server_message(lines)?;
            record_wire(fixture.wire_marker, duplicate.as_ref())?;
        }
        "restart_unresolved" => {
            stdout.write_all(b"not-a-valid-jsonl-frame\n")?;
            stdout.flush()?;
        }
        _ => {
            let permissions_granted = first
                .as_ref()
                .and_then(|value| value.pointer("/result/permissions/network/enabled"))
                .and_then(Value::as_bool)
                == Some(true);
            let status = if first_decision == "accept" || permissions_granted {
                "completed"
            } else {
                "failed"
            };
            send_turn_completed(stdout, thread_id, turn_id, status)?;
        }
    }
    Ok(())
}

fn record_wire(marker: Option<&Path>, value: Option<&Value>) -> io::Result<()> {
    let Some(marker) = marker else {
        return Ok(());
    };
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(marker)?;
    if let Some(value) = value {
        writeln!(file, "{}", serde_json::to_string(value)?)?;
    }
    Ok(())
}

fn send_approval_request(
    stdout: &mut impl Write,
    id: i64,
    thread_id: &str,
    turn_id: &str,
    approval_key: &str,
    approval_method: &str,
) -> io::Result<()> {
    let params = match approval_method {
        "item/fileChange/requestApproval" => json!({
            "grantRoot": "/tmp/fake-write-root",
            "reason": "deterministic fake file change",
            "itemId": "item-1",
            "startedAtMs": 1,
            "threadId": thread_id,
            "turnId": turn_id,
        }),
        "item/permissions/requestApproval" => json!({
            "cwd": "/tmp/fake-cwd",
            "reason": "deterministic fake permission request",
            "permissions": {
                "fileSystem": { "entries": [
                    {
                        "access": "write",
                        "path": {
                            "type": "special",
                            "value": { "kind": "project_roots", "subpath": "generated" }
                        }
                    },
                    {
                        "access": "read",
                        "path": {
                            "type": "special",
                            "value": {
                                "kind": "unknown",
                                "path": "/tmp/fake-external-root",
                                "subpath": "inputs"
                            }
                        }
                    }
                ] },
                "network": { "enabled": true }
            },
            "itemId": "item-1",
            "startedAtMs": 1,
            "threadId": thread_id,
            "turnId": turn_id,
        }),
        "execCommandApproval" => json!({
            "approvalId": approval_key,
            "callId": "call-1",
            "command": ["echo", "deterministic-fake-approval"],
            "conversationId": thread_id,
            "cwd": "/tmp/fake-cwd",
            "parsedCmd": [],
            "reason": "deterministic fake command",
        }),
        "applyPatchApproval" => json!({
            "callId": "call-1",
            "conversationId": thread_id,
            "fileChanges": { "/tmp/fake-file": { "type": "update", "unified_diff": "@@ -0,0 +1 @@\n+deterministic fake patch\n" } },
            "reason": "deterministic fake patch",
        }),
        _ => json!({
            "approvalId": approval_key,
            "command": "echo deterministic-fake-approval",
            "cwd": "/tmp/fake-cwd",
            "reason": "deterministic fake command",
            "itemId": "item-1",
            "startedAtMs": 1,
            "threadId": thread_id,
            "turnId": turn_id
        }),
    };
    send(
        stdout,
        &json!({
            "id": id,
            "method": approval_method,
            "params": params,
        }),
    )
}

fn read_server_message(
    lines: &mut impl Iterator<Item = io::Result<String>>,
) -> io::Result<Option<Value>> {
    let Some(line) = lines.next() else {
        return Ok(None);
    };
    let line = line?;
    let value: Value = match serde_json::from_str(line.trim()) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    Ok(Some(value))
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
