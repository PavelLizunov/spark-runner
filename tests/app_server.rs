//! Spawns the compiled `fake_app_server` binary and drives a doctor-like
//! flow against it over JSONL, verifying the exact required model and a
//! terminal `turn/completed` status, while tolerating interleaved
//! notifications (ADR-004).

use std::process::Stdio;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command};

const REQUIRED_MODEL: &str = "gpt-5.3-codex-spark";

async fn send(stdin: &mut ChildStdin, id: u64, method: &str, params: Value) {
    let request = if params.is_null() {
        json!({ "id": id, "method": method })
    } else {
        json!({ "id": id, "method": method, "params": params })
    };
    let line = serde_json::to_string(&request).expect("serialize request");
    stdin
        .write_all(line.as_bytes())
        .await
        .expect("write request");
    stdin.write_all(b"\n").await.expect("write newline");
    stdin.flush().await.expect("flush stdin");
}

async fn next_message<R: AsyncBufReadExt + Unpin>(reader: &mut R) -> Value {
    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line).await.expect("read line");
        assert!(bytes_read > 0, "fake_app_server stdout closed unexpectedly");
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        return serde_json::from_str(trimmed).expect("valid JSON line from fake app-server");
    }
}

async fn wait_for_response<R: AsyncBufReadExt + Unpin>(reader: &mut R, id: u64) -> Value {
    loop {
        let message = next_message(reader).await;
        if message.get("id").and_then(Value::as_u64) == Some(id) {
            return message;
        }
    }
}

async fn wait_for_notification<R: AsyncBufReadExt + Unpin>(reader: &mut R, method: &str) -> Value {
    loop {
        let message = next_message(reader).await;
        if message.get("id").is_none()
            && message.get("method").and_then(Value::as_str) == Some(method)
        {
            return message;
        }
    }
}

#[tokio::test]
async fn fake_app_server_completes_one_ephemeral_turn() {
    let exe = env!("CARGO_BIN_EXE_fake_app_server");
    let mut child = Command::new(exe)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn fake_app_server");

    let mut stdin = child.stdin.take().expect("stdin");
    let stdout = child.stdout.take().expect("stdout");
    let mut reader = BufReader::new(stdout);

    send(
        &mut stdin,
        1,
        "initialize",
        json!({ "clientInfo": { "name": "spark-runner-test", "version": "0.0.0" } }),
    )
    .await;
    let initialize_response = wait_for_response(&mut reader, 1).await;
    assert!(initialize_response.get("result").is_some());

    send(&mut stdin, 2, "account/read", json!({})).await;
    let account_response = wait_for_response(&mut reader, 2).await;
    assert_eq!(account_response["result"]["account"]["type"], "chatgpt");

    send(&mut stdin, 3, "model/list", json!({})).await;
    let model_list_response = wait_for_response(&mut reader, 3).await;
    let models = model_list_response["result"]["data"]
        .as_array()
        .expect("data array");
    assert!(models.iter().any(|model| model["id"] == REQUIRED_MODEL));

    send(&mut stdin, 4, "account/rateLimits/read", Value::Null).await;
    let rate_limits_response = wait_for_response(&mut reader, 4).await;
    assert_eq!(
        rate_limits_response["result"]["rateLimits"]["primary"]["usedPercent"],
        0
    );
    assert!(rate_limits_response["result"]["rateLimitsByLimitId"].is_object());

    send(
        &mut stdin,
        5,
        "thread/start",
        json!({
            "sandbox": "read-only",
            "approvalPolicy": "on-request",
            "ephemeral": true,
            "model": REQUIRED_MODEL,
            "cwd": std::env::temp_dir().to_string_lossy(),
        }),
    )
    .await;
    let thread_response = wait_for_response(&mut reader, 5).await;
    assert_eq!(thread_response["result"]["model"], REQUIRED_MODEL);
    let thread_id = thread_response["result"]["thread"]["id"]
        .as_str()
        .expect("thread.id")
        .to_string();

    // Interleaved notification: must be tolerated, not required before the response.
    let thread_started = wait_for_notification(&mut reader, "thread/started").await;
    assert_eq!(thread_started["params"]["thread"]["id"], thread_id);

    send(
        &mut stdin,
        6,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "doctor readiness check" }],
        }),
    )
    .await;
    let turn_start_response = wait_for_response(&mut reader, 6).await;
    assert_eq!(turn_start_response["result"]["turn"]["id"], "fake-turn-1");
    assert_eq!(
        turn_start_response["result"]["turn"]["status"],
        "inProgress"
    );

    // "turn/started" is interleaved before "turn/completed"; must be tolerated.
    let turn_completed = wait_for_notification(&mut reader, "turn/completed").await;
    assert_eq!(turn_completed["params"]["turn"]["status"], "completed");
    assert_eq!(turn_completed["params"]["threadId"], thread_id);

    let _ = child.start_kill();
    let _ = child.wait().await;
}

/// Regression test for the process-group fix: a descendant forked by the
/// spawned launcher (e.g. an npm/Node wrapper spawning the real app-server)
/// must be terminated by `shutdown()` too, not left orphaned holding stderr.
#[cfg(unix)]
#[tokio::test]
async fn shutdown_terminates_process_group_descendant() {
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use spark_runner::process::ChildProcess;

    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let pidfile = std::env::temp_dir().join(format!("spark-runner-pgtest-{unique}.pid"));
    let _ = std::fs::remove_file(&pidfile);

    // The immediate child forks a background descendant that writes to the
    // inherited stderr pipe and then holds it open by sleeping; the parent
    // shell waits on it. Only a process-group kill reaches the descendant.
    let script = format!(
        "(echo descendant-stderr 1>&2; sleep 100) & echo $! > '{}'; wait $!",
        pidfile.display()
    );

    let spawned =
        ChildProcess::spawn("/bin/sh", &["-c".to_string(), script], None).expect("spawn /bin/sh");
    let mut process = spawned.process;

    let deadline = Instant::now() + Duration::from_secs(5);
    let descendant_pid: i32 = loop {
        if let Ok(contents) = std::fs::read_to_string(&pidfile) {
            if let Ok(pid) = contents.trim().parse::<i32>() {
                break pid;
            }
        }
        assert!(
            Instant::now() < deadline,
            "descendant never recorded its pid"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let _ = std::fs::remove_file(&pidfile);

    assert_eq!(
        unsafe { kill(descendant_pid, 0) },
        0,
        "descendant should be alive before shutdown"
    );

    let start = Instant::now();
    process.shutdown().await;
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "shutdown should complete quickly, took {:?}",
        start.elapsed()
    );

    let gone_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if unsafe { kill(descendant_pid, 0) } != 0 {
            break;
        }
        assert!(
            Instant::now() < gone_deadline,
            "descendant should be gone after shutdown"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
