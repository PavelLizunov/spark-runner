//! CP3 regression tests: bounded/malformed JSONL frames and unknown response
//! ids must poison the session (ADR-004), the doctor flow gets exactly one
//! controlled app-server restart on a recoverable desync, and fails closed if
//! the restart also desyncs. All deterministic against the offline
//! `fake_app_server` fixture via its `--fake-mode`/`--fail-marker` flags; no
//! live app-server involved, no sleeps.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use spark_runner::orchestrator::run_doctor_with_fake_server_args;
use spark_runner::process::{ChildProcess, STDERR_TAIL_BYTES};
use tokio::io::AsyncReadExt;

fn unique_marker_path(label: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "spark-runner-cp3-{label}-{}-{unique}.marker",
        std::process::id()
    ))
}

#[tokio::test]
async fn oversized_frame_poisons_session_and_fails_closed() {
    let args = vec!["--fake-mode".to_string(), "oversized_frame".to_string()];

    let result = run_doctor_with_fake_server_args(&args).await;

    let error = result.expect_err("an oversized frame must not be silently tolerated");
    let message = error.to_string();
    assert!(
        !message.contains("xxxxxxxxxx"),
        "oversized frame content must never appear in a sanitized error: {message}"
    );
}

/// T06: an unterminated frame is rejected at MAX_FRAME_LEN+1 without waiting
/// for a newline or retaining the untrusted payload.
#[tokio::test]
async fn t06_oversized_no_newline_frame_fails_closed() {
    let args = vec![
        "--fake-mode".to_string(),
        "oversized_no_newline".to_string(),
    ];
    let error = run_doctor_with_fake_server_args(&args)
        .await
        .expect_err("unterminated oversized frame must fail");
    let message = error.to_string();
    assert!(
        message.contains("oversized protocol frame"),
        "unterminated oversized frame changed failure class: {message}"
    );
}

/// T05: after turn/start has succeeded, a later desync is ambiguous and must
/// not trigger the complete-flow restart that would replay the turn.
#[tokio::test]
async fn t05_non_idempotent_retry_ambiguity_never_replays_turn() {
    let marker = unique_marker_path("post-turn-desync");
    let _ = std::fs::remove_file(&marker);
    let args = vec![
        "--fake-mode".to_string(),
        "desync_after_turn_start".to_string(),
        "--fail-marker".to_string(),
        marker.to_string_lossy().to_string(),
    ];
    let error = run_doctor_with_fake_server_args(&args)
        .await
        .expect_err("post-turn desync is ambiguous");
    assert!(error.to_string().contains("automatic replay denied"));
    assert_eq!(
        std::fs::read_to_string(&marker)
            .expect("single fixture invocation")
            .lines()
            .count(),
        1,
        "turn/start must never be replayed"
    );
    let _ = std::fs::remove_file(&marker);
}

#[tokio::test]
async fn malformed_frame_poisons_session_and_fails_closed() {
    let args = vec!["--fake-mode".to_string(), "malformed_frame".to_string()];

    let result = run_doctor_with_fake_server_args(&args).await;

    let error = result.expect_err("a malformed frame must not be silently skipped");
    let message = error.to_string();
    assert!(
        !message.contains("not-a-valid-jsonl-frame"),
        "malformed frame content must never appear in a sanitized error: {message}"
    );
}

#[tokio::test]
async fn unknown_response_id_poisons_session_and_fails_closed() {
    let args = vec!["--fake-mode".to_string(), "unknown_response_id".to_string()];

    let result = run_doctor_with_fake_server_args(&args).await;

    result.expect_err("a response for an id that was never requested is a protocol desync");
}

#[tokio::test]
async fn restart_recovers_from_one_time_desync() {
    let marker = unique_marker_path("restart-recovers");
    let _ = std::fs::remove_file(&marker);
    let args = vec![
        "--fake-mode".to_string(),
        "unknown_response_id_once".to_string(),
        "--fail-marker".to_string(),
        marker.to_string_lossy().to_string(),
    ];

    let result = run_doctor_with_fake_server_args(&args).await;

    let summary = result
        .expect("the first app-server process desyncs once; the controlled restart must recover");
    assert!(summary.contains("mode=offline"), "summary: {summary}");
    assert!(
        summary.contains("turn_status=completed"),
        "summary: {summary}"
    );

    let attempts = std::fs::read_to_string(&marker)
        .expect("fail marker must have been written by the fake app-server")
        .lines()
        .count();
    assert_eq!(
        attempts, 2,
        "expected exactly one restart (two app-server processes total)"
    );
    let _ = std::fs::remove_file(&marker);
}

#[tokio::test]
async fn fails_closed_after_restart_also_desyncs() {
    let marker = unique_marker_path("fails-closed");
    let _ = std::fs::remove_file(&marker);
    let args = vec![
        "--fake-mode".to_string(),
        "unknown_response_id".to_string(),
        "--fail-marker".to_string(),
        marker.to_string_lossy().to_string(),
    ];

    let result = run_doctor_with_fake_server_args(&args).await;

    result.expect_err("a desync that persists across the restart must fail closed");

    let attempts = std::fs::read_to_string(&marker)
        .expect("fail marker must have been written by the fake app-server")
        .lines()
        .count();
    assert_eq!(
        attempts, 2,
        "expected exactly one restart attempt, not zero or unlimited retries"
    );
    let _ = std::fs::remove_file(&marker);
}

/// T07: stderr uses a byte cap (not merely a line cap), including a single
/// oversized line and many short lines. Child-controlled bytes never escape
/// through the sanitized diagnostic snapshot.
#[cfg(unix)]
#[tokio::test]
async fn t07_stderr_tail_is_byte_bounded_and_not_disclosed() {
    let script = format!(
        "head -c {} /dev/zero | tr '\\0' X 1>&2; for i in $(seq 1 200); do echo line-$i 1>&2; done; echo fixture-complete",
        STDERR_TAIL_BYTES * 2
    );
    let spawned = ChildProcess::spawn("/bin/sh", &["-c".to_string(), script], None)
        .expect("spawn stderr fixture");
    let mut stdout = spawned.stdout;
    let mut marker = Vec::new();
    stdout
        .read_to_end(&mut marker)
        .await
        .expect("read completion marker");
    assert_eq!(marker, b"fixture-complete\n");
    let mut process = spawned.process;
    process.shutdown().await;
    assert!(
        process.stderr_tail_len().await <= STDERR_TAIL_BYTES,
        "stderr retention exceeded its byte cap"
    );
    let diagnostic = process.stderr_tail().await;
    assert!(diagnostic.starts_with("stderr_bytes_seen="));
    assert!(!diagnostic.contains("line-"));
}

/// T08: a child receives no ambient credential canaries and gets a dedicated
/// owner-only CODEX_HOME under the controlled working directory.
#[cfg(unix)]
#[tokio::test]
async fn t08_child_environment_excludes_secret_canaries_and_uses_private_codex_home() {
    use std::os::unix::fs::PermissionsExt;

    let cwd = unique_marker_path("private-codex-home");
    std::fs::create_dir_all(&cwd).expect("create controlled cwd");
    let canaries = [
        ("OPENAI_API_KEY", "cp6-openai-canary"),
        ("AWS_SECRET_ACCESS_KEY", "cp6-aws-canary"),
        ("HTTPS_PROXY", "https://cp6-proxy-canary.invalid"),
        ("SSH_AUTH_SOCK", "/tmp/cp6-ssh-canary"),
        ("CODEX_HOME", "/tmp/cp6-ambient-codex-home"),
    ];
    for (name, value) in &canaries {
        std::env::set_var(name, value);
    }
    let spawned = ChildProcess::spawn("/usr/bin/env", &[], Some(&cwd)).expect("spawn env fixture");
    let mut stdout = spawned.stdout;
    let mut bytes = Vec::new();
    stdout
        .read_to_end(&mut bytes)
        .await
        .expect("read env output");
    let mut process = spawned.process;
    process.shutdown().await;
    for (name, _) in &canaries {
        std::env::remove_var(name);
    }

    let output = String::from_utf8(bytes).expect("env is UTF-8");
    for (_, value) in &canaries[..4] {
        assert!(
            !output.contains(value),
            "secret canary reached child: {value}"
        );
    }
    let codex_home = output
        .lines()
        .find_map(|line| line.strip_prefix("CODEX_HOME="))
        .expect("private CODEX_HOME is set");
    assert_eq!(std::path::Path::new(codex_home), cwd.join("codex-home"));
    assert_eq!(
        std::fs::metadata(codex_home)
            .expect("CODEX_HOME metadata")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let _ = std::fs::remove_dir_all(cwd);
}

/// T10: runtime model reroute is observed after admission and fails closed;
/// it cannot be represented as a successful required-model turn.
#[tokio::test]
async fn t10_runtime_model_reroute_fails_closed() {
    let args = vec!["--fake-mode".to_string(), "model_rerouted".to_string()];
    let error = run_doctor_with_fake_server_args(&args)
        .await
        .expect_err("a post-admission reroute must fail closed");
    assert!(error.to_string().contains("substituted model"));
}

/// T10 admission also rejects an exhausted runtime quota before a thread or
/// turn can be started.
#[tokio::test]
async fn t10_exhausted_runtime_quota_fails_before_turn_start() {
    let args = vec!["--fake-mode".to_string(), "quota_exhausted".to_string()];
    let error = run_doctor_with_fake_server_args(&args)
        .await
        .expect_err("exhausted quota must block admission");
    assert!(error.to_string().contains("no remaining quota"));
}

/// T11: `initialized` is emitted as a notification immediately after the
/// successful initialize response, before any account/admission call.
#[tokio::test]
async fn t11_strict_initialize_then_initialized() {
    let args = vec!["--fake-mode".to_string(), "strict_initialize".to_string()];
    let summary = run_doctor_with_fake_server_args(&args)
        .await
        .expect("strict initialized handshake must complete");
    assert!(summary.contains("turn_status=completed"));
}
