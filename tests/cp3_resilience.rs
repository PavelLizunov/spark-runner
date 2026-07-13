//! CP3 regression tests: bounded/malformed JSONL frames and unknown response
//! ids must poison the session (ADR-004), the doctor flow gets exactly one
//! controlled app-server restart on a recoverable desync, and fails closed if
//! the restart also desyncs. All deterministic against the offline
//! `fake_app_server` fixture via its `--fake-mode`/`--fail-marker` flags; no
//! live app-server involved, no sleeps.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use spark_runner::orchestrator::run_doctor_with_fake_server_args;

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
    assert!(error.to_string().contains("oversized protocol frame"));
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
