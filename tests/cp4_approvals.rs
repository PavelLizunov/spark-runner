//! CP4 approval lifecycle regression tests. These use only the deterministic
//! fake app-server; no live model is started.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use spark_runner::client::ApprovalPolicy;
use spark_runner::orchestrator::{
    run_doctor_with_fake_server_args, run_doctor_with_fake_server_args_and_approval_policy,
};

fn unique_marker_path(label: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "spark-runner-cp4-{label}-{}-{unique}.marker",
        std::process::id()
    ))
}

#[tokio::test]
async fn owner_allow_approval_completes_turn() {
    let args = vec!["--approval-mode".to_string(), "allow".to_string()];

    let summary =
        run_doctor_with_fake_server_args_and_approval_policy(&args, ApprovalPolicy::AllowForTests)
            .await
            .expect("owner-origin approval should allow the fake command");

    assert!(
        summary.contains("turn_status=completed"),
        "summary: {summary}"
    );
}

#[tokio::test]
async fn default_deny_approval_interrupts_and_fails_turn_closed() {
    let args = vec!["--approval-mode".to_string(), "deny".to_string()];

    let summary = run_doctor_with_fake_server_args(&args)
        .await
        .expect("deny is a handled fail-closed approval decision");

    assert!(summary.contains("turn_status=failed"), "summary: {summary}");
}

#[tokio::test]
async fn approval_disconnect_fails_closed_without_hanging() {
    let args = vec!["--approval-mode".to_string(), "timeout".to_string()];

    let error = run_doctor_with_fake_server_args(&args)
        .await
        .expect_err("approval stream close must fail closed");

    let message = error.to_string();
    assert!(
        message.contains("io error") || message.contains("stdout closed"),
        "error: {message}"
    );
}

#[tokio::test]
async fn duplicate_approval_request_fails_closed() {
    let args = vec!["--approval-mode".to_string(), "duplicate".to_string()];

    let error =
        run_doctor_with_fake_server_args_and_approval_policy(&args, ApprovalPolicy::AllowForTests)
            .await
            .expect_err("duplicate approval ids must fail closed");

    assert!(
        error.to_string().contains("duplicate approval request"),
        "error: {error}"
    );
}

#[tokio::test]
async fn approval_boundary_blocks_restart_after_unresolved_desync() {
    let marker = unique_marker_path("restart-unresolved");
    let _ = std::fs::remove_file(&marker);
    let args = vec![
        "--approval-mode".to_string(),
        "restart_unresolved".to_string(),
        "--fail-marker".to_string(),
        marker.to_string_lossy().to_string(),
    ];

    let error =
        run_doctor_with_fake_server_args_and_approval_policy(&args, ApprovalPolicy::AllowForTests)
            .await
            .expect_err("desync after approval must not restart into ambiguous state");

    assert!(
        error.to_string().contains("restart denied fail-closed"),
        "error: {error}"
    );
    let attempts = std::fs::read_to_string(&marker)
        .expect("approval marker must be written")
        .lines()
        .count();
    assert_eq!(attempts, 1, "must not restart after approval boundary");
    let _ = std::fs::remove_file(&marker);
}
