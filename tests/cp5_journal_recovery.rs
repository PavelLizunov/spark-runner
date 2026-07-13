//! CP5 deterministic journal/recovery gate. No live model and no external
//! service is used: the test writes append-only SQLite events, simulates a
//! kill by stopping the writer without terminal decisions, reopens the file,
//! and rebuilds the projection.

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use serde_json::json;
use spark_runner::journal::{
    project_recovery, ApprovalRecoveryState, ApprovalTerminalDecision, ExecutionRecoveryState,
    ExecutionTerminalStatus, JournalConfig, JournalEvent, JournalWriter,
};

fn unique_journal(label: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "spark-runner-cp5-{label}-{}-{unique}.sqlite3",
        std::process::id()
    ))
}

#[tokio::test]
async fn restart_projection_marks_unknown_and_denied_without_replay() {
    let path = unique_journal("restart-projection");
    let writer = JournalWriter::open(JournalConfig::new(&path)).expect("open journal");

    writer
        .append(JournalEvent::ExecutionStarted {
            execution_id: "exec-killed".to_string(),
        })
        .await
        .expect("execution started");
    writer
        .append(JournalEvent::TurnStarted {
            execution_id: "exec-killed".to_string(),
            turn_id: "turn-never-replay".to_string(),
        })
        .await
        .expect("turn started");
    writer
        .append(JournalEvent::ApprovalRequested {
            execution_id: "exec-killed".to_string(),
            request_key: "approval-never-replay".to_string(),
            method: "item/commandExecution/requestApproval".to_string(),
        })
        .await
        .expect("approval requested");
    writer
        .shutdown()
        .await
        .expect("clean kill simulation flush");

    let restarted_writer = JournalWriter::open(JournalConfig::new(&path)).expect("reopen journal");
    restarted_writer
        .append(JournalEvent::Incident {
            execution_id: None,
            class: "restart".to_string(),
            message: "deterministic restart projection".to_string(),
        })
        .await
        .expect("restart incident");
    restarted_writer
        .shutdown()
        .await
        .expect("shutdown restarted writer");

    let projection = project_recovery(&path).expect("project recovery");
    assert_eq!(
        projection.executions.get("exec-killed"),
        Some(&ExecutionRecoveryState::UnknownAfterRestart)
    );
    assert_eq!(
        projection.approvals.get("approval-never-replay"),
        Some(&ApprovalRecoveryState::DeniedOnRestart)
    );
    assert_eq!(projection.replayed_turns, 0, "turns must never replay");
    assert_eq!(
        projection.replayed_approvals, 0,
        "approvals must never replay"
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
}

#[tokio::test]
async fn terminal_states_survive_projection() {
    let path = unique_journal("terminal-states");
    let writer = JournalWriter::open(JournalConfig::new(&path)).expect("open journal");
    writer
        .append(JournalEvent::ExecutionStarted {
            execution_id: "exec-done".to_string(),
        })
        .await
        .unwrap();
    writer
        .append(JournalEvent::ExecutionCompleted {
            execution_id: "exec-done".to_string(),
            status: ExecutionTerminalStatus::Completed,
        })
        .await
        .unwrap();
    writer
        .append(JournalEvent::ApprovalRequested {
            execution_id: "exec-done".to_string(),
            request_key: "approval-denied".to_string(),
            method: "item/fileChange/requestApproval".to_string(),
        })
        .await
        .unwrap();
    writer
        .append(JournalEvent::ApprovalDecided {
            execution_id: "exec-done".to_string(),
            request_key: "approval-denied".to_string(),
            method: "item/fileChange/requestApproval".to_string(),
            decision: ApprovalTerminalDecision::Denied,
        })
        .await
        .unwrap();
    writer.shutdown().await.unwrap();

    let projection = project_recovery(&path).unwrap();
    assert_eq!(
        projection.executions.get("exec-done"),
        Some(&ExecutionRecoveryState::Completed)
    );
    assert_eq!(
        projection.approvals.get("approval-denied"),
        Some(&ApprovalRecoveryState::Denied)
    );
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn redacts_before_persistence_and_raw_capture_requires_ttl_opt_in() {
    let path = unique_journal("redaction");
    let writer = JournalWriter::open(JournalConfig::new(&path)).expect("open journal");
    writer
        .append_with_capture(
            JournalEvent::RateLimitSnapshot {
                execution_id: "exec-redact".to_string(),
                snapshot: json!({ "remaining": 1, "token": "sk-secret-value" }),
            },
            Some("terminal leaked ghp_secret".to_string()),
            Some(json!({ "authorization": "Bearer raw-secret" })),
        )
        .await
        .unwrap();
    writer.shutdown().await.unwrap();

    let connection = Connection::open(&path).unwrap();
    let (payload, terminal_output, raw_capture): (String, Option<String>, Option<String>) = connection
        .query_row(
            "SELECT payload_json, terminal_output, raw_capture_json FROM journal_events LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert!(!payload.contains("sk-secret-value"));
    assert!(payload.contains("[REDACTED]"));
    assert!(
        terminal_output.is_none(),
        "terminal output needs explicit TTL"
    );
    assert!(raw_capture.is_none(), "raw capture needs explicit TTL");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn opt_in_capture_is_redacted_and_pruned_by_ttl() {
    let path = unique_journal("ttl");
    let mut config = JournalConfig::new(&path);
    config.terminal_output_ttl = Some(Duration::from_millis(1));
    config.raw_capture_ttl = Some(Duration::from_millis(1));
    let writer = JournalWriter::open(config).expect("open journal");
    writer
        .append_with_capture(
            JournalEvent::Incident {
                execution_id: Some("exec-ttl".to_string()),
                class: "capture".to_string(),
                message: "safe message".to_string(),
            },
            Some("terminal sk-terminal-secret".to_string()),
            Some(json!({ "token": "sk-raw-secret" })),
        )
        .await
        .unwrap();

    let connection = Connection::open(&path).unwrap();
    let (terminal_output, raw_capture): (Option<String>, Option<String>) = connection
        .query_row(
            "SELECT terminal_output, raw_capture_json FROM journal_events LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let terminal_output = terminal_output.expect("terminal output persisted by opt-in TTL");
    let raw_capture = raw_capture.expect("raw capture persisted by opt-in TTL");
    assert!(!terminal_output.contains("sk-terminal-secret"));
    assert!(!raw_capture.contains("sk-raw-secret"));

    tokio::time::sleep(Duration::from_millis(5)).await;
    let pruned = writer.prune_expired().await.unwrap();
    assert_eq!(pruned, 1);
    writer.shutdown().await.unwrap();
    let _ = std::fs::remove_file(&path);
}
