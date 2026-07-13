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
    let terminal_output: String = connection
        .query_row(
            "SELECT data FROM journal_captures WHERE kind = 'terminal_output'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let raw_capture: String = connection
        .query_row(
            "SELECT data FROM journal_captures WHERE kind = 'raw_capture'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!terminal_output.contains("sk-terminal-secret"));
    assert!(!raw_capture.contains("sk-raw-secret"));

    tokio::time::sleep(Duration::from_millis(5)).await;
    let pruned = writer.prune_expired().await.unwrap();
    assert_eq!(pruned, 2);
    let remaining_events: i64 = connection
        .query_row("SELECT COUNT(*) FROM journal_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        remaining_events, 1,
        "core audit event must remain append-only"
    );
    writer.shutdown().await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn legacy_event_capture_is_migrated_without_deleting_audit_event() {
    let path = unique_journal("legacy-capture");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE journal_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_ms INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                execution_id TEXT,
                turn_id TEXT,
                approval_key TEXT,
                payload_json TEXT NOT NULL,
                terminal_output TEXT,
                raw_capture_json TEXT,
                expires_at_ms INTEGER
            );
            INSERT INTO journal_events VALUES (1, 0, 'incident', NULL, NULL, NULL, '{}', 'legacy output', '{\"safe\":true}', 1);",
        )
        .unwrap();
    drop(connection);

    let writer = JournalWriter::open(JournalConfig::new(&path)).unwrap();
    let migrated = Connection::open(&path).unwrap();
    let captures: i64 = migrated
        .query_row("SELECT COUNT(*) FROM journal_captures", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(captures, 2);
    writer.prune_expired().await.unwrap();
    let events: i64 = migrated
        .query_row("SELECT COUNT(*) FROM journal_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(events, 1);
    writer.shutdown().await.unwrap();
    let _ = std::fs::remove_file(&path);
}
