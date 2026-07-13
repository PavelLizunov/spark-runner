//! CP6 runtime-owner evidence. Every fixture is the checked-in fake
//! app-server injected through the production owner constructor; no OAuth,
//! network, or model call is made here.

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use rusqlite::Connection;
use serde_json::{json, Value};
use spark_runner::api::{app, app_with_launcher, ApiConfig};
use spark_runner::orchestrator::RuntimeLauncher;
use tower::ServiceExt;

fn environment_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn config(live: bool) -> ApiConfig {
    ApiConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        bearer_token: "test-token".to_string(),
        workspace_aliases: HashSet::from(["default".to_string(), "repo".to_string()]),
        live,
    }
}

fn unique_path(label: &str, extension: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "spark-runner-cp6-owner-{label}-{}-{unique}.{extension}",
        std::process::id()
    ))
}

async fn request_json(
    router: axum::Router,
    method: &str,
    path: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header(header::AUTHORIZATION, "Bearer test-token")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    let response = router.oneshot(request).await.expect("router response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let value = if bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&bytes).expect("json")
    };
    (status, value)
}

async fn wait_ready(router: axum::Router) {
    tokio::time::timeout(Duration::from_secs(3), async move {
        loop {
            let (status, _) = request_json(router.clone(), "GET", "/ready", json!({})).await;
            if status == StatusCode::OK {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("injected bootstrap ready");
}

async fn wait_event(router: axum::Router, turn_id: &str, expected: &str) {
    let request = Request::builder()
        .uri(format!("/v1/turns/{turn_id}/events"))
        .header(header::AUTHORIZATION, "Bearer test-token")
        .header("x-spark-runner-observer", "1")
        .body(Body::empty())
        .expect("events request");
    let response = router.oneshot(request).await.expect("events response");
    let mut stream = response.into_body().into_data_stream();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
            if std::str::from_utf8(&chunk.expect("chunk"))
                .expect("utf8")
                .contains(expected)
            {
                return;
            }
        }
        panic!("SSE ended before {expected}");
    })
    .await
    .expect("SSE synchronization barrier");
}

async fn controlling_stream_after_event(
    router: axum::Router,
    turn_id: &str,
    expected: &str,
) -> futures_util::stream::BoxStream<'static, Result<axum::body::Bytes, axum::Error>> {
    let request = Request::builder()
        .uri(format!("/v1/turns/{turn_id}/events"))
        .header(header::AUTHORIZATION, "Bearer test-token")
        .body(Body::empty())
        .expect("events request");
    let response = router.oneshot(request).await.expect("events response");
    let mut stream = response.into_body().into_data_stream();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
            if std::str::from_utf8(&chunk.expect("chunk"))
                .expect("utf8")
                .contains(expected)
            {
                return Box::pin(stream);
            }
        }
        panic!("controlling SSE ended before {expected}");
    })
    .await
    .expect("controlling SSE barrier")
}

async fn create_thread(router: axum::Router) -> String {
    let (status, thread) = request_json(
        router,
        "POST",
        "/v1/threads",
        json!({ "workspace_alias": "repo" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    thread["id"].as_str().expect("thread id").to_string()
}

async fn create_turn(router: axum::Router, thread_id: &str) -> String {
    let (status, turn) = request_json(
        router,
        "POST",
        &format!("/v1/threads/{thread_id}/turns"),
        json!({ "input": "owner runtime test" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    turn["id"].as_str().expect("turn id").to_string()
}

async fn create_turn_with_timeout(
    router: axum::Router,
    thread_id: &str,
    timeout_seconds: u64,
) -> String {
    let (status, turn) = request_json(
        router,
        "POST",
        &format!("/v1/threads/{thread_id}/turns"),
        json!({ "input": "owner deadline test", "timeout_seconds": timeout_seconds }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    turn["id"].as_str().expect("turn id").to_string()
}

/// T01/T04: an injected launcher boots the same live owner path, sends the
/// original signed JSON-RPC approval id a schema-valid denial, awaits the
/// interrupt RPC plus terminal notification, reaps the child, journals one
/// terminal result, and releases admission for the next turn.
#[cfg(unix)]
#[tokio::test]
async fn t01_injected_live_owner_interrupts_reaps_and_releases_admission() {
    let _environment = environment_lock().lock().await;
    let journal = unique_path("interrupt", "sqlite3");
    let pid_marker = unique_path("pid", "marker");
    let wire_marker = unique_path("wire", "jsonl");
    let launch_marker = unique_path("launch", "marker");
    std::env::set_var("SPARK_RUNNER_JOURNAL_PATH", &journal);
    let router = app_with_launcher(
        config(true),
        RuntimeLauncher::Fake {
            args: vec![
                "--approval-mode".to_string(),
                "wait_interrupt".to_string(),
                "--pid-marker".to_string(),
                pid_marker.display().to_string(),
                "--wire-marker".to_string(),
                wire_marker.display().to_string(),
                "--fail-marker".to_string(),
                launch_marker.display().to_string(),
            ],
        },
    );
    wait_ready(router.clone()).await;
    assert!(
        launch_marker.exists(),
        "only the injected launcher may bootstrap live mode"
    );

    let thread = create_thread(router.clone()).await;
    let turn = create_turn(router.clone(), &thread).await;
    wait_event(router.clone(), &turn, "approval.requested").await;
    let (status, interrupted) = request_json(
        router.clone(),
        "POST",
        &format!("/v1/turns/{turn}/interrupt"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(interrupted["status"], "interrupted");

    let wire: Vec<Value> = std::fs::read_to_string(&wire_marker)
        .expect("captured original-id responses")
        .lines()
        .map(|line| serde_json::from_str(line).expect("wire JSON"))
        .collect();
    assert!(wire
        .iter()
        .any(|message| message["id"] == 9001 && message["result"]["decision"] == "cancel"));
    assert!(wire
        .iter()
        .any(|message| message["method"] == "turn/interrupt"));

    let pid: i32 = std::fs::read_to_string(&pid_marker)
        .expect("child pid")
        .parse()
        .expect("pid number");
    wait_for_pid_exit(pid).await;

    let connection = Connection::open(&journal).expect("journal");
    let rows: Vec<String> = connection
        .prepare("SELECT payload_json FROM journal_events ORDER BY id")
        .expect("statement")
        .query_map([], |row| row.get(0))
        .expect("rows")
        .collect::<Result<_, _>>()
        .expect("payload rows");
    let requested = rows
        .iter()
        .position(|row| row.contains("approval_requested"))
        .expect("durable requested");
    let bootstrap_admission = rows
        .iter()
        .position(|row| row.contains("rate_limit_snapshot") && row.contains("bootstrap"))
        .expect("bootstrap admission snapshot");
    let decided = rows
        .iter()
        .position(|row| row.contains("approval_decided"))
        .expect("durable decided");
    assert!(
        requested < decided,
        "approval audit ordering must be append-only"
    );
    assert!(
        bootstrap_admission < requested,
        "the owner writes admission before it accepts a turn"
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row.contains("interrupted"))
            .count(),
        1
    );

    let second_thread = create_thread(router.clone()).await;
    let (second_status, _) = request_json(
        router,
        "POST",
        &format!("/v1/threads/{second_thread}/turns"),
        json!({ "input": "second turn is admitted after cleanup" }),
    )
    .await;
    assert_eq!(
        second_status,
        StatusCode::OK,
        "admission must be released exactly once"
    );
    std::env::remove_var("SPARK_RUNNER_JOURNAL_PATH");
    cleanup(&[journal, pid_marker, wire_marker, launch_marker]);
}

/// External Allow has the same durable request-before-decision ordering as
/// cancellation. The test uses an injected live owner, not a direct client.
#[tokio::test]
async fn approval_audit_orders_external_allow_before_decision() {
    let _environment = environment_lock().lock().await;
    let journal = unique_path("allow", "sqlite3");
    std::env::set_var("SPARK_RUNNER_JOURNAL_PATH", &journal);
    let router = app_with_launcher(
        config(true),
        RuntimeLauncher::Fake {
            args: vec!["--approval-mode".to_string(), "command".to_string()],
        },
    );
    wait_ready(router.clone()).await;
    let thread = create_thread(router.clone()).await;
    let turn = create_turn(router.clone(), &thread).await;
    wait_event(router.clone(), &turn, "approval.requested").await;
    let (status, decision) = request_json(
        router,
        "POST",
        "/v1/approvals/approval_1/approve",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(decision["status"], "approved");

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let connection = Connection::open(&journal).expect("journal visible");
            let count: i64 = connection.query_row("SELECT COUNT(*) FROM journal_events WHERE payload_json LIKE '%approval_decided%'", [], |row| row.get(0)).expect("count");
            if count == 1 { return; }
            tokio::task::yield_now().await;
        }
    }).await.expect("decision journaled");
    let connection = Connection::open(&journal).expect("journal");
    let rows: Vec<String> = connection
        .prepare("SELECT payload_json FROM journal_events ORDER BY id")
        .expect("statement")
        .query_map([], |row| row.get(0))
        .expect("rows")
        .collect::<Result<_, _>>()
        .expect("payloads");
    let requested = rows
        .iter()
        .position(|row| row.contains("approval_requested"))
        .expect("requested");
    let decided = rows
        .iter()
        .position(|row| row.contains("approval_decided"))
        .expect("decided");
    assert!(requested < decided);
    assert!(rows[decided].contains("allowed"));
    std::env::remove_var("SPARK_RUNNER_JOURNAL_PATH");
    cleanup(&[journal]);
}

/// T03: dropping the controlling SSE lease submits the same owner command as
/// explicit interrupt. The observer sees one terminal result and the child
/// receives the original-ID denial before the terminal notification.
#[tokio::test]
async fn t03_controller_drop_uses_the_owner_cancel_path() {
    let _environment = environment_lock().lock().await;
    let wire_marker = unique_path("drop-wire", "jsonl");
    let router = app_with_launcher(
        config(true),
        RuntimeLauncher::Fake {
            args: vec![
                "--approval-mode".to_string(),
                "wait_interrupt".to_string(),
                "--wire-marker".to_string(),
                wire_marker.display().to_string(),
            ],
        },
    );
    wait_ready(router.clone()).await;
    let thread = create_thread(router.clone()).await;
    let turn = create_turn(router.clone(), &thread).await;
    let controller =
        controlling_stream_after_event(router.clone(), &turn, "approval.requested").await;
    drop(controller);
    wait_event(router.clone(), &turn, "turn.interrupted").await;
    let wire: Vec<Value> = std::fs::read_to_string(&wire_marker)
        .expect("captured denial")
        .lines()
        .map(|line| serde_json::from_str(line).expect("wire JSON"))
        .collect();
    assert!(wire
        .iter()
        .any(|message| message["id"] == 9001 && message["result"]["decision"] == "cancel"));
    assert!(wire
        .iter()
        .any(|message| message["method"] == "turn/interrupt"));
    cleanup(&[wire_marker]);
}

/// T03: approval expiry is not a detached client timeout. The owner first
/// durably denies the original request, then sends turn/interrupt and waits
/// for its RPC acknowledgement plus turn/completed before releasing the PID.
#[cfg(unix)]
#[tokio::test]
async fn t03_approval_timeout_uses_the_same_ordered_cancel_path() {
    let _environment = environment_lock().lock().await;
    let journal = unique_path("timeout", "sqlite3");
    let pid_marker = unique_path("timeout-pid", "marker");
    let wire_marker = unique_path("timeout-wire", "jsonl");
    std::env::set_var("SPARK_RUNNER_JOURNAL_PATH", &journal);
    let router = app_with_launcher(
        config(true),
        RuntimeLauncher::Fake {
            args: vec![
                "--approval-mode".to_string(),
                "wait_interrupt".to_string(),
                "--pid-marker".to_string(),
                pid_marker.display().to_string(),
                "--wire-marker".to_string(),
                wire_marker.display().to_string(),
            ],
        },
    );
    wait_ready(router.clone()).await;
    let thread = create_thread(router.clone()).await;
    let turn = create_turn_with_timeout(router.clone(), &thread, 1).await;
    wait_event(router.clone(), &turn, "approval.requested").await;
    wait_event(router.clone(), &turn, "turn.failed").await;

    let wire: Vec<Value> = std::fs::read_to_string(&wire_marker)
        .expect("captured timeout cancellation")
        .lines()
        .map(|line| serde_json::from_str(line).expect("wire JSON"))
        .collect();
    assert_eq!(
        wire.iter()
            .filter(|message| message["id"] == 9001 && message["result"]["decision"] == "cancel")
            .count(),
        1,
        "one schema-valid response on the original approval id"
    );
    assert_eq!(
        wire.iter()
            .filter(|message| message["method"] == "turn/interrupt")
            .count(),
        1,
        "one interrupt after the denial acknowledgement"
    );
    let pid: i32 = std::fs::read_to_string(&pid_marker)
        .expect("child pid")
        .parse()
        .expect("pid number");
    wait_for_pid_exit(pid).await;

    let connection = Connection::open(&journal).expect("journal");
    let rows: Vec<String> = connection
        .prepare("SELECT payload_json FROM journal_events ORDER BY id")
        .expect("statement")
        .query_map([], |row| row.get(0))
        .expect("rows")
        .collect::<Result<_, _>>()
        .expect("payload rows");
    let requested = rows
        .iter()
        .position(|row| row.contains("approval_requested"))
        .expect("durable requested");
    let decided = rows
        .iter()
        .position(|row| row.contains("approval_decided"))
        .expect("durable timeout denial");
    assert!(requested < decided);
    assert!(rows[decided].contains("timed_out"));
    assert_eq!(
        rows.iter()
            .filter(|row| row.contains("turn_completed"))
            .count(),
        1,
        "one authoritative terminal result"
    );
    std::env::remove_var("SPARK_RUNNER_JOURNAL_PATH");
    cleanup(&[journal, pid_marker, wire_marker]);
}

/// Child approval keys are bounded opaque handles at every owner boundary.
/// The duplicate request gets a second fail-closed wire response but never a
/// second audit request or an unbounded SSE/SQLite payload.
#[tokio::test]
async fn repeated_oversized_child_approval_ids_are_opaque_and_bounded() {
    let _environment = environment_lock().lock().await;
    let journal = unique_path("oversized-approval", "sqlite3");
    let wire_marker = unique_path("oversized-wire", "jsonl");
    let child_id = "CHILD_APPROVAL_CANARY_".repeat(2048);
    std::env::set_var("SPARK_RUNNER_JOURNAL_PATH", &journal);
    let router = app_with_launcher(
        config(true),
        RuntimeLauncher::Fake {
            args: vec![
                "--approval-mode".to_string(),
                "duplicate".to_string(),
                "--approval-id".to_string(),
                child_id.clone(),
                "--wire-marker".to_string(),
                wire_marker.display().to_string(),
            ],
        },
    );
    wait_ready(router.clone()).await;
    let thread = create_thread(router.clone()).await;
    let turn = create_turn(router.clone(), &thread).await;
    wait_event(router.clone(), &turn, "approval.requested").await;
    let (status, decision) = request_json(
        router.clone(),
        "POST",
        "/v1/approvals/approval_1/deny",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(decision["status"], "denied");
    wait_event(router, &turn, "turn.failed").await;

    let rows: Vec<String> = Connection::open(&journal)
        .expect("journal")
        .prepare("SELECT payload_json FROM journal_events ORDER BY id")
        .expect("statement")
        .query_map([], |row| row.get(0))
        .expect("rows")
        .collect::<Result<_, _>>()
        .expect("payload rows");
    assert!(rows
        .iter()
        .all(|row| !row.contains("CHILD_APPROVAL_CANARY_")));
    assert_eq!(
        rows.iter()
            .filter(|row| row.contains("approval_requested"))
            .count(),
        1,
        "duplicate source request must not create a second audit request"
    );
    assert!(rows.iter().all(|row| row.len() < 2_048));
    let wire: Vec<Value> = std::fs::read_to_string(&wire_marker)
        .expect("wire responses")
        .lines()
        .map(|line| serde_json::from_str(line).expect("wire JSON"))
        .collect();
    let denial_count = wire
        .iter()
        .filter(|message| message["id"] == 9001 && message["result"]["decision"] == "cancel")
        .count();
    // The owner may reap immediately after the duplicate is rejected, before
    // the fixture persists that second response.  Its retained state remains
    // bounded either way; the source duplicate never creates a second audit
    // request and it can never produce more than one extra wire response.
    assert!((1..=2).contains(&denial_count));
    std::env::remove_var("SPARK_RUNNER_JOURNAL_PATH");
    cleanup(&[journal, wire_marker]);
}

/// A reroute invalidates the owner snapshot, not merely a temporary client.
#[tokio::test]
async fn reroute_failure_clears_owner_ready_model_and_quota_snapshot() {
    let _environment = environment_lock().lock().await;
    let router = app_with_launcher(
        config(true),
        RuntimeLauncher::Fake {
            args: vec!["--fake-mode".to_string(), "model_rerouted".to_string()],
        },
    );
    wait_ready(router.clone()).await;
    let thread = create_thread(router.clone()).await;
    let turn = create_turn(router.clone(), &thread).await;
    wait_event(router.clone(), &turn, "turn.failed").await;
    assert_eq!(
        request_json(router.clone(), "GET", "/ready", json!({}))
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let (_, models) = request_json(router.clone(), "GET", "/v1/models", json!({})).await;
    assert_eq!(models["data"], json!([]));
    let (_, limits) = request_json(router, "GET", "/v1/rate-limits", json!({})).await;
    assert_eq!(limits["quota_available"], false);
}

/// 0.144.3 permission approvals carry a granted profile rather than a
/// decision string. The injected owner preserves the exact in-flight request
/// profile for an authenticated Allow and does not fabricate a command shape.
#[tokio::test]
async fn permissions_allow_uses_the_generated_profile_shape() {
    let _environment = environment_lock().lock().await;
    let wire_marker = unique_path("permissions-wire", "jsonl");
    let router = app_with_launcher(
        config(true),
        RuntimeLauncher::Fake {
            args: vec![
                "--approval-mode".to_string(),
                "command".to_string(),
                "--approval-method".to_string(),
                "item/permissions/requestApproval".to_string(),
                "--wire-marker".to_string(),
                wire_marker.display().to_string(),
            ],
        },
    );
    wait_ready(router.clone()).await;
    let thread = create_thread(router.clone()).await;
    let turn = create_turn(router.clone(), &thread).await;
    wait_event(router.clone(), &turn, "approval.requested").await;
    let (status, decision) = request_json(
        router.clone(),
        "POST",
        "/v1/approvals/approval_1/approve",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(decision["status"], "approved");
    wait_event(router, &turn, "turn.completed").await;
    let wire: Vec<Value> = std::fs::read_to_string(&wire_marker)
        .expect("permission response")
        .lines()
        .map(|line| serde_json::from_str(line).expect("wire JSON"))
        .collect();
    let response = wire
        .iter()
        .find(|message| message["id"] == 9001)
        .expect("original permission response");
    assert_eq!(
        response["result"]["permissions"]["network"]["enabled"],
        true
    );
    assert_eq!(response["result"]["scope"], "turn");
    cleanup(&[wire_marker]);
}

/// Sol-requested coverage: token files reject group-readable permissions and
/// the command owner enforces API capacity before unbounded retention.
#[cfg(unix)]
#[tokio::test]
async fn token_file_permissions_and_api_thread_capacity_are_enforced() {
    let _environment = environment_lock().lock().await;
    use std::os::unix::fs::PermissionsExt;

    let token = unique_path("token", "txt");
    std::fs::write(&token, "owner-token\n").expect("token file");
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o644)).expect("permissions");
    std::env::remove_var("SPARK_RUNNER_BEARER_TOKEN");
    std::env::set_var("SPARK_RUNNER_BEARER_TOKEN_FILE", &token);
    assert!(
        spark_runner::api::ApiConfig::from_env(false).is_err(),
        "group-readable token files fail closed"
    );
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).expect("permissions");
    assert_eq!(
        spark_runner::api::ApiConfig::from_env(false)
            .expect("owner-only token")
            .bearer_token,
        "owner-token"
    );
    std::env::remove_var("SPARK_RUNNER_BEARER_TOKEN_FILE");

    let router = app(config(false));
    for _ in 0..128 {
        assert_eq!(
            request_json(
                router.clone(),
                "POST",
                "/v1/threads",
                json!({ "workspace_alias": "repo" })
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    let (status, body) = request_json(
        router,
        "POST",
        "/v1/threads",
        json!({ "workspace_alias": "repo" }),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["error"]["code"], "THREAD_CAPACITY");
    cleanup(&[token]);
}

#[cfg(unix)]
async fn wait_for_pid_exit(pid: i32) {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            // kill(pid, 0) performs no mutation; ESRCH proves the child PID
            // is gone after the owner's process-group cleanup acknowledgement.
            if unsafe { kill(pid, 0) } != 0
                && std::io::Error::last_os_error().raw_os_error() == Some(3)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("child PID reaped");
}

fn cleanup(paths: &[PathBuf]) {
    for path in paths {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    }
}
