use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use serde_json::{json, Value};
use spark_runner::api::{app, ApiConfig};
use tower::ServiceExt;

fn config() -> ApiConfig {
    ApiConfig {
        bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787),
        bearer_token: "test-token".to_string(),
        workspace_aliases: HashSet::from(["default".to_string(), "repo".to_string()]),
        live: false,
    }
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
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = if bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

async fn get_json(router: axum::Router, path: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .uri(path)
        .header(header::AUTHORIZATION, "Bearer test-token")
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = if bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

#[tokio::test]
async fn exposes_only_cp6_routes_and_authenticates_from_header() {
    let router = app(config());

    let health = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.clone().oneshot(health).await.unwrap().status(),
        StatusCode::OK
    );

    let unauth = Request::builder()
        .uri("/ready")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.clone().oneshot(unauth).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );

    for path in ["/ready", "/v1/runtime", "/v1/models", "/v1/rate-limits"] {
        assert_eq!(
            get_json(router.clone(), path).await.0,
            StatusCode::OK,
            "{path}"
        );
    }

    let models = get_json(router.clone(), "/v1/models").await.1;
    assert_eq!(models["data"][0]["id"], "gpt-5.3-codex-spark");

    let chat = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::AUTHORIZATION, "Bearer test-token")
        .body(Body::from("{}"))
        .unwrap();
    assert_eq!(
        router.oneshot(chat).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn rejects_payload_token_paths_wrong_model_and_large_contexts() {
    let router = app(config());

    let (status, _) = request_json(
        router.clone(),
        "POST",
        "/v1/threads",
        json!({ "workspace_alias": "../repo", "bearer_token": "ignored" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = request_json(
        router.clone(),
        "POST",
        "/v1/threads",
        json!({ "workspace_alias": "repo", "model": "gpt-other" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, thread) = request_json(
        router.clone(),
        "POST",
        "/v1/threads",
        json!({
            "workspace_alias": "repo",
            "model": "gpt-5.3-codex-spark",
            "sandbox": "read_only",
            "ephemeral": true
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let too_large = "x".repeat(8 * 1024 + 1);
    let (status, _) = request_json(
        router,
        "POST",
        &format!("/v1/threads/{}/turns", thread["id"].as_str().unwrap()),
        json!({ "workspace_alias": "repo", "input": too_large }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn fake_child_sse_resume_keeps_approval_and_terminal_events() {
    let router = app(config());
    let (_, thread) = request_json(
        router.clone(),
        "POST",
        "/v1/threads",
        json!({ "workspace_alias": "repo" }),
    )
    .await;
    let (status, turn) = request_json(
        router.clone(),
        "POST",
        &format!("/v1/threads/{}/turns", thread["id"].as_str().unwrap()),
        json!({ "workspace_alias": "repo", "input": "drive fake child" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let turn_id = turn["id"].as_str().unwrap();

    // The SSE stream is the synchronization barrier: the real app-server
    // approval request must be observed before an HTTP decision is sent.
    let events_request = Request::builder()
        .uri(format!("/v1/turns/{turn_id}/events"))
        .header(header::AUTHORIZATION, "Bearer test-token")
        .header("x-spark-runner-observer", "1")
        .body(Body::empty())
        .unwrap();
    let events_response = router.clone().oneshot(events_request).await.unwrap();
    let mut events = events_response.into_body().into_data_stream();
    let mut saw_approval = false;
    while let Some(chunk) = events.next().await {
        let chunk = chunk.unwrap();
        if std::str::from_utf8(&chunk)
            .unwrap()
            .contains("approval.requested")
        {
            saw_approval = true;
            break;
        }
    }
    assert!(saw_approval, "SSE must report a genuine pending approval");
    let (status, approval) = request_json(
        router.clone(),
        "POST",
        "/v1/approvals/approval_1/approve",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(approval["status"], "approved");
    let resumed = fetch_sse(router, &format!("/v1/turns/{turn_id}/events"), Some(1)).await;
    let event_types: Vec<&str> = resumed
        .iter()
        .map(|event| event["type"].as_str().unwrap())
        .collect();
    assert!(
        event_types.contains(&"approval.requested"),
        "resumed events: {event_types:?}"
    );
    assert!(
        event_types.contains(&"approval.decided"),
        "resumed events: {event_types:?}"
    );
    assert!(
        event_types.contains(&"turn.completed"),
        "resumed events: {event_types:?}"
    );
    assert!(resumed.iter().any(|event| event["terminal"] == true));
}

/// A default fixture denial can terminalize before its unsupported follow-up
/// interrupt RPC. That is a failed/unknown boundary, never a fabricated
/// interrupt or later completion; the injected T01 fixture covers the
/// confirmed real-ID interrupt path.
#[tokio::test]
async fn t01_interrupt_running_turn_is_terminal_once() {
    let router = app(config());
    let (_, thread) = request_json(
        router.clone(),
        "POST",
        "/v1/threads",
        json!({ "workspace_alias": "repo" }),
    )
    .await;
    let (_, turn) = request_json(
        router.clone(),
        "POST",
        &format!("/v1/threads/{}/turns", thread["id"].as_str().unwrap()),
        json!({ "input": "interrupt after server approval" }),
    )
    .await;
    let turn_id = turn["id"].as_str().unwrap();
    let controller = wait_for_sse_event(router.clone(), turn_id, "approval.requested").await;
    let (status, interrupted) = request_json(
        router.clone(),
        "POST",
        &format!("/v1/turns/{turn_id}/interrupt"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(interrupted["status"], "failed");
    let events = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        fetch_sse(router, &format!("/v1/turns/{turn_id}/events"), None),
    )
    .await
    .expect("terminal SSE replay");
    assert_eq!(
        events
            .iter()
            .filter(|event| event["terminal"] == true)
            .count(),
        1
    );
    assert!(events.iter().all(|event| event["type"] != "turn.completed"));
    drop(controller);
}

/// T02: the authenticated deny route controls the single real approval
/// request; it cannot be silently replaced by an allow decision.
#[tokio::test]
async fn t02_deny_approval_authority_fails_closed() {
    let router = app(config());
    let (_, thread) = request_json(
        router.clone(),
        "POST",
        "/v1/threads",
        json!({ "workspace_alias": "repo" }),
    )
    .await;
    let (_, turn) = request_json(
        router.clone(),
        "POST",
        &format!("/v1/threads/{}/turns", thread["id"].as_str().unwrap()),
        json!({ "input": "deny server approval" }),
    )
    .await;
    let turn_id = turn["id"].as_str().unwrap();
    let controller = wait_for_sse_event(router.clone(), turn_id, "approval.requested").await;
    let (status, decision) = request_json(
        router.clone(),
        "POST",
        "/v1/approvals/approval_1/deny",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(decision["status"], "denied");
    let events = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        fetch_sse(router, &format!("/v1/turns/{turn_id}/events"), None),
    )
    .await
    .expect("terminal SSE replay");
    assert!(events
        .iter()
        .any(|event| event["type"] == "approval.decided"));
    assert!(events.iter().any(|event| event["type"] == "turn.failed"));
    drop(controller);
}

/// T03: an unanswered genuine approval reaches its owner deadline exactly
/// once, produces a fail-closed decision event, and terminalises the turn.
#[tokio::test]
async fn t03_approval_timeout_denies_once() {
    let router = app(config());
    let (_, thread) = request_json(
        router.clone(),
        "POST",
        "/v1/threads",
        json!({ "workspace_alias": "repo" }),
    )
    .await;
    let (_, turn) = request_json(
        router.clone(),
        "POST",
        &format!("/v1/threads/{}/turns", thread["id"].as_str().unwrap()),
        json!({ "input": "timeout approval", "timeout_seconds": 1 }),
    )
    .await;
    let turn_id = turn["id"].as_str().unwrap();
    let controller = wait_for_sse_event(router.clone(), turn_id, "approval.requested").await;
    let events = fetch_sse(router, &format!("/v1/turns/{turn_id}/events"), None).await;
    assert_eq!(
        events
            .iter()
            .filter(|event| event["type"] == "approval.decided")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["terminal"] == true)
            .count(),
        1
    );
    assert!(events.iter().any(|event| event["type"] == "turn.failed"));
    drop(controller);
}

/// T03: only the first controlling SSE stream owns cancellation. Dropping it
/// sends the same fail-closed denial path as a timeout; a later observer can
/// prove that there is one terminal transition and no completed turn.
#[tokio::test]
async fn t03_controlling_sse_drop_denies_and_terminalizes() {
    let router = app(config());
    let (_, thread) = request_json(
        router.clone(),
        "POST",
        "/v1/threads",
        json!({ "workspace_alias": "repo" }),
    )
    .await;
    let (_, turn) = request_json(
        router.clone(),
        "POST",
        &format!("/v1/threads/{}/turns", thread["id"].as_str().unwrap()),
        json!({ "input": "controller disconnect" }),
    )
    .await;
    let turn_id = turn["id"].as_str().unwrap();
    let controller =
        wait_for_controlling_sse_event(router.clone(), turn_id, "approval.requested").await;
    drop(controller);

    let events = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        fetch_sse(router, &format!("/v1/turns/{turn_id}/events"), None),
    )
    .await
    .expect("drop must terminalize through the owner");
    assert_eq!(
        events
            .iter()
            .filter(|event| event["terminal"] == true)
            .count(),
        1
    );
    assert!(events
        .iter()
        .any(|event| event["type"] == "approval.decided"));
    assert!(events.iter().all(|event| event["type"] != "turn.completed"));
}

#[tokio::test]
async fn live_metadata_never_falls_back_to_the_fake_runner() {
    let mut live = config();
    live.live = true;
    let router = app(live);
    let (_, thread) = request_json(
        router.clone(),
        "POST",
        "/v1/threads",
        json!({ "workspace_alias": "repo" }),
    )
    .await;
    // T04/T10: an unadmitted live owner fails closed before launch. It cannot
    // substitute the offline fake fixture to fabricate model/quota metadata.
    let (status, body) = request_json(
        router,
        "POST",
        &format!("/v1/threads/{}/turns", thread["id"].as_str().unwrap()),
        json!({ "input": "must not reach fake runtime" }),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["code"], "RUNTIME_NOT_READY");
}

async fn fetch_sse(router: axum::Router, path: &str, last_event_id: Option<u64>) -> Vec<Value> {
    let mut builder = Request::builder()
        .uri(path)
        .header(header::AUTHORIZATION, "Bearer test-token")
        .header("x-spark-runner-observer", "1");
    if let Some(id) = last_event_id {
        builder = builder.header("Last-Event-ID", id.to_string());
    }
    let response = router
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    parse_sse(std::str::from_utf8(&bytes).unwrap())
}

async fn wait_for_sse_event(
    router: axum::Router,
    turn_id: &str,
    expected: &str,
) -> BoxStream<'static, Result<axum::body::Bytes, axum::Error>> {
    let request = Request::builder()
        .uri(format!("/v1/turns/{turn_id}/events"))
        .header(header::AUTHORIZATION, "Bearer test-token")
        .header("x-spark-runner-observer", "1")
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    let mut stream = response.into_body().into_data_stream();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(chunk) = stream.next().await {
            if std::str::from_utf8(&chunk.unwrap())
                .unwrap()
                .contains(expected)
            {
                return Box::pin(stream);
            }
        }
        panic!("SSE ended before {expected}");
    })
    .await
    .expect("SSE event barrier")
}

async fn wait_for_controlling_sse_event(
    router: axum::Router,
    turn_id: &str,
    expected: &str,
) -> BoxStream<'static, Result<axum::body::Bytes, axum::Error>> {
    let request = Request::builder()
        .uri(format!("/v1/turns/{turn_id}/events"))
        .header(header::AUTHORIZATION, "Bearer test-token")
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    let mut stream = response.into_body().into_data_stream();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(chunk) = stream.next().await {
            if std::str::from_utf8(&chunk.unwrap())
                .unwrap()
                .contains(expected)
            {
                return Box::pin(stream);
            }
        }
        panic!("SSE ended before {expected}");
    })
    .await
    .expect("controlling SSE event barrier")
}

fn parse_sse(raw: &str) -> Vec<Value> {
    raw.split("\n\n")
        .filter_map(|block| {
            let data = block.lines().find_map(|line| line.strip_prefix("data: "))?;
            serde_json::from_str(data).ok()
        })
        .collect()
}
