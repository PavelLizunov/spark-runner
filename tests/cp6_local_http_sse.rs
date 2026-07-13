use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use serde_json::{json, Value};
use spark_runner::api::{app, ApiConfig};
use tokio::time::{sleep, Duration};
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

    sleep(Duration::from_millis(300)).await;
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

#[tokio::test]
async fn live_metadata_never_falls_back_to_the_fake_runner() {
    let mut live = config();
    live.live = true;
    let router = app(live);
    assert_eq!(
        get_json(router.clone(), "/ready").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let (_, thread) = request_json(
        router.clone(),
        "POST",
        "/v1/threads",
        json!({ "workspace_alias": "repo" }),
    )
    .await;
    let (status, _) = request_json(
        router,
        "POST",
        &format!("/v1/threads/{}/turns", thread["id"].as_str().unwrap()),
        json!({ "workspace_alias": "repo", "input": "must not use fake" }),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

async fn fetch_sse(router: axum::Router, path: &str, last_event_id: Option<u64>) -> Vec<Value> {
    let mut builder = Request::builder()
        .uri(path)
        .header(header::AUTHORIZATION, "Bearer test-token");
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

fn parse_sse(raw: &str) -> Vec<Value> {
    raw.split("\n\n")
        .filter_map(|block| {
            let data = block.lines().find_map(|line| line.strip_prefix("data: "))?;
            serde_json::from_str(data).ok()
        })
        .collect()
}
