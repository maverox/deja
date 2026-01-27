//! Embedded Mock HTTP Backend
//!
//! A simple axum-based HTTP server for testing, providing:
//! - /echo - Echo back request details
//! - /delay/:ms - Respond after delay
//! - /status/:code - Return specific status code
//! - /random - Return random data
//! - /order - Track request order for verification

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::info;

/// Shared state for the mock backend
#[derive(Default)]
pub struct MockBackendState {
    /// Track order of requests for verification
    pub request_order: Mutex<Vec<String>>,
}

/// Response from echo endpoint
#[derive(Debug, Serialize)]
pub struct EchoResponse {
    pub message: String,
    pub path: String,
    pub timestamp: i64,
}

/// Response from order endpoint
#[derive(Debug, Serialize)]
pub struct OrderResponse {
    pub request_id: String,
    pub position: usize,
}

/// Query params for delay endpoint
#[derive(Debug, Deserialize)]
pub struct DelayParams {
    #[serde(default)]
    pub jitter_ms: u64,
}

/// Start the mock backend server
pub async fn start_mock_backend(
    addr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = Arc::new(MockBackendState::default());

    let app = Router::new()
        .route("/echo", get(echo_handler))
        .route("/echo", post(echo_post_handler))
        .route("/delay/:ms", get(delay_handler))
        .route("/status/:code", get(status_handler))
        .route("/random", get(random_handler))
        .route("/order/:request_id", get(order_handler))
        .route("/reset", post(reset_handler))
        .route("/health", get(health_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("Mock backend listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

/// Echo handler - returns request details
async fn echo_handler() -> impl IntoResponse {
    let now = chrono::Utc::now().timestamp_millis();
    Json(EchoResponse {
        message: "Hello from mock backend".to_string(),
        path: "/echo".to_string(),
        timestamp: now,
    })
}

/// Echo POST handler
async fn echo_post_handler(body: String) -> impl IntoResponse {
    let now = chrono::Utc::now().timestamp_millis();
    Json(serde_json::json!({
        "message": "Echo POST",
        "body": body,
        "timestamp": now,
    }))
}

/// Delay handler - waits before responding
async fn delay_handler(
    Path(ms): Path<u64>,
    Query(params): Query<DelayParams>,
) -> impl IntoResponse {
    let total_delay = ms + params.jitter_ms;
    tokio::time::sleep(Duration::from_millis(total_delay)).await;

    Json(serde_json::json!({
        "delayed_ms": total_delay,
        "message": format!("Responded after {}ms", total_delay),
    }))
}

/// Status handler - returns specified status code
async fn status_handler(Path(code): Path<u16>) -> impl IntoResponse {
    let status = StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    (
        status,
        Json(serde_json::json!({
            "status": code,
            "message": format!("Returned status {}", code),
        })),
    )
}

/// Random handler - returns random bytes
async fn random_handler() -> impl IntoResponse {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let random_bytes: Vec<u8> = (0..16).map(|_| rng.gen()).collect();
    let random_hex: String = random_bytes.iter().map(|b| format!("{:02x}", b)).collect();

    Json(serde_json::json!({
        "random_hex": random_hex,
        "length": 16,
    }))
}

/// Order handler - tracks request order
async fn order_handler(
    Path(request_id): Path<String>,
    State(state): State<Arc<MockBackendState>>,
) -> impl IntoResponse {
    let position = {
        let mut order = state.request_order.lock().unwrap();
        order.push(request_id.clone());
        order.len() - 1
    };

    Json(OrderResponse {
        request_id,
        position,
    })
}

/// Reset handler - clears state
async fn reset_handler(State(state): State<Arc<MockBackendState>>) -> impl IntoResponse {
    let mut order = state.request_order.lock().unwrap();
    order.clear();

    Json(serde_json::json!({
        "message": "State reset",
    }))
}

/// Health check handler
async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
    }))
}
