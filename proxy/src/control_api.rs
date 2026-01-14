//! Control API for NetworkRuntime communication
//!
//! Provides HTTP endpoints for the SDK to record and replay non-deterministic values:
//! - POST /capture - Record a value (uuid, time, random, etc.)
//! - GET /replay - Retrieve a recorded value by trace_id and kind

use bytes::Bytes;
use deja_core::events::{non_deterministic_event, recorded_event, NonDeterministicEvent, RecordedEvent};
use deja_core::recording::Recorder;
use deja_core::replay::ReplayEngine;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Incoming, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// Request body for /capture endpoint
#[derive(Debug, Deserialize)]
pub struct CaptureRequest {
    pub trace_id: String,
    pub kind: String,  // "uuid", "time", "random", "task_spawn", etc.
    pub value: String,
}

/// Response body for /replay endpoint
#[derive(Debug, Serialize)]
pub struct ReplayResponse {
    pub value: String,
    pub found: bool,
}

/// Shared state for the control API
pub struct ControlApiState {
    pub recorder: Option<Arc<Recorder>>,
    pub replay_engine: Option<Arc<Mutex<ReplayEngine>>>,
}

/// Start the control API server
pub async fn start_control_api(
    port: u16,
    state: Arc<ControlApiState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    println!("[ControlAPI] Listening on {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let state = state.clone();
                async move { handle_request(req, state).await }
            });

            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("[ControlAPI] Connection error: {}", e);
            }
        });
    }
}

async fn handle_request(
    req: Request<Incoming>,
    state: Arc<ControlApiState>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let path = req.uri().path();
    let method = req.method();

    match (method, path) {
        (&Method::POST, "/capture") => handle_capture(req, state).await,
        (&Method::GET, "/replay") => handle_replay(req, state).await,
        (&Method::GET, "/health") => Ok(Response::new(Full::new(Bytes::from("ok")))),
        _ => {
            let mut response = Response::new(Full::new(Bytes::from("Not Found")));
            *response.status_mut() = StatusCode::NOT_FOUND;
            Ok(response)
        }
    }
}

async fn handle_capture(
    req: Request<Incoming>,
    state: Arc<ControlApiState>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    // Read body
    let body = req.collect().await?.to_bytes();
    let capture_req: CaptureRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            let mut response = Response::new(Full::new(Bytes::from(format!("Invalid JSON: {}", e))));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(response);
        }
    };

    // Record if we have a recorder
    if let Some(recorder) = &state.recorder {
        let event = create_non_deterministic_event(&capture_req);
        if let Err(e) = recorder.save_event(&event).await {
            eprintln!("[ControlAPI] Failed to save event: {}", e);
        }
    }

    Ok(Response::new(Full::new(Bytes::from(r#"{"status":"ok"}"#))))
}

async fn handle_replay(
    req: Request<Incoming>,
    state: Arc<ControlApiState>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    // Parse query parameters
    let query = req.uri().query().unwrap_or("");
    let params: std::collections::HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();

    let _trace_id = params.get("trace_id").cloned().unwrap_or_default();
    let kind = params.get("kind").cloned().unwrap_or_default();
    // TODO: Use trace_id for scoped replay (get next value for this specific trace)

    // Try to get value from replay engine
    let response = if let Some(engine_lock) = &state.replay_engine {
        let mut engine = engine_lock.lock().await;

        let value = match kind.as_str() {
            "uuid" => engine.handle_uuid_request(),
            "time" => engine.handle_time_request().map(|t| t.to_string()),
            "random" => engine.handle_random_request().map(|r| r.to_string()),
            _ => None,
        };

        match value {
            Some(v) => ReplayResponse { value: v, found: true },
            None => ReplayResponse { value: String::new(), found: false },
        }
    } else {
        ReplayResponse { value: String::new(), found: false }
    };

    let json = serde_json::to_string(&response).unwrap_or_else(|_| r#"{"found":false}"#.to_string());
    Ok(Response::new(Full::new(Bytes::from(json))))
}

fn create_non_deterministic_event(req: &CaptureRequest) -> RecordedEvent {
    let kind = match req.kind.as_str() {
        "uuid" => Some(non_deterministic_event::Kind::UuidCapture(req.value.clone())),
        "time" => {
            let ns: u64 = req.value.parse().unwrap_or(0);
            Some(non_deterministic_event::Kind::TimeCaptureNs(ns))
        }
        "random" => {
            let seed: u64 = req.value.parse().unwrap_or(0);
            Some(non_deterministic_event::Kind::RandomSeedCapture(seed))
        }
        _ => None,
    };

    RecordedEvent {
        trace_id: req.trace_id.clone(),
        span_id: uuid::Uuid::new_v4().to_string(),
        parent_span_id: None,
        timestamp_ns: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0),
        sequence: 0,
        connection_id: String::new(),
        metadata: std::collections::HashMap::new(),
        event: kind.map(|k| {
            recorded_event::Event::NonDeterministic(NonDeterministicEvent { kind: Some(k) })
        }),
    }
}
