//! Control API for SDK communication and Trace Correlation
//!
//! Provides HTTP endpoints for the SDK to:
//! - POST /capture - Record a value (uuid, time, random, etc.)
//! - POST /captures/batch - Batch record values
//! - GET /replay - Retrieve a recorded value by trace_id and kind
//! - POST /orchestrate - Trigger orchestrated replay for a specific trace
//! - GET /traces - List all available trace IDs
//! - POST /control/trace - Trace lifecycle events (start/end/associate)

use crate::correlation::{ControlOrderingDecision, TraceCorrelator};
use bytes::Bytes;
use deja_common::{ControlMessage, ScopeId};
use deja_core::events::{
    non_deterministic_event, recorded_event, EventDirection, NonDeterministicEvent, RecordedEvent,
};
use deja_core::recording::Recorder;
use deja_core::replay::ReplayEngine;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Incoming, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::debug;

/// Request body for /capture endpoint
#[derive(Debug, Deserialize)]
pub struct CaptureRequest {
    pub trace_id: String,
    pub kind: String,
    #[serde(default)]
    pub seq: u64,
    pub value: String,
}

/// Request body for /captures/batch endpoint
#[derive(Debug, Deserialize)]
pub struct BatchCaptureRequest {
    pub trace_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    pub captures: Vec<CapturedValue>,
}

/// A single captured value in a batch
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CapturedValue {
    pub kind: String,
    pub seq: u64,
    pub value: String,
}

/// Response body for /replay endpoint
#[derive(Debug, Serialize)]
pub struct ReplayResponse {
    pub value: String,
    pub found: bool,
}

/// Request body for /orchestrate endpoint
#[derive(Debug, Deserialize)]
pub struct OrchestrateRequest {
    pub trace_id: String,
    #[serde(default)]
    pub service_url: Option<String>,
}

/// Response body for /orchestrate endpoint
#[derive(Debug, Serialize)]
pub struct OrchestrateResponse {
    pub pass: bool,
    pub trace_id: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_diff: Option<(u32, u32)>,
}

/// Response body for /traces endpoint
#[derive(Debug, Serialize)]
pub struct TracesResponse {
    pub traces: Vec<String>,
}

/// Shared state for the control API
pub struct ControlApiState {
    pub recorder: Option<Arc<Recorder>>,
    pub replay_engine: Option<Arc<Mutex<ReplayEngine>>>,
    pub correlator: Arc<TraceCorrelator>,
    pub ingress_port: Option<u16>,
    pub orchestration_mutex: Mutex<()>,
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
        (&Method::POST, "/captures/batch") => handle_batch_capture(req, state).await,
        (&Method::GET, "/replay") => handle_replay(req, state).await,
        (&Method::POST, "/orchestrate") => handle_orchestrate(req, state).await,
        (&Method::GET, "/traces") => handle_list_traces(state).await,
        (&Method::POST, "/control/trace") => handle_control_trace(req, state).await,
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
    let body = req.collect().await?.to_bytes();
    let capture_req: CaptureRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            let mut response =
                Response::new(Full::new(Bytes::from(format!("Invalid JSON: {}", e))));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(response);
        }
    };

    if let Some(recorder) = &state.recorder {
        let event = match create_non_deterministic_event(&capture_req) {
            Ok(e) => e,
            Err(e) => {
                let mut response = Response::new(Full::new(Bytes::from(format!(
                    "Invalid capture value: {}",
                    e
                ))));
                *response.status_mut() = StatusCode::BAD_REQUEST;
                return Ok(response);
            }
        };

        if let Err(e) = recorder.save_event(&event).await {
            eprintln!("[ControlAPI] Failed to save event: {}", e);
        }
    }

    Ok(Response::new(Full::new(Bytes::from(r#"{"status":"ok"}"#))))
}

async fn handle_batch_capture(
    req: Request<Incoming>,
    state: Arc<ControlApiState>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let body = req.collect().await?.to_bytes();
    let batch_req: BatchCaptureRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            let mut response =
                Response::new(Full::new(Bytes::from(format!("Invalid JSON: {}", e))));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(response);
        }
    };

    if let Some(recorder) = &state.recorder {
        let mut success_count = 0;
        let mut error_count = 0;

        for capture in &batch_req.captures {
            let event = match create_non_deterministic_event_with_seq(
                &batch_req.trace_id,
                batch_req.task_id.as_deref(),
                &capture.kind,
                capture.seq,
                &capture.value,
            ) {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("[ControlAPI] Failed to create batch event: {}", e);
                    error_count += 1;
                    continue;
                }
            };

            if let Err(e) = recorder.save_event(&event).await {
                eprintln!("[ControlAPI] Failed to save batch event: {}", e);
                error_count += 1;
            } else {
                success_count += 1;
            }
        }

        let response = serde_json::json!({
            "status": "ok",
            "captured": success_count,
            "errors": error_count
        });
        Ok(Response::new(Full::new(Bytes::from(response.to_string()))))
    } else {
        Ok(Response::new(Full::new(Bytes::from(
            r#"{"status":"ok","captured":0,"errors":0}"#,
        ))))
    }
}

async fn handle_replay(
    req: Request<Incoming>,
    state: Arc<ControlApiState>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let query = req.uri().query().unwrap_or("");
    let params: std::collections::HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();

    let trace_id = params.get("trace_id").cloned().unwrap_or_default();
    let kind = params.get("kind").cloned().unwrap_or_default();
    let seq: Option<u64> = params.get("seq").and_then(|s| s.parse().ok());
    let task_id = params.get("task_id").cloned();

    let response = if let Some(engine_lock) = &state.replay_engine {
        let mut engine = engine_lock.lock().await;

        let value = if let Some(seq_num) = seq {
            engine.handle_runtime_request_with_seq(&trace_id, task_id.as_deref(), &kind, seq_num)
        } else {
            engine.handle_runtime_request(&trace_id, &kind)
        };

        match value {
            Some(v) => ReplayResponse {
                value: v,
                found: true,
            },
            None => ReplayResponse {
                value: String::new(),
                found: false,
            },
        }
    } else {
        ReplayResponse {
            value: String::new(),
            found: false,
        }
    };

    let json = serde_json::to_string(&response).map_err(|e| {
        eprintln!("Serialization error in handle_replay: {}", e);
        e
    });

    match json {
        Ok(j) => Ok(Response::new(Full::new(Bytes::from(j)))),
        Err(_) => {
            let mut resp = Response::new(Full::new(Bytes::from("Internal Server Error")));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            Ok(resp)
        }
    }
}

async fn handle_orchestrate(
    req: Request<Incoming>,
    state: Arc<ControlApiState>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let body = req.collect().await?.to_bytes();
    let orch_req: OrchestrateRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            let mut response =
                Response::new(Full::new(Bytes::from(format!("Invalid JSON: {}", e))));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(response);
        }
    };

    println!(
        "[Orchestrate] Starting replay for trace: {}",
        orch_req.trace_id
    );

    let _orch_lock = state.orchestration_mutex.lock().await;

    // Start trace in correlator
    state
        .correlator
        .start_trace(
            orch_req.trace_id.clone(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
        )
        .await;

    let Some(engine_lock) = &state.replay_engine else {
        let response = OrchestrateResponse {
            pass: false,
            trace_id: orch_req.trace_id,
            message: "No replay engine available".to_string(),
            status_diff: None,
        };
        let json = serde_json::to_string(&response).unwrap_or_default();
        return Ok(Response::new(Full::new(Bytes::from(json))));
    };

    let engine = engine_lock.lock().await;

    let trigger = match engine.get_trigger_event(&orch_req.trace_id) {
        Some(t) => t.clone(),
        None => {
            let response = OrchestrateResponse {
                pass: false,
                trace_id: orch_req.trace_id,
                message: "No trigger event found for trace".to_string(),
                status_diff: None,
            };
            let json = serde_json::to_string(&response).unwrap_or_default();
            return Ok(Response::new(Full::new(Bytes::from(json))));
        }
    };

    let expected = match engine.get_expected_response(&orch_req.trace_id) {
        Some(e) => e.clone(),
        None => {
            let response = OrchestrateResponse {
                pass: false,
                trace_id: orch_req.trace_id,
                message: "No expected response found for trace".to_string(),
                status_diff: None,
            };
            let json = serde_json::to_string(&response).unwrap_or_default();
            return Ok(Response::new(Full::new(Bytes::from(json))));
        }
    };

    let is_grpc = engine.is_grpc_trigger(&trigger);
    drop(engine);

    if is_grpc {
        // gRPC trigger flow
        let engine = engine_lock.lock().await;
        let (service, method, request_body) = match engine.get_grpc_request_info(&trigger) {
            Some(info) => info,
            None => {
                let response = OrchestrateResponse {
                    pass: false,
                    trace_id: orch_req.trace_id,
                    message: "Failed to extract gRPC request info".to_string(),
                    status_diff: None,
                };
                let json = serde_json::to_string(&response).unwrap_or_default();
                return Ok(Response::new(Full::new(Bytes::from(json))));
            }
        };
        drop(engine);

        let grpc_path = format!("/{}/{}", service, method);
        let base_url = if let Some(url) = &orch_req.service_url {
            url.trim_end_matches('/').to_string()
        } else if let Some(port) = state.ingress_port {
            format!("http://127.0.0.1:{}", port)
        } else {
            let response = OrchestrateResponse {
                pass: false,
                trace_id: orch_req.trace_id,
                message: "No service_url and no ingress port configured".to_string(),
                status_diff: None,
            };
            let json = serde_json::to_string(&response).unwrap_or_default();
            return Ok(Response::new(Full::new(Bytes::from(json))));
        };

        let url = format!("{}{}", base_url, grpc_path);

        let client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        let mut framed_body = Vec::with_capacity(5 + request_body.len());
        framed_body.push(0);
        framed_body.extend_from_slice(&(request_body.len() as u32).to_be_bytes());
        framed_body.extend_from_slice(&request_body);

        let grpc_response = match client
            .post(&url)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("x-trace-id", &orch_req.trace_id)
            .body(framed_body)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                let response = OrchestrateResponse {
                    pass: false,
                    trace_id: orch_req.trace_id,
                    message: format!("Failed to send gRPC request: {}", e),
                    status_diff: None,
                };
                let json = serde_json::to_string(&response).unwrap_or_default();
                return Ok(Response::new(Full::new(Bytes::from(json))));
            }
        };

        let grpc_status = grpc_response
            .headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0);

        let body_bytes = grpc_response.bytes().await.unwrap_or_default();
        let response_body = if body_bytes.len() > 5 {
            &body_bytes[5..]
        } else {
            &body_bytes[..]
        };

        let engine = engine_lock.lock().await;
        let result = engine.compare_grpc_responses(&expected, response_body, grpc_status);

        let orch_response = OrchestrateResponse {
            pass: result.pass,
            trace_id: orch_req.trace_id,
            message: result.message,
            status_diff: result.diff.and_then(|d| d.status_diff),
        };

        let json = serde_json::to_string(&orch_response).unwrap_or_default();
        Ok(Response::new(Full::new(Bytes::from(json))))
    } else {
        // HTTP trigger flow
        let base_url = if let Some(url) = &orch_req.service_url {
            url.trim_end_matches('/').to_string()
        } else if let Some(port) = state.ingress_port {
            format!("http://127.0.0.1:{}", port)
        } else {
            let response = OrchestrateResponse {
                pass: false,
                trace_id: orch_req.trace_id,
                message: "No service_url and no ingress port configured".to_string(),
                status_diff: None,
            };
            let json = serde_json::to_string(&response).unwrap_or_default();
            return Ok(Response::new(Full::new(Bytes::from(json))));
        };

        let engine = engine_lock.lock().await;
        let (method, path, headers, body) =
            if let Some(recorded_event::Event::HttpRequest(req)) = &trigger.event {
                (
                    req.method.clone(),
                    req.path.clone(),
                    req.headers.clone(),
                    req.body.clone(),
                )
            } else {
                let response = OrchestrateResponse {
                    pass: false,
                    trace_id: orch_req.trace_id,
                    message: "Trigger event is not an HTTP request".to_string(),
                    status_diff: None,
                };
                let json = serde_json::to_string(&response).unwrap_or_default();
                return Ok(Response::new(Full::new(Bytes::from(json))));
            };
        drop(engine);

        let url = format!("{}{}", base_url, path);

        let client = reqwest::Client::new();
        let mut builder = client.request(
            reqwest::Method::from_str(&method).unwrap_or(reqwest::Method::GET),
            &url,
        );

        for (k, v) in headers {
            if k.eq_ignore_ascii_case("host") || k.eq_ignore_ascii_case("content-length") {
                continue;
            }
            builder = builder.header(k, v);
        }
        // Inject trace ID
        builder = builder.header("x-trace-id", &orch_req.trace_id);

        let http_response = match builder.body(body).send().await {
            Ok(resp) => resp,
            Err(e) => {
                let response = OrchestrateResponse {
                    pass: false,
                    trace_id: orch_req.trace_id,
                    message: format!("Failed to send HTTP request: {}", e),
                    status_diff: None,
                };
                let json = serde_json::to_string(&response).unwrap_or_default();
                return Ok(Response::new(Full::new(Bytes::from(json))));
            }
        };

        let status = http_response.status().as_u16() as u32;
        let body_bytes = http_response.bytes().await.unwrap_or_default();

        let engine = engine_lock.lock().await;
        let status_line = format!("HTTP/1.1 {} OK\r\n", status);
        let mut raw_resp = Vec::new();
        raw_resp.extend_from_slice(status_line.as_bytes());
        raw_resp.extend_from_slice(b"\r\n");
        raw_resp.extend_from_slice(&body_bytes);

        let result = engine.compare_responses(&expected, &raw_resp);

        let orch_response = OrchestrateResponse {
            pass: result.pass,
            trace_id: orch_req.trace_id,
            message: result.message,
            status_diff: result.diff.and_then(|d| d.status_diff),
        };

        let json = serde_json::to_string(&orch_response).unwrap_or_default();
        Ok(Response::new(Full::new(Bytes::from(json))))
    }
}

async fn handle_list_traces(
    state: Arc<ControlApiState>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let response = if let Some(engine_lock) = &state.replay_engine {
        let engine = engine_lock.lock().await;
        TracesResponse {
            traces: engine.get_all_trace_ids(),
        }
    } else {
        TracesResponse { traces: vec![] }
    };

    let json = serde_json::to_string(&response).unwrap_or_default();
    Ok(Response::new(Full::new(Bytes::from(json))))
}

/// Handle all control messages: StartTrace, EndTrace, AssociateBySourcePort
async fn handle_control_trace(
    req: Request<Incoming>,
    state: Arc<ControlApiState>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let body = req.collect().await?.to_bytes();
    let message: ControlMessage = match serde_json::from_slice(&body) {
        Ok(msg) => msg,
        Err(e) => {
            let response = create_control_error_response(&format!("Invalid JSON: {}", e));
            return Ok(Response::new(Full::new(Bytes::from(response))));
        }
    };

    let response_body = apply_control_message(state, message).await;

    Ok(Response::new(Full::new(Bytes::from(response_body))))
}

async fn apply_control_message(state: Arc<ControlApiState>, message: ControlMessage) -> String {
    let sender_key = control_sender_key(&message);
    let fingerprint = control_message_fingerprint(&message);
    match state
        .correlator
        .check_control_ordering(&sender_key, message.sender_seq(), &fingerprint)
        .await
    {
        ControlOrderingDecision::Accept => {}
        ControlOrderingDecision::Duplicate => {
            debug!(sender_key = %sender_key, "Duplicate control message ignored idempotently");
            return create_control_success_response();
        }
        ControlOrderingDecision::Reject(reason) => {
            debug!(sender_key = %sender_key, reason = %reason, "Rejected control message ordering");
            return create_control_error_response(&reason);
        }
    }

    match message {
        ControlMessage::StartTrace {
            trace_id,
            timestamp_ns,
            ..
        } => {
            println!("[Correlation] Starting trace: {}", trace_id);
            state.correlator.start_trace(trace_id, timestamp_ns).await;
            create_control_success_response()
        }
        ControlMessage::EndTrace {
            trace_id,
            timestamp_ns,
            ..
        } => {
            println!("[Correlation] Ending trace: {}", trace_id);
            state.correlator.end_trace(trace_id, timestamp_ns).await;
            create_control_success_response()
        }
        ControlMessage::AssociateBySourcePort {
            trace_id,
            source_port,
            protocol,
            ..
        } => {
            println!(
                "[Correlation] Associate source port {} with trace {} ({})",
                source_port, trace_id, protocol
            );
            state
                .correlator
                .associate_by_source_port(&trace_id, source_port, protocol)
                .await;
            create_control_success_response()
        }
        ControlMessage::ReassociateConnection {
            trace_id,
            source_port,
            protocol,
            ..
        } => {
            debug!(
                trace_id = %trace_id,
                source_port = source_port,
                "Reassociating connection to new trace"
            );
            state
                .correlator
                .reassociate(&trace_id, source_port, protocol)
                .await;
            create_control_success_response()
        }
        ControlMessage::PoolCheckout {
            trace_id,
            source_port,
            protocol,
            ..
        } => {
            debug!(
                trace_id = %trace_id,
                source_port = source_port,
                "Pool checkout — reassociating connection"
            );
            state
                .correlator
                .reassociate(&trace_id, source_port, protocol)
                .await;
            create_control_success_response()
        }
        ControlMessage::PoolReturn {
            trace_id,
            source_port,
            ..
        } => {
            debug!(
                trace_id = %trace_id,
                source_port = source_port,
                "Pool return — connection released"
            );
            create_control_success_response()
        }
    }
}

fn control_sender_key(message: &ControlMessage) -> String {
    match message {
        ControlMessage::StartTrace { trace_id, .. }
        | ControlMessage::EndTrace { trace_id, .. }
        | ControlMessage::AssociateBySourcePort { trace_id, .. }
        | ControlMessage::ReassociateConnection { trace_id, .. }
        | ControlMessage::PoolCheckout { trace_id, .. }
        | ControlMessage::PoolReturn { trace_id, .. } => format!("trace:{}", trace_id),
    }
}

fn control_message_fingerprint(message: &ControlMessage) -> String {
    match message {
        ControlMessage::StartTrace {
            trace_id,
            timestamp_ns,
            ..
        } => format!("start_trace:{}:{}", trace_id, timestamp_ns),
        ControlMessage::EndTrace {
            trace_id,
            timestamp_ns,
            ..
        } => format!("end_trace:{}:{}", trace_id, timestamp_ns),
        ControlMessage::AssociateBySourcePort {
            trace_id,
            source_port,
            protocol,
            timestamp_ns,
            ..
        } => format!(
            "associate_by_source_port:{}:{}:{}:{}",
            trace_id, source_port, protocol, timestamp_ns
        ),
        ControlMessage::ReassociateConnection {
            trace_id,
            source_port,
            protocol,
            timestamp_ns,
            ..
        } => format!(
            "reassociate_connection:{}:{}:{}:{}",
            trace_id, source_port, protocol, timestamp_ns
        ),
        ControlMessage::PoolCheckout {
            trace_id,
            source_port,
            protocol,
            pool_id,
            timestamp_ns,
            ..
        } => format!(
            "pool_checkout:{}:{}:{}:{}:{}",
            trace_id,
            source_port,
            protocol,
            pool_id.as_deref().unwrap_or(""),
            timestamp_ns
        ),
        ControlMessage::PoolReturn {
            trace_id,
            source_port,
            protocol,
            timestamp_ns,
            ..
        } => format!(
            "pool_return:{}:{}:{}:{}",
            trace_id, source_port, protocol, timestamp_ns
        ),
    }
}

fn create_control_success_response() -> String {
    r#"{"success":true}"#.to_string()
}

fn create_control_error_response(message: &str) -> String {
    serde_json::json!({
        "success": false,
        "message": message
    })
    .to_string()
}

fn create_non_deterministic_event(req: &CaptureRequest) -> Result<RecordedEvent, String> {
    create_non_deterministic_event_with_seq(&req.trace_id, None, &req.kind, req.seq, &req.value)
}

fn create_non_deterministic_event_with_seq(
    trace_id: &str,
    task_id: Option<&str>,
    kind: &str,
    seq: u64,
    value: &str,
) -> Result<RecordedEvent, String> {
    let nd_kind = match kind {
        "uuid" | "uuid_v7" => Some(non_deterministic_event::Kind::UuidCapture(
            value.to_string(),
        )),
        "time" => {
            let ns: u64 = value
                .parse()
                .map_err(|_| format!("Invalid timestamp: {}", value))?;
            Some(non_deterministic_event::Kind::TimeCaptureNs(ns))
        }
        "random" => {
            let seed: u64 = value
                .parse()
                .map_err(|_| format!("Invalid seed: {}", value))?;
            Some(non_deterministic_event::Kind::RandomSeedCapture(seed))
        }
        "task_spawn" => Some(non_deterministic_event::Kind::TaskSpawnCapture(
            value.to_string(),
        )),
        "random_bytes" => {
            let bytes = hex::decode(value)
                .map_err(|_| format!("Invalid hex for random_bytes: {}", value))?;
            Some(non_deterministic_event::Kind::RandomBytesCapture(bytes))
        }
        "nanoid" => Some(non_deterministic_event::Kind::NanoidCapture(
            value.to_string(),
        )),
        _ => Some(non_deterministic_event::Kind::UuidCapture(format!(
            "{}:{}",
            kind, value
        ))),
    };

    let task_path = task_id.unwrap_or("0");
    let scope_id = ScopeId::task(trace_id, task_path);

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("nd_kind".to_string(), kind.to_string());
    metadata.insert("nd_seq".to_string(), seq.to_string());
    if let Some(tid) = task_id {
        metadata.insert("task_id".to_string(), tid.to_string());
    }

    Ok(RecordedEvent {
        trace_id: trace_id.to_string(),
        scope_id: scope_id.as_str().to_string(),
        scope_sequence: seq,
        global_sequence: 0,
        timestamp_ns: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0),
        direction: EventDirection::ClientToServer as i32,
        metadata,
        event: nd_kind.map(|k| {
            recorded_event::Event::NonDeterministic(NonDeterministicEvent { kind: Some(k) })
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use deja_common::Protocol;

    fn test_state() -> Arc<ControlApiState> {
        Arc::new(ControlApiState {
            recorder: None,
            replay_engine: None,
            correlator: Arc::new(TraceCorrelator::new()),
            ingress_port: None,
            orchestration_mutex: Mutex::new(()),
        })
    }

    #[tokio::test]
    async fn control_message_ordering_duplicate_pool_checkout_is_idempotent() {
        let state = test_state();
        state
            .correlator
            .start_trace("trace-dup".to_string(), 1)
            .await;

        let message = ControlMessage::PoolCheckout {
            trace_id: "trace-dup".to_string(),
            source_port: 55100,
            protocol: Protocol::Postgres,
            pool_id: Some("pool-a".to_string()),
            timestamp_ns: 10,
            sender_seq: Some(7),
        };

        let first = apply_control_message(state.clone(), message.clone()).await;
        assert!(first.contains("\"success\":true"));

        let duplicate = apply_control_message(state.clone(), message).await;
        assert!(duplicate.contains("\"success\":true"));

        let assoc = state
            .correlator
            .on_new_connection(55100, Protocol::Unknown)
            .await
            .expect("connection should remain associated");
        assert_eq!(assoc.trace_id, "trace-dup");
    }

    #[tokio::test]
    async fn control_message_ordering_rejects_stale_sequence() {
        let state = test_state();
        state
            .correlator
            .start_trace("trace-stale".to_string(), 1)
            .await;

        let accepted = apply_control_message(
            state.clone(),
            ControlMessage::StartTrace {
                trace_id: "trace-stale".to_string(),
                timestamp_ns: 10,
                sender_seq: Some(5),
                task_id: None,
                task_path: None,
            },
        )
        .await;
        assert!(accepted.contains("\"success\":true"));

        let stale = apply_control_message(
            state,
            ControlMessage::EndTrace {
                trace_id: "trace-stale".to_string(),
                timestamp_ns: 11,
                sender_seq: Some(4),
                task_id: None,
                task_path: None,
            },
        )
        .await;
        assert!(stale.contains("\"success\":false"));
        assert!(stale.contains("stale sequence"));
    }

    #[tokio::test]
    async fn control_message_ordering_rejects_out_of_order_sequence_deterministically() {
        let state = test_state();
        state
            .correlator
            .start_trace("trace-order".to_string(), 1)
            .await;

        let seq1 = apply_control_message(
            state.clone(),
            ControlMessage::AssociateBySourcePort {
                trace_id: "trace-order".to_string(),
                source_port: 55200,
                protocol: Protocol::Postgres,
                timestamp_ns: 20,
                sender_seq: Some(1),
            },
        )
        .await;
        assert!(seq1.contains("\"success\":true"));

        let seq3 = apply_control_message(
            state.clone(),
            ControlMessage::PoolCheckout {
                trace_id: "trace-order".to_string(),
                source_port: 55200,
                protocol: Protocol::Postgres,
                pool_id: None,
                timestamp_ns: 21,
                sender_seq: Some(3),
            },
        )
        .await;
        assert!(seq3.contains("\"success\":true"));

        let seq2 = apply_control_message(
            state,
            ControlMessage::PoolReturn {
                trace_id: "trace-order".to_string(),
                source_port: 55200,
                protocol: Protocol::Postgres,
                timestamp_ns: 22,
                sender_seq: Some(2),
            },
        )
        .await;
        assert!(seq2.contains("\"success\":false"));
        assert!(seq2.contains("stale sequence 2"));
    }
}
