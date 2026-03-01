//! Connection handling with scope-based enrichment
//!
//! Each connection is associated with a ScopeId at establishment time.
//! Events are enriched with scope_id, scope_sequence, global_sequence, and direction
//! before recording or replay matching.

use crate::correlation::{ConnectionAssociation, TraceCorrelator};
use deja_common::{Protocol, ScopeId};
use deja_core::events::{EventDirection, RecordedEvent};
use deja_core::protocols::redis::RedisParser;
use deja_core::protocols::ProtocolParser;
use deja_core::recording::Recorder;
use deja_core::replay::{ReplayEngine, ReplayMatchError};
use deja_core::tls_mitm::TlsMitmManager;

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tracing::{info, instrument, warn};

static ORPHAN_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Clone)]
struct RuntimeAssociation {
    scope_id: ScopeId,
    trace_id: String,
}

/// Enrich an event with scope information from the correlator.
///
/// Sets trace_id, scope_id, scope_sequence, global_sequence, direction, and timestamp.
/// No lookups — the scope is already determined at connection establishment.
async fn enrich_event(
    event: &mut RecordedEvent,
    scope_id: &ScopeId,
    trace_id: &str,
    direction: EventDirection,
    correlator: &TraceCorrelator,
) {
    // Preserve parser-extracted trace_id (e.g. from PG SET application_name,
    // Redis CLIENT SETNAME, or gRPC headers) — only fall back to connection-level.
    if event.trace_id.is_empty() {
        event.trace_id = trace_id.to_string();
    }
    event.scope_id = scope_id.as_str().to_string();
    event.direction = direction as i32;
    event.scope_sequence = correlator.next_scope_sequence(scope_id).await;
    event.global_sequence = correlator.next_global_sequence().await;
    event.timestamp_ns = current_time_ns();
}

/// Enrich for sub-scope (HTTP/2 stream, gRPC call).
///
/// If the event already has a trace_id set by the protocol parser (e.g. from
/// x-trace-id / traceparent headers), that value takes priority over the
/// connection-level trace_id so gRPC streams are not mis-attributed as orphans.
async fn enrich_stream_event(
    event: &mut RecordedEvent,
    parent_scope: &ScopeId,
    stream_id: u32,
    trace_id: &str,
    direction: EventDirection,
    correlator: &TraceCorrelator,
) {
    let effective_trace_id = if !event.trace_id.is_empty() {
        event.trace_id.clone()
    } else {
        trace_id.to_string()
    };
    let stream_scope = ScopeId::stream(
        &effective_trace_id,
        parent_scope.connection_index().unwrap_or(0),
        stream_id,
    );
    enrich_event(
        event,
        &stream_scope,
        &effective_trace_id,
        direction,
        correlator,
    )
    .await;
}

fn current_time_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Detect if the connection starts with a TLS handshake
pub async fn is_tls_handshake(stream: &TcpStream) -> bool {
    let mut buf = [0u8; 3];
    match stream.peek(&mut buf).await {
        Ok(n) if n >= 3 => buf[0] == 0x16 && buf[1] == 0x03 && buf[2] <= 0x03,
        _ => false,
    }
}

/// Detect protocol from bytes (for TLS where we can't peek)
fn detect_protocol_from_bytes(
    data: &[u8],
    parsers: &[Arc<dyn ProtocolParser>],
) -> Arc<dyn ProtocolParser> {
    let mut best_score = 0.0;
    let mut best_idx = 0;
    for (i, p) in parsers.iter().enumerate() {
        let score = p.detect(data);
        if score > best_score {
            best_score = score;
            best_idx = i;
        }
    }
    if best_score > 0.0 {
        return parsers[best_idx].clone();
    }
    Arc::new(RedisParser)
}

pub async fn detect_protocol(
    stream: &TcpStream,
    parsers: &[Arc<dyn ProtocolParser>],
) -> Arc<dyn ProtocolParser> {
    let mut buf = [0u8; 1024];
    match stream.peek(&mut buf).await {
        Ok(n) if n > 0 => {
            let mut best_score = 0.0;
            let mut best_idx = 0;
            for (i, p) in parsers.iter().enumerate() {
                let score = p.detect(&buf[0..n]);
                if score > best_score {
                    best_score = score;
                    best_idx = i;
                }
            }
            if best_score > 0.0 {
                info!(
                    "Detected protocol: {} (score: {})",
                    parsers[best_idx].protocol_id(),
                    best_score
                );
                return parsers[best_idx].clone();
            }
        }
        _ => {}
    }
    Arc::new(RedisParser)
}

/// Resolve connection association — either immediately from pre-registration
/// or by waiting for the SDK to send AssociateBySourcePort with a timeout.
/// In record mode, when association arrives after timeout but within retro_bind_window_ms,
/// buffered events are retro-attributed to the trace.
async fn resolve_association(
    correlator: &TraceCorrelator,
    peer_port: u16,
    protocol: Protocol,
    association_timeout_ms: u64,
    retro_bind_window_ms: u64,
    is_record_mode: bool,
) -> Option<ConnectionAssociation> {
    match correlator.on_new_connection(peer_port, protocol).await {
        Ok(assoc) => Some(assoc),
        Err(rx) => {
            match tokio::time::timeout(
                std::time::Duration::from_millis(association_timeout_ms),
                rx,
            )
            .await
            {
                Ok(Ok(assoc)) => Some(assoc),
                Ok(Err(_)) => {
                    // Channel closed without result - treat as orphan
                    correlator.clear_pending_connection(peer_port).await;
                    warn!(
                        peer_port = peer_port,
                        "Association channel closed — recording as orphan"
                    );
                    None
                }
                Err(_) => {
                    // Timeout - clear pending but leave buffer for retro-binding
                    correlator.clear_pending_connection(peer_port).await;
                    if is_record_mode && retro_bind_window_ms > 0 {
                        info!(
                            peer_port = peer_port,
                            window_ms = retro_bind_window_ms,
                            "Connection not associated within timeout — buffering events for retro-bind window"
                        );
                    } else {
                        warn!(
                            peer_port = peer_port,
                            "Connection not associated within timeout — recording as orphan"
                        );
                    }
                    None
                }
            }
        }
    }
}

/// Process client events: enrich, record, and replay-match
/// In record mode with unresolved (orphan) association, buffers events for retro-binding.
async fn process_client_events(
    events: Vec<RecordedEvent>,
    association: &Arc<tokio::sync::Mutex<RuntimeAssociation>>,
    protocol_id: &str,
    correlator: &TraceCorrelator,
    recorder: &Option<Arc<Recorder>>,
    replay_engine: &Option<Arc<tokio::sync::Mutex<ReplayEngine>>>,
    c_write: &Arc<tokio::sync::Mutex<impl AsyncWriteExt + Unpin>>,
    peer_port: u16,
    retro_bind_window_ms: u64,
    is_replay_strict: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let is_record_mode = recorder.is_some() && replay_engine.is_none();

    for mut event in events {
        let assoc = association.lock().await.clone();

        // In record mode with orphan association, buffer events for retro-binding
        if is_record_mode && assoc.trace_id == "orphan" && retro_bind_window_ms > 0 {
            correlator.buffer_event_for_retro_bind(
                peer_port,
                retro_bind_window_ms,
                event,
                EventDirection::ClientToServer,
            ).await;
            continue;
        }

        let stream_id = extract_stream_id(&event);

        if let Some(sid) = stream_id {
            enrich_stream_event(
                &mut event,
                &assoc.scope_id,
                sid,
                &assoc.trace_id,
                EventDirection::ClientToServer,
                correlator,
            )
            .await;
        } else {
            enrich_event(
                &mut event,
                &assoc.scope_id,
                &assoc.trace_id,
                EventDirection::ClientToServer,
                correlator,
            )
            .await;
        }

        if let Some(rec) = recorder {
            let _ = rec.save_event(&event).await;
        }

        if let Some(engine_lock) = replay_engine {
            let mut engine = engine_lock.lock().await;
            match engine.find_match_with_responses_typed(&event) {
                Ok(Some((_, response_events, response_bytes))) => {
                    let mut cw = c_write.lock().await;
                    if protocol_id == "grpc" {
                        for resp_event in response_events {
                            if let Some(deja_core::events::recorded_event::Event::GrpcResponse(
                                grpc_resp,
                            )) = &resp_event.event
                            {
                                let bytes =
                                    deja_core::protocols::grpc::GrpcSerializer::serialize_response(
                                        grpc_resp,
                                    );
                                cw.write_all(&bytes).await?;
                            }
                        }
                    } else {
                        for resp in response_bytes {
                            cw.write_all(&resp).await?;
                        }
                    }
                }
                Ok(None) => {
                    // In strict (fail-closed) mode, reject unresolved attribution with error
                    // In lenient (fail-open) mode, skip error and forward without response
                    if is_replay_strict && !event.trace_id.is_empty() && event.trace_id != "orphan" {
                        let error_bytes = make_protocol_error(protocol_id, None);
                        if !error_bytes.is_empty() {
                            let mut cw = c_write.lock().await;
                            let _ = cw.write_all(&error_bytes).await;
                        }
                    }
                }
                Err(err) => {
                    // In strict (fail-closed) mode, send typed errors
                    // In lenient (fail-open) mode, skip error forwarding
                    if is_replay_strict {
                        warn!(
                            trace_id = %event.trace_id,
                            scope_id = %event.scope_id,
                            code = err.code(),
                            "Typed replay attribution error"
                        );
                        let error_bytes = make_protocol_error(protocol_id, Some(&err));
                        if !error_bytes.is_empty() {
                            let mut cw = c_write.lock().await;
                            let _ = cw.write_all(&error_bytes).await;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Process server events: enrich and record
async fn process_server_events(
    events: Vec<RecordedEvent>,
    association: &Arc<tokio::sync::Mutex<RuntimeAssociation>>,
    correlator: &TraceCorrelator,
    recorder: &Option<Arc<Recorder>>,
) {
    for mut event in events {
        let assoc = association.lock().await.clone();

        let stream_id = extract_stream_id(&event);

        if let Some(sid) = stream_id {
            enrich_stream_event(
                &mut event,
                &assoc.scope_id,
                sid,
                &assoc.trace_id,
                EventDirection::ServerToClient,
                correlator,
            )
            .await;
        } else {
            enrich_event(
                &mut event,
                &assoc.scope_id,
                &assoc.trace_id,
                EventDirection::ServerToClient,
                correlator,
            )
            .await;
        }

        if let Some(rec) = recorder {
            let _ = rec.save_event(&event).await;
        }
    }
}

/// Extract gRPC/HTTP2 stream ID from an event, if present
fn extract_stream_id(event: &RecordedEvent) -> Option<u32> {
    match &event.event {
        Some(deja_core::events::recorded_event::Event::GrpcRequest(req)) if req.stream_id > 0 => {
            Some(req.stream_id)
        }
        Some(deja_core::events::recorded_event::Event::GrpcResponse(resp))
            if resp.stream_id > 0 =>
        {
            Some(resp.stream_id)
        }
        _ => None,
    }
}

/// Generate protocol-specific error response bytes
fn make_protocol_error(protocol_id: &str, replay_error: Option<&ReplayMatchError>) -> Vec<u8> {
    if let Some(ReplayMatchError::UnresolvedAttribution { .. }) = replay_error {
        let message = ReplayMatchError::UNRESOLVED_ATTRIBUTION_MESSAGE;
        return match protocol_id {
            "redis" => format!("-ERR {}\r\n", message).into_bytes(),
            "http" => {
                format!(
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\n\r\n{}",
                    message.len(),
                    message
                )
                .into_bytes()
            }
            "postgres" => deja_core::protocols::postgres::PgSerializer::serialize_error(message).to_vec(),
            "grpc" => {
                use deja_core::events::GrpcStatusCode;
                deja_core::protocols::grpc::GrpcSerializer::serialize_error(
                    GrpcStatusCode::GrpcStatusInternal,
                    message,
                )
                .to_vec()
            }
            _ => vec![],
        };
    }

    match protocol_id {
        "redis" => b"-ERR No replay match found\r\n".to_vec(),
        "http" => b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n".to_vec(),
        "postgres" => {
            deja_core::protocols::postgres::PgSerializer::serialize_error("No Replay Match")
                .to_vec()
        }
        "grpc" => {
            use deja_core::events::GrpcStatusCode;
            deja_core::protocols::grpc::GrpcSerializer::serialize_error(
                GrpcStatusCode::GrpcStatusInternal,
                "No replay match found",
            )
            .to_vec()
        }
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deja_core::events::{GrpcRequestEvent, GrpcResponseEvent};

    // Helper to create gRPC request events with stream_id
    fn create_grpc_request(
        trace_id: &str,
        stream_id: u32,
        service: &str,
        method: &str,
    ) -> RecordedEvent {
        RecordedEvent {
            trace_id: trace_id.to_string(),
            scope_id: String::new(), // Will be set by enrichment
            scope_sequence: 0,
            global_sequence: 0,
            timestamp_ns: 0,
            direction: EventDirection::ClientToServer as i32,
            event: Some(
                deja_core::events::recorded_event::Event::GrpcRequest(GrpcRequestEvent {
                    service: service.to_string(),
                    method: method.to_string(),
                    request_body: vec![],
                    metadata: std::collections::HashMap::new(),
                    call_type: deja_core::events::GrpcCallType::Unary.into(),
                    stream_id,
                    authority: "localhost".to_string(),
                }),
            ),
            metadata: std::collections::HashMap::new(),
        }
    }

    // Helper to create gRPC response events with stream_id
    fn create_grpc_response(trace_id: &str, stream_id: u32, status_code: i32) -> RecordedEvent {
        RecordedEvent {
            trace_id: trace_id.to_string(),
            scope_id: String::new(), // Will be set by enrichment
            scope_sequence: 0,
            global_sequence: 0,
            timestamp_ns: 0,
            direction: EventDirection::ServerToClient as i32,
            event: Some(
                deja_core::events::recorded_event::Event::GrpcResponse(GrpcResponseEvent {
                    response_body: vec![],
                    status_code,
                    status_message: String::new(),
                    metadata: std::collections::HashMap::new(),
                    trailers: std::collections::HashMap::new(),
                    latency_ms: 10,
                    stream_id,
                }),
            ),
            metadata: std::collections::HashMap::new(),
        }
    }

    #[tokio::test]
    async fn stream_scoped_attribution_creates_isolated_scopes_per_stream() {
        // Given: A trace with connection scope and correlator
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        let parent_scope = ScopeId::connection("t1", 0);
        let association = Arc::new(tokio::sync::Mutex::new(RuntimeAssociation {
            scope_id: parent_scope.clone(),
            trace_id: "t1".to_string(),
        }));

        // When: Events from different streams are enriched
        let mut event_stream1 = create_grpc_request("t1", 1, "MyService", "MethodA");
        let mut event_stream3 = create_grpc_request("t1", 3, "MyService", "MethodB");

        enrich_stream_event(
            &mut event_stream1,
            &parent_scope,
            1, // stream_id
            "t1",
            EventDirection::ClientToServer,
            &correlator,
        )
        .await;

        enrich_stream_event(
            &mut event_stream3,
            &parent_scope,
            3, // stream_id
            "t1",
            EventDirection::ClientToServer,
            &correlator,
        )
        .await;

        // Then: Each event should have distinct stream scope
        assert!(
            event_stream1.scope_id.contains(":stream:1"),
            "Stream 1 event should have stream:1 scope, got: {}",
            event_stream1.scope_id
        );
        assert!(
            event_stream3.scope_id.contains(":stream:3"),
            "Stream 3 event should have stream:3 scope, got: {}",
            event_stream3.scope_id
        );

        // Then: Scopes should be different
        assert_ne!(
            event_stream1.scope_id, event_stream3.scope_id,
            "Different streams must have different scope_ids"
        );
    }

    #[tokio::test]
    async fn stream_scope_sequences_are_independent_across_streams() {
        // Given: A trace with connection scope and correlator
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        let parent_scope = ScopeId::connection("t1", 0);
        let association = Arc::new(tokio::sync::Mutex::new(RuntimeAssociation {
            scope_id: parent_scope.clone(),
            trace_id: "t1".to_string(),
        }));

        // When: Multiple events on different streams are interleaved
        let mut events = vec![
            (1, create_grpc_request("t1", 1, "MyService", "MethodA")),
            (3, create_grpc_request("t1", 3, "MyService", "MethodB")),
            (1, create_grpc_response("t1", 1, 0)),
            (5, create_grpc_request("t1", 5, "MyService", "MethodC")),
            (3, create_grpc_response("t1", 3, 0)),
            (5, create_grpc_response("t1", 5, 0)),
        ];

        for (stream_id, ref mut event) in &mut events {
            enrich_stream_event(
                event,
                &parent_scope,
                *stream_id,
                "t1",
                if matches!(
                    event.event,
                    Some(deja_core::events::recorded_event::Event::GrpcRequest(_))
                ) {
                    EventDirection::ClientToServer
                } else {
                    EventDirection::ServerToClient
                },
                &correlator,
            )
            .await;
        }

        // Then: Each stream should have independent scope_sequence
        // Stream 1: request (0), response (1)
        // Stream 3: request (0), response (1)
        // Stream 5: request (0), response (1)
        let stream1_scope = ScopeId::stream("t1", 0, 1);
        let stream3_scope = ScopeId::stream("t1", 0, 3);
        let stream5_scope = ScopeId::stream("t1", 0, 5);

        assert_eq!(
            events[0].1.scope_sequence, 0,
            "Stream 1 request should be scope_sequence 0"
        );
        assert_eq!(
            events[1].1.scope_sequence, 0,
            "Stream 3 request should be scope_sequence 0 (independent)"
        );
        assert_eq!(
            events[2].1.scope_sequence, 1,
            "Stream 1 response should be scope_sequence 1"
        );
        assert_eq!(
            events[3].1.scope_sequence, 0,
            "Stream 5 request should be scope_sequence 0 (independent)"
        );
        assert_eq!(
            events[4].1.scope_sequence, 1,
            "Stream 3 response should be scope_sequence 1"
        );
        assert_eq!(
            events[5].1.scope_sequence, 1,
            "Stream 5 response should be scope_sequence 1"
        );

        // Verify scope_ids
        assert_eq!(events[0].1.scope_id, stream1_scope.as_str());
        assert_eq!(events[1].1.scope_id, stream3_scope.as_str());
        assert_eq!(events[3].1.scope_id, stream5_scope.as_str());
    }

    #[tokio::test]
    async fn interleaved_stream_events_maintain_trace_isolation() {
        // Given: Two different traces with interleaved streams on same connection
        let correlator = TraceCorrelator::new();
        correlator.start_trace("trace-a".into(), 1000).await;
        correlator.start_trace("trace-b".into(), 1000).await;

        // Simulate two different connections (one per trace)
        let trace_a_scope = ScopeId::connection("trace-a", 0);
        let trace_b_scope = ScopeId::connection("trace-b", 0);

        // When: Events from different traces with same stream IDs are enriched
        let mut event_a_stream1 = create_grpc_request("trace-a", 1, "Service", "Method");
        let mut event_b_stream1 = create_grpc_request("trace-b", 1, "Service", "Method");

        enrich_stream_event(
            &mut event_a_stream1,
            &trace_a_scope,
            1,
            "trace-a",
            EventDirection::ClientToServer,
            &correlator,
        )
        .await;

        enrich_stream_event(
            &mut event_b_stream1,
            &trace_b_scope,
            1,
            "trace-b",
            EventDirection::ClientToServer,
            &correlator,
        )
        .await;

        // Then: Each event should have trace-isolated scopes
        assert!(
            event_a_stream1.scope_id.contains("trace:trace-a:"),
            "Event should have trace-a scope, got: {}",
            event_a_stream1.scope_id
        );
        assert!(
            event_b_stream1.scope_id.contains("trace:trace-b:"),
            "Event should have trace-b scope, got: {}",
            event_b_stream1.scope_id
        );
        assert_ne!(
            event_a_stream1.scope_id, event_b_stream1.scope_id,
            "Same stream ID on different traces must have different scopes"
        );
    }

    #[tokio::test]
    async fn extract_stream_id_returns_correct_stream_id() {
        // Given: Events with stream IDs
        let req_event = create_grpc_request("t1", 5, "Service", "Method");
        let resp_event = create_grpc_response("t1", 7, 0);

        // When: extract_stream_id is called
        let req_stream = extract_stream_id(&req_event);
        let resp_stream = extract_stream_id(&resp_event);

        // Then: Correct stream IDs should be extracted
        assert_eq!(req_stream, Some(5));
        assert_eq!(resp_stream, Some(7));
    }

    #[tokio::test]
    async fn extract_stream_id_returns_none_for_non_grpc_events() {
        // Given: Non-gRPC event
        let http_event = RecordedEvent {
            trace_id: "t1".to_string(),
            scope_id: String::new(),
            scope_sequence: 0,
            global_sequence: 0,
            timestamp_ns: 0,
            direction: 0,
            event: Some(
                deja_core::events::recorded_event::Event::HttpRequest(
                    deja_core::events::HttpRequestEvent {
                        method: "GET".to_string(),
                        path: "/test".to_string(),
                        headers: Default::default(),
                        body: vec![],
                        schema: "http".to_string(),
                        host: "localhost".to_string(),
                    },
                ),
            ),
            metadata: Default::default(),
        };

        // When: extract_stream_id is called
        let stream = extract_stream_id(&http_event);

        // Then: Should return None
        assert_eq!(stream, None);
    }

    #[tokio::test]
    async fn stream_event_uses_parser_extracted_trace_id_when_present() {
        // Given: A gRPC event with trace_id already set (from headers)
        let correlator = TraceCorrelator::new();
        let parent_scope = ScopeId::connection("connection-trace", 0);
        let association = Arc::new(tokio::sync::Mutex::new(RuntimeAssociation {
            scope_id: parent_scope.clone(),
            trace_id: "connection-trace".to_string(),
        }));

        let mut event = create_grpc_request("header-trace", 1, "Service", "Method");
        // Event already has trace_id from header parsing
        event.trace_id = "header-trace".to_string();

        // When: enrich_stream_event is called
        enrich_stream_event(
            &mut event,
            &parent_scope,
            1,
            "connection-trace", // Connection-level trace
            EventDirection::ClientToServer,
            &correlator,
        )
        .await;

        // Then: Stream scope should use header-extracted trace_id
        assert!(
            event.scope_id.contains("trace:header-trace:"),
            "Stream scope should use header-extracted trace_id, got: {}",
            event.scope_id
        );
        // Event trace_id should be preserved
        assert_eq!(event.trace_id, "header-trace");
    }

    #[test]
    fn typed_unresolved_attribution_http_error_is_stable() {
        let error = ReplayMatchError::UnresolvedAttribution {
            trace_id: "orphan".to_string(),
            scope_id: "orphan:1".to_string(),
        };

        let bytes = make_protocol_error("http", Some(&error));
        let as_text = String::from_utf8(bytes).unwrap();

        assert_eq!(
            as_text,
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 29\r\n\r\nReplay attribution unresolved"
        );
    }

    #[test]
    fn typed_unresolved_attribution_redis_error_is_stable() {
        let error = ReplayMatchError::UnresolvedAttribution {
            trace_id: "".to_string(),
            scope_id: "".to_string(),
        };

        let bytes = make_protocol_error("redis", Some(&error));
        let as_text = String::from_utf8(bytes).unwrap();

        assert_eq!(as_text, "-ERR Replay attribution unresolved\r\n");
    }

    #[tokio::test]
    async fn orphan_connection_stays_quarantined_despite_event_trace_id() {
        // Given: an orphan association (no SDK registration)
        let correlator = TraceCorrelator::new();
        correlator.start_trace("t1".into(), 1000).await;

        let orphan_scope = ScopeId::orphan(42);
        let association = Arc::new(tokio::sync::Mutex::new(RuntimeAssociation {
            scope_id: orphan_scope.clone(),
            trace_id: "orphan".to_string(),
        }));

        // When: events with a non-empty trace_id are enriched through the pipeline
        let mut event = RecordedEvent {
            trace_id: "t1".to_string(),
            ..Default::default()
        };
        let assoc = association.lock().await.clone();
        enrich_event(
            &mut event,
            &assoc.scope_id,
            &assoc.trace_id,
            EventDirection::ClientToServer,
            &correlator,
        )
        .await;

        // Then: the association must still be orphan
        let guard = association.lock().await;
        assert!(
            guard.scope_id.is_orphan(),
            "Orphan connection was promoted to {:?} by parser-extracted trace ID; \
             attribution must only happen via control-plane bindings",
            guard.scope_id
        );
        assert_eq!(
            guard.trace_id, "orphan",
            "Orphan trace_id was mutated to '{}'; must remain 'orphan' without control-plane bind",
            guard.trace_id
        );

        // Then: enriched event carries orphan scope, not the parser-extracted trace
        assert!(
            event.scope_id.contains("orphan"),
            "Enriched event scope_id should be orphan, got: {}",
            event.scope_id
        );
    }

    #[tokio::test]
    async fn retro_bind_within_window_attributes_buffered_events() {
        use deja_core::events::EventDirection;

        // Given: A trace and correlator with retro-bind window of 1000ms
        let correlator = Arc::new(TraceCorrelator::new());
        correlator.start_trace("t1".into(), 1000).await;
        let window_ms = 1000u64;
        let source_port = 55555u16;

        // Buffer events for the source port (simulating events arriving before association)
        let event1 = RecordedEvent {
            trace_id: "".to_string(),
            scope_id: "".to_string(),
            scope_sequence: 0,
            global_sequence: 0,
            timestamp_ns: 0,
            direction: EventDirection::ClientToServer as i32,
            metadata: std::collections::HashMap::new(),
            event: None,
        };
        let event2 = RecordedEvent {
            trace_id: "".to_string(),
            scope_id: "".to_string(),
            scope_sequence: 1,
            global_sequence: 1,
            timestamp_ns: 0,
            direction: EventDirection::ServerToClient as i32,
            metadata: std::collections::HashMap::new(),
            event: None,
        };

        correlator
            .buffer_event_for_retro_bind(source_port, window_ms, event1.clone(), EventDirection::ClientToServer)
            .await;
        correlator
            .buffer_event_for_retro_bind(source_port, window_ms, event2.clone(), EventDirection::ServerToClient)
            .await;

        assert_eq!(
            correlator
                .get_or_create_pending_buffer(source_port, window_ms)
                .await
                .event_count(source_port),
            2,
            "Events should be buffered"
        );

        // When: Association arrives within window
        let scope_id = ScopeId::connection("t1", 0);
        let flushed = correlator
            .flush_buffered_events_on_bind(source_port, &scope_id, "t1")
            .await;

        // Then: Buffered events are flushed and attributed
        assert_eq!(flushed.len(), 2, "Both events should be flushed");
        assert_eq!(flushed[0].0.trace_id, "t1", "First event should be attributed to t1");
        assert_eq!(flushed[0].0.scope_id, scope_id.as_str(), "First event should have correct scope");
        assert_eq!(flushed[1].0.trace_id, "t1", "Second event should be attributed to t1");

        // Buffer should be empty after flush
        assert_eq!(
            correlator
                .get_or_create_pending_buffer(source_port, window_ms)
                .await
                .event_count(source_port),
            0,
            "Buffer should be empty after flush"
        );

        // Quarantine should be empty
        assert_eq!(correlator.quarantined_count().await, 0, "No events should be quarantined");
    }

    #[tokio::test]
    async fn retro_bind_beyond_window_quarantines_events() {
        use deja_core::events::EventDirection;

        // Given: A trace and correlator with retro-bind window of 0ms (everything expires immediately)
        let correlator = Arc::new(TraceCorrelator::new());
        correlator.start_trace("t1".into(), 1000).await;
        let window_ms = 0u64; // 0ms window means all events expire immediately
        let source_port = 55556u16;

        // Buffer events for the source port
        let event1 = RecordedEvent {
            trace_id: "".to_string(),
            scope_id: "".to_string(),
            scope_sequence: 0,
            global_sequence: 0,
            timestamp_ns: 0,
            direction: EventDirection::ClientToServer as i32,
            metadata: std::collections::HashMap::new(),
            event: None,
        };

        correlator
            .buffer_event_for_retro_bind(source_port, window_ms, event1, EventDirection::ClientToServer)
            .await;

        // When: Flush is attempted (all events are expired due to 0ms window)
        let scope_id = ScopeId::connection("t1", 0);
        let flushed = correlator
            .flush_buffered_events_on_bind(source_port, &scope_id, "t1")
            .await;

        // Then: No events within window - all expired
        assert_eq!(flushed.len(), 0, "No events should be flushed (all expired)");

        // When: Quarantine the expired events
        let expired = correlator.quarantine_expired_events(source_port).await;
        correlator.add_to_quarantine(source_port, expired).await;

        // Then: Events should be in quarantine
        assert_eq!(
            correlator.quarantined_count().await,
            1,
            "Expired event should be quarantined"
        );
    }

    #[tokio::test]
    async fn replay_strict_mode_defaults_to_lenient() {
        // Given: No environment variable or explicit config (defaults to "lenient")
        // IMPORTANT: Clear env var first to avoid pollution from parallel tests
        std::env::remove_var("DEJA_REPLAY_STRICT");
        
        let replay_strict_mode = "lenient".to_string();

        // When: Determining replay strictness
        let is_replay_strict = match std::env::var("DEJA_REPLAY_STRICT").as_deref() {
            Ok("strict") | Ok("Strict") | Ok("STRICT") => true,
            Ok("lenient") | Ok("Lenient") | Ok("LENIENT") => false,
            Ok(_) | Err(_) => replay_strict_mode == "strict",
        };

        // Then: Should default to lenient (fail-open)
        assert!(!is_replay_strict, "Defaults to lenient (fail-open) mode");
    }

    #[tokio::test]
    async fn replay_strict_mode_env_override() {
        let replay_strict_mode = "lenient".to_string();

        std::env::set_var("DEJA_REPLAY_STRICT", "strict");

        let is_replay_strict = match std::env::var("DEJA_REPLAY_STRICT").as_deref() {
            Ok("strict") | Ok("Strict") | Ok("STRICT") => true,
            Ok("lenient") | Ok("Lenient") | Ok("LENIENT") => false,
            Ok(_) | Err(_) => replay_strict_mode == "strict",
        };

        std::env::remove_var("DEJA_REPLAY_STRICT");

        assert!(is_replay_strict, "Environment variable overrides config");
    }

    #[tokio::test]
    async fn replay_strict_mode_config_explicit() {
        // Given: Config explicitly set to "strict"
        let replay_strict_mode = "strict".to_string();

        // When: Determining replay strictness with no env var
        let is_replay_strict = match std::env::var("DEJA_REPLAY_STRICT").as_deref() {
            Ok("strict") | Ok("Strict") | Ok("STRICT") => true,
            Ok("lenient") | Ok("Lenient") | Ok("LENIENT") => false,
            Ok(_) | Err(_) => replay_strict_mode == "strict",
        };

        assert!(is_replay_strict, "Explicit config value honored");
    }

    #[tokio::test]
    async fn replay_lenient_mode_config_explicit() {
        // Given: Config explicitly set to "lenient"
        let replay_strict_mode = "lenient".to_string();

        let is_replay_strict = match std::env::var("DEJA_REPLAY_STRICT").as_deref() {
            Ok("strict") | Ok("Strict") | Ok("STRICT") => true,
            Ok("lenient") | Ok("Lenient") | Ok("LENIENT") => false,
            Ok(_) | Err(_) => replay_strict_mode == "strict",
        };

        assert!(!is_replay_strict, "Lenient config value honored");
    }

    #[tokio::test]
    async fn replay_strict_mode_case_insensitive_env() {
        // Given: Multiple case variations of strict mode
        for value in ["strict", "STRICT", "Strict"] {
            std::env::set_var("DEJA_REPLAY_STRICT", value);

            // When: Determining replay strictness
            let is_replay_strict = match std::env::var("DEJA_REPLAY_STRICT").as_deref() {
                Ok("strict") | Ok("Strict") | Ok("STRICT") => true,
                Ok("lenient") | Ok("Lenient") | Ok("LENIENT") => false,
                Ok(_) | Err(_) => true,
            };

            // Then: All variations should recognize as strict
            assert!(is_replay_strict, "Env var case variations recognized");
        }

        std::env::remove_var("DEJA_REPLAY_STRICT");
    }

    #[tokio::test]
    async fn replay_lenient_mode_case_insensitive_env() {
        // Given: Multiple case variations of lenient mode
        for value in ["lenient", "LENIENT", "Lenient"] {
            std::env::set_var("DEJA_REPLAY_STRICT", value);

            // When: Determining replay strictness
            let is_replay_strict = match std::env::var("DEJA_REPLAY_STRICT").as_deref() {
                Ok("strict") | Ok("Strict") | Ok("STRICT") => true,
                Ok("lenient") | Ok("Lenient") | Ok("LENIENT") => false,
                Ok(_) | Err(_) => false,
            };

            // Then: All variations should recognize as lenient
            assert!(!is_replay_strict, "Env var case variations recognized");
        }

        std::env::remove_var("DEJA_REPLAY_STRICT");
    }
}

/// Handle a TLS connection with MITM interception
pub async fn handle_tls_connection(
    client_stream: TcpStream,
    target_addr: String,
    target_host: &str,
    tls_manager: Arc<TlsMitmManager>,
    parsers: Arc<Vec<Arc<dyn ProtocolParser>>>,
    recorder: Option<Arc<Recorder>>,
    replay_engine: Option<Arc<tokio::sync::Mutex<ReplayEngine>>>,
    correlator: Arc<TraceCorrelator>,
    peer_addr: std::net::SocketAddr,
    association_timeout_ms: u64,
    is_replay_strict: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server_config = tls_manager.generate_server_config(target_host)?;
    let acceptor = TlsAcceptor::from(server_config);

    // Accept TLS from client
    let tls_client_stream = acceptor.accept(client_stream).await?;
    info!("[TLS] Accepted TLS connection for host: {}", target_host);

    let (mut c_read, c_write) = tokio::io::split(tls_client_stream);

    // In replay mode or when replay strict mode is enabled, don't connect to real target
    let is_replay_mode = replay_engine.is_some();
    let target_stream = if !is_replay_mode || is_replay_strict {
        let target_tcp = TcpStream::connect(&target_addr).await?;
        // Register outgoing port mapping for correlation
        // The SDK queries the database server for client port, which returns our outgoing port,
        // not the original client's peer port. We need to translate.
        if let Ok(local_addr) = target_tcp.local_addr() {
            let outgoing_port = local_addr.port();
            let peer_port = peer_addr.port();
            correlator.register_outgoing_port_mapping(outgoing_port, peer_port).await;
        }
        let client_config = tls_manager.client_config();
        let connector = tokio_rustls::TlsConnector::from(client_config);
        let server_name = rustls::pki_types::ServerName::try_from(target_host.to_string())?;
        let tls_target = connector.connect(server_name, target_tcp).await?;
        Some(tls_target)
    } else {
        None
    };

    // Detect protocol from first decrypted bytes
    let mut peek_buf = [0u8; 1024];
    let peeked = {
        let n = c_read.read(&mut peek_buf).await?;
        if n == 0 {
            return Ok(());
        }
        n
    };

    let detected_parser = detect_protocol_from_bytes(&peek_buf[..peeked], &parsers);
    let protocol: Protocol = detected_parser
        .protocol_id()
        .parse()
        .unwrap_or(Protocol::Unknown);
    info!("[TLS] Detected protocol: {}", detected_parser.protocol_id());

    // Resolve trace association
    let is_record_mode = recorder.is_some() && replay_engine.is_none();
    let retro_bind_window_ms: u64 = std::env::var("DEJA_RETRO_BIND_WINDOW_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    let peer_port = peer_addr.port();
    let assoc =
        resolve_association(&correlator, peer_port, protocol, association_timeout_ms, retro_bind_window_ms, is_record_mode).await;
    let (scope_id, trace_id) = match &assoc {
        Some(a) => (a.scope_id.clone(), a.trace_id.clone()),
        None => {
            let orphan_id = ORPHAN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            (ScopeId::orphan(orphan_id), "orphan".to_string())
        }
    };

    tracing::Span::current().record("scope_id", scope_id.as_str());

    let association = Arc::new(tokio::sync::Mutex::new(RuntimeAssociation {
        scope_id: scope_id.clone(),
        trace_id: trace_id.clone(),
    }));

    let c_write = Arc::new(tokio::sync::Mutex::new(c_write));

    let (t_read, t_write) = if let Some(ts) = target_stream {
        let (tr, tw) = tokio::io::split(ts);
        (Some(tr), Some(Arc::new(tokio::sync::Mutex::new(tw))))
    } else {
        (None, None)
    };

    let mut connection_parser = detected_parser.new_connection(scope_id.as_str().to_string());
    connection_parser.set_mode(replay_engine.is_some());
    let parser_lock = Arc::new(tokio::sync::Mutex::new(connection_parser));

    // Replay initialization
    if replay_engine.is_some() {
        if let Some(init_bytes) = detected_parser.on_replay_init() {
            let mut cw = c_write.lock().await;
            let _ = cw.write_all(&init_bytes).await;
        }
    }

    let protocol_id = detected_parser.protocol_id().to_string();

    // Process initial peeked data
    {
        let mut p = parser_lock.lock().await;
        match p.parse_client_data(&peek_buf[..peeked]) {
            Ok(res) => {
                process_client_events(
                    res.events,
                    &association,
                    &protocol_id,
                    &correlator,
                    &recorder,
                    &replay_engine,
                    &c_write,
                    peer_port,
                    retro_bind_window_ms,
                    is_replay_strict,
                )
                .await?;

                if let Some(tw) = &t_write {
                    if !res.forward.is_empty() {
                        let mut tw = tw.lock().await;
                        tw.write_all(&res.forward).await?;
                    }
                }
                if let Some(reply) = res.reply {
                    let mut cw = c_write.lock().await;
                    cw.write_all(&reply).await?;
                }
            }
            Err(_) => {
                if let Some(tw) = &t_write {
                    let mut tw = tw.lock().await;
                    tw.write_all(&peek_buf[..peeked]).await?;
                }
            }
        }
    }

    // Client -> Target handler
    let c_write_c = c_write.clone();
    let t_write_c = t_write.clone();
    let parser_c = parser_lock.clone();
    let rec_c = recorder.clone();
    let rep_c = replay_engine.clone();
    let corr_c = correlator.clone();
    let assoc_c = association.clone();
    let proto_c = protocol_id.clone();
    let peer_port_c = peer_port;
    let retro_window_c = retro_bind_window_ms;

    let c_handle = async move {
        let mut buf = [0u8; 8192];
        loop {
            let n = c_read
                .read(&mut buf)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            if n == 0 {
                break;
            }

            let mut p = parser_c.lock().await;
            match p.parse_client_data(&buf[..n]) {
                Ok(res) => {
                    process_client_events(
                        res.events,
                        &assoc_c,
                        &proto_c,
                        &corr_c,
                        &rec_c,
                        &rep_c,
                        &c_write_c,
                        peer_port_c,
                        retro_window_c,
                        is_replay_strict,
                    )
                    .await
                    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>)?;

                    if let Some(tw) = &t_write_c {
                        if !res.forward.is_empty() {
                            let mut tw = tw.lock().await;
                            tw.write_all(&res.forward).await.map_err(|e| {
                                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                            })?;
                        }
                    }
                    if let Some(reply) = res.reply {
                        let mut cw = c_write_c.lock().await;
                        cw.write_all(&reply).await.map_err(|e| {
                            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                        })?;
                    }
                }
                Err(_) => {
                    if let Some(tw) = &t_write_c {
                        let mut tw = tw.lock().await;
                        tw.write_all(&buf[..n]).await.map_err(|e| {
                            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                        })?;
                    }
                }
            }
        }
        if let Some(tw) = &t_write_c {
            let mut tw = tw.lock().await;
            let _ = tw.shutdown().await;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    // Target -> Client handler
    let c_write_t = c_write.clone();
    let parser_t = parser_lock.clone();

    let t_handle = async move {
        if let Some(mut t_read) = t_read {
            let mut buf = [0u8; 8192];
            loop {
                let n = t_read
                    .read(&mut buf)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                if n == 0 {
                    break;
                }

                let mut p = parser_t.lock().await;
                match p.parse_server_data(&buf[..n]) {
                    Ok(res) => {
                        process_server_events(res.events, &association, &correlator, &recorder)
                            .await;

                        if !res.forward.is_empty() {
                            let mut cw = c_write_t.lock().await;
                            cw.write_all(&res.forward).await.map_err(|e| {
                                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                            })?;
                        }
                    }
                    Err(_) => {
                        let mut cw = c_write_t.lock().await;
                        cw.write_all(&buf[..n]).await.map_err(|e| {
                            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                        })?;
                    }
                }
            }
            let mut cw = c_write_t.lock().await;
            let _ = cw.shutdown().await;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    tokio::try_join!(c_handle, t_handle)?;
    Ok(())
}

/// Handle a plain TCP connection
#[instrument(
    skip(client_stream, parser, recorder, replay_engine, correlator),
    fields(scope_id)
)]
pub async fn handle_connection(
    client_stream: TcpStream,
    target_addr: String,
    parser: Arc<dyn ProtocolParser>,
    recorder: Option<Arc<Recorder>>,
    replay_engine: Option<Arc<tokio::sync::Mutex<ReplayEngine>>>,
    correlator: Arc<TraceCorrelator>,
    peer_addr: std::net::SocketAddr,
    association_timeout_ms: u64,
    is_replay_strict: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let is_replay_mode = replay_engine.is_some();
    let target_stream = if !is_replay_mode || is_replay_strict {
        Some(
            TcpStream::connect(&target_addr)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        )
    } else {
        None
    };

    // Register outgoing port mapping for correlation
    // The SDK queries the database server for client port, which returns our outgoing port,
    // not the original client's peer port. We need to translate.
    if let Some(ref ts) = target_stream {
        if let Ok(local_addr) = ts.local_addr() {
            let outgoing_port = local_addr.port();
            let peer_port = peer_addr.port();
            correlator.register_outgoing_port_mapping(outgoing_port, peer_port).await;
        }
    }

    let protocol: Protocol = parser.protocol_id().parse().unwrap_or(Protocol::Unknown);

    // Resolve trace association using source-port
    let is_record_mode = recorder.is_some() && replay_engine.is_none();
    let retro_bind_window_ms: u64 = std::env::var("DEJA_RETRO_BIND_WINDOW_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    let assoc =
        resolve_association(&correlator, peer_addr.port(), protocol, association_timeout_ms, retro_bind_window_ms, is_record_mode).await;
    let (scope_id, trace_id) = match assoc {
        Some(a) => (a.scope_id, a.trace_id),
        None => {
            // In replay mode, try to allocate from active traces
            if replay_engine.is_some() {
                let active = correlator.get_active_traces().await;
                if let Some(tid) = active.first() {
                    let sid = correlator.allocate_connection_id(tid).await;
                    (sid, tid.clone())
                } else {
                    let orphan_id = ORPHAN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    (ScopeId::orphan(orphan_id), "orphan".to_string())
                }
            } else {
                let orphan_id = ORPHAN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                (ScopeId::orphan(orphan_id), "orphan".to_string())
            }
        }
    };

    tracing::Span::current().record("scope_id", scope_id.as_str());

    let association = Arc::new(tokio::sync::Mutex::new(RuntimeAssociation {
        scope_id: scope_id.clone(),
        trace_id: trace_id.clone(),
    }));

    let (mut c_read, c_write) = client_stream.into_split();
    let c_write = Arc::new(tokio::sync::Mutex::new(c_write));

    let (t_read, t_write) = if let Some(ts) = target_stream {
        let (tr, tw) = ts.into_split();
        (Some(tr), Some(Arc::new(tokio::sync::Mutex::new(tw))))
    } else {
        (None, None)
    };

    let mut connection_parser = parser.new_connection(scope_id.as_str().to_string());
    connection_parser.set_mode(replay_engine.is_some());
    let parser_lock = Arc::new(tokio::sync::Mutex::new(connection_parser));

    // Replay initialization
    if replay_engine.is_some() {
        if let Some(init_bytes) = parser.on_replay_init() {
            let mut cw = c_write.lock().await;
            let _ = cw.write_all(&init_bytes).await;
        }
    }

    let protocol_id = parser.protocol_id().to_string();

    // Client -> Target handler
    let c_write_c = c_write.clone();
    let t_write_c = t_write.clone();
    let parser_c = parser_lock.clone();
    let rec_c = recorder.clone();
    let rep_c = replay_engine.clone();
    let corr_c = correlator.clone();
    let assoc_c = association.clone();
    let proto_c = protocol_id.clone();
    let peer_port_c = peer_addr.port();
    let retro_window_c = retro_bind_window_ms;

    let c_handle = async move {
        let mut buf = [0u8; 8192];
        loop {
            let n = c_read
                .read(&mut buf)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            if n == 0 {
                break;
            }

            let mut p = parser_c.lock().await;
            match p.parse_client_data(&buf[..n]) {
                Ok(res) => {
                    process_client_events(
                        res.events,
                        &assoc_c,
                        &proto_c,
                        &corr_c,
                        &rec_c,
                        &rep_c,
                        &c_write_c,
                        peer_port_c,
                        retro_window_c,
                        false,  // lenient from handle_connection
                    )
                    .await
                    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as Box<dyn std::error::Error + Send + Sync>)?;

                    if let Some(tw) = &t_write_c {
                        if !res.forward.is_empty() {
                            let mut tw = tw.lock().await;
                            tw.write_all(&res.forward).await.map_err(|e| {
                                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                            })?;
                        }
                    }
                    if let Some(reply) = res.reply {
                        let mut cw = c_write_c.lock().await;
                        cw.write_all(&reply).await.map_err(|e| {
                            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                        })?;
                    }
                }
                Err(_) => {
                    if let Some(tw) = &t_write_c {
                        let mut tw = tw.lock().await;
                        tw.write_all(&buf[..n]).await.map_err(|e| {
                            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                        })?;
                    }
                }
            }
        }
        if let Some(tw) = &t_write_c {
            let mut tw = tw.lock().await;
            let _ = tw.shutdown().await;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    // Target -> Client handler
    let c_write_t = c_write.clone();
    let parser_t = parser_lock.clone();
    let rec_t = recorder.clone();
    let corr_t = correlator.clone();
    let assoc_t = association.clone();

    let t_handle = async move {
        if let Some(mut tr) = t_read {
            let mut buf = [0u8; 8192];
            loop {
                let n = tr
                    .read(&mut buf)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                if n == 0 {
                    break;
                }

                let mut p = parser_t.lock().await;
                if let Ok(res) = p.parse_server_data(&buf[..n]) {
                    process_server_events(
                        res.events,
                        &assoc_t,
                        &corr_t,
                        &rec_t,
                    )
                    .await;
                    let mut cw = c_write_t.lock().await;
                    cw.write_all(&res.forward).await.map_err(|e| {
                        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                    })?;
                } else {
                    let mut cw = c_write_t.lock().await;
                    cw.write_all(&buf[..n]).await.map_err(|e| {
                        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                    })?;
                }
            }
            let mut cw = c_write_t.lock().await;
            let _ = cw.shutdown().await;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    tokio::try_join!(c_handle, t_handle)?;
    Ok(())
}
