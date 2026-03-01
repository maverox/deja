use deja_core::events::{
    recorded_event, GrpcRequestEvent, GrpcResponseEvent, HttpRequestEvent, HttpResponseEvent,
    RecordedEvent,
};
use deja_core::replay::{ReplayEngine, ReplayMatchError};
use tokio::fs;

// Helper to create events (copied from recording_index tests)
fn create_http_request(trace_id: &str, path: &str, seq: u64) -> RecordedEvent {
    RecordedEvent {
        trace_id: trace_id.to_string(),
        scope_id: format!("trace:{}:conn:0", trace_id),
        scope_sequence: seq,
        global_sequence: 100 + seq,
        timestamp_ns: seq * 1000,
        direction: 1, // CLIENT_TO_SERVER
        event: Some(recorded_event::Event::HttpRequest(HttpRequestEvent {
            method: "GET".to_string(),
            path: path.to_string(),
            headers: Default::default(),
            body: vec![],
            schema: "http".to_string(),
            host: "localhost".to_string(),
        })),
        metadata: Default::default(),
    }
}

fn create_http_response(trace_id: &str, status: u32, seq: u64) -> RecordedEvent {
    RecordedEvent {
        trace_id: trace_id.to_string(),
        scope_id: format!("trace:{}:conn:0", trace_id),
        scope_sequence: seq,
        global_sequence: 100 + seq,
        timestamp_ns: seq * 1000,
        direction: 2, // SERVER_TO_CLIENT
        event: Some(recorded_event::Event::HttpResponse(HttpResponseEvent {
            status,
            headers: Default::default(),
            body: vec![],
            latency_ms: 10,
        })),
        metadata: Default::default(),
    }
}

#[tokio::test]
async fn test_protocol_scoped_replay() {
    let temp_dir = tempfile::tempdir().unwrap();
    let events_file = temp_dir.path().join("events.jsonl");

    // Create a recording with interleaved requests
    // Trace 1: Req A (seq 0), Req B (seq 1)
    let events = vec![
        create_http_request("trace-1", "/a", 0),
        create_http_response("trace-1", 200, 1),
        create_http_request("trace-1", "/b", 2),
        create_http_response("trace-1", 201, 3),
    ];

    // Write events to file
    {
        use tokio::io::AsyncWriteExt;
        let mut file = fs::File::create(&events_file).await.unwrap();
        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            file.write_all(json.as_bytes()).await.unwrap();
            file.write_all(b"\n").await.unwrap();
        }
    }

    // Initialize engine
    let mut engine = ReplayEngine::new(events_file).await.unwrap();

    // Replay Request A
    // We send an incoming event that matches Trace 1, Protocol HTTP.
    // The engine should track that this is the FIRST request (seq 0) for this trace/proto.
    let req_a_incoming = create_http_request("trace-1", "/a", 999); // Different global seq/timestamp

    let (matched, response_events, _response_bytes) =
        engine.find_match_with_responses(&req_a_incoming).unwrap();

    // Check match
    if let Some(recorded_event::Event::HttpRequest(req)) = matched.event {
        assert_eq!(req.path, "/a");
    } else {
        panic!("Wrong event type");
    }
    assert_eq!(response_events.len(), 1); // One response

    // Replay Request B
    // Should match seq 1
    let req_b_incoming = create_http_request("trace-1", "/b", 9999);

    let (matched_b, _, _) = engine.find_match_with_responses(&req_b_incoming).unwrap();

    if let Some(recorded_event::Event::HttpRequest(req)) = matched_b.event {
        assert_eq!(req.path, "/b");
    } else {
        panic!("Wrong event type");
    }
}

#[tokio::test]
async fn test_replay_rejects_payload_match_with_wrong_graph_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let events_file = temp_dir.path().join("events.jsonl");

    let events = vec![
        create_http_request("trace-graph-a", "/same-payload", 0),
        create_http_response("trace-graph-a", 200, 1),
    ];

    {
        use tokio::io::AsyncWriteExt;
        let mut file = fs::File::create(&events_file).await.unwrap();
        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            file.write_all(json.as_bytes()).await.unwrap();
            file.write_all(b"\n").await.unwrap();
        }
    }

    let mut engine = ReplayEngine::new(events_file).await.unwrap();

    let wrong_trace_same_payload = create_http_request("trace-graph-b", "/same-payload", 777);
    let wrong_trace_match = engine.find_match_with_responses(&wrong_trace_same_payload);
    assert!(
        wrong_trace_match.is_none(),
        "Replay matched by payload despite wrong trace_id"
    );

    let correct_graph = create_http_request("trace-graph-a", "/same-payload", 999);
    let correct_match = engine.find_match_with_responses(&correct_graph);
    assert!(
        correct_match.is_some(),
        "Replay should match when graph identity is correct"
    );
}

// Helper to create gRPC request events with stream_id
fn create_grpc_request(
    trace_id: &str,
    scope_id: &str,
    stream_id: u32,
    service: &str,
    method: &str,
    seq: u64,
) -> RecordedEvent {
    RecordedEvent {
        trace_id: trace_id.to_string(),
        scope_id: scope_id.to_string(),
        scope_sequence: seq,
        global_sequence: 100 + seq,
        timestamp_ns: seq * 1000,
        direction: 1, // CLIENT_TO_SERVER
        event: Some(recorded_event::Event::GrpcRequest(GrpcRequestEvent {
            service: service.to_string(),
            method: method.to_string(),
            request_body: vec![],
            metadata: std::collections::HashMap::new(),
            call_type: deja_core::events::GrpcCallType::Unary.into(),
            stream_id,
            authority: "localhost".to_string(),
        })),
        metadata: Default::default(),
    }
}

fn create_grpc_response(
    trace_id: &str,
    scope_id: &str,
    stream_id: u32,
    status_code: i32,
    seq: u64,
) -> RecordedEvent {
    RecordedEvent {
        trace_id: trace_id.to_string(),
        scope_id: scope_id.to_string(),
        scope_sequence: seq,
        global_sequence: 100 + seq,
        timestamp_ns: seq * 1000,
        direction: 2, // SERVER_TO_CLIENT
        event: Some(recorded_event::Event::GrpcResponse(GrpcResponseEvent {
            response_body: vec![],
            status_code,
            status_message: String::new(),
            metadata: std::collections::HashMap::new(),
            trailers: std::collections::HashMap::new(),
            latency_ms: 10,
            stream_id,
        })),
        metadata: Default::default(),
    }
}

#[tokio::test]
async fn test_stream_scoped_replay_matches_correct_stream() {
    let temp_dir = tempfile::tempdir().unwrap();
    let events_file = temp_dir.path().join("events.jsonl");

    // Create a recording with interleaved gRPC streams on same connection
    // Stream 1: Request A, Response A
    // Stream 3: Request B, Response B
    // Stream 5: Request C, Response C
    let events = vec![
        // Stream 1 first
        create_grpc_request("trace-1", "trace:trace-1:conn:0:stream:1", 1, "MyService", "MethodA", 0),
        create_grpc_response("trace-1", "trace:trace-1:conn:0:stream:1", 1, 0, 1),
        // Stream 3 second
        create_grpc_request("trace-1", "trace:trace-1:conn:0:stream:3", 3, "MyService", "MethodB", 0),
        create_grpc_response("trace-1", "trace:trace-1:conn:0:stream:3", 3, 0, 1),
        // Stream 5 third
        create_grpc_request("trace-1", "trace:trace-1:conn:0:stream:5", 5, "MyService", "MethodC", 0),
        create_grpc_response("trace-1", "trace:trace-1:conn:0:stream:5", 5, 0, 1),
    ];

    {
        use tokio::io::AsyncWriteExt;
        let mut file = fs::File::create(&events_file).await.unwrap();
        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            file.write_all(json.as_bytes()).await.unwrap();
            file.write_all(b"\n").await.unwrap();
        }
    }

    let mut engine = ReplayEngine::new(events_file).await.unwrap();

    // Replaying stream 3 should match stream 3's request, not stream 1 or 5
    let stream3_incoming = create_grpc_request("trace-1", "trace:trace-1:conn:0:stream:3", 3, "MyService", "MethodB", 999);

    let (matched, response_events, _response_bytes) =
        engine.find_match_with_responses(&stream3_incoming).unwrap();

    // Verify matched event is from stream 3
    if let Some(recorded_event::Event::GrpcRequest(req)) = &matched.event {
        assert_eq!(req.stream_id, 3, "Should match stream 3, not another stream");
        assert_eq!(req.method, "MethodB");
    } else {
        panic!("Expected gRPC request");
    }
    assert_eq!(response_events.len(), 1);

    // Verify that stream 1 still works after stream 3 match
    let stream1_incoming = create_grpc_request("trace-1", "trace:trace-1:conn:0:stream:1", 1, "MyService", "MethodA", 999);
    let (matched1, _, _) = engine.find_match_with_responses(&stream1_incoming).unwrap();
    if let Some(recorded_event::Event::GrpcRequest(req)) = &matched1.event {
        assert_eq!(req.stream_id, 1, "Should match stream 1");
    } else {
        panic!("Expected gRPC request");
    }
}

#[tokio::test]
async fn test_replay_rejects_orphan_stream_without_scope_binding() {
    let temp_dir = tempfile::tempdir().unwrap();
    let events_file = temp_dir.path().join("events.jsonl");

    // Create a recording with proper stream scope
    let events = vec![
        create_grpc_request("trace-1", "trace:trace-1:conn:0:stream:1", 1, "MyService", "MethodA", 0),
        create_grpc_response("trace-1", "trace:trace-1:conn:0:stream:1", 1, 0, 1),
    ];

    {
        use tokio::io::AsyncWriteExt;
        let mut file = fs::File::create(&events_file).await.unwrap();
        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            file.write_all(json.as_bytes()).await.unwrap();
            file.write_all(b"\n").await.unwrap();
        }
    }

    let mut engine = ReplayEngine::new(events_file).await.unwrap();

    // Try to replay with orphan trace_id (no scope binding)
    let orphan_request = create_grpc_request("orphan", "trace:orphan:conn:0:stream:1", 1, "MyService", "MethodA", 0);

    // Should fail with UnresolvedAttribution error (fail-closed)
    let result = engine.find_match_with_responses_typed(&orphan_request);

    assert!(
        result.is_err(),
        "Replay should reject orphan/unresolved stream binding deterministically"
    );

    match result {
        Err(ReplayMatchError::UnresolvedAttribution { trace_id, scope_id }) => {
            assert_eq!(trace_id, "orphan");
            assert!(scope_id.contains("orphan"));
        }
        _ => panic!("Expected UnresolvedAttribution error"),
    }
}

#[tokio::test]
async fn test_replay_rejects_empty_scope_id() {
    let temp_dir = tempfile::tempdir().unwrap();
    let events_file = temp_dir.path().join("events.jsonl");

    let events = vec![
        create_grpc_request("trace-1", "trace:trace-1:conn:0:stream:1", 1, "MyService", "MethodA", 0),
        create_grpc_response("trace-1", "trace:trace-1:conn:0:stream:1", 1, 0, 1),
    ];

    {
        use tokio::io::AsyncWriteExt;
        let mut file = fs::File::create(&events_file).await.unwrap();
        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            file.write_all(json.as_bytes()).await.unwrap();
            file.write_all(b"\n").await.unwrap();
        }
    }

    let mut engine = ReplayEngine::new(events_file).await.unwrap();

    // Try to replay with empty scope_id
    let mut empty_scope_request = create_grpc_request("trace-1", "", 1, "MyService", "MethodA", 0);
    empty_scope_request.scope_id = String::new();

    let result = engine.find_match_with_responses_typed(&empty_scope_request);

    assert!(
        result.is_err(),
        "Replay should reject empty scope_id deterministically"
    );

    match result {
        Err(ReplayMatchError::UnresolvedAttribution { .. }) => {
            // Expected
        }
        _ => panic!("Expected UnresolvedAttribution error for empty scope_id"),
    }
}

#[tokio::test]
async fn test_concurrent_stream_replay_isolation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let events_file = temp_dir.path().join("events.jsonl");

    // Create recordings for two traces with same stream IDs (different connections)
    let events = vec![
        // Trace A: Stream 1
        create_grpc_request("trace-a", "trace:trace-a:conn:0:stream:1", 1, "Service", "Method", 0),
        create_grpc_response("trace-a", "trace:trace-a:conn:0:stream:1", 1, 0, 1),
        // Trace B: Stream 1 (same stream ID, different trace)
        create_grpc_request("trace-b", "trace:trace-b:conn:0:stream:1", 1, "Service", "Method", 0),
        create_grpc_response("trace-b", "trace:trace-b:conn:0:stream:1", 1, 0, 1),
    ];

    {
        use tokio::io::AsyncWriteExt;
        let mut file = fs::File::create(&events_file).await.unwrap();
        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            file.write_all(json.as_bytes()).await.unwrap();
            file.write_all(b"\n").await.unwrap();
        }
    }

    let mut engine = ReplayEngine::new(events_file).await.unwrap();

    // Replay trace-a stream 1
    let trace_a_stream1 = create_grpc_request("trace-a", "trace:trace-a:conn:0:stream:1", 1, "Service", "Method", 0);
    let (matched_a, _, _) = engine.find_match_with_responses(&trace_a_stream1).unwrap();
    if let Some(recorded_event::Event::GrpcRequest(req)) = &matched_a.event {
        assert_eq!(req.stream_id, 1);
    }

    // Replay trace-b stream 1 - should match trace-b's recording, not trace-a's
    let trace_b_stream1 = create_grpc_request("trace-b", "trace:trace-b:conn:0:stream:1", 1, "Service", "Method", 0);
    let (matched_b, _, _) = engine.find_match_with_responses(&trace_b_stream1).unwrap();
    if let Some(recorded_event::Event::GrpcRequest(req)) = &matched_b.event {
        assert_eq!(req.stream_id, 1);
    }

    // Verify trace_id in matched events
    assert_eq!(matched_a.trace_id, "trace-a");
    assert_eq!(matched_b.trace_id, "trace-b");
}

#[tokio::test]
async fn test_interleaved_stream_ordering_within_scope() {
    let temp_dir = tempfile::tempdir().unwrap();
    let events_file = temp_dir.path().join("events.jsonl");

    // Interleaved streams - events arrive in this order on wire:
    // Stream 1: Request (seq 0)
    // Stream 3: Request (seq 0)
    // Stream 5: Request (seq 0)
    // Stream 1: Response (seq 1)
    // Stream 3: Response (seq 1)
    // Stream 5: Response (seq 1)
    let events = vec![
        create_grpc_request("trace-1", "trace:trace-1:conn:0:stream:1", 1, "Service", "MethodA", 0),
        create_grpc_request("trace-1", "trace:trace-1:conn:0:stream:3", 3, "Service", "MethodB", 0),
        create_grpc_request("trace-1", "trace:trace-1:conn:0:stream:5", 5, "Service", "MethodC", 0),
        create_grpc_response("trace-1", "trace:trace-1:conn:0:stream:1", 1, 0, 1),
        create_grpc_response("trace-1", "trace:trace-1:conn:0:stream:3", 3, 0, 1),
        create_grpc_response("trace-1", "trace:trace-1:conn:0:stream:5", 5, 0, 1),
    ];

    {
        use tokio::io::AsyncWriteExt;
        let mut file = fs::File::create(&events_file).await.unwrap();
        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            file.write_all(json.as_bytes()).await.unwrap();
            file.write_all(b"\n").await.unwrap();
        }
    }

    let mut engine = ReplayEngine::new(events_file).await.unwrap();

    // Each stream should have independent sequence progression
    // Stream 1: request (seq 0), response (seq 1)
    let s1_req = create_grpc_request("trace-1", "trace:trace-1:conn:0:stream:1", 1, "Service", "MethodA", 0);
    let (matched, _, _) = engine.find_match_with_responses(&s1_req).unwrap();
    assert_eq!(matched.scope_sequence, 0);

    // Stream 5: request (seq 0) - should still be seq 0 even though other streams progressed
    let s5_req = create_grpc_request("trace-1", "trace:trace-1:conn:0:stream:5", 5, "Service", "MethodC", 0);
    let (matched5, _, _) = engine.find_match_with_responses(&s5_req).unwrap();
    assert_eq!(matched5.scope_sequence, 0);
    assert_eq!(matched5.scope_id, "trace:trace-1:conn:0:stream:5");
}
