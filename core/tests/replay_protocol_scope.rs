use deja_core::events::{recorded_event, HttpRequestEvent, HttpResponseEvent, RecordedEvent};
use deja_core::replay::ReplayEngine;
use tokio::fs;

// Helper to create events (copied from recording_index tests)
fn create_http_request(trace_id: &str, path: &str, seq: u64) -> RecordedEvent {
    RecordedEvent {
        trace_id: trace_id.to_string(),
        span_id: "span".to_string(),
        parent_span_id: None,
        sequence: 100 + seq, // Global sequence
        timestamp_ns: seq * 1000,
        connection_id: "conn-1".to_string(),
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
        span_id: "span".to_string(),
        parent_span_id: None,
        sequence: 100 + seq,
        timestamp_ns: seq * 1000,
        connection_id: "conn-1".to_string(),
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
