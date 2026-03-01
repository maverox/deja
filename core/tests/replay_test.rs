use deja_core::events::{
    non_deterministic_event, recorded_event, NonDeterministicEvent, RecordedEvent,
};
use deja_core::replay::{ReplayEngine, ReplayMatchError};
use std::collections::HashMap;

// Mock function to creating a RecordedEvent
fn create_time_event(seq: u64, time: u64) -> RecordedEvent {
    RecordedEvent {
        trace_id: "test".to_string(),
        scope_id: "trace:test:conn:0".to_string(),
        scope_sequence: seq,
        global_sequence: 0,
        timestamp_ns: 0,
        direction: 0,
        event: Some(recorded_event::Event::NonDeterministic(
            NonDeterministicEvent {
                kind: Some(non_deterministic_event::Kind::TimeCaptureNs(time)),
            },
        )),
        metadata: Default::default(),
    }
}

fn create_http_event(seq: u64, path: &str) -> RecordedEvent {
    RecordedEvent {
        trace_id: "test".to_string(),
        scope_id: "trace:test:conn:0".to_string(),
        scope_sequence: seq,
        global_sequence: 0,
        timestamp_ns: 0,
        direction: 0,
        event: Some(recorded_event::Event::HttpRequest(
            deja_core::events::HttpRequestEvent {
                method: "GET".to_string(),
                path: path.to_string(),
                headers: Default::default(),
                body: vec![],
                schema: "http".to_string(),
                host: "localhost".to_string(),
            },
        )),
        metadata: Default::default(),
    }
}

#[tokio::test]
async fn test_time_travel_sequence() {
    // We can't easily mock the file system read in ReplayEngine::new without refactoring to a trait or mock struct.
    // However, ReplayEngine just takes a path. We can write a temp file.

    let temp_dir = std::env::temp_dir().join("deja_test_replay");
    let _ = tokio::fs::create_dir_all(&temp_dir).await;

    let event1 = create_time_event(0, 1000);
    let event2 = create_time_event(1, 2000);

    // Write to a jsonl file
    let file_path = temp_dir.join("recording.jsonl");
    let mut lines = String::new();
    lines.push_str(&serde_json::to_string(&event1).unwrap());
    lines.push('\n');
    lines.push_str(&serde_json::to_string(&event2).unwrap());
    lines.push('\n');

    tokio::fs::write(&file_path, lines).await.unwrap();

    // Test
    let mut engine = ReplayEngine::new(temp_dir.clone()).await.unwrap();

    // First call
    let t1 = engine.handle_time_request();
    assert_eq!(t1, Some(1000));

    // Second call
    let t2 = engine.handle_time_request();
    assert_eq!(t2, Some(2000));

    // Third call - exhausted
    let t3 = engine.handle_time_request();
    assert_eq!(t3, None);

    // Cleanup
    let _ = tokio::fs::remove_dir_all(temp_dir).await;
}

#[tokio::test]
async fn test_sequence_matching() {
    let temp_dir = std::env::temp_dir().join("deja_test_seq");
    let _ = tokio::fs::create_dir_all(&temp_dir).await;

    let e1 = create_http_event(0, "/a");
    let e2 = create_http_event(1, "/a"); // Same event, matches order
    let e3 = create_http_event(2, "/b");

    let file_path = temp_dir.join("rec.jsonl");
    let mut lines = String::new();
    lines.push_str(&serde_json::to_string(&e1).unwrap());
    lines.push('\n');
    lines.push_str(&serde_json::to_string(&e2).unwrap());
    lines.push('\n');
    lines.push_str(&serde_json::to_string(&e3).unwrap());
    lines.push('\n');

    tokio::fs::write(&file_path, lines).await.unwrap();

    let mut engine = ReplayEngine::new(temp_dir.clone()).await.unwrap();

    // Match /a (1st)
    let m1 = engine.find_match(&e1);
    assert!(m1.is_some());
    assert_eq!(m1.unwrap().scope_sequence, 0);

    // Match /a (2nd)
    let m2 = engine.find_match(&e1); // Incoming is same structure
    assert!(m2.is_some());
    assert_eq!(m2.unwrap().scope_sequence, 1);

    // Match /b (3rd)
    let m3 = engine.find_match(&e3);
    assert!(m3.is_some());
    assert_eq!(m3.unwrap().scope_sequence, 2);

    // Cleanup
    let _ = tokio::fs::remove_dir_all(temp_dir).await;
}

#[tokio::test]
async fn test_fuzzy_matching() {
    let temp_dir = std::env::temp_dir().join("deja_test_fuzzy");
    let _ = tokio::fs::create_dir_all(&temp_dir).await;

    // Record: SET key val EX 60
    let rec_event = RecordedEvent {
        trace_id: "t1".to_string(),
        scope_id: "trace:t1:conn:0".to_string(),
        scope_sequence: 0,
        global_sequence: 0,
        timestamp_ns: 0,
        direction: 0,
        event: Some(recorded_event::Event::RedisCommand(
            deja_core::events::RedisCommandEvent {
                command: "SET".to_string(),
                args: vec![
                    deja_core::events::RedisValue {
                        kind: Some(deja_core::events::redis_value::Kind::SimpleString(
                            "key".to_string(),
                        )),
                    },
                    deja_core::events::RedisValue {
                        kind: Some(deja_core::events::redis_value::Kind::SimpleString(
                            "val".to_string(),
                        )),
                    },
                    deja_core::events::RedisValue {
                        kind: Some(deja_core::events::redis_value::Kind::BulkString(
                            "EX".as_bytes().to_vec(),
                        )),
                    },
                    deja_core::events::RedisValue {
                        kind: Some(deja_core::events::redis_value::Kind::Integer(60)),
                    },
                ],
            },
        )),
        metadata: Default::default(),
    };

    let file_path = temp_dir.join("fuzzy.jsonl");
    let mut lines = String::new();
    lines.push_str(&serde_json::to_string(&rec_event).unwrap());
    lines.push('\n');
    tokio::fs::write(&file_path, lines).await.unwrap();

    let mut engine = ReplayEngine::new(temp_dir.clone()).await.unwrap();

    // Replay: SET key val EX 59 (Should match because diff is 1 <= 1)
    let incoming_success = RecordedEvent {
        trace_id: "t1".to_string(),
        scope_id: "trace:t1:conn:0".to_string(),
        scope_sequence: 0,
        global_sequence: 0,
        timestamp_ns: 0,
        direction: 0,
        event: Some(recorded_event::Event::RedisCommand(
            deja_core::events::RedisCommandEvent {
                command: "SET".to_string(),
                args: vec![
                    deja_core::events::RedisValue {
                        kind: Some(deja_core::events::redis_value::Kind::SimpleString(
                            "key".to_string(),
                        )),
                    },
                    deja_core::events::RedisValue {
                        kind: Some(deja_core::events::redis_value::Kind::SimpleString(
                            "val".to_string(),
                        )),
                    },
                    deja_core::events::RedisValue {
                        kind: Some(deja_core::events::redis_value::Kind::BulkString(
                            "EX".as_bytes().to_vec(),
                        )),
                    },
                    deja_core::events::RedisValue {
                        kind: Some(deja_core::events::redis_value::Kind::Integer(59)),
                    },
                ],
            },
        )),
        metadata: Default::default(),
    };

    assert!(engine.find_match(&incoming_success).is_some());

    // Reset engine (hacky, reload)
    let mut engine = ReplayEngine::new(temp_dir.clone()).await.unwrap();

    // Replay: SET key val EX 58 (Diff 2 > 1) -> No match
    let incoming_fail = RecordedEvent {
        trace_id: "t1".to_string(),
        scope_id: "trace:t1:conn:0".to_string(),
        scope_sequence: 0,
        global_sequence: 0,
        timestamp_ns: 0,
        direction: 0,
        event: Some(recorded_event::Event::RedisCommand(
            deja_core::events::RedisCommandEvent {
                command: "SET".to_string(),
                args: vec![
                    deja_core::events::RedisValue {
                        kind: Some(deja_core::events::redis_value::Kind::SimpleString(
                            "key".to_string(),
                        )),
                    },
                    deja_core::events::RedisValue {
                        kind: Some(deja_core::events::redis_value::Kind::SimpleString(
                            "val".to_string(),
                        )),
                    },
                    deja_core::events::RedisValue {
                        kind: Some(deja_core::events::redis_value::Kind::BulkString(
                            "EX".as_bytes().to_vec(),
                        )),
                    },
                    deja_core::events::RedisValue {
                        kind: Some(deja_core::events::redis_value::Kind::Integer(58)),
                    },
                ],
            },
        )),
        metadata: Default::default(),
    };

    assert!(engine.find_match(&incoming_fail).is_none());

    let _ = tokio::fs::remove_dir_all(temp_dir).await;
}

#[tokio::test]
async fn test_http_matching() {
    let temp_dir = std::env::temp_dir().join("deja_test_http");
    let _ = tokio::fs::create_dir_all(&temp_dir).await;

    let mut headers = HashMap::new();
    headers.insert("Content-Type".to_string(), "application/json".to_string());

    let rec_event = RecordedEvent {
        trace_id: "h1".to_string(),
        scope_id: "trace:h1:conn:0".to_string(),
        scope_sequence: 0,
        global_sequence: 0,
        timestamp_ns: 0,
        direction: 0,
        event: Some(recorded_event::Event::HttpRequest(
            deja_core::events::HttpRequestEvent {
                method: "POST".to_string(),
                path: "/api/v1/resource".to_string(),
                headers: headers.clone(),
                body: b"{\"foo\":\"bar\"}".to_vec(),
                schema: "http".to_string(),
                host: "localhost".to_string(),
            },
        )),
        metadata: Default::default(),
    };

    let file_path = temp_dir.join("http.jsonl");
    let mut lines = String::new();
    lines.push_str(&serde_json::to_string(&rec_event).unwrap());
    lines.push('\n');
    tokio::fs::write(&file_path, lines).await.unwrap();

    // 1. Exact Match
    let mut engine = ReplayEngine::new(temp_dir.clone()).await.unwrap();
    let exact_match = rec_event.clone();
    assert!(engine.find_match(&exact_match).is_some());

    // 2. Mismatch Body
    let mut engine = ReplayEngine::new(temp_dir.clone()).await.unwrap();
    let mut mismatch_body = rec_event.clone();
    if let Some(recorded_event::Event::HttpRequest(req)) = &mut mismatch_body.event {
        req.body = b"{\"foo\":\"baz\"}".to_vec();
    }
    assert!(engine.find_match(&mismatch_body).is_none());

    // 3. Mismatch Header (change Content-Type which IS in the compared set)
    let mut engine = ReplayEngine::new(temp_dir.clone()).await.unwrap();
    let mut mismatch_header = rec_event.clone();
    if let Some(recorded_event::Event::HttpRequest(req)) = &mut mismatch_header.event {
        req.headers
            .insert("Content-Type".to_string(), "text/plain".to_string());
    }
    assert!(engine.find_match(&mismatch_header).is_none());

    let _ = tokio::fs::remove_dir_all(temp_dir).await;
}

#[tokio::test]
async fn test_typed_error_for_unresolved_missing_trace_attribution() {
    let temp_dir = std::env::temp_dir().join("deja_test_typed_missing_trace");
    let _ = tokio::fs::create_dir_all(&temp_dir).await;

    let mut engine = ReplayEngine::new(temp_dir.clone()).await.unwrap();
    let mut incoming = create_http_event(0, "/typed-error");
    incoming.trace_id.clear();

    let result = engine.find_match_with_responses_typed(&incoming);
    assert!(matches!(
        result,
        Err(ReplayMatchError::UnresolvedAttribution { trace_id, scope_id })
            if trace_id.is_empty() && scope_id == "trace:test:conn:0"
    ));

    let _ = tokio::fs::remove_dir_all(temp_dir).await;
}

#[tokio::test]
async fn test_typed_error_for_unresolved_orphan_attribution() {
    let temp_dir = std::env::temp_dir().join("deja_test_typed_orphan_trace");
    let _ = tokio::fs::create_dir_all(&temp_dir).await;

    let mut engine = ReplayEngine::new(temp_dir.clone()).await.unwrap();
    let mut incoming = create_http_event(0, "/typed-error");
    incoming.trace_id = "orphan".to_string();
    incoming.scope_id = "orphan:42".to_string();

    let result = engine.find_match_with_responses_typed(&incoming);
    assert!(matches!(
        result,
        Err(ReplayMatchError::UnresolvedAttribution { trace_id, scope_id })
            if trace_id == "orphan" && scope_id == "orphan:42"
    ));

    let _ = tokio::fs::remove_dir_all(temp_dir).await;
}
