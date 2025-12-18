use deja_core::diff::{calculate_diff, NoiseConfig};
use deja_core::events::{recorded_event::Event, HttpRequestEvent, RecordedEvent};
use serde_json::json;
use std::collections::HashMap;

#[test]
fn test_diff_json_bodies() {
    let body_a = json!({"user": {"id": 1, "name": "Alice"}, "timestamp": 12345});
    let body_b = json!({"user": {"id": 1, "name": "Bob"}, "timestamp": 67890});

    let event_a = RecordedEvent {
        trace_id: "t1".into(),
        event: Some(Event::HttpRequest(HttpRequestEvent {
            method: "POST".into(),
            path: "/users".into(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&body_a).unwrap().into(),
            ..Default::default()
        })),
        ..Default::default()
    };

    let event_b = RecordedEvent {
        trace_id: "t1".into(), // Same trace id for setup
        event: Some(Event::HttpRequest(HttpRequestEvent {
            method: "POST".into(),
            path: "/users".into(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&body_b).unwrap().into(),
            ..Default::default()
        })),
        ..Default::default()
    };

    // 1. Without noise config, should find 2 diffs (name and timestamp)
    let config = NoiseConfig::default();
    let mismatch = calculate_diff(&event_a, &event_b, &config).expect("Should have mismatches");

    // Debug print
    for d in &mismatch.diffs {
        println!("Diff: {:?}", d);
    }

    assert_eq!(
        mismatch.diffs.len(),
        2,
        "Expected 2 diffs (name and timestamp)"
    );

    // Verify paths
    let paths: Vec<&str> = mismatch.diffs.iter().map(|d| d.path.as_str()).collect();
    assert!(paths.contains(&"http_request.body.user.name"));
    assert!(paths.contains(&"http_request.body.timestamp"));

    // 2. With noise config, ignore timestamp
    let config = NoiseConfig {
        json_ignored_keys: vec!["timestamp".into()],
        ..Default::default()
    };
    let mismatch = calculate_diff(&event_a, &event_b, &config).expect("Should have mismatches");
    assert_eq!(
        mismatch.diffs.len(),
        1,
        "Expected 1 diff (name only, timestamp ignored)"
    );
    assert_eq!(mismatch.diffs[0].path, "http_request.body.user.name");
}

#[test]
fn test_diff_headers_noise() {
    let event_a = RecordedEvent {
        event: Some(Event::HttpRequest(HttpRequestEvent {
            headers: HashMap::from([
                ("content-type".into(), "application/json".into()),
                ("date".into(), "Tue, 15 Nov 1994 08:12:31 GMT".into()),
            ]),
            ..Default::default()
        })),
        ..Default::default()
    };

    let event_b = RecordedEvent {
        event: Some(Event::HttpRequest(HttpRequestEvent {
            headers: HashMap::from([
                ("content-type".into(), "application/json".into()),
                ("date".into(), "Wed, 16 Nov 1994 08:12:31 GMT".into()),
            ]),
            ..Default::default()
        })),
        ..Default::default()
    };

    let config = NoiseConfig {
        ignored_paths: vec!["http_request.headers.date".into()],
        ..Default::default()
    };

    let mismatch = calculate_diff(&event_a, &event_b, &config);
    assert!(mismatch.is_none(), "Should ignore date header diff");
}
