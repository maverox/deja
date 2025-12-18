use deja_core::events::RecordedEvent;
use deja_core::protocols::http::HttpParser;
use deja_core::protocols::ProtocolParser;
use deja_core::recording::Recorder;
use tokio::fs;

#[tokio::test]
async fn test_http_parse_and_record() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup Recorder
    let temp_dir = tempfile::tempdir()?;
    let recorder = Recorder::new(temp_dir.path()).await;

    // 2. Setup Parser
    let parser = HttpParser;
    let mut connection = parser.new_connection("test-conn".into());

    // 3. Simulate incoming HTTP Request
    let raw_request =
        b"GET /api/v1/users HTTP/1.1\r\nHost: example.com\r\nUser-Agent: DejaTest\r\n\r\n";

    // 4. Parse
    let result = connection.parse_client_data(raw_request)?;

    assert!(!result.events.is_empty(), "Should have generated an event");
    assert_eq!(result.events.len(), 1);

    let event = &result.events[0];

    // Verify event details
    if let Some(deja_core::events::recorded_event::Event::HttpRequest(req)) = &event.event {
        assert_eq!(req.method, "GET");
        assert_eq!(req.path, "/api/v1/users");
        assert_eq!(
            req.headers.get("Host").map(|s| s.as_str()),
            Some("example.com")
        );
    } else {
        panic!("Expected HttpRequest event");
    }

    // 5. Record
    recorder.save_event(event).await?;

    // 6. Verify Disk Storage
    let mut dir = fs::read_dir(temp_dir.path()).await?;
    let entry = dir.next_entry().await?.expect("Should have one file");

    let content = fs::read_to_string(entry.path()).await?;
    println!("Recorded content: {}", content);

    let saved_event: RecordedEvent = serde_json::from_str(&content)?;
    assert_eq!(saved_event.trace_id, event.trace_id);

    Ok(())
}
