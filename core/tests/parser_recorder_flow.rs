use deja_core::events::RecordedEvent;
use deja_core::protocols::http::HttpParser;
use deja_core::protocols::ProtocolParser;
use deja_core::recording::Recorder;
use std::io::Read;

#[tokio::test]
async fn test_http_parse_and_record() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup Recorder
    let temp_dir = tempfile::tempdir()?;
    let recorder = Recorder::new(temp_dir.path()).await;
    let session_id = recorder.get_session_id().to_string();

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
    // Events are stored in binary format at sessions/{session_id}/events.bin
    let events_path = temp_dir.path()
        .join("sessions")
        .join(&session_id)
        .join("events.bin");

    assert!(events_path.exists(), "Events file should exist");

    // Read the binary file (format: [length:u32][event_bytes])
    let mut file = std::fs::File::open(&events_path)?;
    let mut length_bytes = [0u8; 4];
    file.read_exact(&mut length_bytes)?;
    let length = u32::from_be_bytes(length_bytes) as usize;

    let mut event_bytes = vec![0u8; length];
    file.read_exact(&mut event_bytes)?;

    let saved_event: RecordedEvent = bincode::deserialize(&event_bytes)?;
    assert_eq!(saved_event.trace_id, event.trace_id);

    // Note: index.bin is no longer created at record time — RecordingIndex
    // is built in-memory from events during replay.

    Ok(())
}
