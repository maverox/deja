use deja_core::events::{
    pg_message_event, recorded_event, PgCommandComplete, PgDataRow, PgMessageEvent, PgQuery,
    RecordedEvent,
};
use deja_core::replay::ReplayEngine;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn test_postgres_replay_responses() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let file_path = temp_dir.path().join("trace_pg.jsonl");

    // 1. Query
    let evt1 = RecordedEvent {
        sequence: 1,
        event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
            message: Some(pg_message_event::Message::Query(PgQuery {
                query: "SELECT 1".to_string(),
            })),
        })),
        ..Default::default()
    };
    // 2. DataRow
    let evt2 = RecordedEvent {
        sequence: 2,
        event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
            message: Some(pg_message_event::Message::DataRow(PgDataRow {
                values: vec![vec![49]], // "1"
            })),
        })),
        ..Default::default()
    };
    // 3. CommandComplete
    let evt3 = RecordedEvent {
        sequence: 3,
        event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
            message: Some(pg_message_event::Message::CommandComplete(
                PgCommandComplete {
                    tag: "SELECT 1".to_string(),
                },
            )),
        })),
        ..Default::default()
    };

    // Write events
    let mut file = File::create(&file_path).await?;
    for evt in vec![evt1, evt2, evt3] {
        let json = serde_json::to_string(&evt)?;
        file.write_all(json.as_bytes()).await?;
        file.write_all(b"\n").await?;
    }

    // Load Engine
    let mut engine = ReplayEngine::new(temp_dir.path()).await?;

    // Match Query
    let incoming = RecordedEvent {
        event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
            message: Some(pg_message_event::Message::Query(PgQuery {
                query: "SELECT 1".to_string(),
            })),
        })),
        ..Default::default()
    };

    let result = engine.find_match_with_responses(&incoming);
    assert!(result.is_some(), "Should match query");

    let (_matched, responses) = result.unwrap();
    // Expect DataRow, CommandComplete, and synthesized ReadyForQuery = 3
    assert_eq!(responses.len(), 3, "Should have 3 response packets");

    // Verify content logic (byte matching is hard without parsing back, but length check is okay)
    // DataRow 'D' ...
    assert_eq!(responses[0][0], b'D');
    // CommandComplete 'C' ...
    assert_eq!(responses[1][0], b'C');
    // ReadyForQuery 'Z' ...
    assert_eq!(responses[2][0], b'Z');

    Ok(())
}
