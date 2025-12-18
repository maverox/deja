use bytes::{BufMut, BytesMut};
use deja_core::events::{pg_message_event, recorded_event};
use deja_core::protocols::postgres::PostgresParser;
use deja_core::protocols::ProtocolParser;
#[test]
fn test_postgres_parser_flow() {
    let parser = PostgresParser;
    let mut connection = parser.new_connection("test-conn".into());

    // 1. Startup Packet
    // len: 4 bytes (len of packet including itself)
    // version: 196608 (3.0)
    // user\0test\0database\0testdb\0\0
    let mut startup = BytesMut::new();
    startup.put_u32(0); // placeholder for len
    startup.put_u32(196608);
    startup.put_slice(b"user\0test\0database\0testdb\0\0");
    let len = startup.len() as u32;
    // patch len
    let mut head = &mut startup[0..4];
    // Write BE u32 manually or use put if it were a fresh buffer.
    // BytesMut is weird about patching logic in place easily with put_u32 unless cursor.
    // Easier:
    startup[0] = (len >> 24) as u8;
    startup[1] = (len >> 16) as u8;
    startup[2] = (len >> 8) as u8;
    startup[3] = len as u8;

    let result = connection
        .parse_client_data(&startup)
        .expect("Startup parse failed");

    // Verify Startup Event
    assert_eq!(result.events.len(), 1);
    if let Some(recorded_event::Event::PgMessage(msg)) = &result.events[0].event {
        if let Some(pg_message_event::Message::Startup(s)) = &msg.message {
            assert_eq!(s.user, "test");
            assert_eq!(s.database, "testdb");
        } else {
            panic!("Expected Startup message");
        }
    }

    // 2. Query Packet
    // 'Q' + len + "SELECT 1\0"
    let mut query = BytesMut::new();
    query.put_u8(b'Q');
    query.put_u32(4 + 9); // 4 bytes for len + 9 bytes for string null term
    query.put_slice(b"SELECT 1\0");

    let result = connection
        .parse_client_data(&query)
        .expect("Query parse failed");

    // Verify Query Event
    assert_eq!(result.events.len(), 1);
    if let Some(recorded_event::Event::PgMessage(msg)) = &result.events[0].event {
        if let Some(pg_message_event::Message::Query(q)) = &msg.message {
            assert_eq!(q.query, "SELECT 1");
        } else {
            panic!("Expected Query message");
        }
    }
}
