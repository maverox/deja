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

/// SET application_name = 'deja:<trace_id>' must NOT populate trace_id on
/// parsed events. Correlation is exclusively out-of-band via the control plane.
#[test]
fn test_pg_set_application_name_does_not_set_trace_id() {
    let parser = PostgresParser;
    let mut connection = parser.new_connection("test-conn".into());

    // Startup packet first
    let mut startup = BytesMut::new();
    startup.put_u32(0);
    startup.put_u32(196608);
    startup.put_slice(b"user\0test\0database\0testdb\0\0");
    let len = startup.len() as u32;
    startup[0] = (len >> 24) as u8;
    startup[1] = (len >> 16) as u8;
    startup[2] = (len >> 8) as u8;
    startup[3] = len as u8;
    let _ = connection.parse_client_data(&startup).unwrap();

    // Query: SET application_name = 'deja:trace-xyz-789'
    let query_str = "SET application_name = 'deja:trace-xyz-789'";
    let mut query = BytesMut::new();
    query.put_u8(b'Q');
    query.put_u32((4 + query_str.len() + 1) as u32);
    query.put_slice(query_str.as_bytes());
    query.put_u8(0);

    let result = connection.parse_client_data(&query).expect("Parse failed");

    assert_eq!(result.events.len(), 1);
    if let Some(recorded_event::Event::PgMessage(msg)) = &result.events[0].event {
        if let Some(pg_message_event::Message::Query(q)) = &msg.message {
            assert_eq!(q.query, query_str);
        } else {
            panic!("Expected Query message");
        }
    }

    assert!(
        result.events[0].trace_id.is_empty(),
        "SET application_name = 'deja:<id>' must not drive correlation attribution; \
         trace_id should be empty but was: '{}'",
        result.events[0].trace_id
    );
}

/// SET LOCAL application_name also must not drive attribution.
#[test]
fn test_pg_set_local_application_name_does_not_set_trace_id() {
    let parser = PostgresParser;
    let mut connection = parser.new_connection("test-conn".into());

    let mut startup = BytesMut::new();
    startup.put_u32(0);
    startup.put_u32(196608);
    startup.put_slice(b"user\0test\0database\0testdb\0\0");
    let len = startup.len() as u32;
    startup[0] = (len >> 24) as u8;
    startup[1] = (len >> 16) as u8;
    startup[2] = (len >> 8) as u8;
    startup[3] = len as u8;
    let _ = connection.parse_client_data(&startup).unwrap();

    let query_str = "SET LOCAL application_name = 'deja:local-trace-456'";
    let mut query = BytesMut::new();
    query.put_u8(b'Q');
    query.put_u32((4 + query_str.len() + 1) as u32);
    query.put_slice(query_str.as_bytes());
    query.put_u8(0);

    let result = connection.parse_client_data(&query).expect("Parse failed");

    assert_eq!(result.events.len(), 1);
    assert!(
        result.events[0].trace_id.is_empty(),
        "SET LOCAL application_name = 'deja:<id>' must not drive correlation attribution; \
         trace_id should be empty but was: '{}'",
        result.events[0].trace_id
    );
}

/// Parse message (extended query) with SET application_name must also not
/// drive attribution.
#[test]
fn test_pg_parse_message_set_application_name_does_not_set_trace_id() {
    let parser = PostgresParser;
    let mut connection = parser.new_connection("test-conn".into());

    let mut startup = BytesMut::new();
    startup.put_u32(0);
    startup.put_u32(196608);
    startup.put_slice(b"user\0test\0database\0testdb\0\0");
    let len = startup.len() as u32;
    startup[0] = (len >> 24) as u8;
    startup[1] = (len >> 16) as u8;
    startup[2] = (len >> 8) as u8;
    startup[3] = len as u8;
    let _ = connection.parse_client_data(&startup).unwrap();

    // Parse message: 'P' tag
    let stmt_name = "";
    let query_str = "SET application_name = 'deja:parse-trace-001'";
    let mut msg = BytesMut::new();
    msg.put_u8(b'P');
    let body_len = 4 + stmt_name.len() + 1 + query_str.len() + 1 + 2; // len field + name + query + num_params
    msg.put_u32(body_len as u32);
    msg.put_slice(stmt_name.as_bytes());
    msg.put_u8(0);
    msg.put_slice(query_str.as_bytes());
    msg.put_u8(0);
    msg.put_u16(0); // no param types

    let result = connection.parse_client_data(&msg).expect("Parse failed");

    assert_eq!(result.events.len(), 1);
    assert!(
        result.events[0].trace_id.is_empty(),
        "Parse message with SET application_name = 'deja:<id>' must not drive attribution; \
         trace_id should be empty but was: '{}'",
        result.events[0].trace_id
    );
}

/// Normal queries (SELECT, INSERT) must remain unaffected.
#[test]
fn test_pg_normal_queries_unaffected_by_inband_removal() {
    let parser = PostgresParser;
    let mut connection = parser.new_connection("test-conn".into());

    let mut startup = BytesMut::new();
    startup.put_u32(0);
    startup.put_u32(196608);
    startup.put_slice(b"user\0test\0database\0testdb\0\0");
    let len = startup.len() as u32;
    startup[0] = (len >> 24) as u8;
    startup[1] = (len >> 16) as u8;
    startup[2] = (len >> 8) as u8;
    startup[3] = len as u8;
    let _ = connection.parse_client_data(&startup).unwrap();

    let query_str = "INSERT INTO users (name) VALUES ('alice')";
    let mut query = BytesMut::new();
    query.put_u8(b'Q');
    query.put_u32((4 + query_str.len() + 1) as u32);
    query.put_slice(query_str.as_bytes());
    query.put_u8(0);

    let result = connection.parse_client_data(&query).expect("Parse failed");
    assert_eq!(result.events.len(), 1);
    if let Some(recorded_event::Event::PgMessage(msg)) = &result.events[0].event {
        if let Some(pg_message_event::Message::Query(q)) = &msg.message {
            assert_eq!(q.query, query_str);
        } else {
            panic!("Expected Query message");
        }
    }
}
