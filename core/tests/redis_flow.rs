use deja_core::events::{recorded_event, redis_value};
use deja_core::protocols::redis::RedisParser;
use deja_core::protocols::{ConnectionParser, ProtocolParser};

#[test]
fn test_redis_parse_command() {
    let parser = RedisParser;
    let mut connection = parser.new_connection("test-conn".into());

    // *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
    let raw = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";

    let result = connection.parse_client_data(raw).expect("Parse failed");

    assert_eq!(result.events.len(), 1);

    if let Some(recorded_event::Event::RedisCommand(cmd)) = &result.events[0].event {
        assert_eq!(cmd.command, "SET");
        assert_eq!(cmd.args.len(), 2);

        // Arg 1: key
        if let Some(redis_value::Kind::BulkString(bs)) = &cmd.args[0].kind {
            assert_eq!(bs, b"key");
        } else {
            panic!("Expected BulkString for key");
        }

        // Arg 2: value
        if let Some(redis_value::Kind::BulkString(bs)) = &cmd.args[1].kind {
            assert_eq!(bs, b"value");
        } else {
            panic!("Expected BulkString for value");
        }
    } else {
        panic!("Expected RedisCommand event");
    }
}

#[test]
fn test_redis_detect() {
    let parser = RedisParser;
    assert!(parser.detect(b"*2\r\n") > 0.8);
    assert!(parser.detect(b"GET ") == 0.0); // Simple strings not usually client commands start
}

/// In-band correlation extraction (CLIENT SETNAME deja:<trace_id>) must NOT
/// populate trace_id on parsed events. Correlation is now exclusively out-of-band
/// via the control plane.
#[test]
fn test_redis_client_setname_does_not_set_trace_id() {
    let parser = RedisParser;
    let mut connection = parser.new_connection("test-conn".into());

    // CLIENT SETNAME deja:abc-123
    // *3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$12\r\ndeja:abc-123\r\n
    let raw = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$12\r\ndeja:abc-123\r\n";

    let result = connection.parse_client_data(raw).expect("Parse failed");

    // Command must still be parsed
    assert_eq!(result.events.len(), 1);
    if let Some(recorded_event::Event::RedisCommand(cmd)) = &result.events[0].event {
        assert_eq!(cmd.command, "CLIENT");
    } else {
        panic!("Expected RedisCommand event for CLIENT SETNAME");
    }

    // But trace_id must NOT be populated from the in-band marker
    assert!(
        result.events[0].trace_id.is_empty(),
        "CLIENT SETNAME deja:<id> must not drive correlation attribution; \
         trace_id should be empty but was: '{}'",
        result.events[0].trace_id
    );
}

/// Verify that a regular CLIENT SETNAME (without deja: prefix) also has empty trace_id.
#[test]
fn test_redis_client_setname_non_deja_has_empty_trace_id() {
    let parser = RedisParser;
    let mut connection = parser.new_connection("test-conn".into());

    // CLIENT SETNAME my-app
    let raw = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$6\r\nmy-app\r\n";

    let result = connection.parse_client_data(raw).expect("Parse failed");
    assert_eq!(result.events.len(), 1);
    assert!(
        result.events[0].trace_id.is_empty(),
        "Non-deja CLIENT SETNAME must have empty trace_id"
    );
}

/// Normal Redis commands (SET, GET) must remain unaffected.
#[test]
fn test_redis_normal_commands_unaffected_by_inband_removal() {
    let parser = RedisParser;
    let mut connection = parser.new_connection("test-conn".into());

    // SET key value followed by GET key
    let set_raw = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let get_raw = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";

    let r1 = connection
        .parse_client_data(set_raw)
        .expect("SET parse failed");
    assert_eq!(r1.events.len(), 1);
    if let Some(recorded_event::Event::RedisCommand(cmd)) = &r1.events[0].event {
        assert_eq!(cmd.command, "SET");
    }

    let r2 = connection
        .parse_client_data(get_raw)
        .expect("GET parse failed");
    assert_eq!(r2.events.len(), 1);
    if let Some(recorded_event::Event::RedisCommand(cmd)) = &r2.events[0].event {
        assert_eq!(cmd.command, "GET");
    }
}
