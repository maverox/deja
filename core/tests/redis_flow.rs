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
