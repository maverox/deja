use super::{ConnectionParser, ParseError, ParseResult, ProtocolParser};
use crate::events::{
    recorded_event, redis_value, RecordedEvent, RedisArray, RedisCommandEvent, RedisResponseEvent,
    RedisValue,
};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use redis_protocol::resp2::{decode::decode_bytes_mut, types::BytesFrame};
use std::str;

pub struct RedisParser;

#[async_trait]
impl ProtocolParser for RedisParser {
    fn protocol_id(&self) -> &'static str {
        "redis"
    }

    fn detect(&self, peek: &[u8]) -> f32 {
        if peek.is_empty() {
            return 0.0;
        }
        // RESP arrays start with '*' (commands are arrays of bulk strings)
        // Inline commands might check for "PING" etc.
        match peek[0] {
            b'*' => 0.9,
            b'+' | b'-' | b':' | b'$' | b'%' | b'~' | b'|' | b'#' | b'_' | b',' | b'(' | b'!'
            | b'=' => 0.8, // RESP types
            _ => {
                // HELLO?
                if peek.len() >= 5 && (peek[..5].eq_ignore_ascii_case(b"HELLO")) {
                    0.9
                } else {
                    0.0
                }
            }
        }
    }

    fn new_connection(&self, connection_id: String) -> Box<dyn ConnectionParser> {
        Box::new(RedisConnectionParser::new(connection_id))
    }
}

pub struct RedisConnectionParser {
    client_buf: BytesMut,
    server_buf: BytesMut,
    sequence: u64,
    connection_id: String,
}

impl RedisConnectionParser {
    pub fn new(connection_id: String) -> Self {
        Self {
            client_buf: BytesMut::new(),
            server_buf: BytesMut::new(),
            sequence: 0,
            connection_id,
        }
    }

    // Helper to convert BytesFrame to RedisValue
    fn frame_to_redis_value(frame: BytesFrame) -> RedisValue {
        use crate::events::redis_value::Kind;
        use redis_protocol::resp2::types::BytesFrame::*;

        let kind = match frame {
            SimpleString(b) => Some(Kind::SimpleString(String::from_utf8_lossy(&b).to_string())),
            Error(b) => Some(Kind::Error((*b).to_string())),
            Integer(i) => Some(Kind::Integer(i)),
            BulkString(b) => Some(Kind::BulkString(b.to_vec())),
            Array(frames) => {
                let values = frames.into_iter().map(Self::frame_to_redis_value).collect();
                Some(Kind::Array(RedisArray { values }))
            }
            Null => Some(Kind::Null(true)),
        };
        RedisValue { kind }
    }
}

#[async_trait]
impl ConnectionParser for RedisConnectionParser {
    fn parse_client_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.client_buf.extend_from_slice(data);

        let mut events = Vec::new();
        // decode_bytes_mut modifies the buffer in place (advancing read pointer conceptually? or we need to slice?)
        // redis-protocol 6.0 `decode_bytes_mut` takes &mut BytesMut and returns Ok(Some((frame, len))) ?
        // Let's rely on the example pattern: it returns the frame and the amount of bytes consumed.

        loop {
            // We'll use a loop to decode as many frames as possible
            if self.client_buf.is_empty() {
                break;
            }

            match decode_bytes_mut(&mut self.client_buf) {
                Ok(Some((frame, _amt, _buf))) => {
                    // Frame decoded successfully
                    // Note: decode_bytes_mut in some versions REMOVES the bytes from the buffer.
                    // In others it might not. The example shows `amt` being returned.
                    // If it modifies client_buf, we don't need to split.
                    // Let's assume standard behavior: it consumes valid bytes from the start.
                    // Wait, if I'm not sure, I should check behavior.
                    // But assuming it consumes based on typical `tokio_util::codec` patterns.
                    // Actually, the example showed: `decode_bytes_mut(&mut bytes)`.

                    // Convert frame to event
                    let val = Self::frame_to_redis_value(frame);

                    // Verify if it is a command (Array of BulkStrings)
                    if let Some(redis_value::Kind::Array(arr)) = val.kind.clone() {
                        if !arr.values.is_empty() {
                            let cmd_name = if let Some(Some(redis_value::Kind::BulkString(bs))) =
                                arr.values.first().map(|v| &v.kind)
                            {
                                String::from_utf8_lossy(bs).to_string()
                            } else {
                                "UNKNOWN".to_string()
                            };

                            let args = arr.values.into_iter().skip(1).collect();
                            let event = RecordedEvent {
                                trace_id: uuid::Uuid::new_v4().to_string(), // TODO correlation
                                span_id: uuid::Uuid::new_v4().to_string(),
                                sequence: self.sequence,
                                timestamp_ns: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_nanos()
                                    as u64,
                                connection_id: self.connection_id.clone(),
                                event: Some(recorded_event::Event::RedisCommand(
                                    RedisCommandEvent {
                                        command: cmd_name,
                                        args,
                                    },
                                )),
                                ..Default::default()
                            };
                            events.push(event);
                            self.sequence += 1;
                        }
                    }

                    // Since decode_bytes_mut removes the bytes, we don't need to manually advance total_consumed
                    // relative to the original buffer if we passed `&mut self.client_buf`.
                    // BUT, `decode_bytes_mut` might not remove them?
                    // NOTE: In redis-protocol 6.0, `decode_bytes_mut` DOES advance the buffer.
                    // So we are good.
                }
                Ok(None) => {
                    // Incomplete frame
                    break;
                }
                Err(e) => {
                    // Error decoding?
                    return Err(ParseError::Protocol(format!("Redis decode error: {:?}", e)));
                }
            }
        }

        Ok(ParseResult {
            events,
            forward: Bytes::copy_from_slice(data),
            needs_more: false, // We always forward anyway
            reply: None,
        })
    }

    fn parse_server_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.server_buf.extend_from_slice(data);

        let mut events = Vec::new();

        loop {
            if self.server_buf.is_empty() {
                break;
            }

            match decode_bytes_mut(&mut self.server_buf) {
                Ok(Some((frame, _amt, _buf))) => {
                    let val = Self::frame_to_redis_value(frame);

                    let event = RecordedEvent {
                        trace_id: uuid::Uuid::new_v4().to_string(),
                        span_id: uuid::Uuid::new_v4().to_string(),
                        sequence: self.sequence,
                        timestamp_ns: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_nanos() as u64,
                        connection_id: self.connection_id.clone(),
                        event: Some(recorded_event::Event::RedisResponse(RedisResponseEvent {
                            value: Some(val),
                        })),
                        ..Default::default()
                    };
                    events.push(event);
                    self.sequence += 1;
                }
                Ok(None) => break,
                Err(e) => return Err(ParseError::Protocol(format!("Redis decode error: {:?}", e))),
            }
        }

        Ok(ParseResult {
            events,
            forward: Bytes::copy_from_slice(data),
            needs_more: false,
            reply: None,
        })
    }

    fn connection_id(&self) -> &str {
        &self.connection_id
    }

    fn reset(&mut self) {
        self.client_buf.clear();
        self.server_buf.clear();
    }

    fn set_mode(&mut self, _is_replay: bool) {}
}

#[derive(Debug)]
pub struct RedisSerializer;

impl RedisSerializer {
    pub fn serialize_message(event: &crate::events::recorded_event::Event) -> Option<Bytes> {
        match event {
            crate::events::recorded_event::Event::RedisResponse(resp) => {
                resp.value.as_ref().map(|val| Self::serialize_value(val))
            }
            _ => None,
        }
    }

    pub fn serialize_value(val: &RedisValue) -> Bytes {
        let mut buf = BytesMut::new();
        Self::encode_value(&mut buf, val);
        buf.freeze()
    }

    fn encode_value(buf: &mut BytesMut, val: &RedisValue) {
        use crate::events::redis_value::Kind;
        match &val.kind {
            Some(Kind::SimpleString(s)) => {
                buf.put_u8(b'+');
                buf.put_slice(s.as_bytes());
                buf.put_slice(b"\r\n");
            }
            Some(Kind::Error(e)) => {
                buf.put_u8(b'-');
                buf.put_slice(e.as_bytes());
                buf.put_slice(b"\r\n");
            }
            Some(Kind::Integer(i)) => {
                buf.put_u8(b':');
                buf.put_slice(i.to_string().as_bytes());
                buf.put_slice(b"\r\n");
            }
            Some(Kind::BulkString(b)) => {
                buf.put_u8(b'$');
                buf.put_slice(b.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(b);
                buf.put_slice(b"\r\n");
            }
            Some(Kind::Array(arr)) => {
                buf.put_u8(b'*');
                buf.put_slice(arr.values.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                for v in &arr.values {
                    Self::encode_value(buf, v);
                }
            }
            Some(Kind::Null(_)) => {
                buf.put_slice(b"$-1\r\n");
            }
            Some(Kind::NullResp3(_)) => {
                buf.put_slice(b"_\r\n");
            }
            Some(Kind::Boolean(b)) => {
                buf.put_u8(b'#');
                buf.put_u8(if *b { b't' } else { b'f' });
                buf.put_slice(b"\r\n");
            }
            Some(Kind::Double(d)) => {
                buf.put_u8(b',');
                buf.put_slice(d.to_string().as_bytes());
                buf.put_slice(b"\r\n");
            }
            Some(Kind::Map(m)) => {
                buf.put_u8(b'%');
                buf.put_slice(m.entries.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                for entry in &m.entries {
                    if let Some(key) = &entry.key {
                        Self::encode_value(buf, key);
                    }
                    if let Some(value) = &entry.value {
                        Self::encode_value(buf, value);
                    }
                }
            }
            Some(Kind::VerbatimString(b)) => {
                buf.put_u8(b'=');
                buf.put_slice(b.len().to_string().as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(b);
                buf.put_slice(b"\r\n");
            }
            None => {
                buf.put_slice(b"$-1\r\n");
            }
        }
    }
}
