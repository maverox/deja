use super::{ConnectionParser, ParseError, ParseResult, ProtocolParser};
use crate::events::{
    recorded_event, redis_value, RecordedEvent, RedisArray, RedisCommandEvent, RedisResponseEvent,
    RedisValue,
};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
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

    // ... parse_resp helper is unchanged ...
    fn parse_resp(buf: &mut BytesMut) -> Result<Option<(RedisValue, usize)>, ParseError> {
        if buf.is_empty() {
            return Ok(None);
        }

        let type_byte = buf[0];
        // Ensure we have a line ending for simple types
        let line_end = match buf.windows(2).position(|w| w == b"\r\n") {
            Some(pos) => pos,
            None => {
                // Need more data (unless it's bulk string with length header incomplete)
                return Ok(None);
            }
        };

        match type_byte {
            b'+' => {
                let s = String::from_utf8_lossy(&buf[1..line_end]).to_string();
                Ok(Some((
                    RedisValue {
                        kind: Some(redis_value::Kind::SimpleString(s)),
                    },
                    line_end + 2,
                )))
            }
            b'-' => {
                let s = String::from_utf8_lossy(&buf[1..line_end]).to_string();
                Ok(Some((
                    RedisValue {
                        kind: Some(redis_value::Kind::Error(s)),
                    },
                    line_end + 2,
                )))
            }
            b':' => {
                let s = str::from_utf8(&buf[1..line_end])
                    .map_err(|_| ParseError::Protocol("Invalid integer".into()))?;
                let n = s
                    .parse::<i64>()
                    .map_err(|_| ParseError::Protocol("Invalid integer format".into()))?;
                Ok(Some((
                    RedisValue {
                        kind: Some(redis_value::Kind::Integer(n)),
                    },
                    line_end + 2,
                )))
            }
            b'$' => {
                // Bulk String: $6\r\nfoobar\r\n
                let len_str = str::from_utf8(&buf[1..line_end])
                    .map_err(|_| ParseError::Protocol("Invalid bulk length".into()))?;
                let len = len_str
                    .parse::<i64>()
                    .map_err(|_| ParseError::Protocol("Invalid bulk length format".into()))?;

                if len == -1 {
                    return Ok(Some((
                        RedisValue {
                            kind: Some(redis_value::Kind::Null(true)),
                        },
                        line_end + 2,
                    )));
                }

                let len = len as usize;
                let content_start = line_end + 2;
                if buf.len() < content_start + len + 2 {
                    return Ok(None);
                }

                let data = buf[content_start..content_start + len].to_vec();
                Ok(Some((
                    RedisValue {
                        kind: Some(redis_value::Kind::BulkString(data)),
                    },
                    content_start + len + 2,
                )))
            }
            b'*' => {
                // Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
                let count_str = str::from_utf8(&buf[1..line_end])
                    .map_err(|_| ParseError::Protocol("Invalid array count".into()))?;
                let count = count_str
                    .parse::<i64>()
                    .map_err(|_| ParseError::Protocol("Invalid array count format".into()))?;

                if count == -1 {
                    return Ok(Some((
                        RedisValue {
                            kind: Some(redis_value::Kind::Null(true)),
                        },
                        line_end + 2,
                    )));
                }

                let mut consumed = line_end + 2;
                let mut values = Vec::new();

                // We need to parse 'count' elements recursively
                // This implies we need a way to slice the buffer repeatedly WITHOUT consuming it yet
                // Or we consume carefully.
                // Better approach: Slice tracking.

                let start_offset = consumed;
                let mut temp_slice = BytesMut::from(&buf[start_offset..]);

                for _ in 0..count {
                    match Self::parse_resp(&mut temp_slice)? {
                        Some((val, bytes_read)) => {
                            values.push(val);
                            consumed += bytes_read;
                            // Advance temp_slice
                            let _ = temp_slice.split_to(bytes_read);
                        }
                        None => return Ok(None),
                    }
                }

                Ok(Some((
                    RedisValue {
                        kind: Some(redis_value::Kind::Array(RedisArray { values })),
                    },
                    consumed,
                )))
            }
            b'%' => {
                // Map: %2\r\n$1\r\na\r\n$1\r\nb\r\n
                let count_str = str::from_utf8(&buf[1..line_end])
                    .map_err(|_| ParseError::Protocol("Invalid map count".into()))?;
                let count = count_str
                    .parse::<i64>()
                    .map_err(|_| ParseError::Protocol("Invalid map count format".into()))?;

                let mut consumed = line_end + 2;
                let mut entries = Vec::new();

                let mut temp_slice = BytesMut::from(&buf[consumed..]);

                for _ in 0..count {
                    // Parse Key
                    let key = match Self::parse_resp(&mut temp_slice)? {
                        Some((val, bytes_read)) => {
                            consumed += bytes_read;
                            let _ = temp_slice.split_to(bytes_read);
                            val
                        }
                        None => return Ok(None),
                    };

                    // Parse Value
                    let value = match Self::parse_resp(&mut temp_slice)? {
                        Some((val, bytes_read)) => {
                            consumed += bytes_read;
                            let _ = temp_slice.split_to(bytes_read);
                            val
                        }
                        None => return Ok(None),
                    };

                    entries.push(crate::events::RedisKeyValuePair {
                        key: Some(key),
                        value: Some(value),
                    });
                }

                Ok(Some((
                    RedisValue {
                        kind: Some(redis_value::Kind::Map(crate::events::RedisMap { entries })),
                    },
                    consumed,
                )))
            }
            b'#' => {
                // Boolean: #t\r\n or #f\r\n
                let b = match buf[1] {
                    b't' => true,
                    b'f' => false,
                    _ => return Err(ParseError::Protocol("Invalid boolean".into())),
                };
                Ok(Some((
                    RedisValue {
                        kind: Some(redis_value::Kind::Boolean(b)),
                    },
                    line_end + 2,
                )))
            }
            b'_' => {
                // Null: _\r\n
                Ok(Some((
                    RedisValue {
                        kind: Some(redis_value::Kind::NullResp3(true)),
                    },
                    line_end + 2,
                )))
            }
            b',' => {
                // Double: ,1.23\r\n
                let s = str::from_utf8(&buf[1..line_end])
                    .map_err(|_| ParseError::Protocol("Invalid double".into()))?;
                let d = s
                    .parse::<f64>()
                    .map_err(|_| ParseError::Protocol("Invalid double format".into()))?;
                Ok(Some((
                    RedisValue {
                        kind: Some(redis_value::Kind::Double(d)),
                    },
                    line_end + 2,
                )))
            }
            b'=' => {
                // Verbatim String: =15\r\ntxt:some content\r\n
                let len_str = str::from_utf8(&buf[1..line_end])
                    .map_err(|_| ParseError::Protocol("Invalid verbatim length".into()))?;
                let len = len_str
                    .parse::<usize>()
                    .map_err(|_| ParseError::Protocol("Invalid verbatim length format".into()))?;

                let content_start = line_end + 2;
                if buf.len() < content_start + len + 2 {
                    return Ok(None);
                }

                let data = buf[content_start..content_start + len].to_vec();
                Ok(Some((
                    RedisValue {
                        kind: Some(redis_value::Kind::VerbatimString(data)),
                    },
                    content_start + len + 2,
                )))
            }
            _ => {
                // Potential Inline Command (e.g. "HELLO 3", "PING")
                // Format: space-separated strings, ending in \r\n
                // We treat the entire line as a single command for recording transparency,
                // or try to tokenize it into a RedisCommand structure.

                // 1. Find line end
                let line_end = match buf.windows(2).position(|w| w == b"\r\n") {
                    Some(pos) => pos,
                    None => return Ok(None),
                };

                let line = String::from_utf8_lossy(&buf[0..line_end]).to_string();

                // 2. Tokenize
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.is_empty() {
                    // Empty line? Consume
                    return Ok(Some((
                        RedisValue { kind: None }, // No-op
                        line_end + 2,
                    )));
                }

                let values: Vec<RedisValue> = parts
                    .into_iter()
                    .map(|s| RedisValue {
                        kind: Some(redis_value::Kind::BulkString(s.as_bytes().to_vec())),
                    })
                    .collect();

                Ok(Some((
                    RedisValue {
                        kind: Some(redis_value::Kind::Array(RedisArray { values })),
                    },
                    line_end + 2,
                )))
            }
        }
    }
}

#[async_trait]
impl ConnectionParser for RedisConnectionParser {
    fn parse_client_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.client_buf.extend_from_slice(data);

        // Loop to consume all full commands in buffer
        let mut events = Vec::new();
        let mut total_consumed = 0;

        loop {
            // We need to peek/clone because if parse fails (needs more data), we shouldn't consume.
            // My helper parse_resp splits.
            // Let's create a temporary view.
            let mut temp_buf = BytesMut::from(&self.client_buf[total_consumed..]);

            match Self::parse_resp(&mut temp_buf)? {
                Some((val, bytes_read)) => {
                    // Assert it's an Array (typical command)
                    // If it's a command, it's usually an array of bulk strings.
                    if let Some(redis_value::Kind::Array(arr)) = val.kind.clone() {
                        if !arr.values.is_empty() {
                            // Extract command name
                            let cmd_name = if let Some(Some(redis_value::Kind::BulkString(bs))) =
                                arr.values.get(0).map(|v| &v.kind)
                            {
                                String::from_utf8_lossy(bs).to_string()
                            } else {
                                "UNKNOWN".to_string()
                            };

                            let args = arr.values.into_iter().skip(1).collect();

                            let event = RecordedEvent {
                                trace_id: uuid::Uuid::new_v4().to_string(), // TODO
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
                    total_consumed += bytes_read;
                }
                None => break,
            }
        }

        let _ = self.client_buf.split_to(total_consumed);

        Ok(ParseResult {
            events,
            forward: Bytes::copy_from_slice(data),
            needs_more: false,
            reply: None,
        })
    }

    fn parse_server_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.server_buf.extend_from_slice(data);

        let mut events = Vec::new();
        let mut total_consumed = 0;

        loop {
            let mut temp_buf = BytesMut::from(&self.server_buf[total_consumed..]);
            match Self::parse_resp(&mut temp_buf)? {
                Some((val, bytes_read)) => {
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
                    total_consumed += bytes_read;
                }
                None => break,
            }
        }

        let _ = self.server_buf.split_to(total_consumed);

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
}

#[derive(Debug)]
pub struct RedisSerializer;

impl RedisSerializer {
    pub fn serialize_message(event: &crate::events::recorded_event::Event) -> Option<Bytes> {
        match event {
            crate::events::recorded_event::Event::RedisResponse(resp) => {
                if let Some(val) = &resp.value {
                    Some(Self::serialize_value(val))
                } else {
                    None
                }
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
