use super::{ConnectionParser, ParseError, ParseResult, ProtocolParser};
use crate::events::{recorded_event, HttpRequestEvent, RecordedEvent};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;

pub struct HttpParser;

#[async_trait]
impl ProtocolParser for HttpParser {
    fn protocol_id(&self) -> &'static str {
        "http"
    }
    fn detect(&self, peek: &[u8]) -> f32 {
        if peek.len() < 4 {
            return 0.0;
        }
        // Simple heuristic for HTTP methods
        match &peek[0..4] {
            b"GET " | b"POST" | b"PUT " | b"HEAD" | b"DELE" => 0.9,
            _ => 0.0,
        }
    }

    fn new_connection(&self, connection_id: String) -> Box<dyn ConnectionParser> {
        Box::new(HttpConnectionParser::new(connection_id))
    }
}

pub struct HttpConnectionParser {
    client_buf: BytesMut,
    server_buf: BytesMut,
    connection_id: String,
}

impl HttpConnectionParser {
    pub fn new(connection_id: String) -> Self {
        Self {
            client_buf: BytesMut::new(),
            server_buf: BytesMut::new(),
            connection_id,
        }
    }
}

#[async_trait]
impl ConnectionParser for HttpConnectionParser {
    fn parse_client_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.client_buf.extend_from_slice(data);

        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut headers);

        let status = req
            .parse(&self.client_buf)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;

        match status {
            httparse::Status::Complete(len) => {
                // For simplicity in Phase 1, we assume no body or body is read later.
                // A real implementation needs proper state machine for body reading (Content-Length / Chunked).
                // Here we just grab the head.

                let method = req.method.unwrap_or("").to_string();
                let path = req.path.unwrap_or("").to_string();
                let mut header_map = HashMap::new();
                for h in req.headers.iter() {
                    header_map.insert(
                        h.name.to_string(),
                        String::from_utf8_lossy(h.value).to_string(),
                    );
                }

                let host = header_map
                    .get("Host")
                    .or_else(|| header_map.get("host"))
                    .cloned()
                    .unwrap_or_default();

                // Check for Content-Length
                let content_length = header_map
                    .get("Content-Length")
                    .or_else(|| header_map.get("content-length"))
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(0);

                // Consume the head
                // Status::Complete(len) gives us the length of the HEAD (request line + headers)
                if self.client_buf.len() < len + content_length {
                    // We have the head, but not the full body yet
                    return Ok(ParseResult {
                        events: vec![],
                        forward: Bytes::copy_from_slice(data),
                        needs_more: true,
                        reply: None,
                    });
                }

                let _head = self.client_buf.split_to(len);
                let body = self.client_buf.split_to(content_length).to_vec();
                // We do NOT clear the buffer here blindly, strictly split what we need.
                // Any remaining bytes are for the next request (pipeline).

                // Construct Protobuf Event
                let event = RecordedEvent {
                    trace_id: String::new(),
                    scope_id: String::new(),
                    scope_sequence: 0,
                    global_sequence: 0,
                    timestamp_ns: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64,
                    direction: 0,
                    event: Some(recorded_event::Event::HttpRequest(HttpRequestEvent {
                        method,
                        path,
                        headers: header_map,
                        body,
                        schema: "http".to_string(),
                        host, // Extracted from headers
                    })),
                    metadata: Default::default(),
                };

                Ok(ParseResult {
                    events: vec![event],
                    forward: Bytes::copy_from_slice(data), // We forward everything transparently
                    needs_more: false,
                    reply: None,
                })
            }
            httparse::Status::Partial => Ok(ParseResult {
                events: vec![],
                forward: Bytes::copy_from_slice(data),
                needs_more: true,
                reply: None,
            }),
        }
    }

    fn parse_server_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.server_buf.extend_from_slice(data);

        // Similar to client parsing but for Response
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut res = httparse::Response::new(&mut headers);

        let status = res
            .parse(&self.server_buf)
            .map_err(|e| ParseError::Invalid(e.to_string()))?;

        match status {
            httparse::Status::Complete(len) => {
                let status_code = res.code.unwrap_or(0) as u32;
                let mut header_map = HashMap::new();
                for h in res.headers.iter() {
                    header_map.insert(
                        h.name.to_string(),
                        String::from_utf8_lossy(h.value).to_string(),
                    );
                }

                // Consume buffer
                let _head = self.server_buf.split_to(len);
                // Simple body capture: take everything remaining in the buffer
                // Real implementation needs to respect Content-Length or Chunked encoding
                let body = self.server_buf.to_vec();
                self.server_buf.clear();

                use crate::events::HttpResponseEvent;

                let event = RecordedEvent {
                    trace_id: String::new(),
                    scope_id: String::new(),
                    scope_sequence: 0,
                    global_sequence: 0,
                    timestamp_ns: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64,
                    direction: 0,
                    event: Some(recorded_event::Event::HttpResponse(HttpResponseEvent {
                        status: status_code,
                        headers: header_map,
                        body,
                        latency_ms: 0,
                    })),
                    metadata: Default::default(),
                };

                Ok(ParseResult {
                    events: vec![event],
                    forward: Bytes::copy_from_slice(data),
                    needs_more: false,
                    reply: None,
                })
            }
            httparse::Status::Partial => Ok(ParseResult {
                events: vec![],
                forward: Bytes::copy_from_slice(data),
                needs_more: true,
                reply: None,
            }),
        }
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

pub struct HttpSerializer;

impl HttpSerializer {
    pub fn serialize_message(event: &crate::events::recorded_event::Event) -> Option<Bytes> {
        match event {
            crate::events::recorded_event::Event::HttpResponse(res) => {
                let mut buf = BytesMut::new();
                // Status Line
                buf.extend_from_slice(b"HTTP/1.1 ");
                buf.extend_from_slice(res.status.to_string().as_bytes());
                buf.extend_from_slice(b" OK\r\n"); // Simplified reason phrase

                // Headers
                for (k, v) in &res.headers {
                    buf.extend_from_slice(k.as_bytes());
                    buf.extend_from_slice(b": ");
                    buf.extend_from_slice(v.as_bytes());
                    buf.extend_from_slice(b"\r\n");
                }
                buf.extend_from_slice(b"\r\n");

                // Body
                buf.extend_from_slice(&res.body);

                Some(buf.freeze())
            }
            _ => None,
        }
    }
}
