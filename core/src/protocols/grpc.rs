//! gRPC protocol parser for Deja proxy
//!
//! gRPC uses HTTP/2 as transport with Protocol Buffers for serialization.
//! This module provides parsing and serialization for gRPC traffic.
//!
//! # HTTP/2 Detection
//! gRPC connections start with the HTTP/2 connection preface:
//! `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n`
//!
//! # gRPC Path Format
//! gRPC uses the path format: `/{service}/{method}`
//! Example: `/grpc.health.v1.Health/Check`

pub mod framing;
pub mod serializer;

use super::{ConnectionParser, ParseError, ParseResult, ProtocolParser};
use crate::events::{
    recorded_event, GrpcCallType, GrpcRequestEvent, GrpcResponseEvent, RecordedEvent,
};
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use framing::{parse_grpc_path, GrpcFrame, GRPC_HEADER_SIZE};
use std::collections::HashMap;

/// HTTP/2 connection preface (client sends this first)
const HTTP2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const HTTP2_PREFACE_LEN: usize = 24;

/// gRPC content type header value (for future use in content-type validation)
#[allow(dead_code)]
const GRPC_CONTENT_TYPE: &str = "application/grpc";

/// Protocol parser factory for gRPC connections
pub struct GrpcParser;

#[async_trait]
impl ProtocolParser for GrpcParser {
    fn protocol_id(&self) -> &'static str {
        "grpc"
    }

    fn detect(&self, peek: &[u8]) -> f32 {
        // Check for HTTP/2 preface
        if peek.len() >= HTTP2_PREFACE_LEN && &peek[..HTTP2_PREFACE_LEN] == HTTP2_PREFACE {
            return 0.95;
        }

        // Check for HTTP/2 frame format (for mid-connection detection)
        // HTTP/2 frames start with: length (3 bytes) + type (1 byte) + flags (1 byte) + stream_id (4 bytes)
        if peek.len() >= 9 {
            let frame_length =
                ((peek[0] as u32) << 16) | ((peek[1] as u32) << 8) | (peek[2] as u32);
            let frame_type = peek[3];
            let _stream_id = u32::from_be_bytes([peek[5] & 0x7F, peek[6], peek[7], peek[8]]);

            // Valid HTTP/2 frame types: 0-9
            // SETTINGS (0x04) is commonly sent early
            if frame_type <= 9 && frame_length < 16777216 {
                // Check for SETTINGS frame (common in gRPC startup)
                if frame_type == 0x04 {
                    return 0.9;
                }
                // Other valid frame types get lower confidence
                return 0.7;
            }
        }

        0.0
    }

    fn new_connection(&self, connection_id: String) -> Box<dyn ConnectionParser> {
        Box::new(GrpcConnectionParser::new(connection_id))
    }

    fn on_replay_init(&self) -> Option<Bytes> {
        // Send a basic HTTP/2 SETTINGS frame
        // length=0, type=0x04 (SETTINGS), flags=0, stream_id=0
        Some(Bytes::from_static(&[
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]))
    }
}

/// HTTP/2 frame types
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum H2FrameType {
    Data = 0x0,
    Headers = 0x1,
    Priority = 0x2,
    RstStream = 0x3,
    Settings = 0x4,
    PushPromise = 0x5,
    Ping = 0x6,
    GoAway = 0x7,
    WindowUpdate = 0x8,
    Continuation = 0x9,
    Unknown = 0xFF,
}

impl From<u8> for H2FrameType {
    fn from(v: u8) -> Self {
        match v {
            0x0 => Self::Data,
            0x1 => Self::Headers,
            0x2 => Self::Priority,
            0x3 => Self::RstStream,
            0x4 => Self::Settings,
            0x5 => Self::PushPromise,
            0x6 => Self::Ping,
            0x7 => Self::GoAway,
            0x8 => Self::WindowUpdate,
            0x9 => Self::Continuation,
            _ => Self::Unknown,
        }
    }
}

/// HTTP/2 frame header (9 bytes)
#[derive(Debug, Clone)]
struct H2FrameHeader {
    length: u32,
    frame_type: H2FrameType,
    flags: u8,
    stream_id: u32,
}

impl H2FrameHeader {
    fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 9 {
            return None;
        }

        let length = ((data[0] as u32) << 16) | ((data[1] as u32) << 8) | (data[2] as u32);
        let frame_type = H2FrameType::from(data[3]);
        let flags = data[4];
        // Stream ID uses only 31 bits (MSB is reserved)
        let stream_id = u32::from_be_bytes([data[5] & 0x7F, data[6], data[7], data[8]]);

        Some(Self {
            length,
            frame_type,
            flags,
            stream_id,
        })
    }
}

/// State for a single gRPC stream (HTTP/2 stream)
#[derive(Debug, Clone, Default)]
struct StreamState {
    /// gRPC service name
    service: String,
    /// gRPC method name
    method: String,
    /// Request metadata (headers)
    request_metadata: HashMap<String, String>,
    /// Accumulated request body
    request_body: BytesMut,
    /// Accumulated response body
    response_body: BytesMut,
    /// Response metadata (headers)
    response_metadata: HashMap<String, String>,
    /// Response trailers
    response_trailers: HashMap<String, String>,
    /// Request start time (for latency calculation)
    start_time_ns: u64,
    /// Whether we've emitted the request event
    request_emitted: bool,
    /// Trace ID extracted from request headers (for correlation)
    trace_id: Option<String>,
}

/// Connection-level parser for gRPC (HTTP/2)
pub struct GrpcConnectionParser {
    connection_id: String,
    client_buf: BytesMut,
    server_buf: BytesMut,
    /// Stream states indexed by HTTP/2 stream ID
    streams: HashMap<u32, StreamState>,
    /// Whether we've seen the HTTP/2 preface
    preface_seen: bool,
    /// Pending events to emit
    pending_events: Vec<RecordedEvent>,
    /// HPACK decoders for both directions
    client_decoder: hpack::Decoder<'static>,
    server_decoder: hpack::Decoder<'static>,
    /// Replies to send back to client (e.g. SETTINGS ACK)
    pending_replies: Vec<Bytes>,
    /// Whether the connection is in replay mode
    is_replay: bool,
}

impl GrpcConnectionParser {
    pub fn new(connection_id: String) -> Self {
        Self {
            connection_id,
            client_buf: BytesMut::new(),
            server_buf: BytesMut::new(),
            streams: HashMap::new(),
            preface_seen: false,
            pending_events: Vec::new(),
            client_decoder: hpack::Decoder::new(),
            server_decoder: hpack::Decoder::new(),
            pending_replies: Vec::new(),
            is_replay: false,
        }
    }

    /// Parse HTTP/2 frames from client data
    fn parse_client_frames(&mut self) -> Result<(), ParseError> {
        // Handle HTTP/2 preface first
        if !self.preface_seen {
            if self.client_buf.len() >= HTTP2_PREFACE_LEN {
                if &self.client_buf[..HTTP2_PREFACE_LEN] == HTTP2_PREFACE {
                    self.client_buf.advance(HTTP2_PREFACE_LEN);
                    self.preface_seen = true;
                } else {
                    // Not HTTP/2, this might be a different protocol
                    return Err(ParseError::Invalid("Invalid HTTP/2 preface".to_string()));
                }
            } else {
                return Ok(()); // Need more data
            }
        }

        // Parse HTTP/2 frames
        while self.client_buf.len() >= 9 {
            let header = match H2FrameHeader::parse(&self.client_buf) {
                Some(h) => h,
                None => break,
            };

            let frame_len = 9 + header.length as usize;
            if self.client_buf.len() < frame_len {
                break; // Need more data
            }

            // Extract frame payload
            let frame_data = self.client_buf.split_to(frame_len);
            let payload = &frame_data[9..];

            self.handle_client_frame(&header, payload)?;
        }

        Ok(())
    }

    /// Handle a single HTTP/2 frame from the client
    fn handle_client_frame(
        &mut self,
        header: &H2FrameHeader,
        payload: &[u8],
    ) -> Result<(), ParseError> {
        match header.frame_type {
            H2FrameType::Headers => {
                self.handle_client_headers(header.stream_id, header.flags, payload)?;
            }
            H2FrameType::Data => {
                self.handle_client_data(header.stream_id, header.flags, payload)?;
            }
            H2FrameType::Settings => {
                // If this is not an ACK, we should reply with a SETTINGS ACK
                let is_ack = (header.flags & 0x01) != 0;
                if !is_ack && self.is_replay {
                    // Send SETTINGS ACK
                    let mut ack = BytesMut::with_capacity(9);
                    ack.put_u32(0); // length=0 (3 bytes) + type=0x04 (1 byte)
                    ack[3] = 0x04;
                    ack.put_u8(0x01); // flags=ACK
                    ack.put_u32(0); // stream_id=0

                    self.pending_replies.push(ack.freeze());
                }
            }
            H2FrameType::Ping => {
                if self.is_replay {
                    // Return PING ACK with same payload
                    let mut ack = BytesMut::with_capacity(9 + payload.len());
                    let len = payload.len() as u32;
                    ack.put_u8((len >> 16) as u8);
                    ack.put_u8((len >> 8) as u8);
                    ack.put_u8(len as u8);
                    ack.put_u8(0x06); // type=PING
                    ack.put_u8(0x01); // flags=ACK
                    ack.put_u32(0); // stream_id=0
                    ack.extend_from_slice(payload);

                    self.pending_replies.push(ack.freeze());
                }
            }
            _ => {
                // Other frames: Priority, RstStream, WindowUpdate, etc.
            }
        }
        Ok(())
    }

    /// Handle HEADERS frame from client (request headers)
    fn handle_client_headers(
        &mut self,
        stream_id: u32,
        flags: u8,
        payload: &[u8],
    ) -> Result<(), ParseError> {
        let state = self.streams.entry(stream_id).or_default();
        state.start_time_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Parse HPACK-encoded headers
        if let Ok(headers) = self.client_decoder.decode(payload) {
            for (name, value) in headers {
                let name_str = String::from_utf8_lossy(&name).to_string();
                let value_str = String::from_utf8_lossy(&value).to_string();
                state.request_metadata.insert(name_str, value_str);
            }
        }

        // Extract service and method from :path
        if let Some(path) = state.request_metadata.get(":path") {
            if let Some((service, method)) = parse_grpc_path(path) {
                state.service = service;
                state.method = method;
            }
        }

        // Extract trace ID from headers — priority: traceparent > x-trace-id > x-request-id > x-b3-traceid
        if state.trace_id.is_none() {
            state.trace_id = state
                .request_metadata
                .get("traceparent")
                .or_else(|| state.request_metadata.get("x-trace-id"))
                .or_else(|| state.request_metadata.get("x-request-id"))
                .or_else(|| state.request_metadata.get("x-b3-traceid"))
                .cloned();
        }

        // END_HEADERS flag (0x04) indicates headers are complete
        // END_STREAM flag (0x01) indicates no body follows (unusual for gRPC requests but possible)
        let end_stream = (flags & 0x01) != 0;
        if end_stream {
            // Request with no body - emit event immediately
            self.emit_request_event(stream_id);
        }

        Ok(())
    }

    /// Handle DATA frame from client (request body)
    fn handle_client_data(
        &mut self,
        stream_id: u32,
        flags: u8,
        payload: &[u8],
    ) -> Result<(), ParseError> {
        if let Some(state) = self.streams.get_mut(&stream_id) {
            state.request_body.extend_from_slice(payload);

            // END_STREAM flag (0x01) indicates request is complete
            let end_stream = (flags & 0x01) != 0;
            if end_stream && !state.request_emitted {
                self.emit_request_event(stream_id);
            }
        }
        Ok(())
    }

    /// Parse HTTP/2 frames from server data
    fn parse_server_frames(&mut self) -> Result<(), ParseError> {
        // Parse HTTP/2 frames from server
        while self.server_buf.len() >= 9 {
            let header = match H2FrameHeader::parse(&self.server_buf) {
                Some(h) => h,
                None => break,
            };

            let frame_len = 9 + header.length as usize;
            if self.server_buf.len() < frame_len {
                break;
            }

            let frame_data = self.server_buf.split_to(frame_len);
            let payload = &frame_data[9..];

            self.handle_server_frame(&header, payload)?;
        }

        Ok(())
    }

    /// Handle a single HTTP/2 frame from the server
    fn handle_server_frame(
        &mut self,
        header: &H2FrameHeader,
        payload: &[u8],
    ) -> Result<(), ParseError> {
        match header.frame_type {
            H2FrameType::Headers => {
                self.handle_server_headers(header.stream_id, header.flags, payload)?;
            }
            H2FrameType::Data => {
                self.handle_server_data(header.stream_id, header.flags, payload)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Handle HEADERS frame from server (response headers or trailers)
    fn handle_server_headers(
        &mut self,
        stream_id: u32,
        flags: u8,
        payload: &[u8],
    ) -> Result<(), ParseError> {
        if let Some(state) = self.streams.get_mut(&stream_id) {
            let end_stream = (flags & 0x01) != 0;

            if let Ok(headers) = self.server_decoder.decode(payload) {
                let target = if state.response_metadata.is_empty() {
                    &mut state.response_metadata
                } else {
                    &mut state.response_trailers
                };

                for (name, value) in headers {
                    let name_str = String::from_utf8_lossy(&name).to_string();
                    let value_str = String::from_utf8_lossy(&value).to_string();
                    target.insert(name_str, value_str);
                }
            }

            if end_stream {
                self.emit_response_event(stream_id);
            }
        }
        Ok(())
    }

    /// Handle DATA frame from server (response body)
    fn handle_server_data(
        &mut self,
        stream_id: u32,
        flags: u8,
        payload: &[u8],
    ) -> Result<(), ParseError> {
        if let Some(state) = self.streams.get_mut(&stream_id) {
            state.response_body.extend_from_slice(payload);

            let end_stream = (flags & 0x01) != 0;
            if end_stream {
                self.emit_response_event(stream_id);
            }
        }
        Ok(())
    }

    /// Emit a gRPC request event
    fn emit_request_event(&mut self, stream_id: u32) {
        // Check if we should emit and extract data
        let Some(state) = self.streams.get(&stream_id) else {
            return;
        };
        if state.request_emitted {
            return;
        }

        // Clone data we need before mutating
        let service = state.service.clone();
        let method = state.method.clone();
        let request_metadata = state.request_metadata.clone();
        let request_body_raw = state.request_body.clone();
        let start_time_ns = state.start_time_ns;
        let authority = state
            .request_metadata
            .get(":authority")
            .cloned()
            .unwrap_or_default();
        // Use extracted trace_id or fallback to a new UUID
        let trace_id = state
            .trace_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // Mark as emitted
        if let Some(state) = self.streams.get_mut(&stream_id) {
            state.request_emitted = true;
        }

        // Extract the actual message body (skip gRPC framing if present)
        let request_body = extract_grpc_message(&request_body_raw);

        let event = RecordedEvent {
            trace_id,
            scope_id: String::new(),
            scope_sequence: 0,
            global_sequence: 0,
            timestamp_ns: start_time_ns,
            direction: 0,
            event: Some(recorded_event::Event::GrpcRequest(GrpcRequestEvent {
                service,
                method,
                request_body,
                metadata: request_metadata,
                call_type: GrpcCallType::Unary.into(),
                stream_id,
                authority,
            })),
            metadata: Default::default(),
        };

        self.pending_events.push(event);
    }

    /// Emit a gRPC response event
    fn emit_response_event(&mut self, stream_id: u32) {
        // Extract data before mutating
        let Some(state) = self.streams.get(&stream_id) else {
            return;
        };

        let start_time_ns = state.start_time_ns;
        let response_body_raw = state.response_body.clone();
        let response_metadata = state.response_metadata.clone();
        let response_trailers = state.response_trailers.clone();
        // Reuse the trace_id from the request for correlation
        let trace_id = state
            .trace_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let latency_ms = (now_ns.saturating_sub(start_time_ns)) / 1_000_000;

        // Extract gRPC status from trailers
        let status_code = response_trailers
            .get("grpc-status")
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);

        let status_message = response_trailers
            .get("grpc-message")
            .cloned()
            .unwrap_or_default();

        // Extract the actual message body
        let response_body = extract_grpc_message(&response_body_raw);

        let event = RecordedEvent {
            trace_id,
            scope_id: String::new(),
            scope_sequence: 0,
            global_sequence: 0,
            timestamp_ns: now_ns,
            direction: 0,
            event: Some(recorded_event::Event::GrpcResponse(GrpcResponseEvent {
                response_body,
                status_code,
                status_message,
                metadata: response_metadata,
                trailers: response_trailers,
                latency_ms,
                stream_id,
            })),
            metadata: Default::default(),
        };

        self.pending_events.push(event);
    }
}

#[async_trait]
impl ConnectionParser for GrpcConnectionParser {
    fn parse_client_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.client_buf.extend_from_slice(data);
        self.parse_client_frames()?;

        let events = std::mem::take(&mut self.pending_events);
        let reply = if self.pending_replies.is_empty() {
            None
        } else {
            let mut buf = BytesMut::new();
            for r in std::mem::take(&mut self.pending_replies) {
                buf.extend_from_slice(&r);
            }
            Some(buf.freeze())
        };

        Ok(ParseResult {
            events,
            forward: Bytes::copy_from_slice(data),
            needs_more: false,
            reply,
        })
    }

    fn parse_server_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.server_buf.extend_from_slice(data);
        self.parse_server_frames()?;

        let events = std::mem::take(&mut self.pending_events);
        let reply = if self.pending_replies.is_empty() {
            None
        } else {
            let mut buf = BytesMut::new();
            for r in std::mem::take(&mut self.pending_replies) {
                buf.extend_from_slice(&r);
            }
            Some(buf.freeze())
        };

        Ok(ParseResult {
            events,
            forward: Bytes::copy_from_slice(data),
            needs_more: false,
            reply,
        })
    }

    fn connection_id(&self) -> &str {
        &self.connection_id
    }

    fn reset(&mut self) {
        self.client_buf.clear();
        self.server_buf.clear();
        self.streams.clear();
        self.preface_seen = false;
        self.pending_events.clear();
    }

    fn set_mode(&mut self, is_replay: bool) {
        self.is_replay = is_replay;
    }
}

pub use serializer::GrpcSerializer;

/// Extract the gRPC message body from framed data
fn extract_grpc_message(data: &BytesMut) -> Vec<u8> {
    if data.len() < GRPC_HEADER_SIZE {
        return data.to_vec();
    }

    // Try to parse gRPC framing
    let mut buf = data.clone();
    if let Some(frame) = GrpcFrame::parse(&mut buf) {
        frame.data.to_vec()
    } else {
        // Return raw data if framing fails
        data.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_parser_detect_preface() {
        let parser = GrpcParser;

        // HTTP/2 preface
        let preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
        assert!(parser.detect(preface) > 0.9);
    }

    #[test]
    fn test_grpc_parser_detect_settings_frame() {
        let parser = GrpcParser;

        // HTTP/2 SETTINGS frame header: length=0, type=4 (SETTINGS), flags=0, stream_id=0
        let settings_frame = [0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert!(parser.detect(&settings_frame) > 0.8);
    }

    #[test]
    fn test_grpc_parser_no_detect_http1() {
        let parser = GrpcParser;

        // HTTP/1.1 request
        let http1 = b"GET /path HTTP/1.1\r\n";
        assert_eq!(parser.detect(http1), 0.0);
    }

    #[test]
    fn test_grpc_path_parsing() {
        let (service, method) = parse_grpc_path("/grpc.health.v1.Health/Check").unwrap();
        assert_eq!(service, "grpc.health.v1.Health");
        assert_eq!(method, "Check");
    }

    #[test]
    fn test_h2_frame_header_parse() {
        // SETTINGS frame: length=6, type=4, flags=0, stream_id=0
        let frame_bytes = [0x00, 0x00, 0x06, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00];
        let header = H2FrameHeader::parse(&frame_bytes).unwrap();

        assert_eq!(header.length, 6);
        assert_eq!(header.frame_type, H2FrameType::Settings);
        assert_eq!(header.flags, 0);
        assert_eq!(header.stream_id, 0);
    }
}
