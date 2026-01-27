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
use bytes::{Buf, Bytes, BytesMut};
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
            let frame_length = ((peek[0] as u32) << 16) | ((peek[1] as u32) << 8) | (peek[2] as u32);
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
}

/// Connection-level parser for gRPC (HTTP/2)
pub struct GrpcConnectionParser {
    connection_id: String,
    client_buf: BytesMut,
    server_buf: BytesMut,
    sequence: u64,
    /// Stream states indexed by HTTP/2 stream ID
    streams: HashMap<u32, StreamState>,
    /// Whether we've seen the HTTP/2 preface
    preface_seen: bool,
    /// Pending events to emit
    pending_events: Vec<RecordedEvent>,
}

impl GrpcConnectionParser {
    pub fn new(connection_id: String) -> Self {
        Self {
            connection_id,
            client_buf: BytesMut::new(),
            server_buf: BytesMut::new(),
            sequence: 0,
            streams: HashMap::new(),
            preface_seen: false,
            pending_events: Vec::new(),
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
    fn handle_client_frame(&mut self, header: &H2FrameHeader, payload: &[u8]) -> Result<(), ParseError> {
        match header.frame_type {
            H2FrameType::Headers => {
                self.handle_client_headers(header.stream_id, header.flags, payload)?;
            }
            H2FrameType::Data => {
                self.handle_client_data(header.stream_id, header.flags, payload)?;
            }
            H2FrameType::Settings | H2FrameType::WindowUpdate | H2FrameType::Ping => {
                // Connection-level frames, no action needed for gRPC parsing
            }
            _ => {
                // Other frames: Priority, RstStream, etc.
            }
        }
        Ok(())
    }

    /// Handle HEADERS frame from client (request headers)
    fn handle_client_headers(&mut self, stream_id: u32, flags: u8, payload: &[u8]) -> Result<(), ParseError> {
        let state = self.streams.entry(stream_id).or_default();
        state.start_time_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Parse HPACK-encoded headers (simplified - in production use h2 crate)
        // For now, we'll do a best-effort extraction of key headers
        parse_headers_simple(payload, &mut state.request_metadata);

        // Extract service and method from :path
        if let Some(path) = state.request_metadata.get(":path") {
            if let Some((service, method)) = parse_grpc_path(path) {
                state.service = service;
                state.method = method;
            }
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
    fn handle_client_data(&mut self, stream_id: u32, flags: u8, payload: &[u8]) -> Result<(), ParseError> {
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
    fn handle_server_frame(&mut self, header: &H2FrameHeader, payload: &[u8]) -> Result<(), ParseError> {
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
    fn handle_server_headers(&mut self, stream_id: u32, flags: u8, payload: &[u8]) -> Result<(), ParseError> {
        if let Some(state) = self.streams.get_mut(&stream_id) {
            let end_stream = (flags & 0x01) != 0;

            if state.response_metadata.is_empty() {
                // First HEADERS = response headers
                parse_headers_simple(payload, &mut state.response_metadata);
            } else {
                // Subsequent HEADERS = trailers
                parse_headers_simple(payload, &mut state.response_trailers);
            }

            if end_stream {
                self.emit_response_event(stream_id);
            }
        }
        Ok(())
    }

    /// Handle DATA frame from server (response body)
    fn handle_server_data(&mut self, stream_id: u32, flags: u8, payload: &[u8]) -> Result<(), ParseError> {
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
        let authority = state.request_metadata.get(":authority").cloned().unwrap_or_default();

        // Mark as emitted
        if let Some(state) = self.streams.get_mut(&stream_id) {
            state.request_emitted = true;
        }

        // Extract the actual message body (skip gRPC framing if present)
        let request_body = extract_grpc_message(&request_body_raw);

        let event = RecordedEvent {
            trace_id: uuid::Uuid::new_v4().to_string(),
            span_id: uuid::Uuid::new_v4().to_string(),
            sequence: self.sequence,
            timestamp_ns: start_time_ns,
            connection_id: self.connection_id.clone(),
            event: Some(recorded_event::Event::GrpcRequest(GrpcRequestEvent {
                service,
                method,
                request_body,
                metadata: request_metadata,
                call_type: GrpcCallType::Unary.into(),
                stream_id,
                authority,
            })),
            ..Default::default()
        };

        self.sequence += 1;
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
            trace_id: uuid::Uuid::new_v4().to_string(),
            span_id: uuid::Uuid::new_v4().to_string(),
            sequence: self.sequence,
            timestamp_ns: now_ns,
            connection_id: self.connection_id.clone(),
            event: Some(recorded_event::Event::GrpcResponse(GrpcResponseEvent {
                response_body,
                status_code,
                status_message,
                metadata: response_metadata,
                trailers: response_trailers,
                latency_ms,
                stream_id,
            })),
            ..Default::default()
        };

        self.sequence += 1;
        self.pending_events.push(event);
    }
}

#[async_trait]
impl ConnectionParser for GrpcConnectionParser {
    fn parse_client_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.client_buf.extend_from_slice(data);
        self.parse_client_frames()?;

        let events = std::mem::take(&mut self.pending_events);

        Ok(ParseResult {
            events,
            forward: Bytes::copy_from_slice(data),
            needs_more: false,
            reply: None,
        })
    }

    fn parse_server_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.server_buf.extend_from_slice(data);
        self.parse_server_frames()?;

        let events = std::mem::take(&mut self.pending_events);

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
        self.streams.clear();
        self.preface_seen = false;
        self.pending_events.clear();
    }
}

pub use serializer::GrpcSerializer;

/// Simplified header parsing - in production, use proper HPACK decoder
fn parse_headers_simple(payload: &[u8], headers: &mut HashMap<String, String>) {
    // This is a simplified parser that looks for common patterns
    // Real implementation should use h2's HPACK decoder
    // For now, we'll try to extract literal headers

    let mut pos = 0;
    while pos < payload.len() {
        // Check for literal header field with incremental indexing (0x40)
        // or literal header field without indexing (0x00 with prefix)
        let byte = payload[pos];

        if byte & 0xC0 == 0x40 || byte & 0xF0 == 0x00 {
            // Try to parse as literal header
            if let Some((name, value, consumed)) = try_parse_literal_header(&payload[pos..]) {
                headers.insert(name, value);
                pos += consumed;
                continue;
            }
        }

        // Skip unknown bytes
        pos += 1;
    }
}

/// Try to parse a literal header from HPACK data
fn try_parse_literal_header(data: &[u8]) -> Option<(String, String, usize)> {
    if data.is_empty() {
        return None;
    }

    let mut pos = 0;
    let first_byte = data[pos];
    pos += 1;

    // Check for literal header with indexing (0x40 prefix) or without (0x00 prefix)
    let name_index = if first_byte & 0xC0 == 0x40 {
        first_byte & 0x3F
    } else if first_byte & 0xF0 == 0x00 {
        first_byte & 0x0F
    } else {
        return None;
    };

    let name = if name_index == 0 {
        // Literal name
        if pos >= data.len() {
            return None;
        }
        let name_len_byte = data[pos];
        let huffman = (name_len_byte & 0x80) != 0;
        let name_len = (name_len_byte & 0x7F) as usize;
        pos += 1;

        if pos + name_len > data.len() {
            return None;
        }

        if huffman {
            // Huffman encoded - skip for now
            let _ = pos + name_len;
            return None;
        }

        let name = String::from_utf8_lossy(&data[pos..pos + name_len]).to_string();
        pos += name_len;
        name
    } else {
        // Indexed name - common headers
        match name_index {
            1 => ":authority",
            2 => ":method",
            3 => ":path",
            4 => ":scheme",
            5 => ":status",
            _ => return None,
        }
        .to_string()
    };

    // Parse value
    if pos >= data.len() {
        return None;
    }
    let value_len_byte = data[pos];
    let huffman = (value_len_byte & 0x80) != 0;
    let value_len = (value_len_byte & 0x7F) as usize;
    pos += 1;

    if pos + value_len > data.len() {
        return None;
    }

    if huffman {
        // Huffman encoded - skip for now
        let _ = pos + value_len;
        return None;
    }

    let value = String::from_utf8_lossy(&data[pos..pos + value_len]).to_string();
    let _ = pos + value_len;

    Some((name, value, pos))
}

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
