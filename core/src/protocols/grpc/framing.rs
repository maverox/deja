//! gRPC message framing utilities
//!
//! gRPC uses length-prefixed framing for messages:
//! ```text
//! +---------------+---------------+
//! | Compressed (1)| Length (4)    |
//! +---------------+---------------+
//! | Message Data (Length bytes)   |
//! +-------------------------------+
//! ```
//!
//! This module provides utilities for parsing and constructing gRPC frames.

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// gRPC frame header size: 1 byte compressed flag + 4 bytes length
pub const GRPC_HEADER_SIZE: usize = 5;

/// Maximum gRPC message size (4MB default)
pub const MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// A parsed gRPC message frame
#[derive(Debug, Clone)]
pub struct GrpcFrame {
    /// Whether the message is compressed
    pub compressed: bool,
    /// The message payload
    pub data: Bytes,
}

impl GrpcFrame {
    /// Create a new uncompressed frame
    pub fn new(data: Bytes) -> Self {
        Self {
            compressed: false,
            data,
        }
    }

    /// Parse a gRPC frame from bytes
    ///
    /// Returns None if not enough data is available
    pub fn parse(buf: &mut BytesMut) -> Option<Self> {
        if buf.len() < GRPC_HEADER_SIZE {
            return None;
        }

        let compressed = buf[0] != 0;
        let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

        // Check for oversized messages
        if length > MAX_MESSAGE_SIZE {
            // Skip this malformed frame by advancing past the header
            buf.advance(GRPC_HEADER_SIZE);
            return None;
        }

        // Check if we have the complete message
        if buf.len() < GRPC_HEADER_SIZE + length {
            return None;
        }

        // Consume the frame
        buf.advance(GRPC_HEADER_SIZE);
        let data = buf.split_to(length).freeze();

        Some(Self { compressed, data })
    }

    /// Encode this frame to bytes
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(GRPC_HEADER_SIZE + self.data.len());
        buf.put_u8(if self.compressed { 1 } else { 0 });
        buf.put_u32(self.data.len() as u32);
        buf.extend_from_slice(&self.data);
        buf.freeze()
    }
}

/// Parse the gRPC :path pseudo-header to extract service and method
///
/// Format: /{service}/{method}
/// Example: /grpc.health.v1.Health/Check -> ("grpc.health.v1.Health", "Check")
pub fn parse_grpc_path(path: &str) -> Option<(String, String)> {
    let path = path.trim_start_matches('/');
    let parts: Vec<&str> = path.splitn(2, '/').collect();
    if parts.len() == 2 {
        Some((parts[0].to_string(), parts[1].to_string()))
    } else {
        None
    }
}

/// Encode service and method into a gRPC path
pub fn encode_grpc_path(service: &str, method: &str) -> String {
    format!("/{}/{}", service, method)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_parse() {
        let mut buf = BytesMut::new();
        // Uncompressed frame with "hello" payload
        buf.put_u8(0); // not compressed
        buf.put_u32(5); // length
        buf.extend_from_slice(b"hello");

        let frame = GrpcFrame::parse(&mut buf).unwrap();
        assert!(!frame.compressed);
        assert_eq!(frame.data.as_ref(), b"hello");
        assert!(buf.is_empty());
    }

    #[test]
    fn test_frame_parse_incomplete() {
        let mut buf = BytesMut::new();
        buf.put_u8(0);
        buf.put_u32(10);
        buf.extend_from_slice(b"hello"); // Only 5 bytes, need 10

        let frame = GrpcFrame::parse(&mut buf);
        assert!(frame.is_none());
        // Buffer should be unchanged since we don't have complete data
        assert_eq!(buf.len(), 10);
    }

    #[test]
    fn test_frame_encode() {
        let frame = GrpcFrame::new(Bytes::from_static(b"test"));
        let encoded = frame.encode();

        let expected: &[u8] = &[0, 0, 0, 0, 4, b't', b'e', b's', b't'];
        assert_eq!(encoded.as_ref(), expected);
    }

    #[test]
    fn test_parse_grpc_path() {
        let (service, method) = parse_grpc_path("/grpc.health.v1.Health/Check").unwrap();
        assert_eq!(service, "grpc.health.v1.Health");
        assert_eq!(method, "Check");
    }

    #[test]
    fn test_parse_grpc_path_no_leading_slash() {
        let (service, method) = parse_grpc_path("grpc.health.v1.Health/Check").unwrap();
        assert_eq!(service, "grpc.health.v1.Health");
        assert_eq!(method, "Check");
    }

    #[test]
    fn test_encode_grpc_path() {
        let path = encode_grpc_path("myservice.v1.Service", "Method");
        assert_eq!(path, "/myservice.v1.Service/Method");
    }
}
