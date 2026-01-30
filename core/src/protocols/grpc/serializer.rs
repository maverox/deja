//! gRPC response serializer for replay mode
//!
//! Converts recorded gRPC response events back into wire format (HTTP/2 frames)
//! for sending to clients during replay.

use super::framing::GrpcFrame;
use crate::events::{GrpcResponseEvent, GrpcStatusCode};
use bytes::{BufMut, Bytes, BytesMut};

/// Serializer for gRPC responses
pub struct GrpcSerializer;

impl GrpcSerializer {
    /// Serialize a gRPC response event to wire format bytes
    ///
    /// This creates a simplified HTTP/2 response that most gRPC clients will accept.
    /// For a proper implementation, we'd need full HTTP/2 framing, but for basic
    /// replay, we can use HTTP/1.1 upgrade or direct frame injection.
    /// Serialize a gRPC response event to wire format bytes (HTTP/2 frames)
    ///
    /// This creates a full HTTP/2 response sequence:
    /// 1. HEADERS frame (Initial headers)
    /// 2. DATA frame (gRPC message)
    /// 3. HEADERS frame (Trailers with status)
    pub fn serialize_response(response: &GrpcResponseEvent) -> Bytes {
        let mut buf = BytesMut::new();
        let mut encoder = hpack::Encoder::new();

        // --- 1. HEADERS Frame (Initial) ---
        let mut headers = vec![
            (b":status".to_vec(), b"200".to_vec()),
            (b"content-type".to_vec(), b"application/grpc".to_vec()),
        ];

        // Add recorded metadata that aren't pseudo-headers or restricted
        for (k, v) in &response.metadata {
            if !k.starts_with(':') && k.to_lowercase() != "content-length" {
                headers.push((k.as_bytes().to_vec(), v.as_bytes().to_vec()));
            }
        }

        let header_payload = encoder.encode(headers.iter().map(|(k, v)| (&k[..], &v[..])));

        let mut h_frame = BytesMut::with_capacity(9 + header_payload.len());
        let h_len = header_payload.len() as u32;
        h_frame.put_u8((h_len >> 16) as u8);
        h_frame.put_u8((h_len >> 8) as u8);
        h_frame.put_u8(h_len as u8);
        h_frame.put_u8(0x01); // type=HEADERS
        h_frame.put_u8(0x04); // flags=END_HEADERS (0x04)
        h_frame.put_u32(response.stream_id & 0x7FFFFFFF); // stream_id
        h_frame.extend_from_slice(&header_payload);
        buf.extend_from_slice(&h_frame);

        // --- 2. DATA Frame (gRPC Message) ---
        let msg_frame = GrpcFrame::new(Bytes::copy_from_slice(&response.response_body)).encode();
        let mut d_frame = BytesMut::with_capacity(9 + msg_frame.len());
        let d_len = msg_frame.len() as u32;
        d_frame.put_u8((d_len >> 16) as u8);
        d_frame.put_u8((d_len >> 8) as u8);
        d_frame.put_u8(d_len as u8);
        d_frame.put_u8(0x00); // type=DATA
        d_frame.put_u8(0x00); // flags=0
        d_frame.put_u32(response.stream_id & 0x7FFFFFFF);
        d_frame.extend_from_slice(&msg_frame);
        buf.extend_from_slice(&d_frame);

        // --- 3. HEADERS Frame (Trailers) ---
        let mut trailers = vec![(
            b"grpc-status".to_vec(),
            response.status_code.to_string().into_bytes(),
        )];
        if !response.status_message.is_empty() {
            trailers.push((
                b"grpc-message".to_vec(),
                response.status_message.as_bytes().to_vec(),
            ));
        }
        for (k, v) in &response.trailers {
            if k.to_lowercase() != "grpc-status" && k.to_lowercase() != "grpc-message" {
                trailers.push((k.as_bytes().to_vec(), v.as_bytes().to_vec()));
            }
        }

        let trailer_payload = encoder.encode(trailers.iter().map(|(k, v)| (&k[..], &v[..])));
        let mut t_frame = BytesMut::with_capacity(9 + trailer_payload.len());
        let t_len = trailer_payload.len() as u32;
        t_frame.put_u8((t_len >> 16) as u8);
        t_frame.put_u8((t_len >> 8) as u8);
        t_frame.put_u8(t_len as u8);
        t_frame.put_u8(0x01); // type=HEADERS
        t_frame.put_u8(0x05); // flags=END_STREAM (0x01) | END_HEADERS (0x04)
        t_frame.put_u32(response.stream_id & 0x7FFFFFFF);
        t_frame.extend_from_slice(&trailer_payload);
        buf.extend_from_slice(&t_frame);

        buf.freeze()
    }

    /// Serialize a gRPC error response
    pub fn serialize_error(_status: GrpcStatusCode, _message: &str) -> Bytes {
        // For errors, we return the trailers-only response
        // The actual status is in the trailers, not a message frame
        let mut buf = BytesMut::new();

        // Empty data frame (no message body)
        buf.put_u8(0); // not compressed
        buf.put_u32(0); // zero length

        buf.freeze()
    }

    /// Convert gRPC status code to HTTP status code
    pub fn grpc_to_http_status(grpc_status: &GrpcStatusCode) -> u16 {
        match grpc_status {
            GrpcStatusCode::GrpcStatusOk => 200,
            GrpcStatusCode::GrpcStatusCancelled => 499,
            GrpcStatusCode::GrpcStatusUnknown => 500,
            GrpcStatusCode::GrpcStatusInvalidArgument => 400,
            GrpcStatusCode::GrpcStatusDeadlineExceeded => 504,
            GrpcStatusCode::GrpcStatusNotFound => 404,
            GrpcStatusCode::GrpcStatusAlreadyExists => 409,
            GrpcStatusCode::GrpcStatusPermissionDenied => 403,
            GrpcStatusCode::GrpcStatusResourceExhausted => 429,
            GrpcStatusCode::GrpcStatusFailedPrecondition => 400,
            GrpcStatusCode::GrpcStatusAborted => 409,
            GrpcStatusCode::GrpcStatusOutOfRange => 400,
            GrpcStatusCode::GrpcStatusUnimplemented => 501,
            GrpcStatusCode::GrpcStatusInternal => 500,
            GrpcStatusCode::GrpcStatusUnavailable => 503,
            GrpcStatusCode::GrpcStatusDataLoss => 500,
            GrpcStatusCode::GrpcStatusUnauthenticated => 401,
        }
    }

    /// Convert gRPC status code to string representation
    pub fn status_code_name(status: &GrpcStatusCode) -> &'static str {
        match status {
            GrpcStatusCode::GrpcStatusOk => "OK",
            GrpcStatusCode::GrpcStatusCancelled => "CANCELLED",
            GrpcStatusCode::GrpcStatusUnknown => "UNKNOWN",
            GrpcStatusCode::GrpcStatusInvalidArgument => "INVALID_ARGUMENT",
            GrpcStatusCode::GrpcStatusDeadlineExceeded => "DEADLINE_EXCEEDED",
            GrpcStatusCode::GrpcStatusNotFound => "NOT_FOUND",
            GrpcStatusCode::GrpcStatusAlreadyExists => "ALREADY_EXISTS",
            GrpcStatusCode::GrpcStatusPermissionDenied => "PERMISSION_DENIED",
            GrpcStatusCode::GrpcStatusResourceExhausted => "RESOURCE_EXHAUSTED",
            GrpcStatusCode::GrpcStatusFailedPrecondition => "FAILED_PRECONDITION",
            GrpcStatusCode::GrpcStatusAborted => "ABORTED",
            GrpcStatusCode::GrpcStatusOutOfRange => "OUT_OF_RANGE",
            GrpcStatusCode::GrpcStatusUnimplemented => "UNIMPLEMENTED",
            GrpcStatusCode::GrpcStatusInternal => "INTERNAL",
            GrpcStatusCode::GrpcStatusUnavailable => "UNAVAILABLE",
            GrpcStatusCode::GrpcStatusDataLoss => "DATA_LOSS",
            GrpcStatusCode::GrpcStatusUnauthenticated => "UNAUTHENTICATED",
        }
    }

    /// Serialize gRPC message from event for replay
    pub fn serialize_message(event: &crate::events::recorded_event::Event) -> Option<Bytes> {
        match event {
            crate::events::recorded_event::Event::GrpcResponse(resp) => {
                Some(Self::serialize_response(resp))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_response() {
        let response = GrpcResponseEvent {
            response_body: b"test payload".to_vec(),
            status_code: GrpcStatusCode::GrpcStatusOk.into(),
            status_message: String::new(),
            metadata: Default::default(),
            trailers: Default::default(),
            latency_ms: 10,
            stream_id: 1,
        };

        let bytes = GrpcSerializer::serialize_response(&response);
        // Should have 5-byte header + payload
        assert_eq!(bytes.len(), 5 + 12);
        assert_eq!(bytes[0], 0); // not compressed
        assert_eq!(&bytes[5..], b"test payload");
    }

    #[test]
    fn test_status_code_mapping() {
        assert_eq!(
            GrpcSerializer::grpc_to_http_status(&GrpcStatusCode::GrpcStatusOk),
            200
        );
        assert_eq!(
            GrpcSerializer::grpc_to_http_status(&GrpcStatusCode::GrpcStatusNotFound),
            404
        );
        assert_eq!(
            GrpcSerializer::grpc_to_http_status(&GrpcStatusCode::GrpcStatusInternal),
            500
        );
    }
}
