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
    pub fn serialize_response(response: &GrpcResponseEvent) -> Bytes {
        let mut buf = BytesMut::new();

        // For gRPC-Web or direct frame replay, we just need the length-prefixed message
        // The actual HTTP/2 framing is handled by the transport layer (h2 crate)

        // Build the gRPC message frame
        let frame = GrpcFrame::new(Bytes::copy_from_slice(&response.response_body));
        buf.extend_from_slice(&frame.encode());

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
