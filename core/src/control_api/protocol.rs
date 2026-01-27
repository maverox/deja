//! Control API Protocol
//!
//! Defines the message types for communication between DejaRuntime (service) and
//! Deja Proxy for trace correlation.
//!
//! # Overview
//!
//! The control API is an out-of-band HTTP-based communication channel that allows
//! DejaRuntime to notify the proxy about:
//! - When a new trace starts/ends
//! - When a connection is established and which trace it belongs to
//! - When a connection is reused from a pool (for re-attribution)
//!
//! This enables the proxy to maintain an accurate mapping of:
//! ```text
//! ConnectionId -> TraceId
//! ```
//!
//! # Protocol Details
//!
//! - **Transport**: HTTP POST
//! - **Format**: JSON
//! - **Default Port**: 9999
//! - **Endpoints**:
//!   - `/control/trace` - Trace lifecycle events (start/end)
//!   - `/control/connection` - Connection association/update events

use serde::{Deserialize, Serialize};

/// Control API message from service to proxy
///
/// These messages are sent via HTTP POST to notify the proxy of trace and
/// connection lifecycle events.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ControlMessage {
    /// Notification that a new trace has started
    #[serde(rename = "start_trace")]
    StartTrace {
        /// The unique trace ID for this request
        trace_id: String,
        /// Nanosecond timestamp when trace started
        timestamp_ns: u64,
    },

    /// Notification that a trace has ended
    #[serde(rename = "end_trace")]
    EndTrace {
        /// The trace ID that ended
        trace_id: String,
        /// Nanosecond timestamp when trace ended
        timestamp_ns: u64,
    },

    /// Associate a newly created connection with a trace
    #[serde(rename = "associate_connection")]
    AssociateConnection {
        /// The trace ID this connection belongs to
        trace_id: String,
        /// The connection ID (typically peer address)
        connection_id: String,
        /// Nanosecond timestamp when association occurred
        timestamp_ns: u64,
    },

    /// Update a connection's trace ID (for connection pool reuse)
    ///
    /// When a connection is borrowed from a pool and assigned to a different
    /// request/trace, we need to update the mapping.
    #[serde(rename = "update_connection")]
    UpdateConnection {
        /// The connection ID being updated
        connection_id: String,
        /// The new trace ID this connection now belongs to
        new_trace_id: String,
        /// Nanosecond timestamp when update occurred
        timestamp_ns: u64,
    },
}

impl ControlMessage {
    /// Get the timestamp in nanoseconds for this message
    pub fn timestamp_ns(&self) -> u64 {
        match self {
            Self::StartTrace { timestamp_ns, .. } => *timestamp_ns,
            Self::EndTrace { timestamp_ns, .. } => *timestamp_ns,
            Self::AssociateConnection { timestamp_ns, .. } => *timestamp_ns,
            Self::UpdateConnection { timestamp_ns, .. } => *timestamp_ns,
        }
    }

    /// Create a StartTrace message with current timestamp
    pub fn start_trace(trace_id: impl Into<String>) -> Self {
        Self::StartTrace {
            trace_id: trace_id.into(),
            timestamp_ns: current_time_ns(),
        }
    }

    /// Create an EndTrace message with current timestamp
    pub fn end_trace(trace_id: impl Into<String>) -> Self {
        Self::EndTrace {
            trace_id: trace_id.into(),
            timestamp_ns: current_time_ns(),
        }
    }

    /// Create an AssociateConnection message with current timestamp
    pub fn associate_connection(
        trace_id: impl Into<String>,
        connection_id: impl Into<String>,
    ) -> Self {
        Self::AssociateConnection {
            trace_id: trace_id.into(),
            connection_id: connection_id.into(),
            timestamp_ns: current_time_ns(),
        }
    }

    /// Create an UpdateConnection message with current timestamp
    pub fn update_connection(
        connection_id: impl Into<String>,
        new_trace_id: impl Into<String>,
    ) -> Self {
        Self::UpdateConnection {
            connection_id: connection_id.into(),
            new_trace_id: new_trace_id.into(),
            timestamp_ns: current_time_ns(),
        }
    }
}

/// Response from control API endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlResponse {
    /// Whether the request succeeded
    pub success: bool,
    /// Optional error/status message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl ControlResponse {
    /// Create a successful response
    pub fn ok() -> Self {
        Self {
            success: true,
            message: None,
        }
    }

    /// Create an error response
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: Some(message.into()),
        }
    }
}

/// Helper to get current time in nanoseconds since epoch
fn current_time_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start_trace_serialization() {
        let msg = ControlMessage::StartTrace {
            trace_id: "test-123".to_string(),
            timestamp_ns: 1000000,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"start_trace\""));
        assert!(json.contains("\"trace_id\":\"test-123\""));
        assert!(json.contains("\"timestamp_ns\":1000000"));
    }

    #[test]
    fn test_end_trace_serialization() {
        let msg = ControlMessage::EndTrace {
            trace_id: "test-456".to_string(),
            timestamp_ns: 2000000,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"end_trace\""));
        assert!(json.contains("\"trace_id\":\"test-456\""));
    }

    #[test]
    fn test_associate_connection_serialization() {
        let msg = ControlMessage::AssociateConnection {
            trace_id: "trace-789".to_string(),
            connection_id: "127.0.0.1:5433".to_string(),
            timestamp_ns: 3000000,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"associate_connection\""));
        assert!(json.contains("\"trace_id\":\"trace-789\""));
        assert!(json.contains("\"connection_id\":\"127.0.0.1:5433\""));
    }

    #[test]
    fn test_update_connection_serialization() {
        let msg = ControlMessage::UpdateConnection {
            connection_id: "127.0.0.1:5433".to_string(),
            new_trace_id: "trace-new".to_string(),
            timestamp_ns: 4000000,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"update_connection\""));
        assert!(json.contains("\"new_trace_id\":\"trace-new\""));
    }

    #[test]
    fn test_deserialization() {
        let json = r#"{"type":"start_trace","trace_id":"test-123","timestamp_ns":1000000}"#;
        let msg: ControlMessage = serde_json::from_str(json).unwrap();

        match msg {
            ControlMessage::StartTrace {
                trace_id,
                timestamp_ns,
            } => {
                assert_eq!(trace_id, "test-123");
                assert_eq!(timestamp_ns, 1000000);
            }
            _ => panic!("Expected StartTrace"),
        }
    }

    #[test]
    fn test_control_response() {
        let ok = ControlResponse::ok();
        assert!(ok.success);
        assert!(ok.message.is_none());

        let err = ControlResponse::error("Test error");
        assert!(!err.success);
        assert_eq!(err.message, Some("Test error".to_string()));
    }

    #[test]
    fn test_helper_methods() {
        let msg = ControlMessage::start_trace("test-123");
        match msg {
            ControlMessage::StartTrace {
                trace_id,
                timestamp_ns,
            } => {
                assert_eq!(trace_id, "test-123");
                assert!(timestamp_ns > 0); // Should have current time
            }
            _ => panic!("Expected StartTrace"),
        }
    }
}
