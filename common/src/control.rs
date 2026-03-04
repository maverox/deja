//! Control API for Deja
//!
//! Defines the protocol and client for communication between DejaRuntime and the Proxy.
//! Simplified to three message types: StartTrace, EndTrace, AssociateBySourcePort.

use crate::Protocol;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

// ============================================================================
// Protocol
// ============================================================================

/// Control API message from service to proxy
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ControlMessage {
    /// Notification that a new trace has started
    #[serde(rename = "start_trace")]
    StartTrace {
        trace_id: String,
        timestamp_ns: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sender_seq: Option<u64>,
        /// Task ID for hierarchical task tracking (e.g., "0", "0.1", "0.1.0")
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_id: Option<String>,
        /// Task path for scope identification (same as task_id, for clarity)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_path: Option<String>,
    },

    /// Notification that a trace has ended
    #[serde(rename = "end_trace")]
    EndTrace {
        trace_id: String,
        timestamp_ns: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sender_seq: Option<u64>,
        /// Task ID for hierarchical task tracking (e.g., "0", "0.1", "0.1.0")
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_id: Option<String>,
        /// Task path for scope identification (same as task_id, for clarity)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_path: Option<String>,
    },

    /// Associate a source port with a trace (replaces SetActiveTrace + AssociateConnection)
    ///
    /// The SDK sends this after establishing an outgoing connection, providing
    /// the local source port so the proxy can correlate the connection to the trace.
    #[serde(rename = "associate_by_source_port")]
    AssociateBySourcePort {
        trace_id: String,
        source_port: u16,
        protocol: Protocol,
        timestamp_ns: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sender_seq: Option<u64>,
    },

    /// Re-associate an existing connection with a new trace (for pool reuse).
    /// Sent by SDK when a pooled connection is checked out by a different trace.
    #[serde(rename = "reassociate_connection")]
    ReassociateConnection {
        trace_id: String,
        source_port: u16,
        protocol: Protocol,
        timestamp_ns: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sender_seq: Option<u64>,
    },

    /// SDK notifies proxy that a trace has checked out a pooled connection.
    #[serde(rename = "pool_checkout")]
    PoolCheckout {
        trace_id: String,
        source_port: u16,
        protocol: Protocol,
        pool_id: Option<String>,
        timestamp_ns: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sender_seq: Option<u64>,
    },

    /// SDK notifies proxy that a trace has returned a pooled connection.
    #[serde(rename = "pool_return")]
    PoolReturn {
        trace_id: String,
        source_port: u16,
        protocol: Protocol,
        timestamp_ns: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sender_seq: Option<u64>,
    },
}

impl ControlMessage {
    pub fn timestamp_ns(&self) -> u64 {
        match self {
            Self::StartTrace { timestamp_ns, .. } => *timestamp_ns,
            Self::EndTrace { timestamp_ns, .. } => *timestamp_ns,
            Self::AssociateBySourcePort { timestamp_ns, .. } => *timestamp_ns,
            Self::ReassociateConnection { timestamp_ns, .. } => *timestamp_ns,
            Self::PoolCheckout { timestamp_ns, .. } => *timestamp_ns,
            Self::PoolReturn { timestamp_ns, .. } => *timestamp_ns,
        }
    }

    pub fn task_id(&self) -> Option<&str> {
        match self {
            Self::StartTrace { task_id, .. } => task_id.as_deref(),
            Self::EndTrace { task_id, .. } => task_id.as_deref(),
            _ => None,
        }
    }

    pub fn task_path(&self) -> Option<&str> {
        match self {
            Self::StartTrace { task_path, .. } => task_path.as_deref(),
            Self::EndTrace { task_path, .. } => task_path.as_deref(),
            _ => None,
        }
    }

    pub fn sender_seq(&self) -> Option<u64> {
        match self {
            Self::StartTrace { sender_seq, .. } => *sender_seq,
            Self::EndTrace { sender_seq, .. } => *sender_seq,
            Self::AssociateBySourcePort { sender_seq, .. } => *sender_seq,
            Self::ReassociateConnection { sender_seq, .. } => *sender_seq,
            Self::PoolCheckout { sender_seq, .. } => *sender_seq,
            Self::PoolReturn { sender_seq, .. } => *sender_seq,
        }
    }

    pub fn with_sender_seq(self, sender_seq: u64) -> Self {
        match self {
            Self::StartTrace {
                trace_id,
                timestamp_ns,
                sender_seq: _,
                task_id,
                task_path,
            } => Self::StartTrace {
                trace_id,
                timestamp_ns,
                sender_seq: Some(sender_seq),
                task_id,
                task_path,
            },
            Self::EndTrace {
                trace_id,
                timestamp_ns,
                sender_seq: _,
                task_id,
                task_path,
            } => Self::EndTrace {
                trace_id,
                timestamp_ns,
                sender_seq: Some(sender_seq),
                task_id,
                task_path,
            },
            Self::AssociateBySourcePort {
                trace_id,
                source_port,
                protocol,
                timestamp_ns,
                ..
            } => Self::AssociateBySourcePort {
                trace_id,
                source_port,
                protocol,
                timestamp_ns,
                sender_seq: Some(sender_seq),
            },
            Self::ReassociateConnection {
                trace_id,
                source_port,
                protocol,
                timestamp_ns,
                ..
            } => Self::ReassociateConnection {
                trace_id,
                source_port,
                protocol,
                timestamp_ns,
                sender_seq: Some(sender_seq),
            },
            Self::PoolCheckout {
                trace_id,
                source_port,
                protocol,
                pool_id,
                timestamp_ns,
                ..
            } => Self::PoolCheckout {
                trace_id,
                source_port,
                protocol,
                pool_id,
                timestamp_ns,
                sender_seq: Some(sender_seq),
            },
            Self::PoolReturn {
                trace_id,
                source_port,
                protocol,
                timestamp_ns,
                ..
            } => Self::PoolReturn {
                trace_id,
                source_port,
                protocol,
                timestamp_ns,
                sender_seq: Some(sender_seq),
            },
        }
    }

    pub fn start_trace(trace_id: impl Into<String>) -> Self {
        Self::StartTrace {
            trace_id: trace_id.into(),
            timestamp_ns: current_time_ns(),
            sender_seq: None,
            task_id: None,
            task_path: None,
        }
    }

    pub fn start_trace_with_task(trace_id: impl Into<String>, task_id: impl Into<String>) -> Self {
        let task_id_str = task_id.into();
        Self::StartTrace {
            trace_id: trace_id.into(),
            timestamp_ns: current_time_ns(),
            sender_seq: None,
            task_id: Some(task_id_str.clone()),
            task_path: Some(task_id_str),
        }
    }

    pub fn end_trace(trace_id: impl Into<String>) -> Self {
        Self::EndTrace {
            trace_id: trace_id.into(),
            timestamp_ns: current_time_ns(),
            sender_seq: None,
            task_id: None,
            task_path: None,
        }
    }

    pub fn end_trace_with_task(trace_id: impl Into<String>, task_id: impl Into<String>) -> Self {
        let task_id_str = task_id.into();
        Self::EndTrace {
            trace_id: trace_id.into(),
            timestamp_ns: current_time_ns(),
            sender_seq: None,
            task_id: Some(task_id_str.clone()),
            task_path: Some(task_id_str),
        }
    }

    pub fn associate_by_source_port(
        trace_id: impl Into<String>,
        source_port: u16,
        protocol: Protocol,
    ) -> Self {
        Self::AssociateBySourcePort {
            trace_id: trace_id.into(),
            source_port,
            protocol,
            timestamp_ns: current_time_ns(),
            sender_seq: None,
        }
    }

    pub fn reassociate_connection(
        trace_id: impl Into<String>,
        source_port: u16,
        protocol: Protocol,
    ) -> Self {
        Self::ReassociateConnection {
            trace_id: trace_id.into(),
            source_port,
            protocol,
            timestamp_ns: current_time_ns(),
            sender_seq: None,
        }
    }

    pub fn pool_checkout(
        trace_id: impl Into<String>,
        source_port: u16,
        protocol: Protocol,
        pool_id: Option<String>,
    ) -> Self {
        Self::PoolCheckout {
            trace_id: trace_id.into(),
            source_port,
            protocol,
            pool_id,
            timestamp_ns: current_time_ns(),
            sender_seq: None,
        }
    }

    pub fn pool_return(trace_id: impl Into<String>, source_port: u16, protocol: Protocol) -> Self {
        Self::PoolReturn {
            trace_id: trace_id.into(),
            source_port,
            protocol,
            timestamp_ns: current_time_ns(),
            sender_seq: None,
        }
    }
}

/// Response from control API endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl ControlResponse {
    pub fn ok() -> Self {
        Self {
            success: true,
            message: None,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: Some(message.into()),
        }
    }
}

fn current_time_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

// ============================================================================
// Client
// ============================================================================

/// HTTP client for sending control messages to Deja Proxy
#[derive(Clone)]
pub struct ControlClient {
    base_url: String,
    client: reqwest::Client,
}

impl ControlClient {
    pub fn new(host: &str, port: u16) -> Self {
        let base_url = format!("http://{}:{}", host, port);
        Self {
            base_url,
            client: reqwest::Client::new(),
        }
    }

    pub fn from_env() -> Self {
        let host = std::env::var("DEJA_CONTROL_HOST").unwrap_or_else(|_| "localhost".into());
        let port = std::env::var("DEJA_CONTROL_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(9999);

        Self::new(&host, port)
    }

    /// Send a control message to the proxy
    pub async fn send(&self, message: ControlMessage) -> Result<(), ControlError> {
        let url = format!("{}/control/trace", self.base_url);

        match self.client.post(&url).json(&message).send().await {
            Ok(response) => match response.json::<ControlResponse>().await {
                Ok(control_resp) => {
                    if control_resp.success {
                        debug!("Control message accepted by proxy");
                        Ok(())
                    } else {
                        let msg = control_resp
                            .message
                            .unwrap_or_else(|| "Unknown error".to_string());
                        warn!(message = %msg, "Control message rejected by proxy");
                        Err(ControlError::Rejected(msg))
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to parse proxy response");
                    Err(ControlError::ParseError(e.to_string()))
                }
            },
            Err(e) => {
                warn!(error = %e, "Failed to send control message to proxy");
                Err(ControlError::ConnectionFailed(e.to_string()))
            }
        }
    }

    /// Send a control message, logging errors but not failing
    pub async fn send_best_effort(&self, message: ControlMessage) {
        if let Err(e) = self.send(message).await {
            debug!(error = ?e, "Control message failed (best effort)");
        }
    }
}

/// Errors that can occur when sending control messages
#[derive(Debug, Clone)]
pub enum ControlError {
    /// Failed to connect to the proxy
    ConnectionFailed(String),
    /// Message was rejected by the proxy
    Rejected(String),
    /// Failed to parse the response
    ParseError(String),
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionFailed(msg) => write!(f, "Connection failed: {}", msg),
            Self::Rejected(msg) => write!(f, "Rejected: {}", msg),
            Self::ParseError(msg) => write!(f, "Parse error: {}", msg),
        }
    }
}

impl std::error::Error for ControlError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reassociate_serialization_roundtrip() {
        let msg = ControlMessage::reassociate_connection("trace-123", 5433, Protocol::Postgres);
        let json = serde_json::to_string(&msg).expect("serialize");
        let deserialized: ControlMessage = serde_json::from_str(&json).expect("deserialize");

        match deserialized {
            ControlMessage::ReassociateConnection {
                trace_id,
                source_port,
                protocol,
                timestamp_ns,
                sender_seq,
            } => {
                assert_eq!(trace_id, "trace-123");
                assert_eq!(source_port, 5433);
                assert_eq!(protocol, Protocol::Postgres);
                assert!(timestamp_ns > 0);
                assert_eq!(sender_seq, None);
            }
            _ => panic!("Expected ReassociateConnection variant"),
        }
    }

    #[test]
    fn test_pool_checkout_serialization_roundtrip_with_pool_id() {
        let msg = ControlMessage::pool_checkout(
            "trace-456",
            6380,
            Protocol::Redis,
            Some("pool-1".to_string()),
        );
        let json = serde_json::to_string(&msg).expect("serialize");
        let deserialized: ControlMessage = serde_json::from_str(&json).expect("deserialize");

        match deserialized {
            ControlMessage::PoolCheckout {
                trace_id,
                source_port,
                protocol,
                pool_id,
                timestamp_ns,
                sender_seq,
            } => {
                assert_eq!(trace_id, "trace-456");
                assert_eq!(source_port, 6380);
                assert_eq!(protocol, Protocol::Redis);
                assert_eq!(pool_id, Some("pool-1".to_string()));
                assert!(timestamp_ns > 0);
                assert_eq!(sender_seq, None);
            }
            _ => panic!("Expected PoolCheckout variant"),
        }
    }

    #[test]
    fn test_pool_checkout_serialization_roundtrip_without_pool_id() {
        let msg = ControlMessage::pool_checkout("trace-789", 8080, Protocol::Http, None);
        let json = serde_json::to_string(&msg).expect("serialize");
        let deserialized: ControlMessage = serde_json::from_str(&json).expect("deserialize");

        match deserialized {
            ControlMessage::PoolCheckout {
                trace_id,
                source_port,
                protocol,
                pool_id,
                timestamp_ns,
                sender_seq,
            } => {
                assert_eq!(trace_id, "trace-789");
                assert_eq!(source_port, 8080);
                assert_eq!(protocol, Protocol::Http);
                assert_eq!(pool_id, None);
                assert!(timestamp_ns > 0);
                assert_eq!(sender_seq, None);
            }
            _ => panic!("Expected PoolCheckout variant"),
        }
    }

    #[test]
    fn test_pool_return_serialization_roundtrip() {
        let msg = ControlMessage::pool_return("trace-999", 5432, Protocol::Postgres);
        let json = serde_json::to_string(&msg).expect("serialize");
        let deserialized: ControlMessage = serde_json::from_str(&json).expect("deserialize");

        match deserialized {
            ControlMessage::PoolReturn {
                trace_id,
                source_port,
                protocol,
                timestamp_ns,
                sender_seq,
            } => {
                assert_eq!(trace_id, "trace-999");
                assert_eq!(source_port, 5432);
                assert_eq!(protocol, Protocol::Postgres);
                assert!(timestamp_ns > 0);
                assert_eq!(sender_seq, None);
            }
            _ => panic!("Expected PoolReturn variant"),
        }
    }

    #[test]
    fn test_timestamp_ns_all_variants() {
        let start_trace = ControlMessage::start_trace("trace-1");
        assert!(start_trace.timestamp_ns() > 0);

        let end_trace = ControlMessage::end_trace("trace-2");
        assert!(end_trace.timestamp_ns() > 0);

        let assoc = ControlMessage::associate_by_source_port("trace-3", 5433, Protocol::Postgres);
        assert!(assoc.timestamp_ns() > 0);

        let reassoc = ControlMessage::reassociate_connection("trace-4", 5433, Protocol::Postgres);
        assert!(reassoc.timestamp_ns() > 0);

        let checkout = ControlMessage::pool_checkout("trace-5", 6380, Protocol::Redis, None);
        assert!(checkout.timestamp_ns() > 0);

        let return_msg = ControlMessage::pool_return("trace-6", 5432, Protocol::Postgres);
        assert!(return_msg.timestamp_ns() > 0);
    }

    #[test]
    fn test_sender_seq_roundtrip_and_backward_compat() {
        let with_seq = ControlMessage::start_trace("trace-1").with_sender_seq(42);
        let json = serde_json::to_string(&with_seq).expect("serialize");
        let deserialized: ControlMessage = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.sender_seq(), Some(42));

        let without_seq_json = r#"{"type":"start_trace","trace_id":"trace-2","timestamp_ns":1}"#;
        let deserialized_without_seq: ControlMessage =
            serde_json::from_str(without_seq_json).expect("deserialize without seq");
        assert_eq!(deserialized_without_seq.sender_seq(), None);
    }

    #[test]
    fn test_start_trace_with_task_metadata() {
        let msg = ControlMessage::start_trace_with_task("trace-123", "0.1.2");
        match msg {
            ControlMessage::StartTrace {
                trace_id,
                task_id,
                task_path,
                ..
            } => {
                assert_eq!(trace_id, "trace-123");
                assert_eq!(task_id, Some("0.1.2".to_string()));
                assert_eq!(task_path, Some("0.1.2".to_string()));
            }
            _ => panic!("Expected StartTrace variant"),
        }
    }

    #[test]
    fn test_end_trace_with_task_metadata() {
        let msg = ControlMessage::end_trace_with_task("trace-456", "0.2");
        match msg {
            ControlMessage::EndTrace {
                trace_id,
                task_id,
                task_path,
                ..
            } => {
                assert_eq!(trace_id, "trace-456");
                assert_eq!(task_id, Some("0.2".to_string()));
                assert_eq!(task_path, Some("0.2".to_string()));
            }
            _ => panic!("Expected EndTrace variant"),
        }
    }

    #[test]
    fn test_start_trace_without_task_backward_compat() {
        let msg = ControlMessage::start_trace("trace-789");
        match msg {
            ControlMessage::StartTrace {
                trace_id,
                task_id,
                task_path,
                ..
            } => {
                assert_eq!(trace_id, "trace-789");
                assert_eq!(task_id, None);
                assert_eq!(task_path, None);
            }
            _ => panic!("Expected StartTrace variant"),
        }
    }

    #[test]
    fn test_end_trace_without_task_backward_compat() {
        let msg = ControlMessage::end_trace("trace-999");
        match msg {
            ControlMessage::EndTrace {
                trace_id,
                task_id,
                task_path,
                ..
            } => {
                assert_eq!(trace_id, "trace-999");
                assert_eq!(task_id, None);
                assert_eq!(task_path, None);
            }
            _ => panic!("Expected EndTrace variant"),
        }
    }

    #[test]
    fn test_start_trace_with_task_serialization_roundtrip() {
        let msg = ControlMessage::start_trace_with_task("trace-111", "0.0.1");
        let json = serde_json::to_string(&msg).expect("serialize");
        let deserialized: ControlMessage = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.task_id(), Some("0.0.1"));
        assert_eq!(deserialized.task_path(), Some("0.0.1"));
    }

    #[test]
    fn test_end_trace_with_task_serialization_roundtrip() {
        let msg = ControlMessage::end_trace_with_task("trace-222", "0.1.0");
        let json = serde_json::to_string(&msg).expect("serialize");
        let deserialized: ControlMessage = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.task_id(), Some("0.1.0"));
        assert_eq!(deserialized.task_path(), Some("0.1.0"));
    }

    #[test]
    fn test_task_metadata_with_sender_seq() {
        let msg = ControlMessage::start_trace_with_task("trace-333", "0.2.1").with_sender_seq(99);
        match msg {
            ControlMessage::StartTrace {
                trace_id,
                task_id,
                task_path,
                sender_seq,
                ..
            } => {
                assert_eq!(trace_id, "trace-333");
                assert_eq!(task_id, Some("0.2.1".to_string()));
                assert_eq!(task_path, Some("0.2.1".to_string()));
                assert_eq!(sender_seq, Some(99));
            }
            _ => panic!("Expected StartTrace variant"),
        }
    }
}
