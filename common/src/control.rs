//! Control API for Deja
//!
//! Defines the protocol and client for communication between DejaRuntime and the Proxy.

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
    StartTrace { trace_id: String, timestamp_ns: u64 },

    /// Notification that a trace has ended
    #[serde(rename = "end_trace")]
    EndTrace { trace_id: String, timestamp_ns: u64 },

    /// Associate a newly created connection with a trace
    #[serde(rename = "associate_connection")]
    AssociateConnection {
        trace_id: String,
        connection_id: String,
        timestamp_ns: u64,
        #[serde(default)]
        protocol: Option<Protocol>,
    },

    /// Update a connection's trace ID (for connection pool reuse)
    #[serde(rename = "update_connection")]
    UpdateConnection {
        connection_id: String,
        new_trace_id: String,
        timestamp_ns: u64,
    },
}

impl ControlMessage {
    pub fn timestamp_ns(&self) -> u64 {
        match self {
            Self::StartTrace { timestamp_ns, .. } => *timestamp_ns,
            Self::EndTrace { timestamp_ns, .. } => *timestamp_ns,
            Self::AssociateConnection { timestamp_ns, .. } => *timestamp_ns,
            Self::UpdateConnection { timestamp_ns, .. } => *timestamp_ns,
        }
    }

    pub fn start_trace(trace_id: impl Into<String>) -> Self {
        Self::StartTrace {
            trace_id: trace_id.into(),
            timestamp_ns: current_time_ns(),
        }
    }

    pub fn end_trace(trace_id: impl Into<String>) -> Self {
        Self::EndTrace {
            trace_id: trace_id.into(),
            timestamp_ns: current_time_ns(),
        }
    }

    pub fn associate_connection(
        trace_id: impl Into<String>,
        connection_id: impl Into<String>,
    ) -> Self {
        Self::AssociateConnection {
            trace_id: trace_id.into(),
            connection_id: connection_id.into(),
            timestamp_ns: current_time_ns(),
            protocol: None,
        }
    }

    pub fn associate_connection_with_protocol(
        trace_id: impl Into<String>,
        connection_id: impl Into<String>,
        protocol: Protocol,
    ) -> Self {
        Self::AssociateConnection {
            trace_id: trace_id.into(),
            connection_id: connection_id.into(),
            timestamp_ns: current_time_ns(),
            protocol: Some(protocol),
        }
    }

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
    ///
    /// Returns Ok(()) on success, or an error describing what went wrong.
    /// Callers should decide whether to:
    /// - Fail the operation (strict mode)
    /// - Log and continue (fire-and-forget mode)
    pub async fn send(&self, message: ControlMessage) -> Result<(), ControlError> {
        let endpoint = match message {
            ControlMessage::StartTrace { .. } | ControlMessage::EndTrace { .. } => "/control/trace",
            ControlMessage::AssociateConnection { .. }
            | ControlMessage::UpdateConnection { .. } => "/control/connection",
        };

        let url = format!("{}{}", self.base_url, endpoint);

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
    ///
    /// Use this for fire-and-forget scenarios where the operation should
    /// proceed even if the proxy is unavailable.
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
