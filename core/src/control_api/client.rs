//! Control API Client
//!
//! Client for sending control messages to the Deja Proxy from the service side.

use super::protocol::{ControlMessage, ControlResponse};
use tracing::{debug, warn};

/// HTTP client for sending control messages to Deja Proxy
#[derive(Clone)]
pub struct ControlClient {
    /// Base URL of the proxy (e.g., http://localhost:9999)
    base_url: String,
    /// HTTP client for making requests
    client: reqwest::Client,
}

impl ControlClient {
    /// Create a new control client
    ///
    /// # Arguments
    ///
    /// * `host` - The proxy host (e.g., "localhost")
    /// * `port` - The proxy control API port (e.g., 9999)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let client = ControlClient::new("localhost", 9999);
    /// ```
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let base_url = format!("http://{}:{}", host.into(), port);
        Self {
            base_url,
            client: reqwest::Client::new(),
        }
    }

    /// Create a client from environment variables
    ///
    /// Reads:
    /// - `DEJA_CONTROL_HOST` (default: "localhost")
    /// - `DEJA_CONTROL_PORT` (default: 9999)
    pub fn from_env() -> Self {
        let host = std::env::var("DEJA_CONTROL_HOST").unwrap_or_else(|_| "localhost".into());
        let port = std::env::var("DEJA_CONTROL_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(9999);

        Self::new(host, port)
    }

    /// Send a control message to the proxy
    ///
    /// This is a fire-and-forget operation. Network errors are logged but
    /// don't fail the request - we don't want to fail the actual request
    /// if we can't reach the proxy.
    ///
    /// # Arguments
    ///
    /// * `message` - The control message to send
    ///
    /// # Example
    ///
    /// ```ignore
    /// let msg = ControlMessage::start_trace("request-123");
    /// client.send(msg).await;
    /// ```
    pub async fn send(&self, message: ControlMessage) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = match message {
            ControlMessage::StartTrace { .. } | ControlMessage::EndTrace { .. } => {
                "/control/trace"
            }
            ControlMessage::AssociateConnection { .. } | ControlMessage::UpdateConnection { .. } => {
                "/control/connection"
            }
        };

        let url = format!("{}{}", self.base_url, endpoint);

        debug!(
            url = %url,
            message_type = ?message,
            "Sending control message to proxy"
        );

        match self.client.post(&url).json(&message).send().await {
            Ok(response) => {
                match response.json::<ControlResponse>().await {
                    Ok(control_resp) => {
                        if control_resp.success {
                            debug!("Control message accepted by proxy");
                            Ok(())
                        } else {
                            warn!(
                                message = control_resp.message,
                                "Control message rejected by proxy"
                            );
                            Ok(()) // Still return Ok, don't fail the request
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to parse proxy response");
                        Ok(()) // Don't fail, continue with request
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to send control message to proxy");
                Ok(()) // Don't fail, continue with request
            }
        }
    }

    /// Send multiple control messages in a batch
    ///
    /// Sends messages sequentially. If any fails, continues with the rest.
    pub async fn send_batch(
        &self,
        messages: Vec<ControlMessage>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for msg in messages {
            let _ = self.send(msg).await; // Ignore errors
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = ControlClient::new("localhost", 9999);
        assert_eq!(client.base_url, "http://localhost:9999");
    }

    #[test]
    fn test_client_from_env() {
        // This test won't validate the actual env vars, just that it doesn't panic
        let client = ControlClient::from_env();
        assert!(!client.base_url.is_empty());
    }

    #[tokio::test]
    async fn test_send_handles_network_error() {
        let client = ControlClient::new("localhost", 9999);
        let msg = ControlMessage::start_trace("test-123");

        // This should not panic even though the proxy isn't running
        let result = client.send(msg).await;
        assert!(result.is_ok()); // Returns Ok even on network error
    }

    #[test]
    fn test_client_clone() {
        let client1 = ControlClient::new("localhost", 9999);
        let client2 = client1.clone();

        assert_eq!(client1.base_url, client2.base_url);
    }
}
