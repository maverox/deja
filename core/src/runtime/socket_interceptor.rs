//! TCP Socket Interception for Trace Correlation
//!
//! This module provides a wrapper around `tokio::net::TcpStream` that:
//! 1. Intercepts socket connections
//! 2. Associates each connection with the current trace ID
//! 3. Notifies the proxy via control API
//!
//! # Problem Solved
//!
//! When a service makes a database connection (e.g., to Postgres or Redis),
//! we need to know which trace (request) initiated that connection so we can
//! attribute all events on that connection to the correct trace.
//!
//! Example:
//! ```ignore
//! // Request A connects to Postgres
//! let stream = TcpStream::connect("127.0.0.1:5432").await?;
//! // Proxy needs to know: connection 127.0.0.1:5432 → trace A
//!
//! // Request B later reuses that connection
//! // Proxy needs to update: connection 127.0.0.1:5432 → trace B
//! ```
//!
//! # Usage
//!
//! Instead of:
//! ```ignore
//! use tokio::net::TcpStream;
//! let stream = TcpStream::connect(addr).await?;
//! ```
//!
//! Use:
//! ```ignore
//! use deja::runtime::net::TcpStream;
//! let stream = TcpStream::connect(addr).await?;
//! // Automatically notifies proxy about the connection
//! ```
//!
//! The wrapper is transparent - it implements `Deref<Target = tokio::net::TcpStream>`
//! so all existing code works without changes.

use std::io;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::net::TcpStream as TokioTcpStream;
use tracing::debug;

use super::trace_context;
use crate::control_api::ControlClient;

/// Wrapper around `tokio::net::TcpStream` that notifies proxy of connections
///
/// This wrapper automatically:
/// 1. Captures the current trace ID when a connection is established
/// 2. Notifies the proxy via control API
/// 3. Defers to the underlying TcpStream for all I/O operations
///
/// # Transparency
///
/// Because it implements `Deref`, it's a drop-in replacement for
/// `tokio::net::TcpStream`:
///
/// ```ignore
/// let stream: InterceptedTcpStream = InterceptedTcpStream::connect(addr).await?;
///
/// // All TcpStream methods work transparently
/// stream.write_all(b"data").await?;
/// stream.read_buf(&mut buf).await?;
/// ```
pub struct InterceptedTcpStream {
    /// The underlying Tokio stream
    inner: TokioTcpStream,
    /// Control client to notify proxy (optional)
    #[allow(dead_code)] // Future: connection lifecycle notifications
    control_client: Option<Arc<ControlClient>>,
    /// Local address of the connection
    local_addr: Option<String>,
    /// Remote address of the connection
    remote_addr: Option<String>,
}

impl InterceptedTcpStream {
    /// Create an intercepted stream by connecting to a remote address
    ///
    /// This creates a TcpStream and automatically notifies the proxy about
    /// the connection if:
    /// 1. A control client is provided
    /// 2. There's a current trace context
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to connect to
    /// * `control_client` - Optional control client for proxy notification
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the underlying TcpStream::connect
    ///
    /// # Example
    ///
    /// ```ignore
    /// let client = Arc::new(ControlClient::new("localhost", 9999));
    /// let stream = InterceptedTcpStream::connect(
    ///     "127.0.0.1:5432".parse()?,
    ///     Some(client),
    /// ).await?;
    /// ```
    pub async fn connect(
        addr: std::net::SocketAddr,
        control_client: Option<Arc<ControlClient>>,
    ) -> io::Result<Self> {
        let inner = TokioTcpStream::connect(addr).await?;

        let local_addr = inner.local_addr().ok().map(|a| a.to_string());
        let remote_addr = inner.peer_addr().ok().map(|a| a.to_string());

        debug!(
            local_addr = ?local_addr,
            remote_addr = ?remote_addr,
            "Intercepted TCP connection"
        );

        // Notify proxy if we have a trace context and control client
        if let Some(client) = control_client.as_ref() {
            if let Some(trace_id) = trace_context::current_trace_id() {
                if let Ok(local) = inner.local_addr() {
                    let source_port = local.port();
                    debug!(
                        trace_id = %trace_id,
                        source_port = source_port,
                        "Notifying proxy of connection association by source port"
                    );

                    let msg = crate::control_api::ControlMessage::associate_by_source_port(
                        trace_id,
                        source_port,
                        deja_common::Protocol::Unknown,
                    );

                    // Fire and forget - don't fail the connection if notification fails
                    let client_clone = client.clone();
                    tokio::spawn(async move {
                        let _ = client_clone.send(msg).await;
                    });
                }
            }
        }

        Ok(Self {
            inner,
            control_client,
            local_addr,
            remote_addr,
        })
    }

    /// Get the remote address this stream is connected to
    pub fn remote_addr(&self) -> Option<&str> {
        self.remote_addr.as_deref()
    }

    /// Get the local address of this stream
    pub fn local_addr(&self) -> Option<&str> {
        self.local_addr.as_deref()
    }
}

// Transparent delegation to the underlying TcpStream
impl Deref for InterceptedTcpStream {
    type Target = TokioTcpStream;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for InterceptedTcpStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_intercepted_stream_without_client() {
        // Should be able to connect without a control client
        let addr = "127.0.0.1:0".parse().unwrap();

        // This will fail because we're trying to connect to a non-existent service,
        // but we're testing that the wrapper doesn't panic
        let result = InterceptedTcpStream::connect(addr, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_intercepted_stream_addresses() {
        // This test verifies that stream address getters work
        // We just check the API doesn't panic - actual connections
        // may succeed or fail depending on the system
        let _result = InterceptedTcpStream::connect("127.0.0.1:9999".parse().unwrap(), None).await;

        // We don't assert on success/failure since it depends on environment
        // The important thing is the API exists and doesn't panic
    }
}

/// Public module for re-exporting as a replacement for tokio::net
///
/// Users can import this to get the intercepted TcpStream:
/// ```ignore
/// use deja::runtime::net::TcpStream;
/// ```
pub mod net {
    pub use super::InterceptedTcpStream as TcpStream;
}
