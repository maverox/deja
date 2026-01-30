//! Control API - Communication with Deja Proxy
//!
//! This module provides the protocol and client for communicating with the Deja Proxy
//! about trace lifecycle and connection associations.
//!
//! # Architecture
//!
//! The control API is a simple HTTP-based out-of-band channel (separate from the
//! main data path) that allows DejaRuntime to notify the proxy about:
//!
//! 1. **Trace Lifecycle**: When traces start/end
//! 2. **Connection Associations**: Which trace owns which connection
//! 3. **Pool Reuse**: When connections are reused for different traces
//!
//! # Example
//!
//! ```ignore
//! use deja::control_api::{ControlClient, ControlMessage};
//!
//! let client = ControlClient::new("localhost", 9999);
//!
//! // Notify proxy when trace starts
//! client.send(ControlMessage::start_trace("request-123")).await?;
//!
//! // Notify proxy when connection is established
//! client.send(ControlMessage::associate_connection(
//!     "request-123",
//!     "127.0.0.1:5433"
//! )).await?;
//! ```

pub mod protocol {
    pub use deja_common::control::ControlMessage;
    pub use deja_common::control::ControlResponse;
}
pub mod client {
    pub use deja_common::control::ControlClient;
}

pub use deja_common::control::*;
