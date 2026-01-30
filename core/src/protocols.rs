use crate::events;
use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

pub mod filter;
pub mod grpc;
pub mod http;
pub mod postgres;
pub mod redis;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Incomplete data")]
    Incomplete,
    #[error("Invalid data: {0}")]
    Invalid(String),
    #[error("Protocol error: {0}")]
    Protocol(String),
}

#[derive(Debug)]
pub struct ParseResult {
    pub events: Vec<events::RecordedEvent>,
    pub forward: Bytes,
    pub needs_more: bool,
    pub reply: Option<Bytes>,
}

#[async_trait]
pub trait ProtocolParser: Send + Sync {
    /// Unique identifier for this protocol
    fn protocol_id(&self) -> &'static str;

    /// Try to detect if this parser handles the given bytes
    /// Returns confidence score 0.0 - 1.0
    fn detect(&self, peek: &[u8]) -> f32;

    /// Create a new connection state machine
    fn new_connection(&self, connection_id: String) -> Box<dyn ConnectionParser>;

    /// Optional: Get initial bytes to send to client when starting a replay connection
    fn on_replay_init(&self) -> Option<Bytes> {
        None
    }
}

/// State machine for a single connection
#[async_trait]
pub trait ConnectionParser: Send {
    /// Feed bytes from client -> server direction
    fn parse_client_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError>;

    /// Feed bytes from server -> client direction  
    fn parse_server_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError>;

    fn connection_id(&self) -> &str;

    fn reset(&mut self);

    /// Set whether the connection is in replay mode.
    /// Used by parsers that need to generate replies (e.g. HTTP/2 ACKs)
    /// only in replay mode.
    fn set_mode(&mut self, is_replay: bool);
}
