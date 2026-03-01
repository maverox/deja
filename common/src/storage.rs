//! Pluggable storage interface for recording and replay
//!
//! Implementations must be safe for concurrent callers.
//! `save_batch` is atomic (all-or-nothing).
//! `flush` ensures durability — after flush returns, data survives process crash.

use async_trait::async_trait;

/// Storage backend for recorded events.
///
/// Thread-safety: implementations must be safe for concurrent callers.
/// save_event may buffer internally; call flush() to ensure durability.
#[async_trait]
pub trait RecordingStore: Send + Sync {
    /// Save a single event. May buffer internally.
    async fn save_event(&self, session_id: &str, event: Vec<u8>) -> Result<(), StorageError>;

    /// Save a batch of events atomically (all succeed or all fail).
    async fn save_batch(&self, session_id: &str, events: Vec<Vec<u8>>) -> Result<(), StorageError>;

    /// Flush buffered events to durable storage.
    async fn flush(&self, session_id: &str) -> Result<(), StorageError>;

    /// Load all events for a session, in recorded order.
    async fn load_events(&self, session_id: &str) -> Result<Vec<Vec<u8>>, StorageError>;
}

/// Storage backend for replay indexes.
#[async_trait]
pub trait IndexStore: Send + Sync {
    /// Save a serialized index for a session.
    async fn save_index(&self, session_id: &str, data: Vec<u8>) -> Result<(), StorageError>;

    /// Load a serialized index for a session. Returns None if not found.
    async fn load_index(&self, session_id: &str) -> Result<Option<Vec<u8>>, StorageError>;
}

/// Configuration for storage backend selection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StorageConfig {
    /// Local filesystem storage (default)
    LocalFile {
        base_path: String,
        /// Storage format: "json" or "binary" (default: "binary")
        format: Option<String>,
    },
    /// Kafka-based storage (requires "kafka" feature)
    Kafka {
        brokers: String,
        topic_prefix: String,
    },
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::LocalFile {
            base_path: "./recordings".to_string(),
            format: None,
        }
    }
}

/// Errors from storage operations.
#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    Serialization(String),
    NotFound(String),
    Other(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Self::NotFound(msg) => write!(f, "Not found: {}", msg),
            Self::Other(msg) => write!(f, "Storage error: {}", msg),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
