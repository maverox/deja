//! Common types shared between deja crates
//!
//! This crate contains shared types and utilities used across:
//! - deja-core (replay engine, recording)
//! - deja-proxy (connection handling, control API)
//! - deja-sdk (client-side instrumentation)

pub mod control;
pub mod correlation_key;
pub mod mode;
mod protocol;
pub mod runtime;
pub mod scope;
pub mod storage;

pub use control::{ControlClient, ControlError, ControlMessage};
pub use correlation_key::ConnectionKey;
pub use mode::DejaMode;
pub use protocol::Protocol;
pub use runtime::{deja_run, deja_run_sync, DejaRuntime, SyncDejaRuntime};
pub use scope::{OrderingSemantic, ScopeId, ScopeSequenceTracker};
pub use storage::{IndexStore, RecordingStore, StorageConfig, StorageError};
