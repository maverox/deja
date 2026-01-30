//! Common types shared between deja crates
//!
//! This crate contains shared types and utilities used across:
//! - deja-core (replay engine, recording)
//! - deja-proxy (connection handling, control API)
//! - deja-sdk (client-side instrumentation)

pub mod control;
mod protocol;
pub mod runtime;
mod sequence;

pub use control::{ControlClient, ControlError, ControlMessage};
pub use protocol::Protocol;
pub use runtime::DejaRuntime;
pub use sequence::{SequenceKey, SequenceTracker};
