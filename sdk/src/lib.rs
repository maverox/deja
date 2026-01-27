//! Deja SDK - Lightweight client for record/replay integration
//!
//! This crate provides the minimal interface for applications to integrate with
//! Deja's record/replay system. It contains:
//!
//! - `DejaRuntime` trait: Interface for non-deterministic operations
//! - `NetworkRuntime`: HTTP client that communicates with the Deja proxy
//! - `ProductionRuntime`: Pass-through implementation for production use
//!
//! # Usage
//!
//! ```ignore
//! use deja_sdk::{DejaRuntime, get_runtime};
//!
//! // Get runtime based on DEJA_MODE environment variable
//! let runtime = get_runtime();
//!
//! // Use for non-deterministic operations
//! let uuid = runtime.uuid().await;
//! let timestamp = runtime.now_millis().await;
//!
//! // Capture custom values
//! runtime.capture("my_tag", "my_value").await;
//! ```

mod runtime;

pub use runtime::{
    capture_sync, deterministic, get_runtime, get_runtime_arc, DejaAsyncBoundary, DejaRuntime,
    NetworkRuntime, ProductionRuntime,
};

pub mod sync_runtime;
pub use sync_runtime::{
    get_sync_runtime, SyncDejaRuntime, SyncNetworkRuntime, SyncProductionRuntime,
};
