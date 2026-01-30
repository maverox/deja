//! DejaRuntime Trait for Deja
//!
//! Provides the core abstraction for non-deterministic operations.

use crate::control::ControlClient;
use async_trait::async_trait;
use std::time::SystemTime;
use uuid::Uuid;

/// Trait for handling non-deterministic operations in a replay-friendly way.
#[async_trait]
pub trait DejaRuntime: Send + Sync {
    /// Generate a UUID v4
    async fn uuid(&self) -> Uuid;

    /// Generate a UUID v7 (time-ordered)
    async fn uuid_v7(&self) -> Uuid;

    /// Get current system time
    async fn now(&self) -> SystemTime;

    /// Get current time as milliseconds since epoch
    async fn now_millis(&self) -> u64;

    /// Generate a random u64
    async fn random_u64(&self) -> u64;

    /// Generate random bytes
    async fn random_bytes(&self, len: usize) -> Vec<u8>;

    /// Generate a nanoid (default 21 chars, alphanumeric)
    async fn nanoid(&self) -> String;

    /// Record a non-deterministic value with a tag.
    async fn capture(&self, tag: &str, value: &str);

    /// Replay a recorded value by tag.
    async fn replay(&self, tag: &str) -> Option<String>;

    /// Set the trace ID for correlating captures with requests.
    fn set_trace_id(&self, trace_id: String);

    /// Get the current trace ID.
    fn get_trace_id(&self) -> String;

    /// Get a ControlClient for communicating with the Proxy
    fn control_client(&self) -> ControlClient;
}
