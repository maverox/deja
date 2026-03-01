//! DejaRuntime Trait
//!
//! Provides the core abstraction for non-deterministic operations with
//! scope-based capture/replay. Each runtime instance is bound to a
//! (trace_id, task_id) pair and manages per-kind sequence counters.
//!
//! # Unified Approach
//!
//! All non-deterministic value capture and replay goes through this single
//! trait. There is no separate `DejaContext` — the Runtime IS the context.
//!
//! # Usage
//!
//! ```ignore
//! use deja_common::runtime::deja_run;
//!
//! // Record mode: captures the generated value
//! // Replay mode: returns the previously recorded value
//! let id = deja_run(&*runtime, "order_id", || Uuid::new_v4()).await;
//! let ts = deja_run(&*runtime, "timestamp", || SystemTime::now()).await;
//! ```

use crate::control::ControlClient;
use crate::{DejaMode, ScopeId};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

/// Core trait for deterministic capture/replay of non-deterministic values.
///
/// Object-safe: all methods work through `dyn DejaRuntime`.
/// For the generic `run()` helper, use [`deja_run`].
#[async_trait]
pub trait DejaRuntime: Send + Sync {
    /// Record a non-deterministic value with a kind/tag.
    /// Internally increments the per-kind sequence counter.
    async fn capture_value(&self, kind: &str, value: String);

    /// Replay a recorded value by kind/tag.
    /// Internally increments the per-kind sequence counter.
    async fn replay_value(&self, kind: &str) -> Option<String>;

    /// Get the trace ID this runtime is bound to.
    fn trace_id(&self) -> String;

    /// Get the task ID (hierarchical: "0", "0.1", "0.1.2").
    fn task_id(&self) -> String;

    /// Get the scope ID for this runtime's task scope.
    fn scope_id(&self) -> ScopeId;

    /// Get the current mode (Record or Replay).
    fn mode(&self) -> DejaMode;

    /// Create a child runtime for spawned tasks.
    /// The child gets a new task_id (e.g., parent "0" → child "0.0").
    fn child(&self) -> Arc<dyn DejaRuntime>;

    /// Flush any pending batched captures to the proxy.
    async fn flush(&self);

    /// Get a control client for proxy communication.
    fn control_client(&self) -> ControlClient;
}

/// Execute a function deterministically through the runtime.
///
/// - **Record mode**: runs the generator, captures the result, returns it
/// - **Replay mode**: returns the previously recorded value, falling back to generator
///
/// This is the primary API for capturing non-deterministic values.
///
/// # Example
///
/// ```ignore
/// let id = deja_run(&*runtime, "order_id", || Uuid::new_v4()).await;
/// let ts = deja_run(&*runtime, "created_at", || SystemTime::now()).await;
/// ```
pub async fn deja_run<T, F>(runtime: &dyn DejaRuntime, kind: &str, generate: F) -> T
where
    T: Serialize + DeserializeOwned + Send,
    F: FnOnce() -> T + Send,
{
    match runtime.mode() {
        DejaMode::Replay => {
            if let Some(val_str) = runtime.replay_value(kind).await {
                if let Ok(val) = serde_json::from_str(&val_str) {
                    return val;
                }
            }
            // Fallback to generation if replay fails
            tracing::warn!(
                kind = %kind,
                "Replay value not found or deserialization failed, generating fresh value"
            );
            generate()
        }
        DejaMode::Record => {
            let value = generate();
            if let Ok(val_str) = serde_json::to_string(&value) {
                runtime.capture_value(kind, val_str).await;
            }
            value
        }
    }
}

/// Synchronous version of the DejaRuntime trait.
pub trait SyncDejaRuntime: Send + Sync {
    fn capture_value(&self, kind: &str, value: String);
    fn replay_value(&self, kind: &str) -> Option<String>;
    fn mode(&self) -> DejaMode;
}

/// Execute a function deterministically (synchronous version).
pub fn deja_run_sync<T, F>(runtime: &dyn SyncDejaRuntime, kind: &str, generate: F) -> T
where
    T: Serialize + DeserializeOwned + Send,
    F: FnOnce() -> T + Send,
{
    match runtime.mode() {
        DejaMode::Replay => {
            if let Some(val_str) = runtime.replay_value(kind) {
                if let Ok(val) = serde_json::from_str(&val_str) {
                    return val;
                }
            }
            generate()
        }
        DejaMode::Record => {
            let value = generate();
            if let Ok(val_str) = serde_json::to_string(&value) {
                runtime.capture_value(kind, val_str);
            }
            value
        }
    }
}
