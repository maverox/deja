//! Task-local trace context storage for trace correlation
//!
//! This module provides task-local storage for trace IDs that propagate through
//! async boundaries, enabling accurate correlation of concurrent requests and
//! background tasks.
//!
//! # Overview
//!
//! When handling concurrent requests or spawning background tasks, we need to track
//! which trace (request) each operation belongs to. Using task-local storage ensures:
//! - Trace IDs propagate through `.await` points
//! - No function signature changes needed
//! - Built on Tokio's task-local mechanism
//!
//! # Example
//!
//! ```ignore
//! // In request handler
//! let trace_id = generate_trace_id();
//! with_trace_id(trace_id.clone(), async {
//!     // This code runs with the trace_id set
//!     assert_eq!(current_trace_id(), Some(trace_id));
//!
//!     // Spawned tasks inherit the trace_id
//!     let handle = tokio::spawn(async {
//!         current_trace_id() // Same as parent
//!     });
//! }).await;
//! ```

use std::future::Future;
use uuid::Uuid;

tokio::task_local! {
    /// Current trace ID for correlation
    /// Empty string means no trace context (should not happen in production)
    pub static TRACE_ID: String;
}

/// Execute a future with a specific trace ID set in task-local storage
///
/// The trace ID is automatically available to the future and all spawned child tasks
/// through the `TRACE_ID` task-local.
///
/// # Arguments
///
/// * `trace_id` - The trace ID to set for this scope
/// * `future` - The async operation to execute within this trace context
///
/// # Example
///
/// ```ignore
/// let result = with_trace_id("request-123".to_string(), async {
///     // trace_id is available here
///     do_work().await
/// }).await;
/// ```
pub async fn with_trace_id<F, R>(trace_id: String, future: F) -> R
where
    F: Future<Output = R>,
{
    TRACE_ID.scope(trace_id, future).await
}

/// Get the current trace ID if available
///
/// Returns `Some(trace_id)` if we're currently within a trace context,
/// or `None` if not.
///
/// # Example
///
/// ```ignore
/// match current_trace_id() {
///     Some(id) => println!("Current trace: {}", id),
///     None => println!("No trace context"),
/// }
/// ```
pub fn current_trace_id() -> Option<String> {
    TRACE_ID.try_with(|id| id.clone()).ok()
}

/// Generate a new trace ID
///
/// Creates a UUID v4 as the trace ID. This is typically called when
/// a new request arrives at the service boundary.
///
/// # Example
///
/// ```ignore
/// let trace_id = generate_trace_id();
/// with_trace_id(trace_id, handler()).await
/// ```
pub fn generate_trace_id() -> String {
    Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_current_trace_id_without_context() {
        // Without setting context, should return None
        assert_eq!(current_trace_id(), None);
    }

    #[tokio::test]
    async fn test_with_trace_id_sets_context() {
        let trace_id = "test-trace-123".to_string();
        let result = with_trace_id(trace_id.clone(), async {
            current_trace_id()
        })
        .await;

        assert_eq!(result, Some(trace_id));
    }

    #[tokio::test]
    async fn test_trace_id_propagates_through_await() {
        let trace_id = "propagation-test".to_string();
        let result = with_trace_id(trace_id.clone(), async {
            // Simulate async work
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            current_trace_id()
        })
        .await;

        assert_eq!(result, Some(trace_id));
    }

    #[tokio::test]
    async fn test_nested_trace_contexts() {
        let outer_id = "outer-123".to_string();
        let inner_id = "inner-456".to_string();

        let result = with_trace_id(outer_id.clone(), async {
            assert_eq!(current_trace_id(), Some(outer_id.clone()));

            // Inner context overrides outer
            let inner_result = with_trace_id(inner_id.clone(), async {
                current_trace_id()
            })
            .await;

            assert_eq!(inner_result, Some(inner_id));

            // Outer context restored after inner block
            current_trace_id()
        })
        .await;

        assert_eq!(result, Some(outer_id));
    }

    #[tokio::test]
    async fn test_generate_trace_id_uniqueness() {
        let id1 = generate_trace_id();
        let id2 = generate_trace_id();

        assert_ne!(id1, id2);
        assert!(!id1.is_empty());
        assert!(!id2.is_empty());
    }
}
