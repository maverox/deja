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
//! # Hierarchical Task IDs
//!
//! For concurrent task tracking, task IDs are hierarchical:
//! - `"0"` - Root task (the main request handler)
//! - `"0.0"` - First child spawned from root
//! - `"0.1"` - Second child spawned from root
//! - `"0.0.0"` - First grandchild (child of first child)
//!
//! This allows deterministic replay of concurrent operations by grouping
//! captures per task.
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
//!     // Spawned tasks inherit the trace_id and get unique task_ids
//!     let handle = spawn_with_task_id(async {
//!         // trace_id is same as parent
//!         // task_id is "0.0" (first child of "0")
//!     });
//! }).await;
//! ```

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;
use uuid::Uuid;

tokio::task_local! {
    /// Current trace ID for correlation
    /// Empty string means no trace context (should not happen in production)
    pub static TRACE_ID: String;

    /// Current task ID for concurrent task tracking
    /// Hierarchical format: "0", "0.1", "0.1.2", etc.
    pub static TASK_ID: String;

    /// Counter for child task IDs (per-task)
    pub static CHILD_COUNTER: AtomicU64;
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

/// Get the current task ID if available
///
/// Returns `Some(task_id)` if we're within a task context,
/// or `None` if not. Task IDs are hierarchical (e.g., "0.1.2").
pub fn current_task_id() -> Option<String> {
    TASK_ID.try_with(|id| id.clone()).ok()
}

/// Execute a future with both trace ID and task ID set
///
/// This is the recommended way to start a trace context.
/// Sets task_id to "0" (root task).
pub async fn with_trace_context<F, R>(trace_id: String, future: F) -> R
where
    F: Future<Output = R>,
{
    TRACE_ID
        .scope(trace_id, async {
            TASK_ID
                .scope("0".to_string(), async {
                    CHILD_COUNTER
                        .scope(AtomicU64::new(0), future)
                        .await
                })
                .await
        })
        .await
}

/// Spawn a task with trace and task ID propagation
///
/// The spawned task inherits the parent's trace_id and gets a new
/// hierarchical task_id (e.g., parent "0" → child "0.0").
///
/// # Returns
/// A tuple of (JoinHandle, child_task_id) for tracking purposes.
pub fn spawn_with_task_id<F, T>(future: F) -> (JoinHandle<T>, String)
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let parent_trace = current_trace_id().unwrap_or_default();
    let parent_task = current_task_id().unwrap_or_else(|| "0".to_string());

    // Get next child number from parent's counter
    let child_num = CHILD_COUNTER
        .try_with(|counter| counter.fetch_add(1, Ordering::SeqCst))
        .unwrap_or(0);

    let child_task_id = format!("{}.{}", parent_task, child_num);
    let task_id_clone = child_task_id.clone();

    let handle = tokio::spawn(async move {
        TRACE_ID
            .scope(parent_trace, async {
                TASK_ID
                    .scope(task_id_clone, async {
                        CHILD_COUNTER
                            .scope(AtomicU64::new(0), future)
                            .await
                    })
                    .await
            })
            .await
    });

    (handle, child_task_id)
}

/// Spawn a task with trace and task ID propagation (handle only)
///
/// Convenience wrapper around `spawn_with_task_id` that only returns the handle.
pub fn spawn_traced<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    spawn_with_task_id(future).0
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

    #[tokio::test]
    async fn test_with_trace_context_sets_root_task_id() {
        let trace_id = "test-trace".to_string();
        let result = with_trace_context(trace_id.clone(), async {
            (current_trace_id(), current_task_id())
        })
        .await;

        assert_eq!(result, (Some(trace_id), Some("0".to_string())));
    }

    #[tokio::test]
    async fn test_spawn_with_task_id_creates_hierarchical_ids() {
        let trace_id = "spawn-test".to_string();

        let result = with_trace_context(trace_id.clone(), async {
            // Spawn first child
            let (handle1, task_id1) = spawn_with_task_id(async {
                current_task_id()
            });

            // Spawn second child
            let (handle2, task_id2) = spawn_with_task_id(async {
                current_task_id()
            });

            let child1_result = handle1.await.unwrap();
            let child2_result = handle2.await.unwrap();

            (task_id1, task_id2, child1_result, child2_result)
        })
        .await;

        assert_eq!(result.0, "0.0"); // First child task ID
        assert_eq!(result.1, "0.1"); // Second child task ID
        assert_eq!(result.2, Some("0.0".to_string())); // Task ID inside first child
        assert_eq!(result.3, Some("0.1".to_string())); // Task ID inside second child
    }

    #[tokio::test]
    async fn test_spawn_nested_creates_deep_hierarchy() {
        let trace_id = "nested-spawn-test".to_string();

        let result = with_trace_context(trace_id.clone(), async {
            let (outer_handle, outer_task_id) = spawn_with_task_id(async {
                // Spawn from inside the child
                let (inner_handle, inner_task_id) = spawn_with_task_id(async {
                    current_task_id()
                });

                let inner_result = inner_handle.await.unwrap();
                (inner_task_id, inner_result)
            });

            let outer_result = outer_handle.await.unwrap();
            (outer_task_id, outer_result)
        })
        .await;

        assert_eq!(result.0, "0.0"); // First child
        assert_eq!((result.1).0, "0.0.0"); // Grandchild task ID
        assert_eq!((result.1).1, Some("0.0.0".to_string())); // Inside grandchild
    }

    #[tokio::test]
    async fn test_spawn_traced_propagates_trace_id() {
        let trace_id = "traced-spawn-test".to_string();

        let result = with_trace_context(trace_id.clone(), async {
            let handle = spawn_traced(async {
                current_trace_id()
            });

            handle.await.unwrap()
        })
        .await;

        assert_eq!(result, Some(trace_id));
    }

    #[tokio::test]
    async fn test_task_lineage_deterministic_single_spawn() {
        let trace_id = "lineage-test-1".to_string();

        let result = with_trace_context(trace_id.clone(), async {
            let (handle, task_id) = spawn_with_task_id(async {
                current_task_id()
            });

            let inner_task_id = handle.await.unwrap();
            (task_id, inner_task_id)
        })
        .await;

        assert_eq!(result.0, "0.0");
        assert_eq!(result.1, Some("0.0".to_string()));
    }

    #[tokio::test]
    async fn test_task_lineage_deterministic_multiple_spawns() {
        let trace_id = "lineage-test-2".to_string();

        let result = with_trace_context(trace_id.clone(), async {
            let mut task_ids = Vec::new();

            for _ in 0..3 {
                let (handle, task_id) = spawn_with_task_id(async {
                    current_task_id()
                });
                let inner_id = handle.await.unwrap();
                task_ids.push((task_id, inner_id));
            }

            task_ids
        })
        .await;

        assert_eq!(result[0].0, "0.0");
        assert_eq!(result[0].1, Some("0.0".to_string()));
        assert_eq!(result[1].0, "0.1");
        assert_eq!(result[1].1, Some("0.1".to_string()));
        assert_eq!(result[2].0, "0.2");
        assert_eq!(result[2].1, Some("0.2".to_string()));
    }

    #[tokio::test]
    async fn test_task_lineage_deterministic_nested_spawns() {
        let trace_id = "lineage-test-3".to_string();

        let result = with_trace_context(trace_id.clone(), async {
            let (outer_handle, outer_task_id) = spawn_with_task_id(async {
                let mut inner_ids = Vec::new();

                for _ in 0..2 {
                    let (handle, task_id) = spawn_with_task_id(async {
                        current_task_id()
                    });
                    let inner_id = handle.await.unwrap();
                    inner_ids.push((task_id, inner_id));
                }

                inner_ids
            });

            let inner_results = outer_handle.await.unwrap();
            (outer_task_id, inner_results)
        })
        .await;

        assert_eq!(result.0, "0.0");
        assert_eq!(result.1[0].0, "0.0.0");
        assert_eq!(result.1[0].1, Some("0.0.0".to_string()));
        assert_eq!(result.1[1].0, "0.0.1");
        assert_eq!(result.1[1].1, Some("0.0.1".to_string()));
    }

    #[tokio::test]
    async fn test_task_lineage_deterministic_across_multiple_traces() {
        let trace_id_1 = "lineage-test-4a".to_string();
        let trace_id_2 = "lineage-test-4b".to_string();

        let result1 = with_trace_context(trace_id_1.clone(), async {
            let (handle, task_id) = spawn_with_task_id(async {
                current_task_id()
            });
            let inner_id = handle.await.unwrap();
            (task_id, inner_id)
        })
        .await;

        let result2 = with_trace_context(trace_id_2.clone(), async {
            let (handle, task_id) = spawn_with_task_id(async {
                current_task_id()
            });
            let inner_id = handle.await.unwrap();
            (task_id, inner_id)
        })
        .await;

        assert_eq!(result1.0, "0.0");
        assert_eq!(result1.1, Some("0.0".to_string()));
assert_eq!(result2.0, "0.0");
        assert_eq!(result2.1, Some("0.0".to_string()));
    }
}
