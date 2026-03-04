//! Task spawning wrappers that propagate trace context
//!
//! This module provides replacements for `tokio::spawn` and `tokio::task::spawn_blocking`
//! that automatically propagate the trace ID from parent to child tasks.
//!
//! # Problem Solved
//!
//! Background tasks spawned after a response is sent lose their parent's trace context.
//! Example:
//! ```ignore
//! // Request received, trace_id = "abc123"
//! tokio::spawn(async { // Child task spawned - loses trace_id!
//!     // This code runs after response sent
//!     audit_log().await // No trace context ❌
//! });
//! ```
//!
//! # Solution
//!
//! Use `deja::spawn` instead:
//! ```ignore
//! // Request received, trace_id = "abc123"
//! deja::spawn(async { // Child task spawned - inherits trace_id!
//!     // This code runs after response sent
//!     audit_log().await // Has parent's trace_id ✓
//! });
//! ```

use std::future::Future;
use tokio::task::JoinHandle;

use super::trace_context;

/// Spawn an async task with trace context propagation
///
/// This is a drop-in replacement for `tokio::spawn` that:
/// 1. Captures the current trace ID from the parent task
/// 2. Executes the future within that trace context
/// 3. Ensures child tasks inherit the parent's trace ID
///
/// # Arguments
///
/// * `future` - The async operation to spawn
///
/// # Returns
///
/// A `JoinHandle` to await the spawned task's result
///
/// # Example
///
/// ```ignore
/// use deja::spawn;
///
/// // In request handler
/// spawn(async {
///     // This code runs with parent's trace_id
///     background_work().await;
/// });
/// ```
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Capture current trace_id from parent task
    let trace_id = trace_context::current_trace_id();

    // Spawn the task
    tokio::spawn(async move {
        // If parent had a trace context, propagate it to child
        match trace_id {
            Some(id) => {
                // Execute future within parent's trace context
                trace_context::with_trace_id(id, future).await
            }
            None => {
                // No trace context in parent (shouldn't happen in production)
                // Just execute the future without context
                future.await
            }
        }
    })
}

/// Spawn a blocking task with trace context propagation
///
/// This is a drop-in replacement for `tokio::task::spawn_blocking` that:
/// 1. Captures the current trace ID from the parent task
/// 2. Stores it in thread-local storage for the blocking task
/// 3. Makes it accessible via `get_blocking_trace_id()` in the blocking context
///
/// # Arguments
///
/// * `f` - The blocking operation to spawn
///
/// # Returns
///
/// A `JoinHandle` to await the spawned task's result
///
/// # Example
///
/// ```ignore
/// use deja::spawn_blocking;
///
/// // In async handler
/// let result = spawn_blocking(|| {
///     // This runs in a thread pool
///     let trace_id = get_blocking_trace_id();
///     expensive_computation(trace_id)
/// }).await;
/// ```
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    // Capture current trace_id from async context
    let trace_id = trace_context::current_trace_id();

    // Spawn blocking task
    tokio::task::spawn_blocking(move || {
        // Store trace_id in thread-local for this blocking task
        if let Some(id) = trace_id {
            BLOCKING_TRACE_ID.with(|cell| {
                *cell.borrow_mut() = Some(id);
            });
        }

        // Execute the blocking function
        f()
    })
}

/// Get the trace ID for a blocking task (thread-local)
///
/// Call this from within code spawned via `spawn_blocking` to access
/// the parent task's trace ID.
///
/// # Example
///
/// ```ignore
/// let result = spawn_blocking(|| {
///     if let Some(trace_id) = get_blocking_trace_id() {
///         println!("Blocking task has trace: {}", trace_id);
///     }
///     // do work
/// }).await;
/// ```
pub fn get_blocking_trace_id() -> Option<String> {
    BLOCKING_TRACE_ID.with(|cell| cell.borrow().clone())
}

// Thread-local storage for trace ID in blocking contexts
// This is necessary because task-local storage doesn't work in synchronous
// blocking tasks. We store the parent's trace ID here so it's accessible
// in the blocking thread.
thread_local! {
    static BLOCKING_TRACE_ID: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::trace_context;

    #[tokio::test]
    async fn test_spawn_propagates_trace_id() {
        let parent_trace_id = "parent-123".to_string();

        let result = trace_context::with_trace_id(parent_trace_id.clone(), async {
            // Verify parent has trace_id
            assert_eq!(
                trace_context::current_trace_id(),
                Some(parent_trace_id.clone())
            );

            // Spawn child task
            let handle = spawn(async {
                // Child should inherit parent's trace_id
                trace_context::current_trace_id()
            });

            handle.await.unwrap()
        })
        .await;

        assert_eq!(result, Some(parent_trace_id));
    }

    #[tokio::test]
    async fn test_spawn_without_trace_context() {
        // Spawning without trace context should work (just won't have trace_id)
        let handle = spawn(async {
            // No trace context set, so should return None
            trace_context::current_trace_id()
        });

        let result = handle.await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_spawn_blocking_propagates_trace_id() {
        let parent_trace_id = "blocking-parent-456".to_string();

        let result = trace_context::with_trace_id(parent_trace_id.clone(), async {
            // Spawn blocking task
            let handle = spawn_blocking(|| {
                // Get trace_id from blocking context
                get_blocking_trace_id()
            });

            handle.await.unwrap()
        })
        .await;

        assert_eq!(result, Some(parent_trace_id));
    }

    #[tokio::test]
    async fn test_spawn_blocking_without_trace_context() {
        let handle = spawn_blocking(|| {
            // No trace context set
            get_blocking_trace_id()
        });

        let result = handle.await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_nested_spawns_propagate_context() {
        let trace_id = "nested-789".to_string();
        let trace_id_clone = trace_id.clone();
        let trace_id_clone2 = trace_id.clone();

        let result = trace_context::with_trace_id(trace_id_clone, async {
            // Spawn level 1
            let handle1 = spawn(async {
                assert_eq!(
                    trace_context::current_trace_id().as_deref(),
                    Some("nested-789")
                );

                // Spawn level 2 from within level 1
                let handle2 = spawn(async { trace_context::current_trace_id() });

                handle2.await.unwrap()
            });

            handle1.await.unwrap()
        })
        .await;

        assert_eq!(result, Some(trace_id_clone2));
    }
}
