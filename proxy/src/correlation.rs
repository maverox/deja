//! Trace Correlation - Connection to Trace ID Mapping
//!
//! Maintains the mapping between connections (identified by socket address) and
//! the trace IDs they belong to. This enables accurate event attribution when
//! multiple requests are processed concurrently.
//!
//! # Overview
//!
//! When the proxy records events from connections, it needs to know which trace
//! each event belongs to. This module maintains that mapping by:
//!
//! 1. **Recording trace starts/ends**: Track active traces and their lifetimes
//! 2. **Associating connections**: Map each connection to its trace
//! 3. **Updating associations**: Handle connection pool reuse (same connection, different trace)

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, trace};

/// Metadata about a trace
#[derive(Clone, Debug)]
pub struct TraceMetadata {
    /// The unique trace ID
    pub trace_id: String,
    /// Timestamp when this trace started (nanoseconds since epoch)
    pub start_time_ns: u64,
    /// Timestamp when this trace ended (None if still active)
    pub end_time_ns: Option<u64>,
    /// Count of connections associated with this trace
    pub connection_count: usize,
}

/// Manages correlation between connections and traces
///
/// This is the main component that tracks:
/// - Which traces are currently active
/// - Which connections belong to which traces
/// - Connection pool reuse patterns
pub struct TraceCorrelator {
    /// Maps connection address (as string) to trace ID
    /// e.g., "127.0.0.1:5433" -> "request-abc123"
    connection_to_trace: Arc<RwLock<HashMap<String, String>>>,

    /// Metadata about active and completed traces
    /// Maps trace_id -> TraceMetadata
    active_traces: Arc<RwLock<HashMap<String, TraceMetadata>>>,
}

impl TraceCorrelator {
    /// Create a new trace correlator
    pub fn new() -> Self {
        Self {
            connection_to_trace: Arc::new(RwLock::new(HashMap::new())),
            active_traces: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record that a new trace has started
    ///
    /// This is called when a request arrives and a new trace context is initialized.
    ///
    /// # Arguments
    ///
    /// * `trace_id` - The unique trace ID for this request
    /// * `timestamp_ns` - When the trace started (nanoseconds since epoch)
    pub async fn start_trace(&self, trace_id: String, timestamp_ns: u64) {
        debug!(trace_id = %trace_id, timestamp_ns = timestamp_ns, "Starting trace");

        let metadata = TraceMetadata {
            trace_id: trace_id.clone(),
            start_time_ns: timestamp_ns,
            end_time_ns: None,
            connection_count: 0,
        };

        self.active_traces
            .write()
            .await
            .insert(trace_id, metadata);
    }

    /// Record that a trace has ended
    ///
    /// This is called when the response is sent for a trace.
    ///
    /// # Arguments
    ///
    /// * `trace_id` - The trace ID that ended
    /// * `timestamp_ns` - When the trace ended (nanoseconds since epoch)
    pub async fn end_trace(&self, trace_id: String, timestamp_ns: u64) {
        debug!(trace_id = %trace_id, timestamp_ns = timestamp_ns, "Ending trace");

        if let Some(metadata) = self.active_traces.write().await.get_mut(&trace_id) {
            metadata.end_time_ns = Some(timestamp_ns);
        }
    }

    /// Associate a connection with a trace
    ///
    /// Called when a new connection is established in the context of a trace.
    ///
    /// # Arguments
    ///
    /// * `connection_id` - The connection identifier (typically peer address)
    /// * `trace_id` - The trace this connection belongs to
    pub async fn associate_connection(&self, connection_id: String, trace_id: String) {
        debug!(
            connection = %connection_id,
            trace = %trace_id,
            "Associating connection with trace"
        );

        // Update the mapping
        self.connection_to_trace
            .write()
            .await
            .insert(connection_id.clone(), trace_id.clone());

        // Increment connection count for this trace
        if let Some(metadata) = self.active_traces.write().await.get_mut(&trace_id) {
            metadata.connection_count += 1;
        }
    }

    /// Update a connection's trace (for connection pool reuse)
    ///
    /// When a connection is borrowed from a pool and used by a different trace,
    /// we need to update its mapping.
    ///
    /// # Arguments
    ///
    /// * `connection_id` - The connection being reused
    /// * `new_trace_id` - The new trace this connection now belongs to
    pub async fn update_connection(&self, connection_id: String, new_trace_id: String) {
        trace!(
            connection = %connection_id,
            new_trace = %new_trace_id,
            "Updating connection trace association"
        );

        self.connection_to_trace
            .write()
            .await
            .insert(connection_id, new_trace_id);
    }

    /// Get the trace ID for a given connection
    ///
    /// This is called by event handlers to determine which trace an event belongs to.
    ///
    /// # Arguments
    ///
    /// * `connection_id` - The connection to look up
    ///
    /// # Returns
    ///
    /// The trace ID if found, or None if this connection is not mapped
    pub async fn get_trace_id(&self, connection_id: &str) -> Option<String> {
        self.connection_to_trace
            .read()
            .await
            .get(connection_id)
            .cloned()
    }

    /// Remove a connection from the mapping (when it closes)
    ///
    /// # Arguments
    ///
    /// * `connection_id` - The connection to remove
    pub async fn cleanup_connection(&self, connection_id: String) {
        trace!(connection = %connection_id, "Cleaning up connection mapping");
        self.connection_to_trace.write().await.remove(&connection_id);
    }

    /// Get metadata about a trace
    ///
    /// # Arguments
    ///
    /// * `trace_id` - The trace to look up
    ///
    /// # Returns
    ///
    /// The trace metadata if the trace is/was active
    pub async fn get_trace_metadata(&self, trace_id: &str) -> Option<TraceMetadata> {
        self.active_traces
            .read()
            .await
            .get(trace_id)
            .cloned()
    }

    /// Get all active trace IDs
    ///
    /// Returns the IDs of all traces that have started but not yet ended.
    pub async fn get_active_traces(&self) -> Vec<String> {
        self.active_traces
            .read()
            .await
            .iter()
            .filter(|(_, metadata)| metadata.end_time_ns.is_none())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get all trace IDs (active and inactive)
    pub async fn get_all_traces(&self) -> Vec<String> {
        self.active_traces
            .read()
            .await
            .keys()
            .cloned()
            .collect()
    }

    /// Cleanup old traces (for memory management)
    ///
    /// Removes traces that ended more than `max_age_ns` nanoseconds ago.
    ///
    /// # Arguments
    ///
    /// * `max_age_ns` - The maximum age in nanoseconds (e.g., 5 minutes = 300_000_000_000)
    pub async fn cleanup_old_traces(&self, max_age_ns: u64) {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let mut traces = self.active_traces.write().await;
        traces.retain(|_, metadata| {
            match metadata.end_time_ns {
                Some(end_time) if (now_ns - end_time) > max_age_ns => false,
                _ => true,
            }
        });
    }
}

impl Default for TraceCorrelator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_start_and_end_trace() {
        let correlator = TraceCorrelator::new();
        let trace_id = "test-123".to_string();

        // Start trace
        correlator.start_trace(trace_id.clone(), 1000).await;
        let metadata = correlator.get_trace_metadata(&trace_id).await;
        assert!(metadata.is_some());
        assert_eq!(metadata.unwrap().start_time_ns, 1000);

        // End trace
        correlator.end_trace(trace_id.clone(), 2000).await;
        let metadata = correlator.get_trace_metadata(&trace_id).await;
        assert!(metadata.is_some());
        assert_eq!(metadata.unwrap().end_time_ns, Some(2000));
    }

    #[tokio::test]
    async fn test_associate_connection() {
        let correlator = TraceCorrelator::new();
        let trace_id = "trace-abc".to_string();
        let conn_id = "127.0.0.1:5432".to_string();

        // Start trace and associate connection
        correlator.start_trace(trace_id.clone(), 1000).await;
        correlator
            .associate_connection(conn_id.clone(), trace_id.clone())
            .await;

        // Verify association
        let mapped = correlator.get_trace_id(&conn_id).await;
        assert_eq!(mapped, Some(trace_id.clone()));

        // Verify connection count
        let metadata = correlator.get_trace_metadata(&trace_id).await.unwrap();
        assert_eq!(metadata.connection_count, 1);
    }

    #[tokio::test]
    async fn test_update_connection_for_reuse() {
        let correlator = TraceCorrelator::new();
        let conn_id = "127.0.0.1:5432".to_string();

        // Start two traces
        correlator.start_trace("trace-1".to_string(), 1000).await;
        correlator.start_trace("trace-2".to_string(), 2000).await;

        // Connection belongs to trace-1
        correlator
            .associate_connection(conn_id.clone(), "trace-1".to_string())
            .await;
        assert_eq!(
            correlator.get_trace_id(&conn_id).await,
            Some("trace-1".to_string())
        );

        // Connection reused by trace-2
        correlator
            .update_connection(conn_id.clone(), "trace-2".to_string())
            .await;
        assert_eq!(
            correlator.get_trace_id(&conn_id).await,
            Some("trace-2".to_string())
        );
    }

    #[tokio::test]
    async fn test_cleanup_connection() {
        let correlator = TraceCorrelator::new();
        let conn_id = "127.0.0.1:5432".to_string();

        correlator.start_trace("trace-1".to_string(), 1000).await;
        correlator
            .associate_connection(conn_id.clone(), "trace-1".to_string())
            .await;

        assert!(correlator.get_trace_id(&conn_id).await.is_some());

        correlator.cleanup_connection(conn_id.clone()).await;
        assert!(correlator.get_trace_id(&conn_id).await.is_none());
    }

    #[tokio::test]
    async fn test_get_active_traces() {
        let correlator = TraceCorrelator::new();

        // Start two traces
        correlator.start_trace("trace-1".to_string(), 1000).await;
        correlator.start_trace("trace-2".to_string(), 2000).await;

        let active = correlator.get_active_traces().await;
        assert_eq!(active.len(), 2);

        // End one trace
        correlator.end_trace("trace-1".to_string(), 3000).await;

        let active = correlator.get_active_traces().await;
        assert_eq!(active.len(), 1);
        assert!(active.contains(&"trace-2".to_string()));
    }

    #[tokio::test]
    async fn test_multiple_connections_per_trace() {
        let correlator = TraceCorrelator::new();
        let trace_id = "trace-xyz".to_string();

        correlator.start_trace(trace_id.clone(), 1000).await;

        // Associate multiple connections
        for i in 0..3 {
            let conn = format!("127.0.0.1:{}", 5000 + i);
            correlator
                .associate_connection(conn, trace_id.clone())
                .await;
        }

        let metadata = correlator.get_trace_metadata(&trace_id).await.unwrap();
        assert_eq!(metadata.connection_count, 3);
    }
}
