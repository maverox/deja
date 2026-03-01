//! Hierarchical scope model for recording and replay
//!
//! Every recorded event belongs to a scope in a tree:
//!
//! ```text
//! Trace (Causality Root)
//! ├── Connection 1 (Transport Lifetime)
//! │   ├── Stream/Request 1 (Semantic Unit)
//! │   │   ├── Event 0 (scope_seq=0)
//! │   │   └── Event 1 (scope_seq=1)
//! │   └── Stream/Request 2
//! ├── Connection 2
//! └── Task 0 (SDK Capture Root)
//!     ├── capture(uuid) → kind="uuid", scope_seq=0
//!     └── Task 0.1 (Child Task)
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Ordering semantic for a scope
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OrderingSemantic {
    /// Events must replay in exact recorded order.
    /// Used for: HTTP/1 connections, single streams, transactions, SDK task captures
    StrictSequence,
    /// Child scopes can replay in any order relative to each other.
    /// Used for: HTTP/2 streams within a connection, gRPC calls, parallel tasks
    Concurrent,
}

/// A scope identifier with parsed components.
///
/// Scope IDs are hierarchical strings that encode the tree path:
///
/// | Level      | Format                                     | Example                          |
/// |------------|-------------------------------------------|----------------------------------|
/// | Trace      | `trace:{trace_id}`                        | `trace:abc123`                   |
/// | Connection | `trace:{trace_id}:conn:{N}`               | `trace:abc123:conn:0`            |
/// | Stream     | `trace:{trace_id}:conn:{N}:stream:{M}`    | `trace:abc123:conn:0:stream:3`   |
/// | Task       | `trace:{trace_id}:task:{path}`            | `trace:abc123:task:0.1.2`        |
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, Default)]
pub struct ScopeId(String);

impl ScopeId {
    /// Create a trace-level scope
    pub fn trace(trace_id: &str) -> Self {
        Self(format!("trace:{}", trace_id))
    }

    /// Create a connection-level scope
    pub fn connection(trace_id: &str, conn_index: u64) -> Self {
        Self(format!("trace:{}:conn:{}", trace_id, conn_index))
    }

    /// Create a stream-level scope (for HTTP/2, gRPC multiplexed streams)
    pub fn stream(trace_id: &str, conn_index: u64, stream_id: u32) -> Self {
        Self(format!(
            "trace:{}:conn:{}:stream:{}",
            trace_id, conn_index, stream_id
        ))
    }

    /// Create a task-level scope (for SDK non-deterministic captures)
    pub fn task(trace_id: &str, task_path: &str) -> Self {
        Self(format!("trace:{}:task:{}", trace_id, task_path))
    }

    /// Create an orphan connection scope (unassociated with any trace).
    /// Each orphan gets a unique scope to prevent sequence counter conflicts.
    pub fn orphan(conn_id: u64) -> Self {
        Self(format!("trace:orphan-{}:conn:0", conn_id))
    }

    /// Create from a raw string (used when deserializing)
    pub fn from_raw(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Get the parent scope, if any.
    ///
    /// - `trace:abc:conn:0:stream:3` → `trace:abc:conn:0`
    /// - `trace:abc:conn:0` → `trace:abc`
    /// - `trace:abc:task:0.1` → `trace:abc:task:0`
    /// - `trace:abc` → None
    pub fn parent(&self) -> Option<ScopeId> {
        // For task scopes with hierarchical paths like "0.1.2", parent is "0.1"
        if let Some(task_path) = self.task_path() {
            if let Some(dot_pos) = task_path.rfind('.') {
                let parent_path = &task_path[..dot_pos];
                let trace_id = self.trace_id();
                return Some(ScopeId::task(trace_id, parent_path));
            }
            // Task root (e.g., "0") → parent is trace
            return Some(ScopeId::trace(self.trace_id()));
        }

        // For other scopes, remove the last `:key:value` pair
        let s = &self.0;
        // Find the second-to-last colon pair
        if let Some(last_colon) = s.rfind(':') {
            let before_last = &s[..last_colon];
            if let Some(second_last_colon) = before_last.rfind(':') {
                let parent = &s[..second_last_colon];
                if !parent.is_empty() {
                    return Some(ScopeId(parent.to_string()));
                }
            }
        }
        None
    }

    /// Extract the trace_id component
    pub fn trace_id(&self) -> &str {
        // Format is always "trace:{trace_id}..." — extract between first ":" and next ":"
        let s = &self.0;
        if let Some(rest) = s.strip_prefix("trace:") {
            // Find the next colon (or end of string)
            if let Some(colon_pos) = rest.find(':') {
                &rest[..colon_pos]
            } else {
                rest
            }
        } else {
            s // Fallback — shouldn't happen with well-formed IDs
        }
    }

    /// Check if this is a connection-level scope
    pub fn is_connection(&self) -> bool {
        self.0.contains(":conn:") && !self.0.contains(":stream:")
    }

    /// Check if this is a stream-level scope
    pub fn is_stream(&self) -> bool {
        self.0.contains(":stream:")
    }

    /// Check if this is a task-level scope
    pub fn is_task(&self) -> bool {
        self.0.contains(":task:")
    }

    /// Check if this is an orphan scope
    pub fn is_orphan(&self) -> bool {
        self.0.contains("orphan-")
    }

    /// Extract connection index if this is a connection or stream scope
    pub fn connection_index(&self) -> Option<u64> {
        let s = &self.0;
        if let Some(conn_pos) = s.find(":conn:") {
            let after_conn = &s[conn_pos + 6..]; // skip ":conn:"
            let end = after_conn.find(':').unwrap_or(after_conn.len());
            after_conn[..end].parse().ok()
        } else {
            None
        }
    }

    /// Extract stream ID if this is a stream scope
    pub fn stream_id(&self) -> Option<u32> {
        let s = &self.0;
        if let Some(stream_pos) = s.find(":stream:") {
            let after_stream = &s[stream_pos + 8..]; // skip ":stream:"
            let end = after_stream.find(':').unwrap_or(after_stream.len());
            after_stream[..end].parse().ok()
        } else {
            None
        }
    }

    /// Extract task path if this is a task scope
    pub fn task_path(&self) -> Option<&str> {
        let s = &self.0;
        if let Some(task_pos) = s.find(":task:") {
            Some(&s[task_pos + 6..]) // skip ":task:"
        } else {
            None
        }
    }

    /// Get the raw string representation
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ScopeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for ScopeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ScopeId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Tracks scope_sequence counters per scope and a global counter for temporal ordering.
///
/// The two-layer model:
/// - `scope_sequence`: independent per scope, used for replay matching
/// - `global_sequence`: monotonically increasing across all scopes, used for debugging
pub struct ScopeSequenceTracker {
    sequences: HashMap<ScopeId, u64>,
    global_counter: AtomicU64,
}

impl ScopeSequenceTracker {
    pub fn new() -> Self {
        Self {
            sequences: HashMap::new(),
            global_counter: AtomicU64::new(0),
        }
    }

    /// Get and increment the next scope_sequence for a given scope
    pub fn next_scope_sequence(&mut self, scope: &ScopeId) -> u64 {
        let seq = self.sequences.entry(scope.clone()).or_insert(0);
        let current = *seq;
        *seq += 1;
        current
    }

    /// Get the next global_sequence (atomic, safe for concurrent use)
    pub fn next_global_sequence(&self) -> u64 {
        self.global_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Peek at the current scope_sequence without incrementing
    pub fn peek_scope_sequence(&self, scope: &ScopeId) -> u64 {
        self.sequences.get(scope).copied().unwrap_or(0)
    }

    /// Reset all scope sequences for a given trace
    pub fn reset_trace(&mut self, trace_id: &str) {
        let prefix = format!("trace:{}", trace_id);
        self.sequences
            .retain(|k, _| !k.as_str().starts_with(&prefix));
    }

    /// Clear all sequences
    pub fn clear(&mut self) {
        self.sequences.clear();
        self.global_counter.store(0, Ordering::SeqCst);
    }
}

impl Default for ScopeSequenceTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── ScopeId Construction ───

    #[test]
    fn test_scope_id_trace() {
        let scope = ScopeId::trace("abc123");
        assert_eq!(scope.as_str(), "trace:abc123");
        assert_eq!(scope.trace_id(), "abc123");
        assert!(!scope.is_connection());
        assert!(!scope.is_stream());
        assert!(!scope.is_task());
    }

    #[test]
    fn test_scope_id_connection() {
        let scope = ScopeId::connection("abc123", 0);
        assert_eq!(scope.as_str(), "trace:abc123:conn:0");
        assert_eq!(scope.trace_id(), "abc123");
        assert!(scope.is_connection());
        assert!(!scope.is_stream());
        assert_eq!(scope.connection_index(), Some(0));
    }

    #[test]
    fn test_scope_id_stream() {
        let scope = ScopeId::stream("abc123", 0, 3);
        assert_eq!(scope.as_str(), "trace:abc123:conn:0:stream:3");
        assert_eq!(scope.trace_id(), "abc123");
        assert!(!scope.is_connection());
        assert!(scope.is_stream());
        assert_eq!(scope.connection_index(), Some(0));
        assert_eq!(scope.stream_id(), Some(3));
    }

    #[test]
    fn test_scope_id_task() {
        let scope = ScopeId::task("abc123", "0.1.2");
        assert_eq!(scope.as_str(), "trace:abc123:task:0.1.2");
        assert_eq!(scope.trace_id(), "abc123");
        assert!(scope.is_task());
        assert_eq!(scope.task_path(), Some("0.1.2"));
    }

    #[test]
    fn test_scope_id_orphan() {
        let scope = ScopeId::orphan(42);
        assert_eq!(scope.as_str(), "trace:orphan-42:conn:0");
        assert!(scope.is_orphan());
        // Each orphan is unique
        let scope2 = ScopeId::orphan(43);
        assert_ne!(scope, scope2);
    }

    // ─── Parent Traversal ───

    #[test]
    fn test_parent_trace_is_none() {
        let scope = ScopeId::trace("abc123");
        assert_eq!(scope.parent(), None);
    }

    #[test]
    fn test_parent_connection() {
        let scope = ScopeId::connection("abc123", 0);
        assert_eq!(scope.parent(), Some(ScopeId::trace("abc123")));
    }

    #[test]
    fn test_parent_stream() {
        let scope = ScopeId::stream("abc123", 0, 3);
        assert_eq!(scope.parent(), Some(ScopeId::connection("abc123", 0)));
    }

    #[test]
    fn test_parent_task() {
        let scope = ScopeId::task("abc123", "0.1.2");
        assert_eq!(scope.parent(), Some(ScopeId::task("abc123", "0.1")));

        let root_task = ScopeId::task("abc123", "0");
        assert_eq!(root_task.parent(), Some(ScopeId::trace("abc123")));
    }

    // ─── ScopeSequenceTracker ───

    #[test]
    fn test_scope_sequence_independent() {
        let mut tracker = ScopeSequenceTracker::new();
        let scope_a = ScopeId::connection("t1", 0);
        let scope_b = ScopeId::connection("t1", 1);

        assert_eq!(tracker.next_scope_sequence(&scope_a), 0);
        assert_eq!(tracker.next_scope_sequence(&scope_a), 1);
        assert_eq!(tracker.next_scope_sequence(&scope_b), 0); // Independent!
        assert_eq!(tracker.next_scope_sequence(&scope_a), 2);
        assert_eq!(tracker.next_scope_sequence(&scope_b), 1);
    }

    #[test]
    fn test_global_sequence_monotonic() {
        let tracker = ScopeSequenceTracker::new();
        assert_eq!(tracker.next_global_sequence(), 0);
        assert_eq!(tracker.next_global_sequence(), 1);
        assert_eq!(tracker.next_global_sequence(), 2);
    }

    #[test]
    fn test_peek_scope_sequence() {
        let mut tracker = ScopeSequenceTracker::new();
        let scope = ScopeId::connection("t1", 0);

        assert_eq!(tracker.peek_scope_sequence(&scope), 0);
        tracker.next_scope_sequence(&scope);
        assert_eq!(tracker.peek_scope_sequence(&scope), 1);
        assert_eq!(tracker.peek_scope_sequence(&scope), 1); // Peek doesn't increment
    }

    #[test]
    fn test_reset_trace() {
        let mut tracker = ScopeSequenceTracker::new();
        let scope_t1 = ScopeId::connection("t1", 0);
        let scope_t2 = ScopeId::connection("t2", 0);

        tracker.next_scope_sequence(&scope_t1);
        tracker.next_scope_sequence(&scope_t1);
        tracker.next_scope_sequence(&scope_t2);

        tracker.reset_trace("t1");

        // t1 scopes are reset
        assert_eq!(tracker.peek_scope_sequence(&scope_t1), 0);
        // t2 scopes are preserved
        assert_eq!(tracker.peek_scope_sequence(&scope_t2), 1);
    }

    // ─── From / Display ───

    #[test]
    fn test_scope_id_display() {
        let scope = ScopeId::connection("abc", 5);
        assert_eq!(format!("{}", scope), "trace:abc:conn:5");
    }

    #[test]
    fn test_scope_id_from_string() {
        let scope: ScopeId = "trace:abc:conn:0".into();
        assert_eq!(scope.trace_id(), "abc");
        assert!(scope.is_connection());
    }
}
