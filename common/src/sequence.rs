//! Sequence tracking for protocol-scoped replay
//!
//! Tracks message sequences per (trace_id, protocol) pair to enable
//! deterministic replay regardless of ephemeral connection IDs.

use crate::Protocol;
use std::collections::HashMap;

/// Key for sequence tracking: (trace_id, protocol)
pub type SequenceKey = (String, Protocol);

/// Tracks message sequences per (trace_id, protocol) pair
///
/// During recording, assigns incrementing sequence numbers to each message
/// within a (trace, protocol) scope. During replay, uses these sequences
/// to deterministically match incoming messages to recorded ones.
///
/// # Why Protocol-Scoped?
///
/// Connection IDs are ephemeral - they change between recording and replay.
/// By tracking sequences at the (trace, protocol) level, we get stable
/// identifiers that work across runs.
///
/// # Example
///
/// ```
/// use deja_common::{SequenceTracker, Protocol};
///
/// let mut tracker = SequenceTracker::new();
/// tracker.start_trace("req-123");
///
/// // Each protocol has independent sequences
/// assert_eq!(tracker.next_sequence("req-123", Protocol::Postgres), 0);
/// assert_eq!(tracker.next_sequence("req-123", Protocol::Postgres), 1);
/// assert_eq!(tracker.next_sequence("req-123", Protocol::Redis), 0);  // Independent!
/// ```
#[derive(Debug, Default)]
pub struct SequenceTracker {
    /// (trace_id, protocol) → next sequence number
    sequences: HashMap<SequenceKey, u64>,
}

impl SequenceTracker {
    /// Create a new sequence tracker
    pub fn new() -> Self {
        Self::default()
    }

    /// Initialize/reset sequences for a new trace
    ///
    /// This should be called when `StartTrace` control message is received.
    /// Resets all protocol sequences for this trace to 0.
    pub fn start_trace(&mut self, trace_id: &str) {
        for protocol in Protocol::known_protocols() {
            self.sequences.insert((trace_id.to_string(), protocol), 0);
        }
    }

    /// End a trace and optionally clean up its sequences
    ///
    /// Called when `EndTrace` control message is received.
    pub fn end_trace(&mut self, trace_id: &str) {
        // Keep sequences around for a bit in case of late messages
        // Actual cleanup can be time-based later
        let _ = trace_id;
    }

    /// Get and increment the next sequence number for a (trace, protocol) pair
    ///
    /// Returns the current sequence and increments for the next call.
    pub fn next_sequence(&mut self, trace_id: &str, protocol: Protocol) -> u64 {
        let key = (trace_id.to_string(), protocol);
        let seq = self.sequences.entry(key).or_insert(0);
        let current = *seq;
        *seq += 1;
        current
    }

    /// Peek the current sequence without incrementing
    pub fn current_sequence(&self, trace_id: &str, protocol: Protocol) -> u64 {
        let key = (trace_id.to_string(), protocol);
        self.sequences.get(&key).copied().unwrap_or(0)
    }

    /// Reset a specific (trace, protocol) sequence to 0
    pub fn reset_sequence(&mut self, trace_id: &str, protocol: Protocol) {
        let key = (trace_id.to_string(), protocol);
        self.sequences.insert(key, 0);
    }

    /// Get total count of tracked sequences
    pub fn len(&self) -> usize {
        self.sequences.len()
    }

    /// Check if tracker is empty
    pub fn is_empty(&self) -> bool {
        self.sequences.is_empty()
    }

    /// Clear all tracked sequences
    pub fn clear(&mut self) {
        self.sequences.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_increments() {
        let mut tracker = SequenceTracker::new();
        tracker.start_trace("trace-1");

        assert_eq!(tracker.next_sequence("trace-1", Protocol::Postgres), 0);
        assert_eq!(tracker.next_sequence("trace-1", Protocol::Postgres), 1);
        assert_eq!(tracker.next_sequence("trace-1", Protocol::Postgres), 2);
    }

    #[test]
    fn test_protocol_independence() {
        let mut tracker = SequenceTracker::new();
        tracker.start_trace("trace-1");

        // Postgres and Redis have independent sequences
        assert_eq!(tracker.next_sequence("trace-1", Protocol::Postgres), 0);
        assert_eq!(tracker.next_sequence("trace-1", Protocol::Redis), 0);
        assert_eq!(tracker.next_sequence("trace-1", Protocol::Postgres), 1);
        assert_eq!(tracker.next_sequence("trace-1", Protocol::Redis), 1);
    }

    #[test]
    fn test_trace_independence() {
        let mut tracker = SequenceTracker::new();
        tracker.start_trace("trace-1");
        tracker.start_trace("trace-2");

        // Different traces have independent sequences
        assert_eq!(tracker.next_sequence("trace-1", Protocol::Postgres), 0);
        assert_eq!(tracker.next_sequence("trace-2", Protocol::Postgres), 0);
        assert_eq!(tracker.next_sequence("trace-1", Protocol::Postgres), 1);
        assert_eq!(tracker.next_sequence("trace-2", Protocol::Postgres), 1);
    }

    #[test]
    fn test_start_trace_resets() {
        let mut tracker = SequenceTracker::new();
        tracker.start_trace("trace-1");

        tracker.next_sequence("trace-1", Protocol::Postgres); // 0
        tracker.next_sequence("trace-1", Protocol::Postgres); // 1

        // Starting trace again resets sequences
        tracker.start_trace("trace-1");
        assert_eq!(tracker.next_sequence("trace-1", Protocol::Postgres), 0);
    }

    #[test]
    fn test_current_sequence() {
        let mut tracker = SequenceTracker::new();
        tracker.start_trace("trace-1");

        assert_eq!(tracker.current_sequence("trace-1", Protocol::Postgres), 0);
        tracker.next_sequence("trace-1", Protocol::Postgres);
        assert_eq!(tracker.current_sequence("trace-1", Protocol::Postgres), 1);
    }

    #[test]
    fn test_auto_initialize() {
        let mut tracker = SequenceTracker::new();
        // Without calling start_trace, sequences still work (start at 0)
        assert_eq!(tracker.next_sequence("trace-1", Protocol::Postgres), 0);
        assert_eq!(tracker.next_sequence("trace-1", Protocol::Postgres), 1);
    }
}
