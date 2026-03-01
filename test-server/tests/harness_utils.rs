//! Test harness utilities for deterministic control-plane event injection
//!
//! Provides `ControlEventHarness` for injecting delays and reordering control messages
//! to reproduce known race conditions deterministically.
//!
//! Example:
//! ```ignore
//! let harness = ControlEventHarness::new();
//! harness.delay_before_control(100); // 100ms delay before control messages
//! harness.reorder_with_seed(42);     // Deterministic reordering with seed 42
//! ```

use deja_common::ControlMessage;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;

/// Configuration for control-plane event injection
#[derive(Debug, Clone)]
pub struct ControlEventConfig {
    /// Delay (in milliseconds) to inject before control messages
    pub delay_before_control_ms: u64,
    /// Seed for deterministic reordering (None = no reordering)
    pub reorder_seed: Option<u64>,
}

impl Default for ControlEventConfig {
    fn default() -> Self {
        Self {
            delay_before_control_ms: 0,
            reorder_seed: None,
        }
    }
}

impl ControlEventConfig {
    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        // Seed must be non-zero if specified
        if let Some(seed) = self.reorder_seed {
            if seed == 0 {
                return Err("reorder_seed must be non-zero".to_string());
            }
        }
        Ok(())
    }
}

/// Harness for injecting control-plane delays and reordering
///
/// This harness allows tests to reproduce race conditions deterministically by:
/// 1. Delaying control messages relative to data-plane events
/// 2. Reordering control messages using a seed-based permutation
///
/// The same seed always produces the same ordering and attribution result.
pub struct ControlEventHarness {
    config: Arc<Mutex<ControlEventConfig>>,
    /// Queue of pending control messages
    pending_messages: Arc<Mutex<VecDeque<ControlMessage>>>,
}

impl ControlEventHarness {
    /// Create a new harness with default (no delay, no reorder) configuration
    pub fn new() -> Self {
        Self {
            config: Arc::new(Mutex::new(ControlEventConfig::default())),
            pending_messages: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Set delay (in milliseconds) to inject before control messages
    ///
    /// This delays the delivery of control messages relative to data-plane events,
    /// allowing tests to reproduce races where data arrives before control signals.
    ///
    /// # Example
    /// ```ignore
    /// harness.delay_before_control(100); // 100ms delay
    /// ```
    pub fn delay_before_control(&self, ms: u64) {
        let mut config = self.config.lock().unwrap();
        config.delay_before_control_ms = ms;
    }

    /// Set seed for deterministic reordering of control messages
    ///
    /// Uses a seed-based permutation to reorder control messages deterministically.
    /// The same seed always produces the same ordering and attribution result.
    ///
    /// # Errors
    /// Returns error if seed is 0 (invalid configuration).
    ///
    /// # Example
    /// ```ignore
    /// harness.reorder_with_seed(42)?; // Deterministic reordering
    /// ```
    pub fn reorder_with_seed(&self, seed: u64) -> Result<(), String> {
        if seed == 0 {
            return Err("reorder_seed must be non-zero".to_string());
        }
        let mut config = self.config.lock().unwrap();
        config.reorder_seed = Some(seed);
        Ok(())
    }

    /// Get current configuration
    pub fn config(&self) -> ControlEventConfig {
        self.config.lock().unwrap().clone()
    }

    /// Queue a control message for delayed/reordered delivery
    pub fn queue_message(&self, message: ControlMessage) {
        let mut queue = self.pending_messages.lock().unwrap();
        queue.push_back(message);
    }

    /// Process queued messages with configured delays and reordering
    ///
    /// This applies the configured delay and reordering to the queued messages,
    /// returning them in the order they should be delivered.
    ///
    /// # Returns
    /// Vector of control messages in delivery order
    pub async fn process_queued_messages(&self) -> Vec<ControlMessage> {
        let config = self.config.lock().unwrap().clone();

        // Validate configuration
        if let Err(e) = config.validate() {
            eprintln!("Invalid harness configuration: {}", e);
            return Vec::new();
        }

        // Apply delay if configured
        if config.delay_before_control_ms > 0 {
            sleep(Duration::from_millis(config.delay_before_control_ms)).await;
        }

        // Extract messages from queue
        let mut messages = {
            let mut queue = self.pending_messages.lock().unwrap();
            queue.drain(..).collect::<Vec<_>>()
        };

        // Apply reordering if seed is configured
        if let Some(seed) = config.reorder_seed {
            messages = Self::reorder_messages(messages, seed);
        }

        messages
    }

    /// Reorder messages deterministically using a seed-based permutation
    ///
    /// Uses a simple linear congruential generator (LCG) seeded with the provided seed
    /// to generate a deterministic permutation of message indices.
    pub fn reorder_messages(messages: Vec<ControlMessage>, seed: u64) -> Vec<ControlMessage> {
        if messages.is_empty() {
            return messages;
        }

        let n = messages.len();
        let mut indices: Vec<usize> = (0..n).collect();

        // Fisher-Yates shuffle with LCG-based randomness
        let mut rng_state = seed;
        for i in (1..n).rev() {
            // LCG: next = (a * prev + c) mod m
            const A: u64 = 1103515245;
            const C: u64 = 12345;
            const M: u64 = 2u64.pow(31);

            rng_state = (A.wrapping_mul(rng_state).wrapping_add(C)) % M;
            let j = (rng_state as usize) % (i + 1);
            indices.swap(i, j);
        }

        // Apply permutation to messages
        indices.iter().map(|&i| messages[i].clone()).collect()
    }

    /// Clear all queued messages
    pub fn clear(&self) {
        self.pending_messages.lock().unwrap().clear();
    }

    /// Get number of queued messages
    pub fn queued_count(&self) -> usize {
        self.pending_messages.lock().unwrap().len()
    }
}

impl Default for ControlEventHarness {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deja_common::Protocol;

    #[test]
    fn test_harness_creation() {
        let harness = ControlEventHarness::new();
        assert_eq!(harness.queued_count(), 0);
        assert_eq!(harness.config().delay_before_control_ms, 0);
        assert_eq!(harness.config().reorder_seed, None);
    }

    #[test]
    fn test_delay_before_control() {
        let harness = ControlEventHarness::new();
        harness.delay_before_control(100);
        assert_eq!(harness.config().delay_before_control_ms, 100);
    }

    #[test]
    fn test_reorder_with_seed_valid() {
        let harness = ControlEventHarness::new();
        let result = harness.reorder_with_seed(42);
        assert!(result.is_ok());
        assert_eq!(harness.config().reorder_seed, Some(42));
    }

    #[test]
    fn test_reorder_with_seed_invalid_zero() {
        let harness = ControlEventHarness::new();
        let result = harness.reorder_with_seed(0);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "reorder_seed must be non-zero");
    }

    #[test]
    fn test_config_validation_invalid_seed() {
        let config = ControlEventConfig {
            delay_before_control_ms: 0,
            reorder_seed: Some(0),
        };
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_validation_valid() {
        let config = ControlEventConfig {
            delay_before_control_ms: 100,
            reorder_seed: Some(42),
        };
        let result = config.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_queue_and_clear() {
        let harness = ControlEventHarness::new();
        let msg = ControlMessage::start_trace("trace-1");
        harness.queue_message(msg);
        assert_eq!(harness.queued_count(), 1);
        harness.clear();
        assert_eq!(harness.queued_count(), 0);
    }

    #[test]
    fn test_reorder_messages_deterministic() {
        let messages = vec![
            ControlMessage::start_trace("trace-1"),
            ControlMessage::associate_by_source_port("trace-1", 5433, Protocol::Postgres),
            ControlMessage::pool_checkout("trace-1", 5433, Protocol::Postgres, None),
        ];

        // Same seed should produce same ordering
        let reordered1 = ControlEventHarness::reorder_messages(messages.clone(), 42);
        let reordered2 = ControlEventHarness::reorder_messages(messages.clone(), 42);

        assert_eq!(reordered1.len(), reordered2.len());
        for (m1, m2) in reordered1.iter().zip(reordered2.iter()) {
            // Compare by serialization since ControlMessage doesn't derive Eq
            let s1 = serde_json::to_string(m1).unwrap();
            let s2 = serde_json::to_string(m2).unwrap();
            assert_eq!(s1, s2);
        }
    }

    #[test]
    fn test_reorder_messages_different_seeds() {
        let messages = vec![
            ControlMessage::start_trace("trace-1"),
            ControlMessage::associate_by_source_port("trace-1", 5433, Protocol::Postgres),
            ControlMessage::pool_checkout("trace-1", 5433, Protocol::Postgres, None),
        ];

        let reordered1 = ControlEventHarness::reorder_messages(messages.clone(), 42);
        let reordered2 = ControlEventHarness::reorder_messages(messages.clone(), 99);

        // Different seeds may produce different orderings (not guaranteed, but likely)
        // At minimum, both should have same length and contain all messages
        assert_eq!(reordered1.len(), reordered2.len());
        assert_eq!(reordered1.len(), messages.len());
    }

    #[test]
    fn test_reorder_messages_empty() {
        let messages: Vec<ControlMessage> = vec![];
        let reordered = ControlEventHarness::reorder_messages(messages, 42);
        assert_eq!(reordered.len(), 0);
    }

    #[test]
    fn test_reorder_messages_single() {
        let messages = vec![ControlMessage::start_trace("trace-1")];
        let reordered = ControlEventHarness::reorder_messages(messages.clone(), 42);
        assert_eq!(reordered.len(), 1);
        let s1 = serde_json::to_string(&messages[0]).unwrap();
        let s2 = serde_json::to_string(&reordered[0]).unwrap();
        assert_eq!(s1, s2);
    }

    #[tokio::test]
    async fn test_process_queued_messages_no_config() {
        let harness = ControlEventHarness::new();
        let msg = ControlMessage::start_trace("trace-1");
        harness.queue_message(msg.clone());

        let processed = harness.process_queued_messages().await;
        assert_eq!(processed.len(), 1);
        assert_eq!(harness.queued_count(), 0); // Queue should be cleared
    }

    #[tokio::test]
    async fn test_process_queued_messages_with_delay() {
        let harness = ControlEventHarness::new();
        harness.delay_before_control(10); // 10ms delay

        let msg = ControlMessage::start_trace("trace-1");
        harness.queue_message(msg);

        let start = std::time::Instant::now();
        let processed = harness.process_queued_messages().await;
        let elapsed = start.elapsed();

        assert_eq!(processed.len(), 1);
        assert!(elapsed.as_millis() >= 10, "Expected at least 10ms delay");
    }

    #[tokio::test]
    async fn test_process_queued_messages_with_reorder() {
        let harness = ControlEventHarness::new();
        harness.reorder_with_seed(42).unwrap();

        let msg1 = ControlMessage::start_trace("trace-1");
        let msg2 = ControlMessage::associate_by_source_port("trace-1", 5433, Protocol::Postgres);
        let msg3 = ControlMessage::pool_checkout("trace-1", 5433, Protocol::Postgres, None);

        harness.queue_message(msg1);
        harness.queue_message(msg2);
        harness.queue_message(msg3);

        let processed = harness.process_queued_messages().await;
        assert_eq!(processed.len(), 3);
        assert_eq!(harness.queued_count(), 0);
    }

    #[test]
    fn test_reorder_messages_all_permutations_present() {
        // Verify that reordering produces a valid permutation (all messages present)
        let messages = vec![
            ControlMessage::start_trace("trace-1"),
            ControlMessage::associate_by_source_port("trace-1", 5433, Protocol::Postgres),
            ControlMessage::pool_checkout("trace-1", 5433, Protocol::Postgres, None),
        ];

        let reordered = ControlEventHarness::reorder_messages(messages.clone(), 42);

        // Convert to JSON strings for comparison
        let original_strs: std::collections::HashSet<_> = messages
            .iter()
            .map(|m| serde_json::to_string(m).unwrap())
            .collect();

        let reordered_strs: std::collections::HashSet<_> = reordered
            .iter()
            .map(|m| serde_json::to_string(m).unwrap())
            .collect();

        assert_eq!(original_strs, reordered_strs);
    }

    #[tokio::test]
    async fn test_race_data_before_start_trace_with_delay() {
        let harness = ControlEventHarness::new();
        harness.delay_before_control(50);

        let start_trace = ControlMessage::start_trace("trace-1");
        let assoc = ControlMessage::associate_by_source_port("trace-1", 5433, Protocol::Postgres);

        harness.queue_message(start_trace);
        harness.queue_message(assoc);

        let processed = harness.process_queued_messages().await;

        assert_eq!(processed.len(), 2);
        assert_eq!(harness.queued_count(), 0);

        let first_json = serde_json::to_string(&processed[0]).unwrap();
        assert!(first_json.contains("start_trace"));
    }

    #[tokio::test]
    async fn test_race_reordered_control_messages_deterministic() {
        let harness = ControlEventHarness::new();
        harness.reorder_with_seed(12345).unwrap();

        let start_trace = ControlMessage::start_trace("trace-1");
        let assoc = ControlMessage::associate_by_source_port("trace-1", 5433, Protocol::Postgres);
        let checkout = ControlMessage::pool_checkout("trace-1", 5433, Protocol::Postgres, None);

        harness.queue_message(start_trace.clone());
        harness.queue_message(assoc.clone());
        harness.queue_message(checkout.clone());

        let processed1 = harness.process_queued_messages().await;

        harness.queue_message(start_trace);
        harness.queue_message(assoc);
        harness.queue_message(checkout);

        let processed2 = harness.process_queued_messages().await;

        assert_eq!(processed1.len(), processed2.len());
        for (m1, m2) in processed1.iter().zip(processed2.iter()) {
            let s1 = serde_json::to_string(m1).unwrap();
            let s2 = serde_json::to_string(m2).unwrap();
            assert_eq!(s1, s2, "Same seed should produce same ordering");
        }
    }

    #[tokio::test]
    async fn test_race_combined_delay_and_reorder() {
        let harness = ControlEventHarness::new();
        harness.delay_before_control(20);
        harness.reorder_with_seed(999).unwrap();

        let start_trace = ControlMessage::start_trace("trace-1");
        let assoc = ControlMessage::associate_by_source_port("trace-1", 5433, Protocol::Postgres);

        harness.queue_message(start_trace);
        harness.queue_message(assoc);

        let start = std::time::Instant::now();
        let processed = harness.process_queued_messages().await;
        let elapsed = start.elapsed();

        assert_eq!(processed.len(), 2);
        assert!(elapsed.as_millis() >= 20, "Expected at least 20ms delay");
    }
}
