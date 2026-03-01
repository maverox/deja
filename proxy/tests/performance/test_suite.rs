//! Buffering performance regression test suite
//!
//! Validates that buffering overhead remains acceptable under stress workload
//! in CI-mode profile. Uses invariant/threshold checks with deterministic inputs.

mod common;
mod regression;

// Run all performance tests when cargo test is invoked
// These tests validate O(N) scalability, O(1) lookup latency, and quarantine efficiency
