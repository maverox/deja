//! Performance regression tests for buffering path
//! 
//! These tests validate that buffering overhead remains acceptable under stress workload
//! in CI-mode profile. They measure O(N) scalability, O(1) lookup latency, and quarantine efficiency.
