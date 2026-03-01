//! Buffering performance regression tests

pub mod common;
use common::create_test_event;

use crate::lib::buffer::PendingEventBuffer;
use crate::lib::common::ScopeId;
use crate::lib::core::events::{EventDirection, RecordedEvent};
use std::time::Instant;

/// Test 1: Buffer flush is O(N) with reasonable factor
#[tokio::test]
async fn test_buffer_flush_scalability() {
    // Stress window: 100ms ensures events expire
    let stress_window_ms = 100;
    let mut buffer = PendingEventBuffer::new(stress_window_ms);

    // Burn-in: Establish baseline with realistic load
    let burn_in_event_count = 100;
    for i in 0..burn_in_event_count {
        buffer.buffer_event(
            5001, // peer_port
            create_test_event(),
            EventDirection::ClientToServer,
        );
    }

    // Verify burn-in was processed correctly
    assert_eq!(buffer.event_count(5001), burn_in_event_count);

    // Test stretch: Add more events to test scaling
    let stretch_event_count = 1000;
    for i in 0..stretch_event_count {
        buffer.buffer_event(
            5001,
            create_test_event(),
            EventDirection::ClientToServer,
        );
    }

    // Record baseline flush time for reference
    let baseline = Instant::now();
    buffer.flush_for_bind(5001, &ScopeId::connection("test_trace", 0), "test_trace");
    let baseline_duration = baseline.elapsed().as_nanos() as u64;

    // Add significant stress load to test performance regression
    let stress_event_count = 5000;
    for i in 0..stress_event_count {
        buffer.buffer_event(5001, create_test_event(), EventDirection::ClientToServer);
        buffer.buffer_event(5002, create_test_event(), EventDirection::ServerToClient);
    }

    // Record stress flush time
    let stress = Instant::now();
    buffer.flush_for_bind(5001, &ScopeId::connection("test_trace", 0), "test_trace");
    let stress_duration = stress.elapsed().as_nanos() as u64;

    // Calculate speed ratio: baseline_speed / stress_speed
    // Lower is better (faster processing)
    let baseline_speed = burn_in_event_count as u64 / baseline_duration;
    let stress_speed = (burn_in_event_count + stretch_event_count + stress_event_count) as u64 / stress_duration;

    let speed_ratio = baseline_speed as f64 / stress_speed.max(1);

    // Performance threshold: speed ratio should not degrade by more than 2x
    // (2x degradation implies O(2N) vs O(N) scalability)
    let degradation_threshold = 2.5;

    println!(
        "Buffer flush scalability - speed ratio: {:.2} (baseline speed: {}, stress speed: {})",
        speed_ratio, baseline_speed, stress_speed
    );

    assert!(
        speed_ratio <= degradation_threshold,
        "Buffer flush scalability degraded by factor of {:.2}x (threshold: {}x). This may indicate O(N^2) behavior.",
        speed_ratio,
        degradation_threshold
    );
}

/// Test 2: Buffer lookup is O(1) with bounded latency
#[tokio::test]
async fn test_buffer_lookup_latency() {
    let stress_window_ms = 100;
    let mut buffer = PendingEventBuffer::new(stress_window_ms);

    // Burn-in: Establish baseline with realistic load
    let burn_in_event_count = 1000;
    for i in 0..burn_in_event_count {
        buffer.buffer_event(6001, create_test_event(), EventDirection::ClientToServer);
        buffer.buffer_event(6002, create_test_event(), EventDirection::ServerToClient);
        buffer.buffer_event(6003, create_test_event(), EventDirection::ClientToServer);
    }

    // Record baseline lookup latency
    let baseline = Instant::now();
    for _ in 0..1000 {
        buffer.event_count(6001);
        buffer.has_events(6002);
    }
    let baseline_duration = baseline.elapsed().as_nanos() as u64;

    // Stress load: Add more events to test scaling
    let stress_event_count = 10000;
    for i in 0..stress_event_count {
        buffer.buffer_event(6001, create_test_event(), EventDirection::ClientToServer);
    }

    // Record stress lookup latency
    let stress = Instant::now();
    for _ in 0..10000 {
        buffer.event_count(6001);
        buffer.has_events(6002);
    }
    let stress_duration = stress.elapsed().as_nanos() as u64;

    // Calculate lookup speed ratio: baseline_speed / stress_speed
    let baseline_speed = 1000u64 / baseline_duration;
    let stress_speed = 10000u64 / stress_duration;

    let speed_ratio = baseline_speed as f64 / stress_speed.max(1);

    // Performance threshold: lookup speed should not degrade by more than 3x
    let degradation_threshold = 3.5;

    println!(
        "Buffer lookup latency - speed ratio: {:.2} (baseline: {} ops/ns, stress: {} ops/ns)",
        speed_ratio, baseline_speed, stress_speed
    );

    assert!(
        speed_ratio <= degradation_threshold,
        "Buffer lookup latency degraded by factor of {:.2}x (threshold: {}x). O(1) check failed.",
        speed_ratio,
        degradation_threshold
    );
}

/// Test 3: expired event quarantine is efficient
#[tokio::test]
async fn test_quarantine_efficiency() {
    // Retro-binding window of 10ms ensures events expire quickly for testing
    let retro_window_ms = 10;
    let mut buffer = PendingEventBuffer::new(retro_window_ms);

    // Simulate delayed buffer flush (some events expired)
    for _ in 0..1000 {
        buffer.buffer_event(7001, create_test_event(), EventDirection::ClientToServer);
    }

    // Add expired events by simulating delayed flush
    let expired_count = 500;
    for _ in 0..expired_count {
        buffer.buffer_event(7001, create_test_event(), EventDirection::ServerToClient);
    }

    // Verify buffer has events
    assert_eq!(buffer.event_count(7001), 1000 + expired_count);

    // Record quarantine operation
    let quarantine_start = Instant::now();
    let _ = buffer.quarantine_expired(7001);
    let quarantine_duration = quarantine_start.elapsed().as_nanos() as u64;

    // Check that deleted events are no longer in buffer
    assert_eq!(buffer.event_count(7001), 1000);

    println!(
        "Quarantine efficiency - processed {} events in {}ns, {} retained",
        expired_count,
        quarantine_duration,
        1000
    );

    // Performance threshold: quarantine should complete in O(N) with reasonable factor
    // 1000 events should complete in < 5µs (5000ns) on modern hardware
    let max_duration_ns = 5000;
    assert!(
        quarantine_duration < max_duration_ns,
        "Quarantine took {}ns to process {} events. Threshold: {}ns.",
        quarantine_duration,
        expired_count,
        max_duration_ns
    );
}

/// Test 4: Retro-binding integration completes within window
#[tokio::test]
async fn test_retro_bind_integration_latency() {
    let retro_window_ms = 100;
    let mut buffer = PendingEventBuffer::new(retro_window_ms);

    // Simulate high load event precipitation before association
    let precipitation_count = 5000;
    for _ in 0..precipitation_count {
        buffer.buffer_event(8001, create_test_event(), EventDirection::ClientToServer);
    }

    let scope_id = ScopeId::connection("retro_bind_trace", 0);

    // Record end-to-end retro-binding latency
    let binding_start = Instant::now();
    let attributed = buffer.flush_for_bind(8001, &scope_id, "retro_bind_trace");
    let binding_duration = binding_start.elapsed().as_nanos() as u64;

    // Verify correct attribution
    assert_eq!(attributed.len(), precipitation_count);
    let all_bounded = attributed
        .iter()
        .all(|(ev, _)| !ev.trace_id.is_empty() && ev.trace_id == "retro_bind_trace");

    assert!(all_bounded, "All events should be attributed to trace");

    println!(
        "Retro-binding integration - processed {} events in {}ns (window: {}ms)",
        precipitation_count,
        binding_duration,
        retro_window_ms
    );

    // Performance threshold: flush <= retro_window_ms in practice
    let window_ns_limit = retro_window_ms * 1_000_000;
    assert!(
        binding_duration <= window_ns_limit,
        "Retro-binding integration took {}ns with window {}. Threshold: {}ns.",
        binding_duration,
        window_ns_limit,
        window_ns_limit
    );
}

/// Test 5: Buffer conflict resolution is O(1)
#[tokio::test]
async fn test_buffer_contention_resilience() {
    let retro_window_ms = 100;
    let mut buffer = PendingEventBuffer::new(retro_window_ms);

    let burn_in_event_count = 5000;
    for i in 0..burn_in_event_count {
        buffer.buffer_event(9001, create_test_event(), EventDirection::ClientToServer);
    }

    // Stress: Rapid buffer write + flush cycles
    let contention_cycles = 1000;
    let start = Instant::now();

    for i in 0..contention_cycles {
        // Write new events
        buffer.buffer_event(9001, create_test_event(), EventDirection::ServerToClient);

        // Flush with new scope_id (simulating association update)
        let scope_id = ScopeId::connection(&format!("trace_{}", i), 0);
        _ = buffer.flush_for_bind(9001, &scope_id, &format!("trace_{}", i));

        // Write more events
        buffer.buffer_event(9001, create_test_event(), EventDirection::ClientToServer);
    }

    let contention_duration = start.elapsed().as_nanos() as u64;

    println!(
        "Buffer contention - {} cycles in {}ns (average: {:.2}ns per cycle)",
        contention_cycles, contention_duration, contention_duration / contention_cycles as u64
    );

    // Performance threshold: Each cycle should complete < 100ns
    let cycle_ns_limit = 100;
    let avg_duration_ns = contention_duration / contention_cycles as u64;

    assert!(
        avg_duration_ns < cycle_ns_limit,
        "Buffer contention took {}ns per cycle on average. O(1) check failed (threshold: {}ns).",
        avg_duration_ns,
        cycle_ns_limit
    );
}
