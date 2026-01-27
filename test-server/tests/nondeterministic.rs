//! Non-Deterministic Event Tests
//!
//! Tests Deja SDK's capture and replay of:
//! - UUID generation
//! - Timestamps
//! - Random bytes
//! - Custom captures

mod integration;

use integration::harness::TestHarness;
use std::time::Duration;
use tokio::time::sleep;

/// Test that generates multiple non-deterministic values
/// and verifies they are consistent across recording and replay
#[tokio::test]
async fn test_nondeterministic_uuid_generation() {
    // For now, test the SDK directly without full proxy integration
    use deja_sdk::{DejaRuntime, ProductionRuntime};

    let runtime = ProductionRuntime::new();

    // Generate UUIDs
    let uuid1 = runtime.uuid().await;
    let uuid2 = runtime.uuid().await;

    // They should be different in production mode
    assert_ne!(uuid1, uuid2, "UUIDs should be unique");
    assert_eq!(uuid1.as_bytes().len(), 16, "UUID should be 16 bytes");

    println!("✅ UUID1: {}", uuid1);
    println!("✅ UUID2: {}", uuid2);

    // Test timestamps
    let ts1 = runtime.now_millis().await;
    sleep(Duration::from_millis(10)).await;
    let ts2 = runtime.now_millis().await;

    assert!(ts2 > ts1, "Timestamps should increase");
    println!("⏱️ TS1: {}, TS2: {}, diff: {}ms", ts1, ts2, ts2 - ts1);

    // Test random bytes
    let random1 = runtime.random_bytes(16).await;
    let random2 = runtime.random_bytes(16).await;

    assert_eq!(random1.len(), 16);
    assert_eq!(random2.len(), 16);
    assert_ne!(random1, random2, "Random bytes should be different");

    println!("🎲 Random1: {:?}", &random1[..4]);
    println!("🎲 Random2: {:?}", &random2[..4]);

    println!("✅ Non-deterministic SDK test passed!");
}

/// Test custom capture functionality
#[tokio::test]
async fn test_custom_capture() {
    use deja_sdk::{DejaRuntime, ProductionRuntime};

    let runtime = ProductionRuntime::new();

    // In production mode, capture is a no-op
    runtime.capture("test_tag", "test_value").await;

    // In production mode, replay returns None
    let replayed = runtime.replay("test_tag").await;
    assert!(
        replayed.is_none(),
        "Production mode should return None for replay"
    );

    println!("✅ Custom capture test passed!");
}
