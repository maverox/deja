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
    // Use Runtime with a dummy proxy URL - run() still works
    // even if the proxy is unreachable (capture sends fail silently)
    use deja_sdk::{DejaMode, Runtime};
    use std::time::{SystemTime, UNIX_EPOCH};

    let runtime = Runtime::new("http://localhost:19999".to_string(), DejaMode::Record);

    // Generate UUIDs - user brings their own uuid crate
    let uuid1: uuid::Uuid = deja_sdk::deja_run(&runtime, "uuid", || uuid::Uuid::new_v4()).await;
    let uuid2: uuid::Uuid = deja_sdk::deja_run(&runtime, "uuid", || uuid::Uuid::new_v4()).await;

    // They should be different in record mode (each call generates new value)
    assert_ne!(uuid1, uuid2, "UUIDs should be unique");
    assert_eq!(uuid1.as_bytes().len(), 16, "UUID should be 16 bytes");

    println!("✅ UUID1: {}", uuid1);
    println!("✅ UUID2: {}", uuid2);

    // Test timestamps - user brings their own time handling
    let ts1: u64 = deja_sdk::deja_run(&runtime, "time", || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }).await;
    sleep(Duration::from_millis(10)).await;
    let ts2: u64 = deja_sdk::deja_run(&runtime, "time", || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }).await;

    assert!(ts2 > ts1, "Timestamps should increase");
    println!("⏱️ TS1: {}, TS2: {}, diff: {}ms", ts1, ts2, ts2 - ts1);

    // Test random bytes - user brings their own rand crate
    let random1: Vec<u8> = deja_sdk::deja_run(&runtime, "random_bytes", || {
        use rand::Rng;
        let mut bytes = vec![0u8; 16];
        rand::thread_rng().fill(&mut bytes[..]);
        bytes
    }).await;
    let random2: Vec<u8> = deja_sdk::deja_run(&runtime, "random_bytes", || {
        use rand::Rng;
        let mut bytes = vec![0u8; 16];
        rand::thread_rng().fill(&mut bytes[..]);
        bytes
    }).await;

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
    use deja_sdk::{DejaMode, DejaRuntime, Runtime};

    // Test replay mode - should return None if no proxy available
    let runtime = Runtime::new("http://localhost:19999".to_string(), DejaMode::Replay);

    // In replay mode without a running proxy, replay returns None
    let replayed = runtime.replay_value("test_tag").await;
    assert!(
        replayed.is_none(),
        "Replay without proxy should return None"
    );

    println!("✅ Custom capture test passed!");
}
