//! Error Scenario Tests
//!
//! Tests for:
//! - Backend timeout handling
//! - Backend 5xx errors
//! - Connection failures
//! - Malformed responses

mod integration;

use integration::harness::TestHarness;
use std::time::Duration;
use tokio::time::sleep;

/// Test backend timeout handling
#[tokio::test]
async fn test_backend_timeout() {
    let harness = TestHarness::new();
    harness.start_mock_backend().await;
    sleep(Duration::from_millis(300)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(200))
        .build()
        .unwrap();

    // Request a 500ms delay with a 200ms client timeout
    let result = client
        .get(&format!("{}/delay/500", harness.mock_backend_url()))
        .send()
        .await;

    assert!(result.is_err(), "Should timeout");
    let err = result.unwrap_err();
    assert!(err.is_timeout(), "Error should be a timeout");
    println!("✅ Backend timeout handled correctly: {}", err);
}

/// Test backend 5xx error responses
#[tokio::test]
async fn test_backend_5xx_errors() {
    let harness = TestHarness::new();
    harness.start_mock_backend().await;
    sleep(Duration::from_millis(300)).await;

    let client = reqwest::Client::new();

    // Test various 5xx status codes
    for status in [500, 502, 503, 504] {
        let response = client
            .get(&format!("{}/status/{}", harness.mock_backend_url(), status))
            .send()
            .await
            .expect("Request should complete");

        assert_eq!(
            response.status().as_u16(),
            status,
            "Should return {} status",
            status
        );
        println!("✅ Backend {} error handled correctly", status);
    }

    println!("✅ All 5xx error scenarios passed!");
}

/// Test backend 4xx client error responses
#[tokio::test]
async fn test_backend_4xx_errors() {
    let harness = TestHarness::new();
    harness.start_mock_backend().await;
    sleep(Duration::from_millis(300)).await;

    let client = reqwest::Client::new();

    // Test various 4xx status codes
    for status in [400, 401, 403, 404, 429] {
        let response = client
            .get(&format!("{}/status/{}", harness.mock_backend_url(), status))
            .send()
            .await
            .expect("Request should complete");

        assert_eq!(
            response.status().as_u16(),
            status,
            "Should return {} status",
            status
        );
        println!("✅ Backend {} error handled correctly", status);
    }

    println!("✅ All 4xx error scenarios passed!");
}

/// Test connection to non-existent server
#[tokio::test]
async fn test_connection_refused() {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    // Try to connect to a port that should be closed
    let result = client.get("http://127.0.0.1:59999/test").send().await;

    assert!(result.is_err(), "Should fail to connect");
    let err = result.unwrap_err();
    assert!(
        err.is_connect(),
        "Error should be a connection error: {}",
        err
    );
    println!("✅ Connection refused handled correctly: {}", err);
}

/// Test empty response handling
#[tokio::test]
async fn test_empty_response() {
    let harness = TestHarness::new();
    harness.start_mock_backend().await;
    sleep(Duration::from_millis(300)).await;

    let client = reqwest::Client::new();

    // 204 No Content should return empty body
    let response = client
        .get(&format!("{}/status/204", harness.mock_backend_url()))
        .send()
        .await
        .expect("Request should complete");

    assert_eq!(response.status().as_u16(), 204);
    // Note: Our mock returns JSON even for 204, but in real scenarios 204 has no body
    println!("✅ 204 No Content handled correctly");
}

/// Test rapid sequential requests (stress test)
#[tokio::test]
async fn test_rapid_sequential_requests() {
    let harness = TestHarness::new();
    harness.start_mock_backend().await;
    sleep(Duration::from_millis(300)).await;

    let client = reqwest::Client::new();
    let mut success_count = 0;

    // Send 50 rapid requests
    for i in 0..50 {
        let response = client
            .get(&format!("{}/order/rapid-{}", harness.mock_backend_url(), i))
            .send()
            .await;

        if response.is_ok() && response.unwrap().status().is_success() {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 50, "All 50 requests should succeed");
    println!(
        "✅ Rapid sequential requests: {}/50 succeeded",
        success_count
    );
}
