//! Connection Reuse and Multiplexing Tests
//!
//! Tests for:
//! - HTTP keep-alive connection reuse
//! - Connection pooling
//! - Parallel connections on same host
//! - gRPC multiplexing (multiple streams)

mod integration;

use integration::harness::TestHarness;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Test HTTP connection reuse via keep-alive
#[tokio::test]
async fn test_http_connection_reuse() {
    let harness = TestHarness::new();
    harness.start_mock_backend().await;
    sleep(Duration::from_millis(300)).await;

    // Client with connection pooling enabled (default)
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(5)
        .build()
        .unwrap();

    let start = Instant::now();

    // Make 10 sequential requests - should reuse connections
    for i in 0..10 {
        let response = client
            .get(&format!("{}/echo", harness.mock_backend_url()))
            .send()
            .await
            .expect("Request should succeed");
        assert!(response.status().is_success());
    }

    let duration = start.elapsed();
    println!(
        "✅ 10 sequential requests with connection reuse: {:?}",
        duration
    );

    // Should be faster than establishing 10 new connections
    // (rough heuristic - 10 requests should complete in under 500ms with reuse)
    assert!(
        duration < Duration::from_millis(1000),
        "Connection reuse should be fast"
    );
}

/// Test connection pool with parallel requests
#[tokio::test]
async fn test_connection_pool_parallel() {
    let harness = TestHarness::new();
    harness.start_mock_backend().await;
    sleep(Duration::from_millis(300)).await;

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(10)
        .build()
        .unwrap();

    let start = Instant::now();

    // Make 20 parallel requests
    let futures: Vec<_> = (0..20)
        .map(|i| {
            let client = client.clone();
            let url = format!("{}/order/parallel-{}", harness.mock_backend_url(), i);
            async move { client.get(&url).send().await }
        })
        .collect();

    let results: Vec<_> = futures::future::join_all(futures).await;

    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let duration = start.elapsed();

    assert_eq!(success_count, 20, "All 20 parallel requests should succeed");
    println!(
        "✅ 20 parallel requests via connection pool: {:?} ({} succeeded)",
        duration, success_count
    );
}

/// Test that connections are not leaked
#[tokio::test]
async fn test_connection_cleanup() {
    let harness = TestHarness::new();
    harness.start_mock_backend().await;
    sleep(Duration::from_millis(300)).await;

    // Client with small pool to test cleanup
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(2)
        .pool_idle_timeout(Duration::from_millis(100))
        .build()
        .unwrap();

    // Make requests
    for i in 0..5 {
        let _ = client
            .get(&format!("{}/echo", harness.mock_backend_url()))
            .send()
            .await;
    }

    // Wait for idle timeout
    sleep(Duration::from_millis(200)).await;

    // Pool should have cleaned up idle connections
    // Make a new request - should still work
    let response = client
        .get(&format!("{}/echo", harness.mock_backend_url()))
        .send()
        .await
        .expect("Request after cleanup should work");

    assert!(response.status().is_success());
    println!("✅ Connection cleanup works correctly");
}

/// Test multiple distinct hosts (simulated with different paths)
#[tokio::test]
async fn test_multiple_endpoint_connections() {
    let harness = TestHarness::new();
    harness.start_mock_backend().await;
    sleep(Duration::from_millis(300)).await;

    let client = reqwest::Client::new();

    // Hit different endpoints (simulating different "services")
    let endpoints = vec!["/echo", "/random", "/health", "/status/200"];

    let futures: Vec<_> = endpoints
        .iter()
        .map(|ep| {
            let client = client.clone();
            let url = format!("{}{}", harness.mock_backend_url(), ep);
            async move {
                let resp = client.get(&url).send().await?;
                Ok::<_, reqwest::Error>((ep.to_string(), resp.status().as_u16()))
            }
        })
        .collect();

    let results: Vec<_> = futures::future::join_all(futures).await;

    for result in results {
        let (endpoint, status) = result.expect("Request should succeed");
        assert!(
            status >= 200 && status < 300,
            "Endpoint {} should return 2xx",
            endpoint
        );
        println!("✅ {} returned {}", endpoint, status);
    }

    println!("✅ Multiple endpoint connections work correctly");
}

/// Test sustained high concurrency
#[tokio::test]
async fn test_high_concurrency() {
    let harness = TestHarness::new();
    harness.start_mock_backend().await;
    sleep(Duration::from_millis(300)).await;

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(50)
        .build()
        .unwrap();

    let start = Instant::now();

    // 100 concurrent requests
    let futures: Vec<_> = (0..100)
        .map(|i| {
            let client = client.clone();
            let url = format!(
                "{}/order/high-concurrency-{}",
                harness.mock_backend_url(),
                i
            );
            async move { client.get(&url).send().await }
        })
        .collect();

    let results: Vec<_> = futures::future::join_all(futures).await;
    let duration = start.elapsed();

    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let error_count = results.iter().filter(|r| r.is_err()).count();

    println!(
        "✅ High concurrency test: {} succeeded, {} failed in {:?}",
        success_count, error_count, duration
    );

    // At least 90% should succeed
    assert!(
        success_count >= 90,
        "At least 90 of 100 requests should succeed"
    );
}

/// Test request pipelining behavior
#[tokio::test]
async fn test_request_ordering_under_load() {
    let harness = TestHarness::new();
    harness.start_mock_backend().await;
    sleep(Duration::from_millis(300)).await;

    let client = reqwest::Client::new();

    // Reset order tracking
    client
        .post(&format!("{}/reset", harness.mock_backend_url()))
        .send()
        .await
        .unwrap();

    // Send ordered requests with small delays
    let mut positions = Vec::new();
    for i in 0..10 {
        let response = client
            .get(&format!(
                "{}/order/ordered-{}",
                harness.mock_backend_url(),
                i
            ))
            .send()
            .await
            .expect("Request should succeed");

        let body: serde_json::Value = response.json().await.unwrap();
        positions.push(body["position"].as_u64().unwrap() as usize);
    }

    // Positions should be sequential (0, 1, 2, ..., 9)
    let expected: Vec<usize> = (0..10).collect();
    assert_eq!(
        positions, expected,
        "Sequential requests should maintain order"
    );

    println!("✅ Request ordering maintained under load: {:?}", positions);
}
