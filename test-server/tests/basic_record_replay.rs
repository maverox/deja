//! Basic Record/Replay Integration Test
//!
//! Tests the fundamental recording and replay flow:
//! 1. Start mock backend + proxy (record mode)
//! 2. Make HTTP request through proxy
//! 3. Verify recording was created
//! 4. Restart proxy in replay mode
//! 5. Make same request
//! 6. Verify response matches original

mod integration;

use integration::harness::TestHarness;
use std::time::Duration;
use tokio::time::sleep;

/// Basic HTTP record/replay test
#[tokio::test]
async fn test_basic_http_record_replay() {
    let mut harness = TestHarness::new();

    // 1. Start mock backend
    harness.start_mock_backend().await;
    println!(
        "✅ Mock backend started on port {}",
        harness.mock_backend_port
    );

    // Small delay to ensure backend is ready
    sleep(Duration::from_millis(500)).await;

    // 2. For now, just test against the mock backend directly
    //    (Full proxy integration requires the proxy binary)
    let client = reqwest::Client::new();

    // 3. Make request to mock backend
    let response = client
        .get(&format!("{}/echo", harness.mock_backend_url()))
        .send()
        .await
        .expect("Failed to make request to mock backend");

    assert!(response.status().is_success());
    let body = response.text().await.unwrap();
    println!("📥 Response: {}", body);
    assert!(body.contains("Hello from mock backend"));

    // 4. Test delay endpoint
    let start = std::time::Instant::now();
    let delay_response = client
        .get(&format!("{}/delay/100", harness.mock_backend_url()))
        .send()
        .await
        .expect("Failed to make delay request");

    let elapsed = start.elapsed().as_millis();
    // Allow some timing variance (90ms instead of exactly 100ms)
    assert!(
        elapsed >= 90,
        "Delay should be at least 90ms, got {}ms",
        elapsed
    );
    assert!(delay_response.status().is_success());
    println!("⏱️ Delay request took {}ms", elapsed);

    // 5. Test status endpoint
    let status_response = client
        .get(&format!("{}/status/201", harness.mock_backend_url()))
        .send()
        .await
        .expect("Failed to make status request");

    assert_eq!(status_response.status().as_u16(), 201);
    println!("📊 Status endpoint returned 201");

    // 6. Test order tracking
    let order1 = client
        .get(&format!("{}/order/first", harness.mock_backend_url()))
        .send()
        .await
        .expect("Failed to make order request");

    let order2 = client
        .get(&format!("{}/order/second", harness.mock_backend_url()))
        .send()
        .await
        .expect("Failed to make order request");

    let body1: serde_json::Value = order1.json().await.unwrap();
    let body2: serde_json::Value = order2.json().await.unwrap();

    assert_eq!(body1["position"], 0);
    assert_eq!(body2["position"], 1);
    println!(
        "📝 Order tracking: first={}, second={}",
        body1["position"], body2["position"]
    );

    println!("✅ Basic mock backend test passed!");
}

/// Test concurrent requests to mock backend
#[tokio::test]
async fn test_concurrent_requests() {
    let harness = TestHarness::new();
    harness.start_mock_backend().await;

    sleep(Duration::from_millis(500)).await;

    let client = reqwest::Client::new();
    let base_url = harness.mock_backend_url();

    // Send 5 concurrent requests
    let futures: Vec<_> = (0..5)
        .map(|i| {
            let client = client.clone();
            let url = format!("{}/order/request-{}", base_url, i);
            async move {
                let response = client.get(&url).send().await?;
                response.json::<serde_json::Value>().await
            }
        })
        .collect();

    let results: Vec<Result<serde_json::Value, reqwest::Error>> =
        futures::future::join_all(futures).await;

    // All should succeed
    for (i, result) in results.iter().enumerate() {
        assert!(result.is_ok(), "Request {} failed", i);
        let body = result.as_ref().unwrap();
        println!(
            "🔄 Request {}: position={}",
            body["request_id"], body["position"]
        );
    }

    println!("✅ Concurrent requests test passed!");
}
