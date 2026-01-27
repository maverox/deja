//! Sequence Mismatch Detection Tests
//!
//! Tests for detecting when request/response sequences differ:
//! - Order changes
//! - Missing requests
//! - Extra requests
//! - Partial matches

mod integration;

use integration::harness::TestHarness;
use std::time::Duration;
use tokio::time::sleep;

/// Recorded sequence of requests with expected order
#[derive(Debug, Clone)]
struct RequestSequence {
    requests: Vec<String>,
    order: Vec<usize>,
}

impl RequestSequence {
    fn new(requests: Vec<&str>) -> Self {
        let requests: Vec<String> = requests.into_iter().map(|s| s.to_string()).collect();
        let order: Vec<usize> = (0..requests.len()).collect();
        Self { requests, order }
    }

    /// Check if actual order matches expected
    fn matches(&self, actual: &[String]) -> bool {
        if self.requests.len() != actual.len() {
            return false;
        }
        self.requests.iter().zip(actual.iter()).all(|(a, b)| a == b)
    }

    /// Generate diff report
    fn diff(&self, actual: &[String]) -> SequenceDiff {
        let mut missing = Vec::new();
        let mut extra = Vec::new();
        let mut reordered = Vec::new();

        for (i, expected) in self.requests.iter().enumerate() {
            if !actual.contains(expected) {
                missing.push(expected.clone());
            } else if actual.get(i) != Some(expected) {
                reordered.push(expected.clone());
            }
        }

        for req in actual {
            if !self.requests.contains(req) {
                extra.push(req.clone());
            }
        }

        SequenceDiff {
            matches: missing.is_empty() && extra.is_empty() && reordered.is_empty(),
            missing,
            extra,
            reordered,
        }
    }
}

#[derive(Debug)]
struct SequenceDiff {
    matches: bool,
    missing: Vec<String>,
    extra: Vec<String>,
    reordered: Vec<String>,
}

/// Test exact sequence matching
#[tokio::test]
async fn test_sequence_exact_match() {
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

    // Define expected sequence
    let expected = RequestSequence::new(vec!["A", "B", "C", "D"]);

    // Execute requests in order
    let mut actual = Vec::new();
    for req in &expected.requests {
        let response = client
            .get(&format!("{}/order/{}", harness.mock_backend_url(), req))
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = response.json().await.unwrap();
        actual.push(body["request_id"].as_str().unwrap().to_string());
    }

    let diff = expected.diff(&actual);
    assert!(diff.matches, "Sequences should match exactly");
    println!("✅ Exact sequence match: {:?}", actual);
}

/// Test sequence order mismatch detection
#[tokio::test]
async fn test_sequence_order_mismatch() {
    let expected = RequestSequence::new(vec!["A", "B", "C"]);
    let actual = vec!["A".to_string(), "C".to_string(), "B".to_string()];

    let diff = expected.diff(&actual);
    assert!(!diff.matches, "Should detect order mismatch");
    assert!(!diff.reordered.is_empty(), "Should have reordered items");
    println!("✅ Order mismatch detected: reordered={:?}", diff.reordered);
}

/// Test missing request detection
#[tokio::test]
async fn test_sequence_missing_request() {
    let expected = RequestSequence::new(vec!["A", "B", "C", "D"]);
    let actual = vec!["A".to_string(), "B".to_string(), "D".to_string()];

    let diff = expected.diff(&actual);
    assert!(!diff.matches, "Should detect missing request");
    assert!(
        diff.missing.contains(&"C".to_string()),
        "C should be missing"
    );
    println!("✅ Missing request detected: {:?}", diff.missing);
}

/// Test extra request detection
#[tokio::test]
async fn test_sequence_extra_request() {
    let expected = RequestSequence::new(vec!["A", "B", "C"]);
    let actual = vec![
        "A".to_string(),
        "B".to_string(),
        "X".to_string(),
        "C".to_string(),
    ];

    let diff = expected.diff(&actual);
    assert!(!diff.matches, "Should detect extra request");
    assert!(diff.extra.contains(&"X".to_string()), "X should be extra");
    println!("✅ Extra request detected: {:?}", diff.extra);
}

/// Test concurrent requests sequence (order may vary)
#[tokio::test]
async fn test_concurrent_sequence_membership() {
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

    // Send requests concurrently
    let expected = vec!["P", "Q", "R", "S", "T"];
    let futures: Vec<_> = expected
        .iter()
        .map(|req| {
            let client = client.clone();
            let url = format!("{}/order/{}", harness.mock_backend_url(), req);
            async move {
                let response = client.get(&url).send().await?;
                response.json::<serde_json::Value>().await
            }
        })
        .collect();

    let results: Vec<Result<serde_json::Value, reqwest::Error>> =
        futures::future::join_all(futures).await;

    // Collect actual request IDs
    let actual: Vec<String> = results
        .into_iter()
        .filter_map(|r| r.ok())
        .map(|v| v["request_id"].as_str().unwrap().to_string())
        .collect();

    // For concurrent requests, order may differ but membership should match
    let expected_set: std::collections::HashSet<_> =
        expected.iter().map(|s| s.to_string()).collect();
    let actual_set: std::collections::HashSet<_> = actual.iter().cloned().collect();

    assert_eq!(expected_set, actual_set, "All requests should be present");
    println!("✅ Concurrent sequence membership verified: {:?}", actual);
}
