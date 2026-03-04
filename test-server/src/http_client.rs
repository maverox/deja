//! HTTP Client for outgoing requests
//!
//! Wraps reqwest to make HTTP calls to backends, supporting:
//! - Trace header propagation
//! - Configurable base URL (for routing through proxy)
//! - Error handling and timing

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, instrument};

#[derive(Error, Debug)]
pub enum HttpClientError {
    #[error("Request failed: {0}")]
    RequestFailed(#[from] reqwest::Error),

    #[error("Timeout after {0}ms")]
    Timeout(u64),

    #[error("Backend error: {status} - {message}")]
    BackendError { status: u16, message: String },
}

/// Result of an HTTP call
#[derive(Debug, Clone)]
pub struct HttpResult {
    pub status_code: u16,
    pub body: String,
    pub latency_ms: u64,
}

/// HTTP client for making outgoing requests
pub struct HttpClientWrapper {
    client: Client,
    base_url: String,
}

impl HttpClientWrapper {
    /// Create a new HTTP client with the given base URL
    pub fn new(base_url: impl Into<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            base_url: base_url.into(),
        }
    }

    /// Make a GET request to the given path
    #[instrument(skip(self))]
    pub async fn get(
        &self,
        path: &str,
        trace_id: Option<&str>,
    ) -> Result<HttpResult, HttpClientError> {
        let url = format!("{}{}", self.base_url, path);
        debug!("GET {}", url);

        let start = Instant::now();

        let mut request = self.client.get(&url);

        // Add trace header if provided
        if let Some(trace_id) = trace_id {
            request = request.header("X-Trace-Id", trace_id);
        }

        let response = request.send().await?;
        let status_code = response.status().as_u16();
        let body = response.text().await?;
        let latency_ms = start.elapsed().as_millis() as u64;

        Ok(HttpResult {
            status_code,
            body,
            latency_ms,
        })
    }

    /// Make a POST request with JSON body
    #[instrument(skip(self, body))]
    pub async fn post<T: Serialize>(
        &self,
        path: &str,
        body: &T,
        trace_id: Option<&str>,
    ) -> Result<HttpResult, HttpClientError> {
        let url = format!("{}{}", self.base_url, path);
        debug!("POST {}", url);

        let start = Instant::now();

        let mut request = self.client.post(&url).json(body);

        if let Some(trace_id) = trace_id {
            request = request.header("X-Trace-Id", trace_id);
        }

        let response = request.send().await?;
        let status_code = response.status().as_u16();
        let body = response.text().await?;
        let latency_ms = start.elapsed().as_millis() as u64;

        Ok(HttpResult {
            status_code,
            body,
            latency_ms,
        })
    }

    /// Make a raw request to an absolute URL
    #[instrument(skip(self))]
    pub async fn get_absolute(
        &self,
        url: &str,
        trace_id: Option<&str>,
    ) -> Result<HttpResult, HttpClientError> {
        debug!("GET (absolute) {}", url);

        let start = Instant::now();

        let mut request = self.client.get(url);

        if let Some(trace_id) = trace_id {
            request = request.header("X-Trace-Id", trace_id);
        }

        let response = request.send().await?;
        let status_code = response.status().as_u16();
        let body = response.text().await?;
        let latency_ms = start.elapsed().as_millis() as u64;

        Ok(HttpResult {
            status_code,
            body,
            latency_ms,
        })
    }

    /// Make multiple parallel requests
    pub async fn get_many(
        &self,
        urls: &[String],
        trace_id: Option<&str>,
    ) -> Vec<Result<HttpResult, HttpClientError>> {
        let futures: Vec<_> = urls
            .iter()
            .map(|url| self.get_absolute(url, trace_id))
            .collect();

        futures::future::join_all(futures).await
    }
}

/// Simple echo request/response for testing
#[derive(Debug, Serialize, Deserialize)]
pub struct EchoPayload {
    pub message: String,
    pub timestamp: i64,
}
