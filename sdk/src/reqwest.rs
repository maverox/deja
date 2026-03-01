//! Reqwest auto-correlation for Deja
//!
//! Provides a wrapper around `reqwest::Client` that automatically:
//! 1. Injects the current Deja trace ID into outgoing headers.
//! 2. Uses per-request clients to ensure connection isolation between traces.
//!
//! Source-port association is handled by the proxy's connection handler
//! when it sees the x-trace-id header.

use crate::{get_runtime, DejaRuntime};
use reqwest::{Client, Request, Response};

/// A wrapper around `reqwest::Client` that handles Deja trace correlation
#[derive(Clone)]
pub struct DejaClient {
    // No shared client - we create a fresh client per request to ensure
    // connection isolation between traces during replay
}

impl DejaClient {
    /// Create a new DejaClient
    pub fn new(_client: Client) -> Self {
        Self {}
    }

    /// Perform a request with automatic trace correlation.
    /// Creates a fresh client for each request to ensure connection isolation.
    pub async fn execute(&self, mut request: Request) -> reqwest::Result<Response> {
        let runtime = get_runtime();
        let trace_id = runtime.trace_id();

        if !trace_id.is_empty() {
            // Inject trace ID into headers — proxy extracts from HTTP directly
            request.headers_mut().insert(
                "x-trace-id",
                trace_id
                    .parse()
                    .unwrap_or_else(|_| "invalid".parse().unwrap()),
            );
        }

        // Create a fresh client per request — prevents HTTP keep-alive from
        // reusing connections across traces
        let client = Client::builder()
            .pool_max_idle_per_host(0)
            .tcp_keepalive(None)
            .build()?;
        client.execute(request).await
    }

    /// Convenience method for GET
    pub async fn get<U: reqwest::IntoUrl>(&self, url: U) -> reqwest::Result<Response> {
        let req = reqwest::Client::new().get(url).build()?;
        self.execute(req).await
    }

    /// Convenience method for POST
    pub async fn post<U: reqwest::IntoUrl>(&self, url: U) -> reqwest::Result<Response> {
        let req = reqwest::Client::new().post(url).build()?;
        self.execute(req).await
    }
}

/// Extension trait for reqwest::RequestBuilder to add Deja correlation
pub trait DejaRequestExt {
    fn with_deja_trace(self) -> Self;
}

impl DejaRequestExt for reqwest::RequestBuilder {
    fn with_deja_trace(self) -> Self {
        let runtime = get_runtime();
        let trace_id = runtime.trace_id();
        if !trace_id.is_empty() {
            self.header("x-trace-id", trace_id)
        } else {
            self
        }
    }
}

/// Configure the reqwest ClientBuilder to trust the Deja proxy CA certificate.
pub fn configure_trust(
    builder: reqwest::ClientBuilder,
    cert_content: Option<&str>,
) -> reqwest::Result<reqwest::ClientBuilder> {
    if let Some(content) = cert_content {
        let cert = reqwest::Certificate::from_pem(content.as_bytes())?;
        return Ok(builder.add_root_certificate(cert));
    }

    if let Ok(path) = std::env::var("DEJA_CA_CERT_PATH") {
        if let Ok(content) = std::fs::read_to_string(&path) {
            let cert = reqwest::Certificate::from_pem(content.as_bytes())?;
            return Ok(builder.add_root_certificate(cert));
        }
    }

    if let Ok(content) = std::env::var("DEJA_CA_CERT") {
        let cert = reqwest::Certificate::from_pem(content.as_bytes())?;
        return Ok(builder.add_root_certificate(cert));
    }

    Ok(builder)
}
