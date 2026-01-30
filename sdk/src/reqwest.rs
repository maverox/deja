//! Reqwest auto-correlation for Deja
//!
//! Provides a wrapper around `reqwest::Client` that automatically:
//! 1. Injects the current Deja trace ID into outgoing headers.
//! 2. Associates the connection with the trace via the Control API.

use crate::{get_runtime_arc, ControlMessage};
use reqwest::{Client, Request, Response};

/// A wrapper around `reqwest::Client` that handles Deja trace correlation
#[derive(Clone)]
pub struct DejaClient {
    inner: Client,
}

impl DejaClient {
    /// Create a new DejaClient wrapping a reqwest::Client
    pub fn new(client: Client) -> Self {
        Self { inner: client }
    }

    /// Perform a request with automatic trace correlation
    pub async fn execute(&self, mut request: Request) -> reqwest::Result<Response> {
        let runtime = get_runtime_arc();
        let trace_id = runtime.get_trace_id();

        if !trace_id.is_empty() {
            // 1. Inject trace ID into headers
            request.headers_mut().insert(
                "x-trace-id",
                trace_id
                    .parse()
                    .unwrap_or_else(|_| "invalid".parse().unwrap()),
            );

            // 2. Associate connection with trace
            // Note: We don't have the local connection ID here easily,
            // but the Proxy can use the trace header to associate.
            // If we want explicit association from SDK, we need a way to ID this client.
            let control_client = runtime.control_client();
            control_client
                .send_best_effort(ControlMessage::associate_connection(
                    &trace_id,
                    "outgoing-http",
                ))
                .await;
        }

        self.inner.execute(request).await
    }

    /// Convenience method for GET
    pub async fn get<U: reqwest::IntoUrl>(&self, url: U) -> reqwest::Result<Response> {
        let req = self.inner.get(url).build()?;
        self.execute(req).await
    }

    /// Convenience method for POST
    pub async fn post<U: reqwest::IntoUrl>(&self, url: U) -> reqwest::Result<Response> {
        let req = self.inner.post(url).build()?;
        self.execute(req).await
    }
}

/// Extension trait for reqwest::RequestBuilder to add Deja correlation
pub trait DejaRequestExt {
    fn with_deja_trace(self) -> Self;
}

impl DejaRequestExt for reqwest::RequestBuilder {
    fn with_deja_trace(self) -> Self {
        let runtime = get_runtime_arc();
        let trace_id = runtime.get_trace_id();
        if !trace_id.is_empty() {
            self.header("x-trace-id", trace_id)
        } else {
            self
        }
    }
}
