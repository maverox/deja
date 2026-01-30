//! Axum/Tower auto-instrumentation for Deja
//!
//! Provides a Tower Layer and Service that automatically:
//! 1. Extracts or generates a trace ID.
//! 2. Notifies the Proxy via Control API on request start/end.
//! 3. Manages the `with_trace_id` scope for the duration of the request.

use crate::{generate_trace_id, get_runtime_arc, with_trace_id, ControlMessage, DejaRuntime};
use axum::{body::Body, extract::Request, response::Response};
use futures::future::BoxFuture;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::info;

/// Layer for Axum/Tower that handles Deja trace correlation
#[derive(Clone, Default)]
pub struct DejaLayer;

impl<S> Layer<S> for DejaLayer {
    type Service = DejaService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DejaService { inner }
    }
}

/// Service for Axum/Tower that handles Deja trace correlation
#[derive(Clone)]
pub struct DejaService<S> {
    inner: S,
}

impl<S> Service<Request<Body>> for DejaService<S>
where
    S: Service<Request<Body>, Response = Response> + Send + Clone + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let mut inner = self.inner.clone();

        // 1. Get or generate trace ID
        let trace_id = req
            .headers()
            .get("x-trace-id")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(generate_trace_id);

        let runtime = get_runtime_arc();

        Box::pin(async move {
            info!(trace_id = %trace_id, "Starting Deja-instrumented request");

            // 2. Notify Proxy of trace start
            let control_client = DejaRuntime::control_client(runtime.as_ref());
            control_client
                .send_best_effort(ControlMessage::start_trace(&trace_id))
                .await;

            // 3. Wrap inner service in trace context
            let res = with_trace_id(trace_id.clone(), inner.call(req)).await;

            // 4. Notify Proxy of trace end (optional but good for boundaries)
            control_client
                .send_best_effort(ControlMessage::end_trace(&trace_id))
                .await;

            res
        })
    }
}
