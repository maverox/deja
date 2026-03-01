//! Axum/Tower auto-instrumentation for Deja
//!
//! Provides a Tower Layer and Service that automatically:
//! 1. Extracts or generates a trace ID.
//! 2. Notifies the Proxy via Control API on request start/end.
//! 3. Manages full trace context for the duration of the request.

use crate::{
    current_task_id, generate_trace_id, get_runtime, with_trace_context, ControlMessage,
    DejaRuntime,
};
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

        let runtime = get_runtime();

        Box::pin(async move {
            info!(trace_id = %trace_id, "Starting Deja-instrumented request");

            let res = with_trace_context(trace_id.clone(), async {
                let control_client = DejaRuntime::control_client(runtime.as_ref());
                let task_id = current_task_id().unwrap_or_else(|| "0".to_string());
                control_client
                    .send_best_effort(ControlMessage::start_trace_with_task(&trace_id, &task_id))
                    .await;

                let result = inner.call(req).await;

                control_client
                    .send_best_effort(ControlMessage::end_trace_with_task(&trace_id, &task_id))
                    .await;

                result
            })
            .await;

            res
        })
    }
}
