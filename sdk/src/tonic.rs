//! Tonic gRPC auto-instrumentation for Deja
//!
//! Provides a Tower Layer and Service for tonic gRPC servers that automatically:
//! 1. Extracts or generates a trace ID from gRPC metadata
//! 2. Notifies the Proxy via Control API on request start/end
//! 3. Manages full trace context for the duration of the request

use crate::{
    current_task_id, generate_trace_id, get_runtime, with_trace_context, ControlMessage,
    DejaRuntime,
};
use futures::future::BoxFuture;
use http::{Request, Response};
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::info;

const TRACE_ID_HEADER: &str = "x-trace-id";
const REQUEST_ID_HEADER: &str = "x-request-id";

/// Layer for Tonic gRPC that handles Deja trace correlation
#[derive(Clone, Default)]
pub struct DejaTonicLayer;

impl<S> Layer<S> for DejaTonicLayer {
    type Service = DejaTonicService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DejaTonicService { inner }
    }
}

/// Service for Tonic gRPC that handles Deja trace correlation
#[derive(Clone)]
pub struct DejaTonicService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for DejaTonicService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Send + Clone + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();

        // 1. Get or generate trace ID from headers
        let trace_id = extract_trace_id(&req).unwrap_or_else(generate_trace_id);

        let runtime = get_runtime();

        Box::pin(async move {
            info!(trace_id = %trace_id, "Starting Deja-instrumented gRPC request");

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

/// Extract trace ID from request headers
fn extract_trace_id<B>(req: &Request<B>) -> Option<String> {
    req.headers()
        .get(TRACE_ID_HEADER)
        .or_else(|| req.headers().get(REQUEST_ID_HEADER))
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
}
