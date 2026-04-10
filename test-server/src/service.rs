//! gRPC Service Implementation
//!
//! Implements the TestService gRPC interface with:
//! - Echo: Simple request/response with backend call
//! - GenerateId: Non-deterministic value generation
//! - MultiBackendCall: Fan-out to multiple backends
//! - ControlledBehavior: Configurable delays/errors

use crate::http_client::HttpClientWrapper;
use crate::test_service::{
    test_service_server::TestService, BackendResult, ControlledRequest, ControlledResponse,
    EchoRequest, EchoResponse, GenerateIdRequest, GenerateIdResponse, MultiBackendRequest,
    MultiBackendResponse, Scenario,
};
use deja_sdk::{current_trace_id, deja_run, get_runtime};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};

/// Implementation of the TestService gRPC service
pub struct TestServiceImpl {
    http_client: HttpClientWrapper,
}

impl TestServiceImpl {
    pub fn new(backend_url: impl Into<String>) -> Self {
        Self {
            http_client: HttpClientWrapper::new(backend_url),
        }
    }

    /// Extract trace ID from gRPC metadata
    fn get_trace_id<T>(request: &Request<T>) -> Option<String> {
        request
            .metadata()
            .get("x-trace-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .or_else(current_trace_id)
    }
}

#[tonic::async_trait]
impl TestService for TestServiceImpl {
    /// Echo with backend HTTP call
    #[instrument(skip(self, request))]
    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        let trace_id = Self::get_trace_id(&request);
        let req = request.into_inner();

        info!("Echo request: message={}", req.message);

        // Get timestamp using Deja runtime (for deterministic replay)
        let runtime = get_runtime();
        let timestamp_ms: u64 = deja_run(&*runtime, "timestamp", || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        })
        .await;

        // Make HTTP call to backend
        let backend_url = if req.backend_url.is_empty() {
            "/echo".to_string()
        } else {
            req.backend_url
        };

        let backend_response = match self
            .http_client
            .get(&backend_url, trace_id.as_deref())
            .await
        {
            Ok(result) => result.body,
            Err(e) => {
                warn!("Backend call failed: {}", e);
                format!("Backend error: {}", e)
            }
        };

        Ok(Response::new(EchoResponse {
            original_message: req.message,
            backend_response,
            timestamp_ms: timestamp_ms as i64,
        }))
    }

    /// Generate non-deterministic values
    #[instrument(skip(self, request))]
    async fn generate_id(
        &self,
        request: Request<GenerateIdRequest>,
    ) -> Result<Response<GenerateIdResponse>, Status> {
        let req = request.into_inner();

        info!(
            "GenerateId request: uuid={}, ts={}, random={}",
            req.generate_uuid, req.generate_timestamp, req.generate_random
        );

        let runtime = get_runtime();

        let uuid = if req.generate_uuid {
            deja_run(&*runtime, "uuid", || uuid::Uuid::new_v4())
                .await
                .to_string()
        } else {
            String::new()
        };

        let timestamp_ms = if req.generate_timestamp {
            deja_run(&*runtime, "timestamp", || {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0)
            })
            .await as i64
        } else {
            0
        };

        let random_bytes = if req.generate_random {
            let count = if req.random_byte_count > 0 {
                req.random_byte_count as usize
            } else {
                16
            };
            deja_run(&*runtime, &format!("random_bytes:{}", count), move || {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let mut bytes = vec![0u8; count];
                rng.fill(&mut bytes[..]);
                bytes
            })
            .await
        } else {
            Vec::new()
        };

        Ok(Response::new(GenerateIdResponse {
            uuid,
            timestamp_ms,
            random_bytes,
        }))
    }

    /// Call multiple backends
    #[instrument(skip(self, request))]
    async fn multi_backend_call(
        &self,
        request: Request<MultiBackendRequest>,
    ) -> Result<Response<MultiBackendResponse>, Status> {
        let trace_id = Self::get_trace_id(&request);
        let req = request.into_inner();

        info!(
            "MultiBackendCall: {} backends, parallel={}",
            req.backend_urls.len(),
            req.parallel
        );

        let start = Instant::now();
        let results: Vec<BackendResult>;

        if req.parallel {
            // Parallel calls
            let http_results = self
                .http_client
                .get_many(&req.backend_urls, trace_id.as_deref())
                .await;
            results = req
                .backend_urls
                .iter()
                .zip(http_results)
                .map(|(url, result)| match result {
                    Ok(r) => BackendResult {
                        url: url.clone(),
                        status_code: r.status_code as i32,
                        response_body: r.body,
                        latency_ms: r.latency_ms as i64,
                        error: String::new(),
                    },
                    Err(e) => BackendResult {
                        url: url.clone(),
                        status_code: 0,
                        response_body: String::new(),
                        latency_ms: 0,
                        error: e.to_string(),
                    },
                })
                .collect();
        } else {
            // Sequential calls
            let mut seq_results = Vec::new();
            for url in &req.backend_urls {
                let result = self
                    .http_client
                    .get_absolute(url, trace_id.as_deref())
                    .await;
                seq_results.push((url.clone(), result));
            }
            results = seq_results
                .into_iter()
                .map(|(url, result)| match result {
                    Ok(r) => BackendResult {
                        url,
                        status_code: r.status_code as i32,
                        response_body: r.body,
                        latency_ms: r.latency_ms as i64,
                        error: String::new(),
                    },
                    Err(e) => BackendResult {
                        url,
                        status_code: 0,
                        response_body: String::new(),
                        latency_ms: 0,
                        error: e.to_string(),
                    },
                })
                .collect();
        }

        let total_latency_ms = start.elapsed().as_millis() as i64;

        Ok(Response::new(MultiBackendResponse {
            results,
            total_latency_ms,
        }))
    }

    /// Controlled behavior for testing edge cases
    #[instrument(skip(self, request))]
    async fn controlled_behavior(
        &self,
        request: Request<ControlledRequest>,
    ) -> Result<Response<ControlledResponse>, Status> {
        let trace_id = Self::get_trace_id(&request);
        let req = request.into_inner();

        info!(
            "ControlledBehavior: delay={}ms, scenario={:?}",
            req.delay_ms, req.scenario
        );

        let start = Instant::now();

        // Inject delay if specified
        if req.delay_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(req.delay_ms as u64)).await;
        }

        // Handle different scenarios
        match Scenario::try_from(req.scenario).unwrap_or(Scenario::Unspecified) {
            Scenario::Unspecified | Scenario::Success => {
                // Make backend call if URL provided
                if !req.backend_url.is_empty() {
                    let _ = self
                        .http_client
                        .get_absolute(&req.backend_url, trace_id.as_deref())
                        .await;
                }

                Ok(Response::new(ControlledResponse {
                    success: true,
                    message: "Success".to_string(),
                    actual_delay_ms: start.elapsed().as_millis() as i64,
                }))
            }
            Scenario::Timeout => {
                // Simulate timeout
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                Ok(Response::new(ControlledResponse {
                    success: false,
                    message: "Timeout".to_string(),
                    actual_delay_ms: start.elapsed().as_millis() as i64,
                }))
            }
            Scenario::BackendError => Err(Status::internal(req.error_message.clone())),
            Scenario::RandomFailure => {
                // Use Deja runtime for deterministic randomness
                let runtime = get_runtime();
                let random: Vec<u8> = deja_run(&*runtime, "random_failure", || {
                    use rand::Rng;
                    vec![rand::thread_rng().gen::<u8>()]
                })
                .await;
                if random.first().map(|b| b % 2 == 0).unwrap_or(false) {
                    Err(Status::internal("Random failure"))
                } else {
                    Ok(Response::new(ControlledResponse {
                        success: true,
                        message: "Lucky success".to_string(),
                        actual_delay_ms: start.elapsed().as_millis() as i64,
                    }))
                }
            }
        }
    }
}
