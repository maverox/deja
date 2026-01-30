//! Connector Service - E2E Test Service for Deja (gRPC Version)
//!
//! Demonstrates:
//! - Incoming gRPC request handling
//! - Outgoing HTTP request via Deja-instrumented Reqwest
//! - Protocol-scoped sequence tracking for Postgres & Redis
//! - Trace context propagation via deja-sdk

use axum::{routing::get, Router};
use deja_sdk::{
    generate_trace_id, get_network_runtime, get_runtime_arc, reqwest::DejaClient, with_trace_id,
    ControlMessage, DejaAsyncBoundary, DejaRuntime,
};
use std::net::SocketAddr;
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;

// Import the generated gRPC types
pub mod connector {
    tonic::include_proto!("connector");
}
use connector::connector_server::{Connector, ConnectorServer};
use connector::{ProcessRequest, ProcessResponse};

// ============================================================================
// Configuration & State
// ============================================================================

struct MyConnector {
    pg_pool: sqlx::PgPool,
    redis_client: redis::Client,
    http_client: DejaClient,
    variant: String,
    proxy_http_port: u16,
}

// ============================================================================
// gRPC Handler Implementation
// ============================================================================

#[tonic::async_trait]
impl Connector for MyConnector {
    async fn process(
        &self,
        request: Request<ProcessRequest>,
    ) -> Result<Response<ProcessResponse>, Status> {
        let req = request.into_inner();

        // 1. Get or generate Trace ID
        let trace_id = if req.id.is_empty() {
            generate_trace_id()
        } else {
            req.id.clone()
        };
        let runtime = get_runtime_arc();
        let control_client = runtime.control_client();

        info!(trace_id = %trace_id, request_id = %req.id, "Starting gRPC process");

        // 2. Wrap in trace context
        let result = with_trace_id(trace_id.clone(), async {
            // Notify proxy of trace start
            control_client
                .send_best_effort(ControlMessage::start_trace(&trace_id))
                .await;

            // --- Capture Non-Deterministic Values ---
            let generated_uuid = runtime.uuid().await.to_string();
            let timestamp_ms = runtime.now_millis().await;

            // --- Outgoing HTTP Work ---
            let http_result = match self.do_http_work(&trace_id).await {
                Ok(res) => res,
                Err(e) => format!("HTTP Error: {}", e),
            };

            // --- Redis Work ---
            let redis_result = match self.do_redis_work(&trace_id, &req.id, &req.data).await {
                Ok(res) => res,
                Err(e) => format!("Redis Error: {}", e),
            };

            // --- Redis Complex Work (Epic 6) ---
            let redis_complex = match self.do_redis_complex_work(&trace_id).await {
                Ok(res) => res,
                Err(e) => format!("Redis Complex Error: {}", e),
            };
            let redis_result = format!("{}, {}", redis_result, redis_complex);

            // --- Postgres Work ---
            let pg_result = match self.do_pg_work(&trace_id, &req.id, &req.data).await {
                Ok(res) => res,
                Err(e) => format!("PG Error: {}", e),
            };

            // Notify proxy of trace end
            control_client
                .send_best_effort(ControlMessage::end_trace(&trace_id))
                .await;

            let status = if self.variant == "B" {
                "Success (v2)".to_string()
            } else {
                "Success".to_string()
            };

            // Demonstrate background task correlation
            if let Some(rt) = get_network_runtime() {
                rt.set_trace_id(trace_id.clone());
                let rt_clone = rt.clone();
                // Spawn a task that generates a UUID - should be part of the trace
                let _ = rt.spawn("bg_uuid_gen", async move {
                    let _bg_uuid = rt_clone.uuid().await;
                    info!("Generated background UUID: {}", _bg_uuid);
                });
            }

            ProcessResponse {
                trace_id: trace_id.clone(),
                pg_result,
                redis_result,
                http_result,
                status,
                generated_uuid,
                timestamp_ms,
            }
        })
        .await;

        Ok(Response::new(result))
    }
}

// ============================================================================
// Internal Work Functions
// ============================================================================

impl MyConnector {
    async fn do_redis_complex_work(&self, trace_id: &str) -> Result<String, String> {
        let mut con = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;

        // 1. Hash (Map)
        let hash_key = format!("connector:hash:{}", trace_id);
        let _: () = redis::cmd("HSET")
            .arg(&hash_key)
            .arg("field1")
            .arg("value1")
            .arg("field2")
            .arg("value2")
            .query_async(&mut con)
            .await
            .map_err(|e| e.to_string())?;

        let hash_all: std::collections::HashMap<String, String> = redis::cmd("HGETALL")
            .arg(&hash_key)
            .query_async(&mut con)
            .await
            .map_err(|e| e.to_string())?;

        // 2. List (Array)
        let list_key = format!("connector:list:{}", trace_id);

        let _: () = redis::cmd("DEL")
            .arg(&list_key)
            .query_async(&mut con)
            .await
            .map_err(|e| e.to_string())?;

        let _: () = redis::cmd("RPUSH")
            .arg(&list_key)
            .arg("item1")
            .arg("item2")
            .query_async(&mut con)
            .await
            .map_err(|e| e.to_string())?;

        let list_range: Vec<String> = redis::cmd("LRANGE")
            .arg(&list_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut con)
            .await
            .map_err(|e| e.to_string())?;

        // Sort hash keys for deterministic output string
        let mut sorted_hash: Vec<_> = hash_all.into_iter().collect();
        sorted_hash.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(format!("Hash: {:?}, List: {:?}", sorted_hash, list_range))
    }
    async fn do_http_work(
        &self,
        _trace_id: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        info!("Performing outgoing HTTP request via DejaClient targeting internal echo server...");

        // Call our internal echo server через proxy
        // Proxy maps proxy_http_port to our internal echo port
        let url = format!("http://localhost:{}/echo", self.proxy_http_port);
        let resp = self.http_client.get(url).await?;

        let status = resp.status();
        let body = resp.text().await?;

        Ok(format!("HTTP {} - Body: {}", status, body))
    }

    async fn do_redis_work(
        &self,
        trace_id: &str,
        id: &str,
        data: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        info!("Performing Redis work...");

        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;
        let runtime = get_runtime_arc();
        let control_client = runtime.control_client();

        // Correlation: Notify proxy
        control_client
            .send_best_effort(ControlMessage::associate_connection(trace_id, "redis-conn"))
            .await;

        let key = format!("connector:{}:{}", id, trace_id);
        let _: () = redis::cmd("SET")
            .arg(&key)
            .arg(data)
            .query_async(&mut conn)
            .await?;
        let val: String = redis::cmd("GET").arg(&key).query_async(&mut conn).await?;

        Ok(format!(
            "Redis key {} set to {}, retrieved {}",
            key, data, val
        ))
    }

    async fn do_pg_work(
        &self,
        trace_id: &str,
        id: &str,
        data: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        info!("Performing Postgres work...");
        let runtime = get_runtime_arc();
        let control_client = runtime.control_client();

        // Correlation: Notify proxy
        control_client
            .send_best_effort(ControlMessage::associate_connection(trace_id, "pg-conn"))
            .await;

        sqlx::query("CREATE TABLE IF NOT EXISTS connector_logs (id TEXT PRIMARY KEY, data TEXT, trace_id TEXT)")
            .execute(&self.pg_pool).await?;

        sqlx::query("INSERT INTO connector_logs (id, data, trace_id) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET data = $2")
            .bind(id).bind(data).bind(trace_id)
            .execute(&self.pg_pool).await?;

        let row: (String, String) =
            sqlx::query_as("SELECT data, trace_id FROM connector_logs WHERE id = $1")
                .bind(id)
                .fetch_one(&self.pg_pool)
                .await?;

        Ok(format!("PG Saved: {}, Trace: {}", row.0, row.1))
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    info!("Starting Test Service (gRPC) with Advanced Fidelity...");

    let variant = std::env::var("SERVICE_VARIANT").unwrap_or_else(|_| "A".into());
    info!("Service Variant: {}", variant);

    // Config
    let listen_addr = std::env::var("GRPC_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".into());
    let addr = listen_addr.parse()?;

    let echo_addr_str = std::env::var("HTTP_ECHO_ADDR").unwrap_or_else(|_| "127.0.0.1:8081".into());
    let echo_addr: SocketAddr = echo_addr_str.parse()?;
    let proxy_http_port = std::env::var("DEJA_HTTP_PROXY_PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()?;

    // Start internal echo server
    tokio::spawn(async move {
        let app = Router::new().route("/echo", get(|| async { "echo" }));
        info!("Starting internal echo server on {}", echo_addr);
        let listener = tokio::net::TcpListener::bind(echo_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    let pg_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://deja_user:password@localhost:5433/deja_test".into());
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6390".into());

    // Initialize Clients
    let pg_pool = sqlx::PgPool::connect(&pg_url).await?;
    let redis_client = redis::Client::open(redis_url)?;
    let http_client = DejaClient::new(reqwest::Client::new());

    let service = MyConnector {
        pg_pool,
        redis_client,
        http_client,
        variant,
        proxy_http_port,
    };

    info!("gRPC server listening on {}", addr);
    Server::builder()
        .add_service(ConnectorServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
