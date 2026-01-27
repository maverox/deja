//! Connector Service - A demo service for Deja record & replay integration.
//!
//! This service demonstrates how to integrate deja-core for:
//! - Trace context propagation
//! - Recording/Replaying database and cache interactions

use axum::{extract::State, routing::post, Json, Router};
use deja_core::control_api::{ControlClient, ControlMessage};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, error};

// ============================================================================
// Configuration
// ============================================================================

/// Application state shared across handlers
#[derive(Clone)]
struct AppState {
    control_client: Arc<ControlClient>,
    pg_pool: sqlx::PgPool,
    redis_client: redis::Client,
}

// ============================================================================
// Request/Response Types
// ============================================================================

#[derive(Debug, Deserialize)]
struct ProcessRequest {
    id: String,
    data: String,
}

#[derive(Debug, Serialize)]
struct ProcessResponse {
    trace_id: String,
    pg_result: String,
    redis_result: String,
    status: String,
}

// ============================================================================
// Handlers
// ============================================================================

async fn process_handler(
    State(state): State<AppState>,
    Json(request): Json<ProcessRequest>,
) -> Json<ProcessResponse> {
    // Generate a trace ID for this request
    let trace_id = deja_core::generate_trace_id();

    // Wrap entire handler in trace context
    let response = deja_core::with_trace_id(trace_id.clone(), async {
        // Notify proxy of trace start
        let _ = state
            .control_client
            .send(ControlMessage::start_trace(&trace_id))
            .await;

        info!(trace_id = %trace_id, request_id = %request.id, "Processing request");

        // --- Redis Interaction ---
        let redis_result = match do_redis_work(&state, &trace_id, &request).await {
            Ok(val) => val,
            Err(e) => {
                error!(error = %e, "Redis operation failed");
                format!("error: {}", e)
            }
        };

        // --- Postgres Interaction ---
        let pg_result = match do_pg_work(&state, &trace_id, &request).await {
            Ok(val) => val,
            Err(e) => {
                error!(error = %e, "Postgres operation failed");
                format!("error: {}", e)
            }
        };

        // Notify proxy of trace end
        let _ = state
            .control_client
            .send(ControlMessage::end_trace(&trace_id))
            .await;

        ProcessResponse {
            trace_id: trace_id.clone(),
            pg_result,
            redis_result,
            status: "ok".to_string(),
        }
    })
    .await;

    Json(response)
}

async fn do_redis_work(
    state: &AppState,
    trace_id: &str,
    request: &ProcessRequest,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Get a connection
    let mut conn = state.redis_client.get_multiplexed_async_connection().await?;
    
    // Notify proxy about this connection (optional, for correlation)
    // In a real scenario, we might use an interceptor layer
    let _ = state
        .control_client
        .send(ControlMessage::associate_connection(trace_id, "redis-conn"))
        .await;

    // SET operation
    let key = format!("connector:{}:{}", request.id, trace_id);
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg(&request.data)
        .query_async(&mut conn)
        .await?;

    // GET operation
    let value: String = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut conn)
        .await?;

    info!(trace_id = %trace_id, key = %key, value = %value, "Redis SET/GET completed");

    Ok(format!("SET {} = {}, GET returned {}", key, request.data, value))
}

async fn do_pg_work(
    state: &AppState,
    trace_id: &str,
    request: &ProcessRequest,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Notify proxy about DB connection
    let _ = state
        .control_client
        .send(ControlMessage::associate_connection(trace_id, "pg-conn"))
        .await;

    // Create table if not exists (for demo purposes)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS connector_data (
            id TEXT PRIMARY KEY,
            trace_id TEXT NOT NULL,
            data TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        "#,
    )
    .execute(&state.pg_pool)
    .await?;

    // Insert data
    sqlx::query(
        r#"
        INSERT INTO connector_data (id, trace_id, data)
        VALUES ($1, $2, $3)
        ON CONFLICT (id) DO UPDATE SET data = $3, trace_id = $2
        "#,
    )
    .bind(&request.id)
    .bind(trace_id)
    .bind(&request.data)
    .execute(&state.pg_pool)
    .await?;

    // Select data back
    let row: (String, String, String) = sqlx::query_as(
        "SELECT id, trace_id, data FROM connector_data WHERE id = $1",
    )
    .bind(&request.id)
    .fetch_one(&state.pg_pool)
    .await?;

    info!(
        trace_id = %trace_id,
        db_id = %row.0,
        db_trace = %row.1,
        db_data = %row.2,
        "Postgres INSERT/SELECT completed"
    );

    Ok(format!(
        "Inserted and retrieved: id={}, trace_id={}, data={}",
        row.0, row.1, row.2
    ))
}

async fn health_handler() -> &'static str {
    "OK"
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "connector_service=info,deja_core=debug".into()),
        )
        .init();

    info!("Starting Connector Service...");

    // --- Configuration from Environment ---
    let pg_host = std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".into());
    let pg_port = std::env::var("POSTGRES_PORT").unwrap_or_else(|_| "5433".into()); // Deja proxy port
    let pg_user = std::env::var("POSTGRES_USER").unwrap_or_else(|_| "deja_user".into());
    let pg_password = std::env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "password".into());
    let pg_db = std::env::var("POSTGRES_DB").unwrap_or_else(|_| "deja_test".into());

    let redis_host = std::env::var("REDIS_HOST").unwrap_or_else(|_| "localhost".into());
    let redis_port = std::env::var("REDIS_PORT").unwrap_or_else(|_| "6390".into()); // Deja proxy port

    let deja_control_host = std::env::var("DEJA_CONTROL_HOST").unwrap_or_else(|_| "localhost".into());
    let deja_control_port: u16 = std::env::var("DEJA_CONTROL_PORT")
        .unwrap_or_else(|_| "9999".into())
        .parse()
        .unwrap_or(9999);

    let listen_addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into());

    // --- Initialize Clients ---
    info!("Connecting to Postgres at {}:{}...", pg_host, pg_port);
    let pg_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        pg_user, pg_password, pg_host, pg_port, pg_db
    );
    let pg_pool = sqlx::PgPool::connect(&pg_url).await?;
    info!("Postgres connection pool established.");

    info!("Connecting to Redis at {}:{}...", redis_host, redis_port);
    let redis_url = format!("redis://{}:{}", redis_host, redis_port);
    let redis_client = redis::Client::open(redis_url)?;
    info!("Redis client created.");

    info!(
        "Connecting to Deja Control API at {}:{}...",
        deja_control_host, deja_control_port
    );
    let control_client = Arc::new(ControlClient::new(&deja_control_host, deja_control_port));
    info!("Deja Control Client initialized.");

    // --- Create App State ---
    let state = AppState {
        control_client,
        pg_pool,
        redis_client,
    };

    // --- Build Router ---
    let app = Router::new()
        .route("/process", post(process_handler))
        .route("/health", axum::routing::get(health_handler))
        .with_state(state);

    // --- Start Server ---
    let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
    info!("Connector Service listening on {}", listen_addr);

    axum::serve(listener, app).await?;

    Ok(())
}
