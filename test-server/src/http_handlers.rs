use actix_web::{web, HttpRequest, HttpResponse, Responder};
use deja_sdk::{
    association::{associate_pg_connection, associate_redis_connection},
    deja_run, generate_trace_id, get_runtime,
    reqwest::DejaClient,
    spawn_with_task_id, with_trace_context, ControlMessage, DejaRuntime,
};

use serde::{Deserialize, Serialize};
use sqlx::Connection;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::info;
use uuid::Uuid;

use crate::db_pools::DatabasePools;

pub struct HttpServiceState {
    pub pools: Arc<DatabasePools>,
    pub http_client: DejaClient,
    pub proxy_http_port: u16,
    pub pg_url: String,
}

#[derive(Debug, Deserialize)]
pub struct ProcessRequest {
    pub id: Option<String>,
    pub data: String,
}

#[derive(Debug, Deserialize)]
pub struct CorrelationTortureRequest {
    pub id: Option<String>,
    pub data: String,
    pub workers: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct ProcessResponse {
    pub trace_id: String,
    pub pg_result: String,
    pub redis_result: String,
    pub http_result: String,
    pub status: String,
    pub generated_uuid: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct CorrelationTortureResponse {
    pub trace_id: String,
    pub workers: usize,
    pub child_task_ids: Vec<String>,
    pub nd_events: usize,
    pub completed_workers: usize,
    pub status: String,
}

pub async fn process_handler(
    req: HttpRequest,
    body: web::Json<ProcessRequest>,
    state: web::Data<HttpServiceState>,
) -> impl Responder {
    let runtime = get_runtime();
    let control_client = runtime.control_client();

    let trace_id = if let Some(id) = &body.id {
        id.clone()
    } else {
        req.headers()
            .get("x-trace-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(generate_trace_id)
    };

    info!(trace_id = %trace_id, "Processing HTTP request");

    with_trace_context(trace_id.clone(), async move {
        let task_id = deja_sdk::current_task_id().unwrap_or_else(|| "0".to_string());
        control_client
            .send_best_effort(ControlMessage::start_trace_with_task(&trace_id, &task_id))
            .await;

        let generated_uuid = deja_run(&*runtime, "uuid", || Uuid::new_v4())
            .await
            .to_string();
        let timestamp_ms = deja_run(&*runtime, "time", || {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        })
        .await;

        let http_result =
            match do_http_work(&state.http_client, state.proxy_http_port, &trace_id).await {
                Ok(res) => res,
                Err(e) => format!("HTTP Error: {}", e),
            };

        let redis_result = match do_redis_work(
            &state.pools,
            &trace_id,
            body.id.as_deref().unwrap_or("http"),
            &body.data,
        )
        .await
        {
            Ok(res) => res,
            Err(e) => format!("Redis Error: {}", e),
        };

        let pg_result = match do_pg_work(
            &state.pg_url,
            &trace_id,
            body.id.as_deref().unwrap_or("http"),
            &body.data,
        )
        .await
        {
            Ok(res) => res,
            Err(e) => format!("PG Error: {}", e),
        };

        let rt_clone = runtime.clone();
        let trace_id_for_bg = trace_id.clone();
        let _ = deja_sdk::spawn_traced(async move {
            let bg_uuid = deja_run(&*rt_clone, "bg_uuid", || Uuid::new_v4()).await;
            info!(trace_id = %trace_id_for_bg, "Generated background UUID: {}", bg_uuid);
        });

        runtime.flush().await;

        control_client
            .send_best_effort(ControlMessage::end_trace_with_task(&trace_id, &task_id))
            .await;

        HttpResponse::Ok().json(ProcessResponse {
            trace_id,
            pg_result,
            redis_result,
            http_result,
            status: "Success".to_string(),
            generated_uuid,
            timestamp_ms,
        })
    })
    .await
}

pub async fn health_handler() -> impl Responder {
    let timestamp_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    HttpResponse::Ok().json(HealthResponse {
        status: "healthy".to_string(),
        timestamp_ms,
    })
}

pub async fn correlation_torture_handler(
    req: HttpRequest,
    body: web::Json<CorrelationTortureRequest>,
    state: web::Data<HttpServiceState>,
) -> impl Responder {
    let runtime = get_runtime();
    let control_client = runtime.control_client();

    let trace_id = if let Some(id) = &body.id {
        id.clone()
    } else {
        req.headers()
            .get("x-trace-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(generate_trace_id)
    };

    let workers = body.workers.unwrap_or(4).clamp(2, 12);

    with_trace_context(trace_id.clone(), async move {
        let task_id = deja_sdk::current_task_id().unwrap_or_else(|| "0".to_string());
        control_client
            .send_best_effort(ControlMessage::start_trace_with_task(&trace_id, &task_id))
            .await;

        let mut handles = Vec::with_capacity(workers);
        let mut task_ids = Vec::with_capacity(workers);

        for worker_idx in 0..workers {
            let trace_for_worker = trace_id.clone();
            let worker_id = format!("{}-worker-{}", trace_for_worker, worker_idx);
            let worker_data = format!("{}-{}", body.data, worker_idx);
            let state_ref = state.clone();
            let runtime_ref = runtime.clone();

            let (handle, task_id) = spawn_with_task_id(async move {
                let _ = deja_run(&*runtime_ref, "uuid", || Uuid::new_v4()).await;
                let _ = deja_run(&*runtime_ref, "time", || {
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0)
                })
                .await;

                let _ = do_http_work(
                    &state_ref.http_client,
                    state_ref.proxy_http_port,
                    &trace_for_worker,
                )
                .await;
                let _ = do_redis_work(
                    &state_ref.pools,
                    &trace_for_worker,
                    &worker_id,
                    &worker_data,
                )
                .await;
                let _ = do_pg_work(
                    &state_ref.pg_url,
                    &trace_for_worker,
                    &worker_id,
                    &worker_data,
                )
                .await;
                true
            });

            task_ids.push(task_id);
            handles.push(handle);
        }

        let runtime_for_bg = runtime.clone();
        let bg_trace = trace_id.clone();
        let _ = deja_sdk::spawn_traced(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            let _ = deja_run(&*runtime_for_bg, "bg_uuid", || Uuid::new_v4()).await;
            info!(trace_id = %bg_trace, "Generated background UUID in torture handler");
        });

        let mut completed_workers = 0usize;
        for handle in handles {
            if let Ok(true) = handle.await {
                completed_workers += 1;
            }
        }

        runtime.flush().await;

        control_client
            .send_best_effort(ControlMessage::end_trace_with_task(&trace_id, &task_id))
            .await;

        HttpResponse::Ok().json(CorrelationTortureResponse {
            trace_id,
            workers,
            child_task_ids: task_ids,
            nd_events: workers * 2 + 1,
            completed_workers,
            status: "ok".to_string(),
        })
    })
    .await
}

async fn do_http_work(
    client: &DejaClient,
    proxy_http_port: u16,
    _trace_id: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("http://localhost:{}/echo", proxy_http_port);
    let resp = client.get(&url).await?;
    let status = resp.status();
    let body = resp.text().await?;
    Ok(format!("HTTP {} - Body: {}", status, body))
}

async fn do_redis_work(
    pools: &DatabasePools,
    trace_id: &str,
    id: &str,
    data: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut conn = pools.redis_pool.get_connection().await?;
    let runtime = get_runtime();
    let _ = associate_redis_connection(&mut conn, runtime.control_client()).await;

    let key = format!("http:{}:{}", id, trace_id);

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
    pg_url: &str,
    trace_id: &str,
    id: &str,
    data: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut conn = sqlx::postgres::PgConnection::connect(pg_url).await?;
    let runtime = get_runtime();
    let _ = associate_pg_connection(&mut conn, runtime.control_client()).await;

    sqlx::query(&format!("SET application_name = 'deja:{}'", trace_id))
        .execute(&mut conn)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS http_logs (id TEXT PRIMARY KEY, data TEXT, trace_id TEXT)",
    )
    .execute(&mut conn)
    .await?;

    sqlx::query("INSERT INTO http_logs (id, data, trace_id) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET data = $2")
        .bind(id).bind(data).bind(trace_id)
        .execute(&mut conn).await?;

    let row: (String, String) =
        sqlx::query_as("SELECT data, trace_id FROM http_logs WHERE id = $1")
            .bind(id)
            .fetch_one(&mut conn)
            .await?;

    Ok(format!("PG Saved: {}, Trace: {}", row.0, row.1))
}
