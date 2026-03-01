use crate::connector::connector_server::Connector;
use crate::connector::{ProcessRequest, ProcessResponse};
use crate::db_pools::DatabasePools;
use deja_sdk::{
    association::{associate_pg_connection, associate_redis_connection},
    deja_run, generate_trace_id, get_runtime, reqwest::DejaClient, trace_context::current_trace_id,
    ControlMessage, DejaAsyncBoundary, DejaRuntime,
};



use sqlx::Connection;
use std::sync::Arc;
use std::time::SystemTime;
use tonic::{Request, Response, Status};
use tracing::info;
use uuid::Uuid;

pub struct ConnectorService {
    pub pools: Arc<DatabasePools>,
    pub http_client: DejaClient,
    pub proxy_http_port: u16,
    pub pg_url: String,
}

#[tonic::async_trait]
impl Connector for ConnectorService {
    async fn process(
        &self,
        request: Request<ProcessRequest>,
    ) -> Result<Response<ProcessResponse>, Status> {
        let req = request.into_inner();
        let runtime = get_runtime();

        let control_client = runtime.control_client();
        let trace_id = if !req.id.is_empty() {
            let id = req.id.clone();
            control_client
                .send_best_effort(ControlMessage::start_trace(&id))
                .await;
            id
        } else {
            current_trace_id().unwrap_or_else(generate_trace_id)
        };

        info!(trace_id = %trace_id, request_id = %req.id, "Processing gRPC request");

        deja_sdk::with_trace_id(trace_id.clone(), async move {
            let generated_uuid = deja_run(&*runtime, "uuid", || Uuid::new_v4()).await.to_string();
            let timestamp_ms = deja_run(&*runtime, "time", || {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0)
            })
            .await;

            let http_result = match self.do_http_work(&trace_id).await {
                Ok(res) => res,
                Err(e) => format!("HTTP Error: {}", e),
            };

            let redis_result = match self.do_redis_work(&trace_id, &req.id, &req.data).await {
                Ok(res) => res,
                Err(e) => format!("Redis Error: {}", e),
            };

            let redis_complex = match self.do_redis_complex_work(&trace_id).await {
                Ok(res) => res,
                Err(e) => format!("Redis Complex Error: {}", e),
            };
            let redis_result = format!("{}, {}", redis_result, redis_complex);

            let pg_result = match self.do_pg_work(&trace_id, &req.id, &req.data).await {
                Ok(res) => res,
                Err(e) => format!("PG Error: {}", e),
            };

            let rt_clone = runtime.clone();
            let trace_id_for_bg = trace_id.clone();
            let _ = deja_sdk::spawn_traced({
                let tid = trace_id_for_bg.clone();
                async move {
                    let _ = deja_sdk::with_trace_id(tid.clone(), async move {
                        let _bg_uuid = deja_run(&*rt_clone, "bg_uuid", || Uuid::new_v4()).await;
                        info!(trace_id = %tid, "Generated background UUID: {}", _bg_uuid);
                    }).await;
                }
            });

            runtime.flush().await;

            control_client
                .send_best_effort(ControlMessage::end_trace(&trace_id))
                .await;

            Ok(Response::new(ProcessResponse {
                trace_id,
                pg_result,
                redis_result,
                http_result,
                status: "Success".to_string(),
                generated_uuid,
                timestamp_ms,
            }))
        })
        .await
    }
}

impl ConnectorService {
    async fn do_redis_complex_work(&self, trace_id: &str) -> Result<String, String> {
        info!("Performing Redis complex work...");

        let mut con = self
            .pools
            .redis_pool
            .get_connection()
            .await
            .map_err(|e| e.to_string())?;

        let runtime = get_runtime();
        let _ = associate_redis_connection(&mut con, runtime.control_client()).await;
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

        let mut sorted_hash: Vec<_> = hash_all.into_iter().collect();
        sorted_hash.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(format!("Hash: {:?}, List: {:?}", sorted_hash, list_range))
    }

    async fn do_http_work(
        &self,
        _trace_id: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        info!("Performing outgoing HTTP request via DejaClient...");

        let url = format!("http://localhost:{}/echo", self.proxy_http_port);
        let resp = self.http_client.get(&url).await?;

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

        let mut conn = self.pools.redis_pool.get_connection().await?;

        let runtime = get_runtime();
        let _ = associate_redis_connection(&mut conn, runtime.control_client()).await;
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

        let mut conn = sqlx::postgres::PgConnection::connect(&self.pg_url).await?;

        let runtime = get_runtime();
        let _ = associate_pg_connection(&mut conn, runtime.control_client()).await;

        sqlx::query(&format!("SET application_name = 'deja:{}'", trace_id))
            .execute(&mut conn)
            .await?;

        sqlx::query("CREATE TABLE IF NOT EXISTS connector_logs (id TEXT PRIMARY KEY, data TEXT, trace_id TEXT)")
            .execute(&mut conn).await?;

        sqlx::query("INSERT INTO connector_logs (id, data, trace_id) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET data = $2")
            .bind(id).bind(data).bind(trace_id)
            .execute(&mut conn).await?;

        let row: (String, String) =
            sqlx::query_as("SELECT data, trace_id FROM connector_logs WHERE id = $1")
                .bind(id)
                .fetch_one(&mut conn)
                .await?;

        Ok(format!("PG Saved: {}, Trace: {}", row.0, row.1))
    }
}
