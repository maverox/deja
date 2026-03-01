use deja_sdk::pool::DejaRedisPool;
use sqlx::PgPool;
use std::sync::Arc;

pub struct DatabasePools {
    pub pg_pool: PgPool,
    pub redis_pool: DejaRedisPool,
}

impl DatabasePools {
    pub async fn new(pg_url: &str, redis_url: &str) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let pg_pool = PgPool::connect(pg_url).await?;
        let redis_client = redis::Client::open(redis_url)?;
        let redis_pool = DejaRedisPool::new(redis_client);

        Ok(Arc::new(Self {
            pg_pool,
            redis_pool,
        }))
    }
}
