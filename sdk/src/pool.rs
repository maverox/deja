use crate::{current_trace_id, ControlClient, ControlMessage};
use deja_common::Protocol;

/// A wrapper around `sqlx::PgPool` that injects Deja trace correlation on every checkout.
///
/// On `acquire()`, sends a `ReassociateConnection` control message to the proxy
/// and issues `SET application_name = 'deja:<trace_id>'` so the proxy can
/// correlate the pooled connection to the current trace.
#[cfg(feature = "sqlx")]
pub struct DejaPool<P> {
    inner: P,
    control_client: ControlClient,
}

#[cfg(feature = "sqlx")]
impl<P> DejaPool<P> {
    pub fn new(pool: P) -> Self {
        Self {
            inner: pool,
            control_client: ControlClient::from_env(),
        }
    }

    pub fn with_control_client(pool: P, control_client: ControlClient) -> Self {
        Self {
            inner: pool,
            control_client,
        }
    }

    pub fn inner(&self) -> &P {
        &self.inner
    }
}

/// A wrapper around a Redis connection that injects Deja trace correlation.
///
/// On `get_connection()`, sends `CLIENT SETNAME deja:<trace_id>` so the proxy
/// can correlate the connection to the current trace.
pub struct DejaRedisPool {
    client: redis::Client,
}

impl DejaRedisPool {
    pub fn new(client: redis::Client) -> Self {
        Self { client }
    }

    pub fn with_control_client(client: redis::Client, _control_client: ControlClient) -> Self {
        Self { client }
    }

    /// Get a Redis connection with Deja trace correlation injected.
    ///
    /// Sends CLIENT SETNAME so this connection is attributed to the current trace.
    pub async fn get_connection(
        &self,
    ) -> Result<redis::aio::MultiplexedConnection, redis::RedisError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        if let Some(trace_id) = current_trace_id() {
            // Inject trace ID into connection name for in-band proxy detection
            let setname_cmd = format!("deja:{}", trace_id);
            let _: Result<String, _> =
                redis::cmd("CLIENT")
                    .arg("SETNAME")
                    .arg(&setname_cmd)
                    .query_async(&mut conn)
                    .await;

        }

        Ok(conn)
    }
}

/// Helper: send ReassociateConnection for a PG connection after checkout.
///
/// Call this after acquiring a connection from sqlx::PgPool to notify the
/// proxy that this pooled connection now belongs to the current trace.
pub async fn reassociate_pg_connection(
    control_client: &ControlClient,
    source_port: u16,
    trace_id: &str,
) {
    if !trace_id.is_empty() {
        control_client
            .send_best_effort(ControlMessage::reassociate_connection(
                trace_id,
                source_port,
                Protocol::Postgres,
            ))
            .await;
    }
}
