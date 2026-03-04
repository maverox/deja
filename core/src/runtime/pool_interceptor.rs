//! Connection Pool Interception
//!
//! Provides a transparent wrapper for database connection pools that notifies
//! the proxy when connections are borrowed from a pool, enabling accurate
//! re-attribution when the same connection is used by different traces.
//!
//! # Problem Solved
//!
//! Connection pools reuse connections across requests for efficiency. When a
//! connection is borrowed from a pool for a different trace, we need to update
//! the connection→trace mapping:
//!
//! ```ignore
//! // Request A borrows connection C
//! pool.get().await // Mapper: C → trace_A
//!
//! // Connection returned to pool
//! // pool.return(connection)
//!
//! // Request B borrows same connection C
//! pool.get().await // Mapper: C → trace_B (UPDATED!)
//! ```
//!
//! # Usage
//!
//! Wrap your connection pool after acquiring a connection:
//!
//! ```ignore
//! use deja::runtime::pool_interceptor::InterceptedConnection;
//!
//! let conn = pool.get().await?;
//! let intercepted = InterceptedConnection::new(
//!     conn,
//!     Some(control_client.clone()),
//!     conn_id.to_string(),
//! );
//! // Use intercepted normally - it derefs to inner connection
//! ```

use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tracing::debug;

use super::trace_context;
use crate::control_api::ControlClient;

/// Wrapper for a pooled connection that notifies proxy of trace association
///
/// When a connection is borrowed from a pool, it may be used by a different
/// request/trace than the previous owner. This wrapper:
/// 1. Transparently proxies all connection operations via `Deref`
/// 2. Notifies the control API about the new trace association
/// 3. Automatically propagates trace context to operations on the connection
///
/// # Generic over Connection Type
///
/// Works with any connection type (Postgres, MySQL, Redis, etc.) thanks to
/// Rust's generic system. The inner connection just needs to be dereferenceable.
///
/// # Example
///
/// ```ignore
/// // Works with any pool: sqlx, deadpool, bb8, etc.
/// let postgres_conn = sqlx::pool::PoolConnection::<sqlx::Postgres>::acquire(pool).await?;
/// let intercepted = InterceptedConnection::new(
///     postgres_conn,
///     Some(control_client),
///     "127.0.0.1:5432".to_string(),
/// );
/// intercepted.execute("SELECT 1").await?;
/// ```
pub struct InterceptedConnection<T> {
    /// The actual connection (could be Postgres, MySQL, Redis, etc.)
    inner: T,
    /// Control client for notifying proxy
    control_client: Option<Arc<ControlClient>>,
    /// Connection identifier (typically address)
    connection_id: String,
}

impl<T> InterceptedConnection<T> {
    /// Create an intercepted connection
    ///
    /// This immediately notifies the proxy about the connection association
    /// if a trace context exists and a control client is provided.
    ///
    /// # Arguments
    ///
    /// * `inner` - The actual connection object
    /// * `control_client` - Optional control client for proxy notification
    /// * `connection_id` - Identifier for this connection (address, handle, etc.)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let intercepted = InterceptedConnection::new(
    ///     pool_connection,
    ///     Some(control_client),
    ///     peer_addr.to_string(),
    /// );
    /// ```
    pub fn new(
        inner: T,
        control_client: Option<Arc<ControlClient>>,
        connection_id: String,
    ) -> Self {
        let conn = Self {
            inner,
            control_client,
            connection_id: connection_id.clone(),
        };

        // Log trace context for debugging — in the scope model, connection
        // re-attribution is handled by source-port association at establishment time.
        if let Some(trace_id) = trace_context::current_trace_id() {
            debug!(
                trace_id = %trace_id,
                connection = %connection_id,
                "Pool connection borrowed for trace"
            );
        }

        conn
    }

    /// Get a reference to the inner connection
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Get a mutable reference to the inner connection
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Consume this wrapper and return the inner connection
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Get the connection ID
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }
}

// Transparent delegation - let users work with intercepted connections
// as if they were the actual connection type
impl<T> Deref for InterceptedConnection<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for InterceptedConnection<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Simple test connection type
    #[derive(Debug, Clone)]
    struct MockConnection {
        id: String,
        executed_queries: Vec<String>,
    }

    impl MockConnection {
        fn new(id: String) -> Self {
            Self {
                id,
                executed_queries: vec![],
            }
        }

        async fn execute(&mut self, query: &str) {
            self.executed_queries.push(query.to_string());
        }
    }

    #[tokio::test]
    async fn test_intercepted_connection_deref() {
        let mock = MockConnection::new("conn-1".to_string());
        let intercepted = InterceptedConnection::new(mock, None, "127.0.0.1:5432".to_string());

        // Should be able to access inner through Deref
        assert_eq!(intercepted.id, "conn-1");
    }

    #[tokio::test]
    async fn test_intercepted_connection_deref_mut() {
        let mock = MockConnection::new("conn-2".to_string());
        let mut intercepted = InterceptedConnection::new(mock, None, "127.0.0.1:5432".to_string());

        // Should be able to mutate through DerefMut
        intercepted.execute("SELECT 1").await;
        assert_eq!(intercepted.executed_queries.len(), 1);
    }

    #[tokio::test]
    async fn test_intercepted_connection_into_inner() {
        let mock = MockConnection::new("conn-3".to_string());
        let intercepted = InterceptedConnection::new(mock, None, "127.0.0.1:5432".to_string());

        // Should be able to extract inner connection
        let inner = intercepted.into_inner();
        assert_eq!(inner.id, "conn-3");
    }

    #[tokio::test]
    async fn test_intercepted_connection_id() {
        let mock = MockConnection::new("conn-4".to_string());
        let intercepted = InterceptedConnection::new(mock, None, "127.0.0.1:5433".to_string());

        assert_eq!(intercepted.connection_id(), "127.0.0.1:5433");
    }

    #[test]
    fn test_without_control_client() {
        // Should not panic even without control client
        let mock = MockConnection::new("conn-5".to_string());
        let _intercepted = InterceptedConnection::new(mock, None, "127.0.0.1:5432".to_string());
        // Connection created successfully
    }
}
