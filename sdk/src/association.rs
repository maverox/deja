//! Connection Association Helpers
//!
//! Provides functions to query the source port of database connections
//! and send `AssociateBySourcePort` control messages to the proxy.
//!
//! # Background
//!
//! When a service connects to a database (PostgreSQL, Redis), the proxy
//! needs to know which trace the connection belongs to. The SDK must send
//! `AssociateBySourcePort` control message with the source port of the
//! connection.
//!
//! The challenge is that database drivers (sqlx, redis) don't expose the
//! underlying socket's source port. This module provides workarounds:
//!
//! - **PostgreSQL**: Query `pg_stat_activity` to get the client port
//! - **Redis**: Query `CLIENT LIST` to get the client address

use crate::{current_trace_id, ControlClient, ControlMessage};
use deja_common::Protocol;
use tracing::{debug, warn};

/// Get the source port of a PostgreSQL connection by querying pg_stat_activity.
///
/// This function executes a query to get the backend PID and then looks up
/// the client port from pg_stat_activity.
///
/// # Arguments
///
/// * `conn` - A mutable reference to the PostgreSQL connection
///
/// # Returns
///
/// The source port of the client connection, or None if it couldn't be determined.
#[cfg(feature = "sqlx")]
pub async fn get_pg_source_port(conn: &mut sqlx::postgres::PgConnection) -> Option<u16> {
    // Get our backend PID
    let pid: i32 = match sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await
    {
        Ok(pid) => pid,
        Err(e) => {
            warn!("Failed to get pg_backend_pid: {}", e);
            return None;
        }
    };

    debug!("PostgreSQL backend PID: {}", pid);

    // Query pg_stat_activity for our client port
    let client_port: Option<i32> =
        match sqlx::query_scalar("SELECT client_port FROM pg_stat_activity WHERE pid = $1")
            .bind(pid)
            .fetch_optional(&mut *conn)
            .await
        {
            Ok(port) => port,
            Err(e) => {
                warn!("Failed to query pg_stat_activity: {}", e);
                return None;
            }
        };

    client_port.and_then(|p| {
        if p > 0 && p <= u16::MAX as i32 {
            Some(p as u16)
        } else {
            debug!("Invalid client port from pg_stat_activity: {}", p);
            None
        }
    })
}

/// Get the source port of a Redis connection by querying CLIENT LIST.
///
/// This function sends CLIENT ID to get our client ID, then parses
/// CLIENT LIST output to find our client's address.
///
/// # Arguments
///
/// * `conn` - A mutable reference to the Redis connection
///
/// # Returns
///
/// The source port of the client connection, or None if it couldn't be determined.
pub async fn get_redis_source_port(conn: &mut redis::aio::MultiplexedConnection) -> Option<u16> {
    // Get our client ID
    let client_id: i64 = match redis::cmd("CLIENT")
        .arg("ID")
        .query_async::<_, i64>(conn)
        .await
    {
        Ok(id) => id,
        Err(e) => {
            warn!("Failed to get Redis CLIENT ID: {}", e);
            return None;
        }
    };

    debug!("Redis client ID: {}", client_id);

    // Get CLIENT LIST output
    let client_list: String = match redis::cmd("CLIENT")
        .arg("LIST")
        .query_async::<_, String>(conn)
        .await
    {
        Ok(list) => list,
        Err(e) => {
            warn!("Failed to get Redis CLIENT LIST: {}", e);
            return None;
        }
    };

    // Parse CLIENT LIST to find our client's address
    // Format: "id=123 addr=127.0.0.1:54321 ..."
    for line in client_list.lines() {
        if line.contains(&format!("id={}", client_id)) {
            // Find addr= field
            for field in line.split_whitespace() {
                if let Some(addr) = field.strip_prefix("addr=") {
                    // Parse "ip:port" format
                    if let Some(port_str) = addr.rsplit(':').next() {
                        if let Ok(port) = port_str.parse::<u16>() {
                            debug!("Redis client source port: {}", port);
                            return Some(port);
                        }
                    }
                }
            }
        }
    }

    warn!(
        "Could not find client port in Redis CLIENT LIST for id={}",
        client_id
    );
    None
}

/// Send an AssociateBySourcePort control message for a PostgreSQL connection.
///
/// This function queries the connection's source port and sends the
/// association message to the proxy.
///
/// # Arguments
///
/// * `conn` - A mutable reference to the PostgreSQL connection
/// * `control_client` - The control client to use for sending the message
///
/// # Returns
///
/// The source port if association was successful, None otherwise.
#[cfg(feature = "sqlx")]
pub async fn associate_pg_connection(
    conn: &mut sqlx::postgres::PgConnection,
    control_client: ControlClient,
) -> Option<u16> {
    let trace_id = current_trace_id()?;
    let source_port = get_pg_source_port(conn).await?;

    debug!(
        trace_id = %trace_id,
        source_port = source_port,
        "Associating PostgreSQL connection with trace"
    );

    control_client
        .send_best_effort(ControlMessage::associate_by_source_port(
            &trace_id,
            source_port,
            Protocol::Postgres,
        ))
        .await;

    Some(source_port)
}

/// Send an AssociateBySourcePort control message for a Redis connection.
///
/// This function queries the connection's source port and sends the
/// association message to the proxy.
///
/// # Arguments
///
/// * `conn` - A mutable reference to the Redis connection
/// * `control_client` - The control client to use for sending the message
///
/// # Returns
///
/// The source port if association was successful, None otherwise.
pub async fn associate_redis_connection(
    conn: &mut redis::aio::MultiplexedConnection,
    control_client: ControlClient,
) -> Option<u16> {
    let trace_id = current_trace_id()?;
    let source_port = get_redis_source_port(conn).await?;

    debug!(
        trace_id = %trace_id,
        source_port = source_port,
        "Associating Redis connection with trace"
    );

    control_client
        .send_best_effort(ControlMessage::associate_by_source_port(
            &trace_id,
            source_port,
            Protocol::Redis,
        ))
        .await;

    Some(source_port)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_redis_client_list() {
        // Test parsing of Redis CLIENT LIST output
        let client_list = "id=5 addr=127.0.0.1:54321 fd=8 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client";

        // Simulate finding the line with our ID
        let client_id = 5i64;
        for line in client_list.lines() {
            if line.contains(&format!("id={}", client_id)) {
                for field in line.split_whitespace() {
                    if let Some(addr) = field.strip_prefix("addr=") {
                        if let Some(port_str) = addr.rsplit(':').next() {
                            let port: u16 = port_str.parse().unwrap();
                            assert_eq!(port, 54321);
                        }
                    }
                }
            }
        }
    }
}
