//! Connection correlation key for durable connection lookup.
//!
//! A `ConnectionKey` identifies a connection by its endpoint identity and
//! optional generation:
//! - Endpoint identity: source/destination IP+port 4-tuple
//! - Generation: reconnect epoch for rapid port reuse / churn disambiguation
//!
//! Used as a HashMap key in proxy correlation tracking to match incoming
//! connections to their recorded counterparts.

use std::net::{IpAddr, SocketAddr};

/// A connection identifier for correlation lookup.
///
/// By default, identity is a 4-tuple (source and destination addresses).
/// `generation` can be incremented to disambiguate reconnect churn where the
/// same endpoint identity is reused rapidly.
/// Implements `Hash` and `Eq` for use as a HashMap key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ConnectionKey {
    pub src_ip: IpAddr,
    pub src_port: u16,
    pub dst_ip: IpAddr,
    pub dst_port: u16,
    #[serde(default)]
    pub generation: u64,
}

impl ConnectionKey {
    /// Create a `ConnectionKey` from socket addresses.
    ///
    /// # Arguments
    /// * `local` - The local socket address (source)
    /// * `peer` - The peer socket address (destination)
    pub fn from_socket_addrs(local: SocketAddr, peer: SocketAddr) -> Self {
        Self::from_socket_addrs_with_generation(local, peer, 0)
    }

    /// Create a `ConnectionKey` from socket addresses with explicit generation.
    ///
    /// Generation is an epoch-like discriminator used to distinguish rapid
    /// reconnects that reuse the same endpoint identity.
    pub fn from_socket_addrs_with_generation(
        local: SocketAddr,
        peer: SocketAddr,
        generation: u64,
    ) -> Self {
        Self {
            src_ip: local.ip(),
            src_port: local.port(),
            dst_ip: peer.ip(),
            dst_port: peer.port(),
            generation,
        }
    }

    /// Create a `ConnectionKey` from a source port only (backward compatibility).
    ///
    /// Uses `0.0.0.0` for both IPs and `0` for destination port.
    /// This is useful for legacy code that only tracks source port.
    pub fn from_source_port(port: u16) -> Self {
        Self {
            src_ip: IpAddr::from([0, 0, 0, 0]),
            src_port: port,
            dst_ip: IpAddr::from([0, 0, 0, 0]),
            dst_port: 0,
            generation: 0,
        }
    }

    pub fn with_generation(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }

    pub fn next_generation(&self) -> Self {
        self.clone()
            .with_generation(self.generation.saturating_add(1))
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn is_source_port_only(&self) -> bool {
        self.src_ip.is_unspecified() && self.dst_ip.is_unspecified() && self.dst_port == 0
    }

    pub fn same_identity(&self, other: &Self) -> bool {
        if self.is_source_port_only() || other.is_source_port_only() {
            self.src_port == other.src_port
        } else {
            self.src_ip == other.src_ip
                && self.src_port == other.src_port
                && self.dst_ip == other.dst_ip
                && self.dst_port == other.dst_port
        }
    }

    /// Get the source port.
    pub fn source_port(&self) -> u16 {
        self.src_port
    }
}

impl std::fmt::Display for ConnectionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.generation == 0 {
            write!(
                f,
                "{}:{}->{}:{}",
                self.src_ip, self.src_port, self.dst_ip, self.dst_port
            )
        } else {
            write!(
                f,
                "{}:{}->{}:{}#g{}",
                self.src_ip, self.src_port, self.dst_ip, self.dst_port, self.generation
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_connection_key_equality() {
        let key1 = ConnectionKey {
            src_ip: IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)),
            src_port: 8080,
            dst_ip: IpAddr::from(Ipv4Addr::new(192, 168, 1, 1)),
            dst_port: 5432,
            generation: 0,
        };

        let key2 = ConnectionKey {
            src_ip: IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)),
            src_port: 8080,
            dst_ip: IpAddr::from(Ipv4Addr::new(192, 168, 1, 1)),
            dst_port: 5432,
            generation: 0,
        };

        assert_eq!(key1, key2);
    }

    #[test]
    fn test_connection_key_hashing() {
        let key1 = ConnectionKey {
            src_ip: IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)),
            src_port: 8080,
            dst_ip: IpAddr::from(Ipv4Addr::new(192, 168, 1, 1)),
            dst_port: 5432,
            generation: 0,
        };

        let key2 = ConnectionKey {
            src_ip: IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)),
            src_port: 8080,
            dst_ip: IpAddr::from(Ipv4Addr::new(192, 168, 1, 1)),
            dst_port: 5432,
            generation: 0,
        };

        let key3 = ConnectionKey {
            src_ip: IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)),
            src_port: 8081,
            dst_ip: IpAddr::from(Ipv4Addr::new(192, 168, 1, 1)),
            dst_port: 5432,
            generation: 0,
        };

        let mut set = HashSet::new();
        set.insert(key1);
        set.insert(key2); // Should not increase set size (duplicate)
        set.insert(key3); // Should increase set size (different)

        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_connection_key_display() {
        let key = ConnectionKey {
            src_ip: IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)),
            src_port: 8080,
            dst_ip: IpAddr::from(Ipv4Addr::new(192, 168, 1, 1)),
            dst_port: 5432,
            generation: 0,
        };

        assert_eq!(format!("{}", key), "127.0.0.1:8080->192.168.1.1:5432");
    }

    #[test]
    fn test_from_socket_addrs() {
        let local: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let peer: SocketAddr = "192.168.1.1:5432".parse().unwrap();

        let key = ConnectionKey::from_socket_addrs(local, peer);

        assert_eq!(key.src_ip, IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)));
        assert_eq!(key.src_port, 8080);
        assert_eq!(key.dst_ip, IpAddr::from(Ipv4Addr::new(192, 168, 1, 1)));
        assert_eq!(key.dst_port, 5432);
        assert_eq!(key.generation(), 0);
    }

    #[test]
    fn test_from_source_port() {
        let key = ConnectionKey::from_source_port(9999);

        assert_eq!(key.src_ip, IpAddr::from(Ipv4Addr::new(0, 0, 0, 0)));
        assert_eq!(key.src_port, 9999);
        assert_eq!(key.dst_ip, IpAddr::from(Ipv4Addr::new(0, 0, 0, 0)));
        assert_eq!(key.dst_port, 0);
        assert_eq!(key.generation(), 0);
        assert!(key.is_source_port_only());
    }

    #[test]
    fn test_source_port_getter() {
        let key = ConnectionKey::from_source_port(7777);
        assert_eq!(key.source_port(), 7777);
    }

    #[test]
    fn test_rapid_source_port_reuse_disambiguates_by_generation() {
        let local: SocketAddr = "127.0.0.1:50000".parse().unwrap();
        let peer: SocketAddr = "10.0.0.10:5432".parse().unwrap();

        let first = ConnectionKey::from_socket_addrs(local, peer);
        let second = first.next_generation();

        assert!(first.same_identity(&second));
        assert_ne!(first, second);
        assert_eq!(second.generation(), first.generation() + 1);

        let mut by_key = HashMap::new();
        by_key.insert(first.clone(), "trace-a");
        assert_eq!(by_key.get(&second), None);

        by_key.insert(second.clone(), "trace-b");
        assert_eq!(by_key.get(&first), Some(&"trace-a"));
        assert_eq!(by_key.get(&second), Some(&"trace-b"));
    }

    #[test]
    fn test_reconnect_churn_same_source_port_disambiguates_by_4tuple() {
        let local: SocketAddr = "127.0.0.1:50001".parse().unwrap();
        let peer_a: SocketAddr = "10.0.0.20:5432".parse().unwrap();
        let peer_b: SocketAddr = "10.0.0.21:5432".parse().unwrap();

        let to_a = ConnectionKey::from_socket_addrs(local, peer_a).with_generation(4);
        let to_b = ConnectionKey::from_socket_addrs(local, peer_b).with_generation(4);

        assert_ne!(to_a, to_b);
        assert!(!to_a.same_identity(&to_b));

        let mut by_key = HashMap::new();
        by_key.insert(to_a.clone(), "trace-a");
        assert_eq!(by_key.get(&to_b), None);
    }

    #[test]
    fn test_source_port_fallback_identity_comparison_semantics() {
        let fallback = ConnectionKey::from_source_port(50002);
        let full: ConnectionKey = ConnectionKey::from_socket_addrs(
            "127.0.0.1:50002".parse().unwrap(),
            "10.0.0.30:6379".parse().unwrap(),
        );
        let full_next = full.next_generation();

        assert!(fallback.same_identity(&full));
        assert!(full.same_identity(&full_next));
        assert_ne!(full, full_next);
    }
}
