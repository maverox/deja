//! Protocol type definitions
//!
//! Defines supported protocols for recording and replay.

use serde::{Deserialize, Serialize};

/// Supported protocols for recording/replay
///
/// This enum represents the different network protocols that Deja can
/// intercept, record, and replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    /// PostgreSQL wire protocol
    Postgres,
    /// Redis RESP protocol
    Redis,
    /// HTTP/1.1 protocol
    Http,
    /// gRPC over HTTP/2
    Grpc,
    /// Unknown or undetected protocol
    Unknown,
}

impl Protocol {
    /// Get protocol name as string
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Redis => "redis",
            Self::Http => "http",
            Self::Grpc => "grpc",
            Self::Unknown => "unknown",
        }
    }

    /// Iterate over all known protocols (excluding Unknown)
    pub fn known_protocols() -> impl Iterator<Item = Self> {
        [Self::Postgres, Self::Redis, Self::Http, Self::Grpc].into_iter()
    }

    /// Check if this is a known protocol
    pub fn is_known(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

impl Default for Protocol {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseProtocolError(String);

impl std::fmt::Display for ParseProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid protocol: {}", self.0)
    }
}

impl std::error::Error for ParseProtocolError {}

impl std::str::FromStr for Protocol {
    type Err = ParseProtocolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "postgres" | "pg" | "postgresql" => Ok(Self::Postgres),
            "redis" => Ok(Self::Redis),
            "http" | "http1" | "http/1.1" => Ok(Self::Http),
            "grpc" | "http2" | "http/2" => Ok(Self::Grpc),
            "unknown" => Ok(Self::Unknown),
            _ => Err(ParseProtocolError(s.to_string())),
        }
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_from_str() {
        assert_eq!(Protocol::from_str("postgres").unwrap(), Protocol::Postgres);
        assert_eq!(Protocol::from_str("PG").unwrap(), Protocol::Postgres);
        assert_eq!(Protocol::from_str("Redis").unwrap(), Protocol::Redis);
        assert_eq!(Protocol::from_str("HTTP").unwrap(), Protocol::Http);
        assert_eq!(Protocol::from_str("grpc").unwrap(), Protocol::Grpc);
        assert_eq!(Protocol::from_str("unknown").unwrap(), Protocol::Unknown);
        assert!(Protocol::from_str("foobar").is_err());
    }

    #[test]
    fn test_known_protocols() {
        let known: Vec<_> = Protocol::known_protocols().collect();
        assert_eq!(known.len(), 4);
        assert!(known.contains(&Protocol::Postgres));
        assert!(!known.contains(&Protocol::Unknown));
    }

    #[test]
    fn test_serde() {
        let proto = Protocol::Postgres;
        let json = serde_json::to_string(&proto).unwrap();
        assert_eq!(json, r#""postgres""#);

        let parsed: Protocol = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, Protocol::Postgres);
    }
}
