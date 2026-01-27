//! Configuration parsing for Deja proxy
//!
//! Supports TOML configuration files for recording filters and other settings.
//!
//! Example configuration:
//! ```toml
//! [recording.filters]
//! default = "include_all"  # or "exclude_all"
//!
//! [[recording.filters.include]]
//! protocol = "grpc"
//! service = "hyperswitch.*"
//! methods = ["Create*", "Process*"]
//!
//! [[recording.filters.exclude]]
//! protocol = "grpc"
//! service = "grpc.health.v1.Health"
//! methods = ["*"]
//! ```

use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Failed to parse config: {0}")]
    ParseError(#[from] toml::de::Error),
}

/// Root configuration structure
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DejaConfig {
    #[serde(default)]
    pub recording: RecordingConfig,
}

/// Recording-specific configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecordingConfig {
    #[serde(default)]
    pub filters: FilterConfig,
}

/// Filter configuration for selective recording
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterConfig {
    /// Default behavior: "include_all" or "exclude_all"
    #[serde(default = "default_filter_mode")]
    pub default: String,

    /// Include rules (evaluated first if default is exclude_all)
    #[serde(default)]
    pub include: Vec<FilterRule>,

    /// Exclude rules (evaluated first if default is include_all)
    #[serde(default)]
    pub exclude: Vec<FilterRule>,
}

fn default_filter_mode() -> String {
    "include_all".to_string()
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            default: default_filter_mode(),
            include: Vec::new(),
            exclude: Vec::new(),
        }
    }
}

/// A single filter rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterRule {
    /// Protocol to match: "grpc", "http", "postgres", "redis"
    #[serde(default)]
    pub protocol: Option<String>,

    /// Service pattern (for gRPC): e.g., "grpc.health.v1.*"
    #[serde(default)]
    pub service: Option<String>,

    /// Method patterns: e.g., ["Check", "Watch*"]
    #[serde(default)]
    pub methods: Vec<String>,

    /// Path patterns (for HTTP): e.g., ["/api/v1/*", "/health"]
    #[serde(default)]
    pub paths: Vec<String>,

    /// Metadata key-value patterns: e.g., {"x-deja-record": "true"}
    #[serde(default)]
    pub metadata: std::collections::HashMap<String, String>,
}

impl DejaConfig {
    /// Load configuration from a TOML file
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: DejaConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// Load configuration from a string
    pub fn from_str(content: &str) -> Result<Self, ConfigError> {
        let config: DejaConfig = toml::from_str(content)?;
        Ok(config)
    }

    /// Create a default configuration
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_config() {
        let toml = r#"
[recording.filters]
default = "include_all"

[[recording.filters.exclude]]
protocol = "grpc"
service = "grpc.health.v1.Health"
methods = ["*"]
"#;

        let config = DejaConfig::from_str(toml).unwrap();
        assert_eq!(config.recording.filters.default, "include_all");
        assert_eq!(config.recording.filters.exclude.len(), 1);
        assert_eq!(
            config.recording.filters.exclude[0].service,
            Some("grpc.health.v1.Health".to_string())
        );
    }

    #[test]
    fn test_parse_complex_config() {
        let toml = r#"
[recording.filters]
default = "exclude_all"

[[recording.filters.include]]
protocol = "grpc"
service = "hyperswitch.*"
methods = ["Create*", "Process*"]

[[recording.filters.include]]
protocol = "http"
paths = ["/api/v1/*", "/webhooks/*"]

[[recording.filters.exclude]]
protocol = "grpc"
service = "grpc.health.v1.Health"
methods = ["*"]
"#;

        let config = DejaConfig::from_str(toml).unwrap();
        assert_eq!(config.recording.filters.default, "exclude_all");
        assert_eq!(config.recording.filters.include.len(), 2);
        assert_eq!(config.recording.filters.exclude.len(), 1);
    }

    #[test]
    fn test_default_config() {
        let config = DejaConfig::new();
        assert_eq!(config.recording.filters.default, "include_all");
        assert!(config.recording.filters.include.is_empty());
        assert!(config.recording.filters.exclude.is_empty());
    }

    #[test]
    fn test_metadata_filter() {
        let toml = r#"
[recording.filters]
default = "exclude_all"

[[recording.filters.include]]
protocol = "grpc"
[recording.filters.include.metadata]
"x-deja-record" = "true"
"#;

        let config = DejaConfig::from_str(toml).unwrap();
        assert_eq!(config.recording.filters.include.len(), 1);
        assert_eq!(
            config.recording.filters.include[0]
                .metadata
                .get("x-deja-record"),
            Some(&"true".to_string())
        );
    }
}
