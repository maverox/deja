//! Recording filter implementation for selective event capture
//!
//! Supports glob pattern matching for service names, methods, paths, and metadata.
//! Used to filter which events get recorded during proxy operation.

use crate::config::{FilterConfig, FilterRule};
use glob::Pattern;
use std::collections::HashMap;

/// Compiled recording filter for efficient matching
#[derive(Debug, Clone)]
pub struct RecordingFilter {
    /// Default behavior: true = include_all, false = exclude_all
    default_include: bool,
    /// Compiled include rules
    include_rules: Vec<CompiledRule>,
    /// Compiled exclude rules
    exclude_rules: Vec<CompiledRule>,
}

/// A compiled filter rule with pre-parsed glob patterns
#[derive(Debug, Clone)]
struct CompiledRule {
    protocol: Option<String>,
    service_pattern: Option<Pattern>,
    method_patterns: Vec<Pattern>,
    path_patterns: Vec<Pattern>,
    metadata_patterns: HashMap<String, Pattern>,
}

impl CompiledRule {
    fn from_filter_rule(rule: &FilterRule) -> Self {
        Self {
            protocol: rule.protocol.clone(),
            service_pattern: rule
                .service
                .as_ref()
                .and_then(|s| Pattern::new(s).ok()),
            method_patterns: rule
                .methods
                .iter()
                .filter_map(|m| Pattern::new(m).ok())
                .collect(),
            path_patterns: rule
                .paths
                .iter()
                .filter_map(|p| Pattern::new(p).ok())
                .collect(),
            metadata_patterns: rule
                .metadata
                .iter()
                .filter_map(|(k, v)| Pattern::new(v).ok().map(|p| (k.clone(), p)))
                .collect(),
        }
    }

    /// Check if this rule matches the given request context
    fn matches(&self, ctx: &FilterContext) -> bool {
        // Protocol must match if specified
        if let Some(ref proto) = self.protocol {
            if proto != &ctx.protocol {
                return false;
            }
        }

        // For gRPC, check service and method patterns
        if ctx.protocol == "grpc" {
            // Service pattern must match if specified
            if let Some(ref pattern) = self.service_pattern {
                if let Some(ref service) = ctx.service {
                    if !pattern.matches(service) {
                        return false;
                    }
                } else {
                    return false;
                }
            }

            // Method patterns: at least one must match if any are specified
            if !self.method_patterns.is_empty() {
                if let Some(ref method) = ctx.method {
                    if !self.method_patterns.iter().any(|p| p.matches(method)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }

        // For HTTP, check path patterns
        if ctx.protocol == "http"
            && !self.path_patterns.is_empty() {
                if let Some(ref path) = ctx.path {
                    if !self.path_patterns.iter().any(|p| p.matches(path)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }

        // Check metadata patterns: all specified patterns must match
        for (key, pattern) in &self.metadata_patterns {
            if let Some(value) = ctx.metadata.get(key) {
                if !pattern.matches(value) {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }
}

/// Context for filter matching, extracted from incoming requests
#[derive(Debug, Clone, Default)]
pub struct FilterContext {
    /// Protocol identifier: "grpc", "http", "postgres", "redis"
    pub protocol: String,
    /// gRPC service name (e.g., "grpc.health.v1.Health")
    pub service: Option<String>,
    /// gRPC method name or HTTP method (e.g., "Check" or "POST")
    pub method: Option<String>,
    /// HTTP path (e.g., "/api/v1/users")
    pub path: Option<String>,
    /// Request metadata/headers
    pub metadata: HashMap<String, String>,
}

impl FilterContext {
    /// Create a new filter context for gRPC
    pub fn grpc(service: &str, method: &str, metadata: HashMap<String, String>) -> Self {
        Self {
            protocol: "grpc".to_string(),
            service: Some(service.to_string()),
            method: Some(method.to_string()),
            path: None,
            metadata,
        }
    }

    /// Create a new filter context for HTTP
    pub fn http(method: &str, path: &str, headers: HashMap<String, String>) -> Self {
        Self {
            protocol: "http".to_string(),
            service: None,
            method: Some(method.to_string()),
            path: Some(path.to_string()),
            metadata: headers,
        }
    }

    /// Create a new filter context for Postgres
    pub fn postgres() -> Self {
        Self {
            protocol: "postgres".to_string(),
            ..Default::default()
        }
    }

    /// Create a new filter context for Redis
    pub fn redis() -> Self {
        Self {
            protocol: "redis".to_string(),
            ..Default::default()
        }
    }
}

impl RecordingFilter {
    /// Create a new filter from configuration
    pub fn from_config(config: &FilterConfig) -> Self {
        let default_include = config.default.to_lowercase() != "exclude_all";

        let include_rules = config
            .include
            .iter()
            .map(CompiledRule::from_filter_rule)
            .collect();

        let exclude_rules = config
            .exclude
            .iter()
            .map(CompiledRule::from_filter_rule)
            .collect();

        Self {
            default_include,
            include_rules,
            exclude_rules,
        }
    }

    /// Create a filter that includes everything (default behavior)
    pub fn include_all() -> Self {
        Self {
            default_include: true,
            include_rules: Vec::new(),
            exclude_rules: Vec::new(),
        }
    }

    /// Create a filter that excludes everything
    pub fn exclude_all() -> Self {
        Self {
            default_include: false,
            include_rules: Vec::new(),
            exclude_rules: Vec::new(),
        }
    }

    /// Check if a request should be recorded based on the filter rules
    ///
    /// Logic:
    /// - If default is "include_all": record unless an exclude rule matches
    /// - If default is "exclude_all": don't record unless an include rule matches
    ///
    /// Special metadata keys:
    /// - `x-deja-record`: "true" forces recording, "false" skips recording
    /// - `x-deja-skip`: "true" skips recording
    pub fn should_record(&self, ctx: &FilterContext) -> bool {
        // Check for explicit override metadata
        if let Some(val) = ctx.metadata.get("x-deja-record") {
            return val.to_lowercase() == "true";
        }
        if let Some(val) = ctx.metadata.get("x-deja-skip") {
            if val.to_lowercase() == "true" {
                return false;
            }
        }

        if self.default_include {
            // Include by default, check exclude rules
            !self.exclude_rules.iter().any(|rule| rule.matches(ctx))
        } else {
            // Exclude by default, check include rules
            self.include_rules.iter().any(|rule| rule.matches(ctx))
        }
    }
}

impl Default for RecordingFilter {
    fn default() -> Self {
        Self::include_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FilterRule;

    #[test]
    fn test_include_all_default() {
        let filter = RecordingFilter::include_all();
        let ctx = FilterContext::grpc("test.Service", "Method", HashMap::new());
        assert!(filter.should_record(&ctx));
    }

    #[test]
    fn test_exclude_all_default() {
        let filter = RecordingFilter::exclude_all();
        let ctx = FilterContext::grpc("test.Service", "Method", HashMap::new());
        assert!(!filter.should_record(&ctx));
    }

    #[test]
    fn test_exclude_health_check() {
        let config = FilterConfig {
            default: "include_all".to_string(),
            include: vec![],
            exclude: vec![FilterRule {
                protocol: Some("grpc".to_string()),
                service: Some("grpc.health.v1.Health".to_string()),
                methods: vec!["*".to_string()],
                paths: vec![],
                metadata: HashMap::new(),
            }],
        };

        let filter = RecordingFilter::from_config(&config);

        // Health check should be excluded
        let health_ctx =
            FilterContext::grpc("grpc.health.v1.Health", "Check", HashMap::new());
        assert!(!filter.should_record(&health_ctx));

        // Other services should be included
        let other_ctx = FilterContext::grpc("myapp.Service", "DoSomething", HashMap::new());
        assert!(filter.should_record(&other_ctx));
    }

    #[test]
    fn test_include_specific_service() {
        let config = FilterConfig {
            default: "exclude_all".to_string(),
            include: vec![FilterRule {
                protocol: Some("grpc".to_string()),
                service: Some("hyperswitch.*".to_string()),
                methods: vec!["Create*".to_string(), "Process*".to_string()],
                paths: vec![],
                metadata: HashMap::new(),
            }],
            exclude: vec![],
        };

        let filter = RecordingFilter::from_config(&config);

        // Matching service and method should be included
        let ctx1 = FilterContext::grpc(
            "hyperswitch.PaymentService",
            "CreatePayment",
            HashMap::new(),
        );
        assert!(filter.should_record(&ctx1));

        // Matching service but non-matching method should be excluded
        let ctx2 =
            FilterContext::grpc("hyperswitch.PaymentService", "GetPayment", HashMap::new());
        assert!(!filter.should_record(&ctx2));

        // Non-matching service should be excluded
        let ctx3 = FilterContext::grpc("other.Service", "CreateSomething", HashMap::new());
        assert!(!filter.should_record(&ctx3));
    }

    #[test]
    fn test_metadata_override() {
        let filter = RecordingFilter::exclude_all();

        // With x-deja-record: true, should record even with exclude_all
        let mut metadata = HashMap::new();
        metadata.insert("x-deja-record".to_string(), "true".to_string());
        let ctx = FilterContext::grpc("test.Service", "Method", metadata);
        assert!(filter.should_record(&ctx));
    }

    #[test]
    fn test_skip_override() {
        let filter = RecordingFilter::include_all();

        // With x-deja-skip: true, should not record even with include_all
        let mut metadata = HashMap::new();
        metadata.insert("x-deja-skip".to_string(), "true".to_string());
        let ctx = FilterContext::grpc("test.Service", "Method", metadata);
        assert!(!filter.should_record(&ctx));
    }

    #[test]
    fn test_http_path_filter() {
        let config = FilterConfig {
            default: "exclude_all".to_string(),
            include: vec![FilterRule {
                protocol: Some("http".to_string()),
                service: None,
                methods: vec![],
                paths: vec!["/api/v1/*".to_string(), "/webhooks/*".to_string()],
                metadata: HashMap::new(),
            }],
            exclude: vec![],
        };

        let filter = RecordingFilter::from_config(&config);

        // Matching path should be included
        let ctx1 = FilterContext::http("POST", "/api/v1/users", HashMap::new());
        assert!(filter.should_record(&ctx1));

        // Non-matching path should be excluded
        let ctx2 = FilterContext::http("GET", "/health", HashMap::new());
        assert!(!filter.should_record(&ctx2));
    }

    #[test]
    fn test_wildcard_method() {
        let config = FilterConfig {
            default: "include_all".to_string(),
            include: vec![],
            exclude: vec![FilterRule {
                protocol: Some("grpc".to_string()),
                service: Some("grpc.reflection.*".to_string()),
                methods: vec!["*".to_string()],
                paths: vec![],
                metadata: HashMap::new(),
            }],
        };

        let filter = RecordingFilter::from_config(&config);

        // All methods of reflection service should be excluded
        let ctx1 = FilterContext::grpc(
            "grpc.reflection.v1alpha.ServerReflection",
            "ServerReflectionInfo",
            HashMap::new(),
        );
        assert!(!filter.should_record(&ctx1));

        // Other services should be included
        let ctx2 = FilterContext::grpc("myapp.Service", "Method", HashMap::new());
        assert!(filter.should_record(&ctx2));
    }
}
