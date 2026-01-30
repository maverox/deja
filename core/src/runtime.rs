//! DejaRuntime - Abstraction for non-deterministic operations
//!
//! This module provides the `DejaRuntime` trait that services integrate with
//! to enable deterministic replay of non-deterministic operations like:
//! - UUID generation
//! - Timestamps
//! - Random number generation
//! - Task spawning (async boundaries)
//!
//! In production mode, these return real values.
//! In recording mode, values are captured and sent to the proxy.
//! In replay mode, recorded values are returned.
//!
//! # Trace Context
//!
//! The trace_context module provides task-local storage for trace IDs that enable
//! accurate correlation of concurrent requests and background tasks.

pub mod pool_interceptor;
pub mod socket_interceptor;
pub mod spawn;
pub mod trace_context;

use async_trait::async_trait;
use deja_common::DejaRuntime;
use std::env;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Helper function for deterministic execution with any runtime.
/// This is separate from the trait because generic functions aren't dyn-compatible.
///
/// Usage:
/// ```ignore
/// let id = deterministic(&runtime, "payment_id", || nanoid!(20, &ALPHABET)).await;
/// ```
pub async fn deterministic<R, F>(runtime: &R, tag: &str, generator: F) -> String
where
    R: DejaRuntime + ?Sized,
    F: FnOnce() -> String + Send,
{
    // Try replay first
    if let Some(value) = runtime.replay(tag).await {
        return value;
    }
    // Generate and capture
    let value = generator();
    runtime.capture(tag, &value).await;
    value
}

/// Trait for async boundary operations (task spawning)
/// These are separate from DejaRuntime because they involve generics that
/// don't work well with async_trait.
pub trait DejaAsyncBoundary: Send + Sync {
    /// Spawn a named task. In recording mode, the spawn is recorded.
    /// In replay mode, spawn ordering may be enforced (future enhancement).
    fn spawn<F, T>(&self, name: &str, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;

    /// Spawn a blocking task with a name
    fn spawn_blocking<F, T>(&self, name: &str, f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static;
}

/// Production runtime - generates real values, no recording/replay
pub struct ProductionRuntime;

impl ProductionRuntime {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ProductionRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl DejaAsyncBoundary for ProductionRuntime {
    fn spawn<F, T>(&self, _name: &str, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // In production mode, just spawn without recording
        tokio::spawn(future)
    }

    fn spawn_blocking<F, T>(&self, _name: &str, f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        // In production mode, just spawn without recording
        tokio::task::spawn_blocking(f)
    }
}

#[async_trait]
impl DejaRuntime for ProductionRuntime {
    async fn uuid(&self) -> Uuid {
        Uuid::new_v4()
    }

    async fn uuid_v7(&self) -> Uuid {
        Uuid::now_v7()
    }

    async fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    async fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    async fn random_u64(&self) -> u64 {
        rand::random()
    }

    async fn random_bytes(&self, len: usize) -> Vec<u8> {
        use rand::RngCore;
        let mut bytes = vec![0u8; len];
        rand::thread_rng().fill_bytes(&mut bytes);
        bytes
    }

    async fn nanoid(&self) -> String {
        // Simple nanoid implementation
        use rand::Rng;
        const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        let mut rng = rand::thread_rng();
        (0..21)
            .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
            .collect()
    }

    async fn capture(&self, _tag: &str, _value: &str) {
        // Production mode: no-op
    }

    async fn replay(&self, _tag: &str) -> Option<String> {
        // Production mode: always return None (use generator)
        None
    }

    fn set_trace_id(&self, _trace_id: String) {}
    fn get_trace_id(&self) -> String {
        String::new()
    }
    fn control_client(&self) -> deja_common::ControlClient {
        deja_common::ControlClient::new("localhost", 9999)
    }
}

/// Network runtime - communicates with Deja proxy for recording/replay
pub struct NetworkRuntime {
    client: reqwest::Client,
    proxy_url: String,
    trace_id: Arc<RwLock<String>>,
    mode: String,
    task_sequence: Arc<AtomicU64>,
}

/// Request body for /capture endpoint
#[derive(serde::Serialize)]
struct CaptureRequest {
    trace_id: String,
    kind: String,
    value: String,
}

/// Response from /replay endpoint
#[derive(serde::Deserialize)]
struct ReplayResponse {
    value: String,
    found: bool,
}

impl NetworkRuntime {
    /// Create from environment variables
    ///
    /// Reads:
    /// - DEJA_PROXY_URL (default: http://localhost:9999)
    /// - DEJA_MODE (default: production)
    pub fn from_env() -> Self {
        Self {
            client: reqwest::Client::new(),
            proxy_url: env::var("DEJA_PROXY_URL")
                .unwrap_or_else(|_| "http://localhost:9999".into()),
            trace_id: Arc::new(RwLock::new(String::new())),
            mode: env::var("DEJA_MODE").unwrap_or_else(|_| "production".into()),
            task_sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create with specific configuration
    pub fn new(proxy_url: String, mode: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            proxy_url,
            trace_id: Arc::new(RwLock::new(String::new())),
            mode,
            task_sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Set the current trace ID (call when handling a new request)
    pub fn set_trace_id(&self, trace_id: String) {
        match self.trace_id.write() {
            Ok(mut guard) => {
                *guard = trace_id;
            }
            Err(e) => {
                tracing::warn!("trace_id lock poisoned, recovering: {}", e);
                *e.into_inner() = trace_id;
            }
        }
    }

    /// Get the current trace ID
    pub fn trace_id(&self) -> String {
        match self.trace_id.read() {
            Ok(guard) => guard.clone(),
            Err(e) => {
                tracing::warn!("trace_id lock poisoned, recovering: {}", e);
                e.into_inner().clone()
            }
        }
    }

    /// Create a clone with a specific trace ID
    pub fn with_trace(&self, trace_id: String) -> Self {
        Self {
            client: self.client.clone(),
            proxy_url: self.proxy_url.clone(),
            trace_id: Arc::new(RwLock::new(trace_id)),
            mode: self.mode.clone(),
            task_sequence: self.task_sequence.clone(),
        }
    }

    /// Check if in recording mode
    pub fn is_recording(&self) -> bool {
        self.mode == "record"
    }

    /// Check if in replay mode
    pub fn is_replaying(&self) -> bool {
        self.mode == "replay"
    }

    /// POST a captured value to the proxy (internal use)
    async fn capture_internal(&self, kind: &str, value: &str) {
        let trace = self.trace_id();
        let req = CaptureRequest {
            trace_id: trace,
            kind: kind.to_string(),
            value: value.to_string(),
        };
        let _ = self
            .client
            .post(format!("{}/capture", self.proxy_url))
            .json(&req)
            .send()
            .await;
    }

    /// GET a replayed value from the proxy (internal use)
    async fn replay_internal(&self, kind: &str) -> Option<String> {
        let trace = self.trace_id();
        let resp = self
            .client
            .get(format!("{}/replay", self.proxy_url))
            .query(&[("trace_id", &trace), ("kind", &kind.to_string())])
            .send()
            .await
            .ok()?;

        let replay_resp: ReplayResponse = resp.json().await.ok()?;
        if replay_resp.found {
            Some(replay_resp.value)
        } else {
            None
        }
    }
}

#[async_trait]
impl DejaRuntime for NetworkRuntime {
    async fn uuid(&self) -> Uuid {
        match self.mode.as_str() {
            "record" => {
                let id = Uuid::new_v4();
                self.capture_internal("uuid", &id.to_string()).await;
                id
            }
            "replay" => {
                if let Some(val) = self.replay_internal("uuid").await {
                    if let Ok(id) = Uuid::parse_str(&val) {
                        return id;
                    }
                }
                // Fallback - generate fresh (breaks determinism but allows forward progress)
                Uuid::new_v4()
            }
            _ => Uuid::new_v4(),
        }
    }

    async fn uuid_v7(&self) -> Uuid {
        match self.mode.as_str() {
            "record" => {
                let id = Uuid::now_v7();
                self.capture_internal("uuid", &id.to_string()).await;
                id
            }
            "replay" => {
                if let Some(val) = self.replay_internal("uuid").await {
                    if let Ok(id) = Uuid::parse_str(&val) {
                        return id;
                    }
                }
                Uuid::now_v7()
            }
            _ => Uuid::now_v7(),
        }
    }

    async fn now(&self) -> SystemTime {
        match self.mode.as_str() {
            "record" => {
                let now = SystemTime::now();
                let ns = now
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                self.capture_internal("time", &ns.to_string()).await;
                now
            }
            "replay" => {
                if let Some(val) = self.replay_internal("time").await {
                    if let Ok(ns) = val.parse::<u64>() {
                        return SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(ns);
                    }
                }
                SystemTime::now()
            }
            _ => SystemTime::now(),
        }
    }

    async fn now_millis(&self) -> u64 {
        let time = self.now().await;
        time.duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    async fn random_u64(&self) -> u64 {
        match self.mode.as_str() {
            "record" => {
                let val: u64 = rand::random();
                self.capture_internal("random", &val.to_string()).await;
                val
            }
            "replay" => {
                if let Some(val) = self.replay_internal("random").await {
                    if let Ok(v) = val.parse::<u64>() {
                        return v;
                    }
                }
                rand::random()
            }
            _ => rand::random(),
        }
    }

    async fn random_bytes(&self, len: usize) -> Vec<u8> {
        use rand::RngCore;
        let mut bytes = vec![0u8; len];
        rand::thread_rng().fill_bytes(&mut bytes);
        bytes
    }

    async fn nanoid(&self) -> String {
        use rand::Rng;
        const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        let mut rng = rand::thread_rng();
        (0..21)
            .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
            .collect()
    }

    // Public trait methods for generic capture/replay
    // Note: These delegate to the private methods with same names via fully qualified syntax
    async fn capture(&self, tag: &str, value: &str) {
        // Only capture in record mode
        if self.mode == "record" {
            // Use private capture method
            self.capture_internal(tag, value).await;
        }
    }

    async fn replay(&self, tag: &str) -> Option<String> {
        // Only replay in replay mode
        if self.mode == "replay" {
            self.replay_internal(tag).await
        } else {
            None
        }
    }

    fn set_trace_id(&self, trace_id: String) {
        self.set_trace_id(trace_id);
    }

    fn get_trace_id(&self) -> String {
        self.trace_id()
    }

    fn control_client(&self) -> deja_common::ControlClient {
        let (host, port) = parse_proxy_url(&self.proxy_url);
        deja_common::ControlClient::new(&host, port)
    }
}

fn parse_proxy_url(url: &str) -> (String, u16) {
    let stripped = url.replace("http://", "").replace("https://", "");
    let parts: Vec<&str> = stripped.split(':').collect();
    let host = parts.get(0).copied().unwrap_or("localhost").to_string();
    let port = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(9999);
    (host, port)
}

impl DejaAsyncBoundary for NetworkRuntime {
    fn spawn<F, T>(&self, name: &str, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let trace_id = self.trace_id();
        let task_seq = self.task_sequence.fetch_add(1, Ordering::SeqCst);
        let task_name = name.to_string();
        let proxy_url = self.proxy_url.clone();
        let client = self.client.clone();
        let mode = self.mode.clone();

        tokio::spawn(async move {
            // Record task spawn in record mode
            if mode == "record" {
                let value = format!("{}:{}", task_name, task_seq);
                let req = CaptureRequest {
                    trace_id: trace_id.clone(),
                    kind: "task_spawn".to_string(),
                    value,
                };
                let _ = client
                    .post(format!("{}/capture", proxy_url))
                    .json(&req)
                    .send()
                    .await;
            }

            // Execute the actual future
            future.await
        })
    }

    fn spawn_blocking<F, T>(&self, name: &str, f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        // For blocking tasks, we record similarly but execute via spawn_blocking
        let trace_id = self.trace_id();
        let task_seq = self.task_sequence.fetch_add(1, Ordering::SeqCst);
        let task_name = name.to_string();
        let mode = self.mode.clone();

        if mode == "record" {
            // Note: We can't easily make a network call here synchronously
            // For now, just log the spawn. Full implementation would need
            // a background queue to handle recording.
            tracing::debug!(
                trace_id = %trace_id,
                task_name = %task_name,
                task_seq = %task_seq,
                "Recording blocking task spawn"
            );
        }

        tokio::task::spawn_blocking(f)
    }
}

impl NetworkRuntime {
    /// Generate a trace ID for a new request
    ///
    /// This should be called when a request arrives. The returned trace ID
    /// can then be used with `handle_request` to establish trace context.
    pub fn generate_trace_id(&self) -> String {
        trace_context::generate_trace_id()
    }

    /// Execute a request handler with trace context
    ///
    /// This method:
    /// 1. Generates a new trace ID (if not already set)
    /// 2. Establishes task-local trace context
    /// 3. Executes the handler future
    /// 4. Cleans up trace context after completion
    ///
    /// The trace ID is automatically available to all spawned tasks through
    /// the task-local TRACE_ID variable.
    ///
    /// # Arguments
    ///
    /// * `handler` - The async operation to execute within trace context
    ///
    /// # Example
    ///
    /// ```ignore
    /// let runtime = NetworkRuntime::from_env();
    /// let result = runtime.handle_request(async {
    ///     // Trace context is available here
    ///     do_work().await
    /// }).await;
    /// ```
    pub async fn handle_request<F, R>(&self, handler: F) -> R
    where
        F: Future<Output = R>,
    {
        let trace_id = self.trace_id();
        if trace_id.is_empty() {
            // Generate new trace ID if not already set
            let new_trace_id = self.generate_trace_id();
            self.set_trace_id(new_trace_id.clone());
            trace_context::with_trace_id(new_trace_id, handler).await
        } else {
            // Use existing trace ID
            trace_context::with_trace_id(trace_id, handler).await
        }
    }
}

/// Get the appropriate runtime based on environment
pub fn get_runtime() -> Box<dyn DejaRuntime> {
    let mode = env::var("DEJA_MODE").unwrap_or_else(|_| "production".into());
    match mode.as_str() {
        "record" | "replay" => Box::new(NetworkRuntime::from_env()),
        _ => Box::new(ProductionRuntime::new()),
    }
}

/// Get the appropriate runtime as Arc for shared ownership
pub fn get_runtime_arc() -> Arc<dyn DejaRuntime> {
    let mode = env::var("DEJA_MODE").unwrap_or_else(|_| "production".into());
    match mode.as_str() {
        "record" | "replay" => Arc::new(NetworkRuntime::from_env()),
        _ => Arc::new(ProductionRuntime::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_production_runtime() {
        let runtime = ProductionRuntime::new();

        let uuid = runtime.uuid().await;
        assert!(!uuid.is_nil());

        let time = runtime.now().await;
        assert!(time > SystemTime::UNIX_EPOCH);

        let random = runtime.random_u64().await;
        // Just ensure it doesn't panic
        let _ = random;

        let nanoid = runtime.nanoid().await;
        assert_eq!(nanoid.len(), 21);
    }

    #[tokio::test]
    async fn test_network_runtime_trace_id() {
        let runtime = NetworkRuntime::new("http://localhost:9999".into(), "production".into());

        assert!(runtime.trace_id().is_empty());

        runtime.set_trace_id("test-trace-123".into());
        assert_eq!(runtime.trace_id(), "test-trace-123");

        let cloned = runtime.with_trace("other-trace".into());
        assert_eq!(cloned.trace_id(), "other-trace");
        assert_eq!(runtime.trace_id(), "test-trace-123"); // Original unchanged
    }
}
