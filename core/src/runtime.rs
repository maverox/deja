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

use async_trait::async_trait;
use std::env;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Trait for handling non-deterministic operations in a replay-friendly way.
///
/// Services should use this trait instead of directly calling `Uuid::new_v4()`,
/// `SystemTime::now()`, etc. to enable deterministic replay.
#[async_trait]
pub trait DejaRuntime: Send + Sync {
    /// Generate a UUID v4
    async fn uuid(&self) -> Uuid;

    /// Generate a UUID v7 (time-ordered)
    async fn uuid_v7(&self) -> Uuid;

    /// Get current system time
    async fn now(&self) -> SystemTime;

    /// Get current time as milliseconds since epoch
    async fn now_millis(&self) -> u64;

    /// Generate a random u64
    async fn random_u64(&self) -> u64;

    /// Generate random bytes
    async fn random_bytes(&self, len: usize) -> Vec<u8>;

    /// Generate a nanoid (default 21 chars, alphanumeric)
    async fn nanoid(&self) -> String;

    // ========== Generic Capture/Replay Interface ==========
    // These methods allow clients to record ANY non-deterministic value
    // without requiring core to enumerate all possible types.

    /// Record a non-deterministic value with a tag.
    /// In production mode: no-op (value is ignored)
    /// In recording mode: sends value to proxy for storage
    async fn capture(&self, tag: &str, value: &str);

    /// Replay a recorded value by tag.
    /// In production mode: returns None
    /// In replay mode: returns the recorded value
    async fn replay(&self, tag: &str) -> Option<String>;
}

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
            proxy_url: env::var("DEJA_PROXY_URL").unwrap_or_else(|_| "http://localhost:9999".into()),
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
        *self.trace_id.write().unwrap() = trace_id;
    }

    /// Get the current trace ID
    pub fn trace_id(&self) -> String {
        self.trace_id.read().unwrap().clone()
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
        let _ = self.client
            .post(format!("{}/capture", self.proxy_url))
            .json(&req)
            .send()
            .await;
    }

    /// GET a replayed value from the proxy (internal use)
    async fn replay_internal(&self, kind: &str) -> Option<String> {
        let trace = self.trace_id();
        let resp = self.client
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
                let ns = now.duration_since(SystemTime::UNIX_EPOCH)
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
