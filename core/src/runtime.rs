pub mod pool_interceptor;
pub mod socket_interceptor;
pub mod spawn;
pub mod trace_context;

use async_trait::async_trait;
use deja_common::{ControlClient, DejaMode, DejaRuntime, ScopeId, SyncDejaRuntime};
use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// Trait for async boundary operations (task spawning)
pub trait DejaAsyncBoundary: Send + Sync {
    /// Spawn a named task.
    fn spawn<F, T>(&self, name: &str, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;

    /// Spawn a blocking task with a name.
    fn spawn_blocking<F, T>(&self, name: &str, future: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static;
}

/// A pending captured value for batched flush
#[derive(Debug, Clone, serde::Serialize)]
struct PendingCapture {
    kind: String,
    seq: u64,
    value: String,
}

#[derive(Debug, Clone)]
struct QueuedCapture {
    trace_id: String,
    task_id: String,
    scope_id: String,
    capture: PendingCapture,
}

/// Batch capture request body
#[derive(serde::Serialize)]
struct BatchCaptureRequest {
    trace_id: String,
    task_id: String,
    scope_id: String,
    captures: Vec<PendingCapture>,
}

/// Request body for /capture endpoint
#[derive(serde::Serialize)]
struct CaptureRequest {
    trace_id: String,
    task_id: String,
    scope_id: String,
    kind: String,
    seq: u64,
    value: String,
}

/// Response from /replay endpoint
#[derive(serde::Deserialize)]
struct ReplayResponse {
    value: String,
    found: bool,
}

/// Unified Runtime for all Deja modes.
///
/// Each instance is bound to a (trace_id, task_id) pair and manages
/// per-kind sequence counters for deterministic capture/replay.
///
/// Use `child()` to create child runtimes for spawned tasks.
pub struct Runtime {
    client: reqwest::Client,
    proxy_url: String,
    trace_id: String,
    task_id: String,
    scope_id: ScopeId,
    mode: DejaMode,

    /// Per-(kind) sequence counters
    sequences: Arc<RwLock<HashMap<String, u64>>>,

    /// Pending captures waiting to be flushed
    pending_captures: Arc<Mutex<Vec<QueuedCapture>>>,

    /// Flush configuration
    flush_threshold: usize,
    last_flush: Arc<Mutex<Instant>>,
    flush_interval: Duration,

    /// Counter for generating child task IDs
    child_counter: Arc<AtomicU64>,

    /// Task sequence counter for spawned tasks
    task_sequence: Arc<AtomicU64>,
}

impl Runtime {
    /// Create from environment variables.
    ///
    /// Creates a root runtime (task_id = "0") with empty trace_id.
    /// The trace_id should be set via `with_trace_id()`.
    pub fn from_env() -> Self {
        let mode_str = env::var("DEJA_MODE").unwrap_or_else(|_| "record".into());
        let mode = mode_str.parse().unwrap_or(DejaMode::Record);
        let proxy_url =
            env::var("DEJA_PROXY_URL").unwrap_or_else(|_| "http://localhost:9999".into());

        Self::new_internal(proxy_url, mode, String::new(), "0".to_string())
    }

    /// Create with specific configuration (root runtime).
    pub fn new(proxy_url: String, mode: DejaMode) -> Self {
        Self::new_internal(proxy_url, mode, String::new(), "0".to_string())
    }

    /// Create a runtime bound to a specific trace and task.
    pub fn for_trace(proxy_url: String, mode: DejaMode, trace_id: String) -> Self {
        Self::new_internal(proxy_url, mode, trace_id, "0".to_string())
    }

    fn new_internal(proxy_url: String, mode: DejaMode, trace_id: String, task_id: String) -> Self {
        let scope_id = if trace_id.is_empty() {
            ScopeId::from_raw("")
        } else {
            ScopeId::task(&trace_id, &task_id)
        };

        Self {
            client: reqwest::Client::new(),
            proxy_url,
            trace_id,
            task_id,
            scope_id,
            mode,
            sequences: Arc::new(RwLock::new(HashMap::new())),
            pending_captures: Arc::new(Mutex::new(Vec::new())),
            flush_threshold: 100,
            last_flush: Arc::new(Mutex::new(Instant::now())),
            flush_interval: Duration::from_secs(5),
            child_counter: Arc::new(AtomicU64::new(0)),
            task_sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a copy of this runtime bound to a new trace_id (root task).
    pub fn with_trace_id(&self, trace_id: String) -> Self {
        Self::new_internal(self.proxy_url.clone(), self.mode, trace_id, "0".to_string())
    }

    /// Get the next sequence number for a given kind, incrementing the counter.
    fn current_trace_id_for_runtime(&self) -> String {
        trace_context::current_trace_id()
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| self.trace_id.clone())
    }

    fn current_task_id_for_runtime(&self) -> String {
        trace_context::current_task_id()
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| self.task_id.clone())
    }

    fn current_scope_id_for_runtime(&self, trace_id: &str, task_id: &str) -> String {
        if trace_id.is_empty() {
            self.scope_id.as_str().to_string()
        } else {
            ScopeId::task(trace_id, task_id).as_str().to_string()
        }
    }

    fn next_seq(&self, kind: &str) -> u64 {
        let trace_id = self.current_trace_id_for_runtime();
        let task_id = self.current_task_id_for_runtime();
        let sequence_key = format!("{}:{}:{}", trace_id, task_id, kind);

        match self.sequences.write() {
            Ok(mut seqs) => {
                let seq = seqs.entry(sequence_key.clone()).or_insert(0);
                let current = *seq;
                *seq += 1;
                current
            }
            Err(e) => {
                let mut seqs = e.into_inner();
                let seq = seqs.entry(sequence_key).or_insert(0);
                let current = *seq;
                *seq += 1;
                current
            }
        }
    }

    /// Check if flush is needed based on threshold or time
    async fn maybe_flush(&self) {
        let should_flush = {
            let pending = self
                .pending_captures
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let last_flush = self.last_flush.lock().unwrap_or_else(|e| e.into_inner());
            pending.len() >= self.flush_threshold || last_flush.elapsed() > self.flush_interval
        };

        if should_flush {
            self.flush_impl().await;
        }
    }

    async fn flush_impl(&self) {
        let captures: Vec<QueuedCapture> = {
            let mut pending = self
                .pending_captures
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            pending.drain(..).collect()
        };

        if captures.is_empty() {
            return;
        }

        // Update last flush time
        {
            let mut last_flush = self.last_flush.lock().unwrap_or_else(|e| e.into_inner());
            *last_flush = Instant::now();
        }

        let mut by_scope: HashMap<(String, String, String), Vec<PendingCapture>> = HashMap::new();
        for queued in captures {
            by_scope
                .entry((queued.trace_id, queued.task_id, queued.scope_id))
                .or_default()
                .push(queued.capture);
        }

        for ((trace_id, task_id, scope_id), captures) in by_scope {
            let req = BatchCaptureRequest {
                trace_id,
                task_id,
                scope_id,
                captures,
            };

            let _ = self
                .client
                .post(format!("{}/captures/batch", self.proxy_url))
                .json(&req)
                .send()
                .await;
        }
    }

    pub fn mode(&self) -> DejaMode {
        self.mode
    }

    pub fn proxy_url(&self) -> &str {
        &self.proxy_url
    }
}

#[async_trait]
impl DejaRuntime for Runtime {
    async fn capture_value(&self, kind: &str, value: String) {
        if self.mode != DejaMode::Record {
            return;
        }

        let trace_id = self.current_trace_id_for_runtime();
        let task_id = self.current_task_id_for_runtime();
        let scope_id = self.current_scope_id_for_runtime(&trace_id, &task_id);
        let seq = self.next_seq(kind);

        tracing::debug!(
            trace_id = %self.trace_id,
            task_id = %self.task_id,
            kind = %kind,
            seq = %seq,
            "Capturing value"
        );

        // Add to pending captures
        {
            let mut pending = self
                .pending_captures
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            pending.push(QueuedCapture {
                trace_id,
                task_id,
                scope_id,
                capture: PendingCapture {
                    kind: kind.to_string(),
                    seq,
                    value: value.clone(),
                },
            });
        }

        self.maybe_flush().await;
    }

    async fn replay_value(&self, kind: &str) -> Option<String> {
        if self.mode != DejaMode::Replay {
            return None;
        }

        let trace_id = self.current_trace_id_for_runtime();
        let task_id = self.current_task_id_for_runtime();
        let scope_id = self.current_scope_id_for_runtime(&trace_id, &task_id);
        let seq = self.next_seq(kind);

        tracing::debug!(
            trace_id = %self.trace_id,
            task_id = %self.task_id,
            kind = %kind,
            seq = %seq,
            "Replaying value"
        );

        let resp = self
            .client
            .get(format!("{}/replay", self.proxy_url))
            .query(&[
                ("trace_id", trace_id.as_str()),
                ("task_id", task_id.as_str()),
                ("scope_id", scope_id.as_str()),
                ("kind", kind),
                ("seq", &seq.to_string()),
            ])
            .send()
            .await
            .ok()?;

        let replay_resp: ReplayResponse = resp.json().await.ok()?;
        if replay_resp.found {
            Some(replay_resp.value)
        } else {
            tracing::warn!(
                trace_id = %self.trace_id,
                task_id = %self.task_id,
                kind = %kind,
                seq = %seq,
                "No recorded value found"
            );
            None
        }
    }

    fn trace_id(&self) -> String {
        // Try task-local first (async context propagation)
        if let Some(id) = trace_context::current_trace_id() {
            if !id.is_empty() {
                return id;
            }
        }
        self.trace_id.clone()
    }

    fn task_id(&self) -> String {
        if let Some(id) = trace_context::current_task_id() {
            if !id.is_empty() {
                return id;
            }
        }
        self.task_id.clone()
    }

    fn scope_id(&self) -> ScopeId {
        self.scope_id.clone()
    }

    fn mode(&self) -> DejaMode {
        self.mode
    }

    fn child(&self) -> Arc<dyn DejaRuntime> {
        let n = self.child_counter.fetch_add(1, Ordering::SeqCst);
        let child_task = format!("{}.{}", self.task_id, n);
        let child_scope = ScopeId::task(&self.trace_id, &child_task);

        Arc::new(Runtime {
            client: self.client.clone(),
            proxy_url: self.proxy_url.clone(),
            trace_id: self.trace_id.clone(),
            task_id: child_task,
            scope_id: child_scope,
            mode: self.mode,
            sequences: Arc::new(RwLock::new(HashMap::new())),
            pending_captures: Arc::new(Mutex::new(Vec::new())),
            flush_threshold: self.flush_threshold,
            last_flush: Arc::new(Mutex::new(Instant::now())),
            flush_interval: self.flush_interval,
            child_counter: Arc::new(AtomicU64::new(0)),
            task_sequence: Arc::new(AtomicU64::new(0)),
        })
    }

    async fn flush(&self) {
        self.flush_impl().await;
    }

    fn control_client(&self) -> ControlClient {
        let stripped = self
            .proxy_url
            .replace("http://", "")
            .replace("https://", "");
        let parts: Vec<&str> = stripped.split(':').collect();
        let host = parts.first().copied().unwrap_or("localhost").to_string();
        let port = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(9999);
        ControlClient::new(&host, port)
    }
}

impl SyncDejaRuntime for Runtime {
    fn capture_value(&self, kind: &str, value: String) {
        if self.mode != DejaMode::Record {
            return;
        }

        let seq = self.next_seq(kind);
        let trace_id = self.current_trace_id_for_runtime();
        let task_id = self.current_task_id_for_runtime();
        let scope_id = self.current_scope_id_for_runtime(&trace_id, &task_id);
        let req = CaptureRequest {
            trace_id,
            task_id,
            scope_id,
            kind: kind.to_string(),
            seq,
            value,
        };

        use std::sync::OnceLock;
        static BLOCKING_CLIENT: OnceLock<reqwest::blocking::Client> = OnceLock::new();
        let client = BLOCKING_CLIENT.get_or_init(reqwest::blocking::Client::new);

        let _ = client
            .post(format!("{}/capture", self.proxy_url))
            .json(&req)
            .send();
    }

    fn replay_value(&self, kind: &str) -> Option<String> {
        if self.mode != DejaMode::Replay {
            return None;
        }

        let seq = self.next_seq(kind);
        let trace_id = self.current_trace_id_for_runtime();
        let task_id = self.current_task_id_for_runtime();
        let scope_id = self.current_scope_id_for_runtime(&trace_id, &task_id);
        let url = format!(
            "{}/replay?trace_id={}&task_id={}&scope_id={}&kind={}&seq={}",
            self.proxy_url, trace_id, task_id, scope_id, kind, seq
        );

        use std::sync::OnceLock;
        static BLOCKING_CLIENT: OnceLock<reqwest::blocking::Client> = OnceLock::new();
        let client = BLOCKING_CLIENT.get_or_init(reqwest::blocking::Client::new);

        let resp = client.get(url).send().ok()?;
        let replay_resp: ReplayResponse = resp.json().ok()?;
        if replay_resp.found {
            Some(replay_resp.value)
        } else {
            tracing::warn!(
                trace_id = %self.trace_id,
                kind = %kind,
                seq = %seq,
                "No recorded value found (sync)"
            );
            None
        }
    }

    fn mode(&self) -> DejaMode {
        self.mode
    }
}

impl DejaAsyncBoundary for Runtime {
    fn spawn<F, T>(&self, name: &str, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let trace_id = self.trace_id.clone();
        let task_seq = self.task_sequence.fetch_add(1, Ordering::SeqCst);
        let task_name = name.to_string();
        let proxy_url = self.proxy_url.clone();
        let client = self.client.clone();
        let mode = self.mode;

        tokio::spawn(async move {
            if mode == DejaMode::Record {
                let value = format!("{}:{}", task_name, task_seq);
                let req = CaptureRequest {
                    trace_id: trace_id.clone(),
                    task_id: "0".to_string(),
                    scope_id: String::new(),
                    kind: "task_spawn".to_string(),
                    seq: task_seq,
                    value,
                };
                let _ = client
                    .post(format!("{}/capture", proxy_url))
                    .json(&req)
                    .send()
                    .await;
            }
            future.await
        })
    }

    fn spawn_blocking<F, T>(&self, _name: &str, f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(f)
    }
}

/// Get the appropriate runtime based on environment (singleton)
pub fn get_runtime() -> Arc<Runtime> {
    use std::sync::OnceLock;
    static RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| Arc::new(Runtime::from_env()))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_trace_id() {
        let runtime = Runtime::for_trace(
            "http://localhost:9999".into(),
            DejaMode::Record,
            "test-trace-123".into(),
        );
        assert_eq!(DejaRuntime::trace_id(&runtime), "test-trace-123");
    }

    #[test]
    fn test_sequence_tracking_increments_per_kind() {
        let runtime = Runtime::new("http://localhost:9999".into(), DejaMode::Record);

        assert_eq!(runtime.next_seq("uuid"), 0);
        assert_eq!(runtime.next_seq("uuid"), 1);
        assert_eq!(runtime.next_seq("uuid"), 2);

        assert_eq!(runtime.next_seq("time"), 0);
        assert_eq!(runtime.next_seq("time"), 1);

        assert_eq!(runtime.next_seq("uuid"), 3);
    }

    #[test]
    fn test_child_runtime() {
        let runtime = Runtime::for_trace(
            "http://localhost:9999".into(),
            DejaMode::Record,
            "t1".into(),
        );

        let child1 = runtime.child();
        assert_eq!(child1.task_id(), "0.0");

        let child2 = runtime.child();
        assert_eq!(child2.task_id(), "0.1");

        // Child scope IDs
        let expected_scope1 = ScopeId::task("t1", "0.0");
        let expected_scope2 = ScopeId::task("t1", "0.1");
        assert_eq!(child1.scope_id(), expected_scope1);
        assert_eq!(child2.scope_id(), expected_scope2);
    }

    #[test]
    fn test_with_trace_id() {
        let runtime = Runtime::new("http://localhost:9999".into(), DejaMode::Record);
        let bound = runtime.with_trace_id("my-trace".into());
        assert_eq!(DejaRuntime::trace_id(&bound), "my-trace");
        assert_eq!(bound.task_id(), "0");
    }
}
