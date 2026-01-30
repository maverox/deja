# Deja: Shortcoming Fixes & Orchestrated Replay PRD

## Overview

This document outlines solutions to identified shortcomings and adds automated orchestrated replay capability. The goal is to make Deja a complete, single-service regression testing tool.

---

## Current Architecture Gaps

| Gap | Impact | Priority |
|-----|--------|----------|
| Race condition: SDK notification after event recorded | Events have empty/wrong trace_id | P0 |
| Connection pool reuse: stale trace mappings | Events attributed to wrong trace | P0 |
| Background tasks: complete after trace ends | Orphaned events, incomplete traces | P1 |
| Manual replay: must manually trigger requests | Not automated regression testing | P0 |

---

## Solution 1: Retroactive Correlation (Race Condition Fix)

### Problem

```
Timeline:
T1: Service opens connection to Redis
T2: Proxy records Redis command with trace_id="" (unknown)
T3: SDK sends associate_connection(trace_id="abc", conn_id="redis-1")
T4: Proxy now knows, but event at T2 already has wrong trace_id
```

### Solution: Event Buffer with Retroactive Update

```rust
// proxy/src/correlation.rs

pub struct TraceCorrelator {
    // Existing
    connection_to_trace: HashMap<String, String>,

    // NEW: Buffer recent events for retroactive correlation
    pending_events: HashMap<String, Vec<PendingEvent>>,  // connection_id -> events
    pending_timeout: Duration,  // How long to hold events (e.g., 100ms)
}

struct PendingEvent {
    event: RecordedEvent,
    received_at: Instant,
}

impl TraceCorrelator {
    /// Called when we record an event but don't know the trace yet
    pub fn buffer_event(&mut self, connection_id: &str, event: RecordedEvent) {
        self.pending_events
            .entry(connection_id.to_string())
            .or_default()
            .push(PendingEvent {
                event,
                received_at: Instant::now(),
            });
    }

    /// Called when SDK sends associate_connection
    /// Returns events that can now be flushed with correct trace_id
    pub fn associate_and_flush(
        &mut self,
        trace_id: &str,
        connection_id: &str
    ) -> Vec<RecordedEvent> {
        // Update mapping
        self.connection_to_trace.insert(connection_id.to_string(), trace_id.to_string());

        // Flush pending events with correct trace_id
        if let Some(pending) = self.pending_events.remove(connection_id) {
            pending.into_iter()
                .map(|p| {
                    let mut event = p.event;
                    event.trace_id = trace_id.to_string();
                    event.sequence = self.next_sequence(trace_id, event.protocol());
                    event
                })
                .collect()
        } else {
            vec![]
        }
    }

    /// Background task: flush timed-out events with best-effort trace lookup
    pub fn flush_expired(&mut self) -> Vec<RecordedEvent> {
        let now = Instant::now();
        let mut flushed = vec![];

        self.pending_events.retain(|conn_id, events| {
            let (expired, remaining): (Vec<_>, Vec<_>) = events
                .drain(..)
                .partition(|e| now.duration_since(e.received_at) > self.pending_timeout);

            for pending in expired {
                let mut event = pending.event;
                // Best effort: use current mapping if available
                if let Some(trace_id) = self.connection_to_trace.get(conn_id) {
                    event.trace_id = trace_id.clone();
                }
                flushed.push(event);
            }

            *events = remaining;
            !events.is_empty()
        });

        flushed
    }
}
```

### Flow After Fix

```
T1: Service opens connection to Redis
T2: Proxy records Redis command → BUFFERED (not written yet)
T3: SDK sends associate_connection(trace_id="abc", conn_id="redis-1")
T4: Proxy flushes buffered event with trace_id="abc" ✓
```

---

## Solution 2: Connection Pool Lifecycle Tracking

### Problem

```
Request A borrows connection C1
├── Uses C1 for Redis SET
├── Returns C1 to pool
└── Trace A ends

Request B borrows connection C1 (SAME!)
├── Uses C1 for Redis GET
└── Proxy still thinks C1 belongs to trace A ✗
```

### Solution: Versioned Connection IDs + Pool Wrapper

```rust
// sdk/src/pool.rs

use std::sync::atomic::{AtomicU64, Ordering};

/// Wrapper for connection pools that tracks borrow/return cycles
pub struct InstrumentedPool<P> {
    inner: P,
    borrow_counter: AtomicU64,
}

impl<P: Pool> InstrumentedPool<P> {
    pub async fn get(&self) -> InstrumentedConnection<P::Connection> {
        let conn = self.inner.get().await?;
        let borrow_id = self.borrow_counter.fetch_add(1, Ordering::SeqCst);

        // Connection ID now includes borrow count for uniqueness
        let versioned_id = format!("{}:borrow-{}", conn.id(), borrow_id);

        // Notify proxy of new borrow with current trace
        let trace_id = current_trace_id().unwrap_or_default();
        control_client()
            .send(ControlMessage::AssociateConnection {
                trace_id,
                connection_id: versioned_id.clone(),
                protocol: Some(P::PROTOCOL),
            })
            .await;

        InstrumentedConnection {
            inner: conn,
            versioned_id,
        }
    }
}

impl<C> Drop for InstrumentedConnection<C> {
    fn drop(&mut self) {
        // Notify proxy that this versioned connection is done
        // (Next borrow will have different version)
        let _ = control_client().send_best_effort(
            ControlMessage::ConnectionClosed {
                connection_id: self.versioned_id.clone(),
            }
        );
    }
}
```

### Alternative: Proxy-Side Pool Detection

```rust
// proxy/src/correlation.rs

impl TraceCorrelator {
    /// Detect when a connection is reused by a different trace
    pub fn maybe_update_connection(&mut self, connection_id: &str, observed_trace: &str) {
        if let Some(current_trace) = self.connection_to_trace.get(connection_id) {
            if current_trace != observed_trace && !observed_trace.is_empty() {
                // Connection was reused! Update mapping
                info!(
                    connection = %connection_id,
                    old_trace = %current_trace,
                    new_trace = %observed_trace,
                    "Connection pool reuse detected, updating mapping"
                );
                self.connection_to_trace.insert(
                    connection_id.to_string(),
                    observed_trace.to_string()
                );
                // Reset sequence for new trace
                self.reset_sequence(observed_trace, connection_id);
            }
        }
    }
}
```

---

## Solution 3: Background Task Lifecycle Tracking

### Problem

```rust
async fn handle_request(req: Request) -> Response {
    let trace_id = generate_trace_id();

    with_trace_id(trace_id.clone(), async {
        control_client.send(StartTrace { trace_id }).await;

        // Spawn background task
        deja::spawn("bg_task", async {
            // This may complete AFTER end_trace!
            redis.set("key", "value").await;
        });

        let response = process(req).await;

        control_client.send(EndTrace { trace_id }).await;  // Too early!
        response
    }).await
}
```

### Solution: Task Registry with Completion Tracking

```rust
// sdk/src/runtime.rs

use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

pub struct TaskRegistry {
    /// Active tasks per trace: trace_id -> set of task names
    active_tasks: RwLock<HashMap<String, HashSet<String>>>,
    /// Completion notifiers
    completion_notifiers: RwLock<HashMap<String, Vec<oneshot::Sender<()>>>>,
}

impl TaskRegistry {
    pub fn register_task(&self, trace_id: &str, task_name: &str) {
        let mut tasks = self.active_tasks.write().await;
        tasks.entry(trace_id.to_string())
            .or_default()
            .insert(task_name.to_string());
    }

    pub fn complete_task(&self, trace_id: &str, task_name: &str) {
        let mut tasks = self.active_tasks.write().await;
        if let Some(set) = tasks.get_mut(trace_id) {
            set.remove(task_name);
            if set.is_empty() {
                tasks.remove(trace_id);
                // Notify waiters
                self.notify_completion(trace_id).await;
            }
        }
    }

    pub async fn wait_for_completion(&self, trace_id: &str, timeout: Duration) -> bool {
        // Check if already complete
        if !self.active_tasks.read().await.contains_key(trace_id) {
            return true;
        }

        // Wait for completion
        let (tx, rx) = oneshot::channel();
        self.completion_notifiers.write().await
            .entry(trace_id.to_string())
            .or_default()
            .push(tx);

        tokio::time::timeout(timeout, rx).await.is_ok()
    }
}

// Updated spawn function
impl NetworkRuntime {
    pub fn spawn<F, T>(&self, name: &str, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let trace_id = self.get_trace_id();
        let task_name = format!("{}:{}", name, self.next_task_id());

        // Register task
        self.task_registry.register_task(&trace_id, &task_name);

        // Capture for recording
        self.capture_task_spawn(&trace_id, &task_name);

        let registry = self.task_registry.clone();
        let trace_id_clone = trace_id.clone();
        let task_name_clone = task_name.clone();

        tokio::spawn(async move {
            let result = future.await;

            // Mark complete
            registry.complete_task(&trace_id_clone, &task_name_clone);

            result
        })
    }
}

// Updated trace wrapper
pub async fn with_trace_id_and_wait<F, T>(trace_id: String, future: F) -> T
where
    F: Future<Output = T>,
{
    let runtime = get_runtime_arc();
    let control_client = runtime.control_client();

    // Start trace
    control_client.send(ControlMessage::start_trace(&trace_id)).await;

    // Execute main work
    let result = with_trace_id(trace_id.clone(), future).await;

    // Wait for spawned tasks to complete (with timeout)
    let completed = runtime.task_registry()
        .wait_for_completion(&trace_id, Duration::from_secs(30))
        .await;

    if !completed {
        warn!(trace_id = %trace_id, "Trace ended with incomplete background tasks");
    }

    // End trace
    control_client.send(ControlMessage::end_trace(&trace_id)).await;

    result
}
```

---

## Solution 4: Orchestrated Replay (Automated Regression Testing)

### The Big Picture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATED REPLAY MODE                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐                                                        │
│  │ Recording    │                                                        │
│  │ Index        │                                                        │
│  │ ─────────    │                                                        │
│  │ Trace 1:     │                                                        │
│  │  - Incoming  │──┐                                                     │
│  │  - Redis x2  │  │                                                     │
│  │  - PG x1     │  │                                                     │
│  │  - Response  │  │                                                     │
│  │              │  │    ┌─────────────────┐    ┌─────────────────┐       │
│  │ Trace 2:     │  │    │                 │    │                 │       │
│  │  - Incoming  │──┼───▶│  Replay         │───▶│    Service      │       │
│  │  - HTTP x1   │  │    │  Orchestrator   │    │    (V2 code)    │       │
│  │  - Response  │  │    │                 │    │                 │       │
│  │              │  │    └────────┬────────┘    └────────┬────────┘       │
│  │ Trace 3:     │  │             │                      │                │
│  │  ...         │──┘             │                      │                │
│  └──────────────┘                │                      │                │
│                                  │                      │                │
│                         1. Send recorded       2. Service makes          │
│                            incoming request       backend calls          │
│                                  │                      │                │
│                                  ▼                      ▼                │
│                           ┌─────────────────────────────────────┐        │
│                           │                                     │        │
│                           │         Deja Proxy (Replay Mode)    │        │
│                           │                                     │        │
│                           │  3. Serve recorded responses        │        │
│                           │     (Redis, Postgres, HTTP)         │        │
│                           │                                     │        │
│                           └─────────────────────────────────────┘        │
│                                          │                               │
│                                          │                               │
│                                  4. Service returns                      │
│                                     response to orchestrator             │
│                                          │                               │
│                                          ▼                               │
│                           ┌─────────────────────────────────────┐        │
│                           │                                     │        │
│                           │         Diff Engine                 │        │
│                           │                                     │        │
│                           │  5. Compare actual vs recorded      │        │
│                           │     response → REGRESSION REPORT    │        │
│                           │                                     │        │
│                           └─────────────────────────────────────┘        │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Implementation

```rust
// core/src/replay/orchestrator.rs

use crate::replay::{RecordingIndex, RecordedTrace};
use crate::diff::{DiffEngine, DiffReport};
use crate::events::RecordedEvent;

/// Orchestrates automated replay of recorded traces
pub struct ReplayOrchestrator {
    /// All recorded traces
    index: RecordingIndex,
    /// HTTP client to send requests to service
    client: reqwest::Client,
    /// Service URL to replay against
    service_url: String,
    /// Diff engine for comparing responses
    diff_engine: DiffEngine,
}

/// Result of replaying a single trace
#[derive(Debug)]
pub struct TraceReplayResult {
    pub trace_id: String,
    pub recorded_request: RecordedEvent,
    pub recorded_response: RecordedEvent,
    pub actual_response: Option<ReplayedResponse>,
    pub diff: Option<DiffReport>,
    pub status: TraceReplayStatus,
}

#[derive(Debug)]
pub enum TraceReplayStatus {
    Success,           // Response matched
    Regression,        // Response differed
    Error(String),     // Couldn't complete replay
    Skipped(String),   // Skipped (e.g., no incoming request found)
}

#[derive(Debug)]
pub struct ReplayedResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub latency_ms: u64,
}

impl ReplayOrchestrator {
    pub fn new(
        recording_path: impl AsRef<Path>,
        service_url: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let recordings = load_recordings(recording_path)?;
        let index = RecordingIndex::from_recordings(&recordings);

        Ok(Self {
            index,
            client: reqwest::Client::new(),
            service_url: service_url.to_string(),
            diff_engine: DiffEngine::new(),
        })
    }

    /// Replay all traces and return regression report
    pub async fn replay_all(&self) -> ReplayReport {
        let mut results = vec![];

        for trace in self.index.traces() {
            let result = self.replay_trace(trace).await;
            results.push(result);
        }

        ReplayReport::from_results(results)
    }

    /// Replay a single trace
    async fn replay_trace(&self, trace: &RecordedTrace) -> TraceReplayResult {
        // 1. Find the incoming request (gRPC or HTTP)
        let incoming = match self.find_incoming_request(trace) {
            Some(req) => req,
            None => return TraceReplayResult {
                trace_id: trace.trace_id.clone(),
                recorded_request: RecordedEvent::default(),
                recorded_response: RecordedEvent::default(),
                actual_response: None,
                diff: None,
                status: TraceReplayStatus::Skipped("No incoming request found".into()),
            },
        };

        // 2. Find the recorded response
        let recorded_response = match self.find_response(trace, &incoming) {
            Some(resp) => resp,
            None => return TraceReplayResult {
                trace_id: trace.trace_id.clone(),
                recorded_request: incoming.clone(),
                recorded_response: RecordedEvent::default(),
                actual_response: None,
                diff: None,
                status: TraceReplayStatus::Skipped("No recorded response found".into()),
            },
        };

        // 3. Reconstruct and send the request
        let actual_response = match self.send_request(&incoming).await {
            Ok(resp) => resp,
            Err(e) => return TraceReplayResult {
                trace_id: trace.trace_id.clone(),
                recorded_request: incoming.clone(),
                recorded_response: recorded_response.clone(),
                actual_response: None,
                diff: None,
                status: TraceReplayStatus::Error(e.to_string()),
            },
        };

        // 4. Compare responses
        let diff = self.diff_engine.compare(
            &recorded_response,
            &actual_response,
        );

        let status = if diff.has_differences() {
            TraceReplayStatus::Regression
        } else {
            TraceReplayStatus::Success
        };

        TraceReplayResult {
            trace_id: trace.trace_id.clone(),
            recorded_request: incoming.clone(),
            recorded_response: recorded_response.clone(),
            actual_response: Some(actual_response),
            diff: Some(diff),
            status,
        }
    }

    /// Find the incoming request in a trace (HTTP or gRPC)
    fn find_incoming_request(&self, trace: &RecordedTrace) -> Option<&RecordedEvent> {
        trace.events.iter().find(|e| {
            matches!(e.event,
                Event::HttpRequest { .. } |
                Event::GrpcRequest { .. }
            ) && e.sequence == 0  // First event is usually the incoming request
        })
    }

    /// Find the response for an incoming request
    fn find_response(&self, trace: &RecordedTrace, request: &RecordedEvent) -> Option<&RecordedEvent> {
        trace.events.iter().find(|e| {
            match (&request.event, &e.event) {
                (Event::HttpRequest { .. }, Event::HttpResponse { .. }) => true,
                (Event::GrpcRequest { .. }, Event::GrpcResponse { .. }) => true,
                _ => false,
            }
        })
    }

    /// Reconstruct and send the recorded request to the service
    async fn send_request(&self, recorded: &RecordedEvent) -> Result<ReplayedResponse, Box<dyn Error>> {
        let start = Instant::now();

        match &recorded.event {
            Event::HttpRequest { method, path, headers, body, .. } => {
                let url = format!("{}{}", self.service_url, path);
                let mut req = self.client.request(
                    method.parse()?,
                    &url,
                );

                // Add recorded headers
                for (key, value) in headers {
                    req = req.header(key, value);
                }

                // Add trace ID header so service knows this is a replay
                req = req.header("X-Deja-Trace-ID", &recorded.trace_id);
                req = req.header("X-Deja-Replay", "true");

                let resp = req.body(body.clone()).send().await?;

                Ok(ReplayedResponse {
                    status: resp.status().as_u16(),
                    headers: resp.headers()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                        .collect(),
                    body: resp.bytes().await?.to_vec(),
                    latency_ms: start.elapsed().as_millis() as u64,
                })
            }

            Event::GrpcRequest { service, method, request_body, metadata, .. } => {
                // For gRPC, we need to reconstruct the HTTP/2 request
                let url = format!("{}/{}/{}", self.service_url, service, method);

                let mut req = self.client.post(&url)
                    .header("content-type", "application/grpc")
                    .header("te", "trailers")
                    .header("X-Deja-Trace-ID", &recorded.trace_id)
                    .header("X-Deja-Replay", "true");

                // Add recorded metadata as headers
                for (key, value) in metadata {
                    if !key.starts_with(':') {  // Skip pseudo-headers
                        req = req.header(key, value);
                    }
                }

                // gRPC body is length-prefixed
                let mut body = vec![0u8];  // No compression
                body.extend((request_body.len() as u32).to_be_bytes());
                body.extend(request_body);

                let resp = req.body(body).send().await?;

                Ok(ReplayedResponse {
                    status: resp.status().as_u16(),
                    headers: resp.headers()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                        .collect(),
                    body: resp.bytes().await?.to_vec(),
                    latency_ms: start.elapsed().as_millis() as u64,
                })
            }

            _ => Err("Not an incoming request event".into()),
        }
    }
}
```

### Replay Report

```rust
// core/src/replay/report.rs

#[derive(Debug)]
pub struct ReplayReport {
    pub total_traces: usize,
    pub successful: usize,
    pub regressions: usize,
    pub errors: usize,
    pub skipped: usize,
    pub results: Vec<TraceReplayResult>,
    pub duration: Duration,
}

impl ReplayReport {
    pub fn from_results(results: Vec<TraceReplayResult>) -> Self {
        let total_traces = results.len();
        let successful = results.iter().filter(|r| matches!(r.status, TraceReplayStatus::Success)).count();
        let regressions = results.iter().filter(|r| matches!(r.status, TraceReplayStatus::Regression)).count();
        let errors = results.iter().filter(|r| matches!(r.status, TraceReplayStatus::Error(_))).count();
        let skipped = results.iter().filter(|r| matches!(r.status, TraceReplayStatus::Skipped(_))).count();

        Self {
            total_traces,
            successful,
            regressions,
            errors,
            skipped,
            results,
            duration: Duration::default(),
        }
    }

    pub fn print_summary(&self) {
        println!("\n╔══════════════════════════════════════════════════════════════╗");
        println!("║                    REPLAY REPORT                             ║");
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║  Total Traces:    {:>6}                                     ║", self.total_traces);
        println!("║  ✅ Successful:   {:>6}                                     ║", self.successful);
        println!("║  ❌ Regressions:  {:>6}                                     ║", self.regressions);
        println!("║  ⚠️  Errors:       {:>6}                                     ║", self.errors);
        println!("║  ⏭️  Skipped:      {:>6}                                     ║", self.skipped);
        println!("║                                                              ║");
        println!("║  Duration:        {:>6.2}s                                   ║", self.duration.as_secs_f64());
        println!("╚══════════════════════════════════════════════════════════════╝");

        if self.regressions > 0 {
            println!("\n🚨 REGRESSIONS DETECTED:\n");
            for result in &self.results {
                if let TraceReplayStatus::Regression = result.status {
                    println!("  Trace: {}", result.trace_id);
                    if let Some(diff) = &result.diff {
                        diff.print_details("    ");
                    }
                    println!();
                }
            }
        }
    }

    /// Return exit code for CI/CD
    pub fn exit_code(&self) -> i32 {
        if self.regressions > 0 {
            1  // Failure
        } else if self.errors > 0 {
            2  // Error
        } else {
            0  // Success
        }
    }
}
```

### CLI Integration

```rust
// cli/src/main.rs

#[derive(Parser)]
enum Commands {
    /// Record traffic
    Record {
        #[arg(long)]
        port_map: Vec<String>,
        #[arg(long)]
        output: PathBuf,
    },

    /// Replay recorded traffic (mock mode)
    Replay {
        #[arg(long)]
        recording: PathBuf,
        #[arg(long)]
        port_map: Vec<String>,
    },

    /// NEW: Orchestrated replay - automatically send recorded requests
    Orchestrate {
        /// Path to recordings
        #[arg(long)]
        recording: PathBuf,

        /// Service URL to replay against
        #[arg(long)]
        service_url: String,

        /// Proxy port for backend mocking
        #[arg(long, default_value = "9998")]
        proxy_port: u16,

        /// Control API port
        #[arg(long, default_value = "9999")]
        control_port: u16,

        /// Output format (text, json, junit)
        #[arg(long, default_value = "text")]
        format: String,

        /// Fail on first regression (for CI)
        #[arg(long)]
        fail_fast: bool,

        /// Filter traces by pattern
        #[arg(long)]
        filter: Option<String>,

        /// Concurrent replay (number of parallel traces)
        #[arg(long, default_value = "1")]
        concurrency: usize,
    },
}

async fn run_orchestrate(args: OrchestrateArgs) -> Result<(), Box<dyn Error>> {
    println!("🎬 Starting Orchestrated Replay...\n");

    // 1. Start proxy in replay mode (for backend mocking)
    let proxy = start_proxy_replay(
        args.recording.clone(),
        args.proxy_port,
        args.control_port,
    ).await?;

    println!("✅ Proxy started on port {}", args.proxy_port);

    // 2. Create orchestrator
    let orchestrator = ReplayOrchestrator::new(
        args.recording,
        &args.service_url,
    )?;

    println!("✅ Loaded {} traces\n", orchestrator.trace_count());

    // 3. Run replay
    let start = Instant::now();
    let mut report = if args.concurrency > 1 {
        orchestrator.replay_concurrent(args.concurrency).await
    } else {
        orchestrator.replay_all().await
    };
    report.duration = start.elapsed();

    // 4. Output results
    match args.format.as_str() {
        "json" => println!("{}", serde_json::to_string_pretty(&report)?),
        "junit" => println!("{}", report.to_junit_xml()),
        _ => report.print_summary(),
    }

    // 5. Cleanup
    proxy.shutdown().await;

    // 6. Exit with appropriate code
    std::process::exit(report.exit_code());
}
```

### Usage Example

```bash
# 1. Record production traffic
deja record \
  --port-map 5433:localhost:5432 \
  --port-map 6380:localhost:6379 \
  --output ./recordings

# 2. Deploy new version of service

# 3. Run orchestrated replay against new version
deja orchestrate \
  --recording ./recordings \
  --service-url http://localhost:8080 \
  --proxy-port 9998 \
  --format junit \
  > test-results.xml

# CI/CD: Fail pipeline if regressions detected
if [ $? -ne 0 ]; then
  echo "Regressions detected!"
  exit 1
fi
```

---

## Solution 5: Full Replay Flow (Putting It All Together)

### Complete Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│                          DEJA ORCHESTRATED REPLAY                           │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         ORCHESTRATOR                                 │   │
│  │                                                                      │   │
│  │  1. Load RecordingIndex                                              │   │
│  │  2. For each trace:                                                  │   │
│  │     a. Extract incoming request (HTTP/gRPC)                          │   │
│  │     b. Send to service with X-Deja-Trace-ID header                   │   │
│  │     c. Collect actual response                                       │   │
│  │     d. Compare with recorded response                                │   │
│  │  3. Generate regression report                                       │   │
│  │                                                                      │   │
│  └────────────────────────────────┬─────────────────────────────────────┘   │
│                                   │                                         │
│                    ┌──────────────┴──────────────┐                          │
│                    │                             │                          │
│                    ▼                             ▼                          │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────┐      │
│  │                             │  │                                  │      │
│  │    SERVICE (V2 Code)        │  │     PROXY (Replay Mode)          │      │
│  │                             │  │                                  │      │
│  │  - Receives request         │  │  - Intercepts backend calls     │      │
│  │  - Extracts trace_id        │  │  - Matches by trace+sequence    │      │
│  │  - Sets DEJA_MODE=replay    │  │  - Returns recorded response    │      │
│  │  - Uses NetworkRuntime      │  │  - Serves captured UUIDs/times  │      │
│  │    (replays UUIDs/times)    │  │                                  │      │
│  │  - Makes backend calls      │──▶  - Validates request matches    │      │
│  │  - Returns response         │  │                                  │      │
│  │                             │  │                                  │      │
│  └─────────────────────────────┘  └─────────────────────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Replay Sequence Diagram

```
Orchestrator          Service (V2)           Proxy              RecordingIndex
     │                     │                   │                      │
     │  Load recordings    │                   │                      │
     │────────────────────────────────────────────────────────────────▶│
     │                     │                   │                      │
     │◀───────────────────────────────────────────────────────────────│
     │  [Trace1, Trace2, ...]                  │                      │
     │                     │                   │                      │
     │  For each trace:    │                   │                      │
     │                     │                   │                      │
     │  POST /api/process  │                   │                      │
     │  X-Deja-Trace-ID: T1│                   │                      │
     │────────────────────▶│                   │                      │
     │                     │                   │                      │
     │                     │  Redis GET        │                      │
     │                     │──────────────────▶│                      │
     │                     │                   │                      │
     │                     │                   │  Lookup (T1, Redis, 0)
     │                     │                   │─────────────────────▶│
     │                     │                   │                      │
     │                     │                   │◀─────────────────────│
     │                     │                   │  Recorded response   │
     │                     │◀──────────────────│                      │
     │                     │  "cached_value"   │                      │
     │                     │                   │                      │
     │                     │  Postgres INSERT  │                      │
     │                     │──────────────────▶│                      │
     │                     │                   │  Lookup (T1, PG, 0)  │
     │                     │                   │─────────────────────▶│
     │                     │                   │◀─────────────────────│
     │                     │◀──────────────────│  Recorded response   │
     │                     │  INSERT OK        │                      │
     │                     │                   │                      │
     │◀────────────────────│                   │                      │
     │  Response: {...}    │                   │                      │
     │                     │                   │                      │
     │  Compare with recorded                  │                      │
     │  response from Trace1                   │                      │
     │                     │                   │                      │
     │  DIFF: status changed                   │                      │
     │        "Success" → "Success (v2)"       │                      │
     │                     │                   │                      │
```

---

## Implementation Priority

| Phase | Component | Effort | Impact |
|-------|-----------|--------|--------|
| **Phase 1** | Orchestrated Replay (basic) | 2 days | High - Enables automated testing |
| **Phase 2** | Retroactive Correlation | 1 day | High - Fixes trace attribution |
| **Phase 3** | Connection Pool Tracking | 1 day | Medium - Fixes pool reuse |
| **Phase 4** | Background Task Registry | 1 day | Medium - Fixes async boundaries |
| **Phase 5** | CLI + CI/CD Integration | 1 day | High - Makes it usable |

---

## Summary

### What We're Building

```
BEFORE: Manual testing tool
─────────────────────────────
1. Start proxy in record mode
2. Manually send requests
3. Stop proxy
4. Start proxy in replay mode
5. Manually send SAME requests
6. Manually compare responses

AFTER: Automated regression testing
────────────────────────────────────
1. deja record --output ./recordings     # Done once
2. deja orchestrate \                    # Run in CI/CD
     --recording ./recordings \
     --service-url http://service:8080
3. Pipeline fails if regressions detected ✓
```

### The End Goal

```bash
# In CI/CD pipeline
git push origin feature-branch

# CI runs:
deja orchestrate \
  --recording ./prod-recordings \
  --service-url http://feature-branch-service:8080 \
  --format junit

# Output:
# ╔══════════════════════════════════════╗
# ║          REPLAY REPORT               ║
# ╠══════════════════════════════════════╣
# ║  Total Traces:    1,247              ║
# ║  ✅ Successful:   1,245              ║
# ║  ❌ Regressions:      2              ║
# ╚══════════════════════════════════════╝
#
# 🚨 REGRESSIONS DETECTED:
#   Trace: user-checkout-abc123
#     - response.total: 99.99 → 100.00 (rounding change)
#   Trace: payment-process-xyz789
#     - response.status: "approved" → "pending" (logic change)
```

This transforms Deja from a recording tool into a full **automated regression testing platform**.
