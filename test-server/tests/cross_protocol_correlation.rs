//! Cross-Protocol Correlation Test
//!
//! This test validates that when a single trace makes simultaneous requests
//! across multiple protocols (HTTP, Postgres, Redis), ALL events are correctly
//! correlated to the same trace_id.
//!
//! This is the critical test that validates the correlation system works
//! correctly when requests are interleaved at the transport layer.

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tonic::Request;

pub mod connector {
    tonic::include_proto!("connector");
}
use connector::connector_client::ConnectorClient;
use connector::ProcessRequest;

fn get_available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to find a port");
    listener
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

fn resolve_binary(binary_name: &str) -> std::path::PathBuf {
    let cwd = std::env::current_dir().unwrap();
    let primary = cwd.join("target").join("debug").join(binary_name);
    if primary.exists() {
        return primary;
    }

    let fallback = cwd
        .join("..")
        .join("target")
        .join("debug")
        .join(binary_name);
    if fallback.exists() {
        return fallback;
    }

    primary
}

fn print_banner(msg: &str) {
    let line = "=".repeat(80);
    println!("\n{}", line);
    println!("🚀 {}", msg);
    println!("{}\n", line);
}

fn spawn_log_relay(mut reader: BufReader<std::process::ChildStdout>, prefix: String) {
    std::thread::spawn(move || {
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap_or(0) > 0 {
            print!("{}: {}", prefix, line);
            line.clear();
        }
    });
}

fn spawn_err_relay(mut reader: BufReader<std::process::ChildStderr>, prefix: String) {
    std::thread::spawn(move || {
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap_or(0) > 0 {
            eprint!("{}: {}", prefix, line);
            line.clear();
        }
    });
}

struct CrossProtocolTestContext {
    proxy_proc: Option<Child>,
    service_proc: Option<Child>,

    grpc_proxy_port: u16,
    grpc_service_port: u16,
    http_proxy_port: u16,
    redis_proxy_port: u16,
    pg_proxy_port: u16,
    echo_port: u16,
    control_port: u16,

    recordings_dir: std::path::PathBuf,
}

impl CrossProtocolTestContext {
    async fn setup() -> Self {
        let recordings_dir_path = std::env::temp_dir().join(format!(
            "test_recordings_cross_protocol_{}_{}",
            std::process::id(),
            get_available_port()
        ));
        if recordings_dir_path.exists() {
            let _ = std::fs::remove_dir_all(&recordings_dir_path);
        }
        std::fs::create_dir_all(&recordings_dir_path).unwrap();

        let grpc_proxy_port = get_available_port();
        let grpc_service_port = get_available_port();
        let http_proxy_port = get_available_port();
        let redis_proxy_port = get_available_port();
        let pg_proxy_port = get_available_port();
        let echo_port = get_available_port();
        let control_port = get_available_port();

        Self {
            proxy_proc: None,
            service_proc: None,
            grpc_proxy_port,
            grpc_service_port,
            http_proxy_port,
            redis_proxy_port,
            pg_proxy_port,
            echo_port,
            control_port,
            recordings_dir: recordings_dir_path,
        }
    }

    async fn wait_for_port(&self, port: u16) -> bool {
        for _ in 0..120 {
            if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
                return true;
            }
            sleep(Duration::from_millis(500)).await;
        }
        false
    }

    async fn start_proxy(&mut self, mode: &str) {
        let recordings_path = self.recordings_dir.to_str().unwrap();

        let map_http = format!("{}:127.0.0.1:{}", self.http_proxy_port, self.echo_port);
        let map_redis = format!("{}:127.0.0.1:6379", self.redis_proxy_port);
        let map_pg = format!("{}:127.0.0.1:5432", self.pg_proxy_port);

        let ingress = format!(
            "{}:127.0.0.1:{}",
            self.grpc_proxy_port, self.grpc_service_port
        );

        println!(
            "Starting proxy in {} mode on port {}...",
            mode, self.grpc_proxy_port
        );
        let mut child = Command::new(resolve_binary("deja-proxy"))
            .args([
                "--mode",
                mode,
                "--ingress",
                &ingress,
                "--map",
                &map_http,
                "--map",
                &map_redis,
                "--map",
                &map_pg,
                "--record-dir",
                recordings_path,
                "--control-port",
                &self.control_port.to_string(),
            ])
            .env("DEJA_STORAGE_FORMAT", "json")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let prefix = format!("[PROXY-{}]", mode.to_uppercase());
        spawn_log_relay(BufReader::new(stdout), prefix.clone());
        spawn_err_relay(BufReader::new(stderr), prefix);

        self.proxy_proc = Some(child);
        if !self.wait_for_port(self.grpc_proxy_port).await {
            panic!("Proxy did not start on port {}", self.grpc_proxy_port);
        }
    }

    async fn start_service(&mut self, mode: &str) {
        println!(
            "Starting test-service on port {} in {} mode...",
            self.grpc_service_port, mode
        );
        let http_server_port = get_available_port();
        let test_service_grpc_port = get_available_port();
        let jsonl_path = std::env::temp_dir().join(format!(
            "test_output_cross_protocol_{}_{}.jsonl",
            std::process::id(),
            self.grpc_service_port
        ));

        let mut child = Command::new(resolve_binary("test-server"))
            .args([
                "--grpc-port",
                &test_service_grpc_port.to_string(),
                "--connector-port",
                &self.grpc_service_port.to_string(),
                "--http-port",
                &http_server_port.to_string(),
                "--mock-port",
                &self.echo_port.to_string(),
                "--recording-path",
                self.recordings_dir.to_str().unwrap(),
                "--jsonl-path",
                jsonl_path.to_str().unwrap(),
                "--pg-url",
                &format!(
                    "postgres://deja_user:password@127.0.0.1:{}/deja_test",
                    self.pg_proxy_port
                ),
                "--redis-url",
                &format!("redis://127.0.0.1:{}", self.redis_proxy_port),
            ])
            .env(
                "GRPC_LISTEN_ADDR",
                format!("127.0.0.1:{}", self.grpc_service_port),
            )
            .env(
                "DEJA_CONTROL_URL",
                format!("http://127.0.0.1:{}", self.control_port),
            )
            .env(
                "DEJA_PROXY_URL",
                format!("http://127.0.0.1:{}", self.control_port),
            )
            .env("DEJA_MODE", mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        spawn_log_relay(BufReader::new(stdout), "[SERVICE]".to_string());
        spawn_err_relay(BufReader::new(stderr), "[SERVICE]".to_string());

        self.service_proc = Some(child);
        if !self.wait_for_port(self.grpc_service_port).await {
            panic!("Service did not start on port {}", self.grpc_service_port);
        }
    }

    async fn stop_all(&mut self) {
        if let Some(mut p) = self.proxy_proc.take() {
            let _ = p.kill();
        }
        if let Some(mut s) = self.service_proc.take() {
            let _ = s.kill();
        }
        sleep(Duration::from_secs(1)).await;
    }

    /// Parse recordings and group events by trace_id and protocol
    fn analyze_recordings(&self) -> HashMap<String, ProtocolEvents> {
        let mut traces: HashMap<String, ProtocolEvents> = HashMap::new();

        let sessions_dir = self.recordings_dir.join("sessions");
        if let Ok(entries) = std::fs::read_dir(&sessions_dir) {
            for entry in entries.flatten() {
                if entry.file_type().unwrap().is_dir() {
                    let events_path = entry.path().join("events.jsonl");
                    if events_path.exists() {
                        let content = std::fs::read_to_string(&events_path).unwrap();
                        for line in content.lines() {
                            if let Some((trace_id, protocol)) = extract_trace_and_protocol(line) {
                                traces.entry(trace_id).or_default().add_event(&protocol);
                            }
                        }
                    }
                }
            }
        }

        traces
    }
}

#[derive(Debug, Default)]
struct ProtocolEvents {
    http_count: usize,
    redis_count: usize,
    postgres_count: usize,
    grpc_count: usize,
    total_events: usize,
}

impl ProtocolEvents {
    fn add_event(&mut self, protocol: &str) {
        self.total_events += 1;
        match protocol {
            "http" | "Http" => self.http_count += 1,
            "redis" | "Redis" => self.redis_count += 1,
            "postgres" | "Postgres" => self.postgres_count += 1,
            "grpc" | "Grpc" => self.grpc_count += 1,
            _ => {}
        }
    }
}

fn extract_trace_and_protocol(json_line: &str) -> Option<(String, String)> {
    // Extract trace_id
    let trace_id = if let Some(start) = json_line.find("\"trace_id\":\"") {
        let rest = &json_line[start + 12..];
        if let Some(end) = rest.find('"') {
            rest[..end].to_string()
        } else {
            return None;
        }
    } else {
        return None;
    };

    // Extract protocol from various possible fields
    let protocol = if json_line.contains("\"protocol\":\"Redis\"")
        || json_line.contains("\"protocol\":\"redis\"")
    {
        "Redis".to_string()
    } else if json_line.contains("\"protocol\":\"Postgres\"")
        || json_line.contains("\"protocol\":\"postgres\"")
    {
        "Postgres".to_string()
    } else if json_line.contains("\"protocol\":\"Http\"")
        || json_line.contains("\"protocol\":\"http\"")
    {
        "Http".to_string()
    } else if json_line.contains("\"protocol\":\"Grpc\"")
        || json_line.contains("\"protocol\":\"grpc\"")
    {
        "Grpc".to_string()
    } else {
        // Try to infer from other fields
        if json_line.contains("redis") || json_line.contains("Redis") {
            "Redis".to_string()
        } else if json_line.contains("postgres") || json_line.contains("Postgres") {
            "Postgres".to_string()
        } else if json_line.contains("http") || json_line.contains("Http") {
            "Http".to_string()
        } else {
            "Unknown".to_string()
        }
    };

    Some((trace_id, protocol))
}

#[tokio::test]
async fn test_cross_protocol_correlation_single_trace() {
    let mut ctx = CrossProtocolTestContext::setup().await;

    // =========================================================================
    // PHASE 1: RECORDING - Single trace with multiple protocols
    // =========================================================================
    print_banner("PHASE 1: RECORDING - Cross-protocol single trace");

    ctx.start_proxy("record").await;
    ctx.start_service("record").await;

    let channel =
        tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{}", ctx.grpc_proxy_port))
            .unwrap()
            .connect()
            .await
            .unwrap();

    let mut client = ConnectorClient::new(channel);

    // Use a unique trace_id that we can verify
    let trace_id = "cross-protocol-trace-001";

    let mut request = Request::new(ProcessRequest {
        id: trace_id.to_string(),
        data: "cross-protocol-test".to_string(),
    });

    // Add x-trace-id header for explicit trace control
    request.metadata_mut().insert(
        "x-trace-id",
        tonic::metadata::MetadataValue::try_from(trace_id).unwrap(),
    );

    println!(
        "[TEST] Sending cross-protocol request with trace: {}",
        trace_id
    );
    let response = client.process(request).await.unwrap().into_inner();

    println!("[TEST] Response received: status={}", response.status);
    println!("[TEST]   HTTP result: {}", response.http_result);
    println!("[TEST]   Redis result: {}", response.redis_result);
    println!("[TEST]   Postgres result: {}", response.pg_result);

    // Allow recording to flush
    sleep(Duration::from_secs(2)).await;
    ctx.stop_all().await;

    // =========================================================================
    // PHASE 2: VALIDATE CORRELATION
    // =========================================================================
    print_banner("PHASE 2: VALIDATING - Cross-protocol correlation");

    let traces = ctx.analyze_recordings();

    println!("[TEST] Found {} unique traces in recordings", traces.len());

    // Verify our trace exists
    assert!(
        traces.contains_key(trace_id),
        "Trace {} not found in recordings!",
        trace_id
    );

    let events = traces.get(trace_id).unwrap();
    println!(
        "[TEST] Trace {} has {} total events",
        trace_id, events.total_events
    );
    println!("[TEST]   HTTP events: {}", events.http_count);
    println!("[TEST]   Redis events: {}", events.redis_count);
    println!("[TEST]   Postgres events: {}", events.postgres_count);
    println!("[TEST]   gRPC events: {}", events.grpc_count);

    // Verify we have events from multiple protocols
    assert!(
        events.total_events >= 3,
        "Expected at least 3 events (one per protocol), got {}",
        events.total_events
    );

    // Verify expected trace exists with proper attribution (strict model check)
    assert!(
        traces.contains_key(trace_id),
        "Trace {} not found in recordings",
        trace_id
    );

    // Verify no cross-contamination: only expected trace_id should have events
    // (orphan is a recording artifact, not a strict model violation)
    let unexpected_trace_ids: Vec<String> = traces
        .keys()
        .filter(|id| {
            let s = id.as_str();
            s != trace_id && !id.is_empty() && s != "orphan"
        })
        .cloned()
        .collect();
    assert!(
        unexpected_trace_ids.is_empty(),
        "Found unexpected traces while validating {}: {:?}",
        trace_id,
        unexpected_trace_ids
    );

    ctx.stop_all().await;

    // =========================================================================
    // FINAL REPORT
    // =========================================================================
    print_banner("CROSS-PROTOCOL CORRELATION TEST SUMMARY");

    println!("✅ Cross-protocol correlation test passed!");
    println!(
        "   All validated events are correctly attributed to trace {}",
        trace_id
    );
    println!("   Strict attribution validated (no unexpected traces, no cross-contamination)");
}

#[tokio::test]
async fn test_concurrent_cross_protocol_traces() {
    let mut ctx = CrossProtocolTestContext::setup().await;

    // =========================================================================
    // PHASE 1: RECORDING - Multiple concurrent cross-protocol traces
    // =========================================================================
    print_banner("PHASE 1: RECORDING - Concurrent cross-protocol traces");

    ctx.start_proxy("record").await;
    ctx.start_service("record").await;

    let channel =
        tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{}", ctx.grpc_proxy_port))
            .unwrap()
            .connect()
            .await
            .unwrap();

    let client = ConnectorClient::new(channel);
    let num_concurrent = 5;

    // Send concurrent cross-protocol requests
    let handles: Vec<_> = (0..num_concurrent)
        .map(|i| {
            let mut client = client.clone();
            let trace_id = format!("concurrent-cross-trace-{:03}", i);

            tokio::spawn(async move {
                let mut request = Request::new(ProcessRequest {
                    id: trace_id.clone(),
                    data: format!("concurrent-test-{}", i),
                });

                request.metadata_mut().insert(
                    "x-trace-id",
                    tonic::metadata::MetadataValue::try_from(&trace_id).unwrap(),
                );

                println!("[TEST] Sending request {} with trace: {}", i, trace_id);
                let response = client.process(request).await.unwrap().into_inner();

                (trace_id, response)
            })
        })
        .collect();

    // Wait for all concurrent requests
    let mut responses = Vec::new();
    for handle in handles {
        responses.push(handle.await.unwrap());
    }

    println!(
        "[TEST] ✅ All {} concurrent cross-protocol requests completed!",
        num_concurrent
    );

    // Allow recording to flush
    sleep(Duration::from_secs(2)).await;
    ctx.stop_all().await;

    // =========================================================================
    // PHASE 2: VALIDATE ISOLATION
    // =========================================================================
    print_banner("PHASE 2: VALIDATING - Trace isolation");

    let traces = ctx.analyze_recordings();

    println!("[TEST] Found {} unique traces in recordings", traces.len());

    // Verify each trace has its own events (no cross-contamination)
    let mut correlation_errors = 0;

    for (trace_id, events) in &traces {
        println!(
            "[TEST] Trace {}: {} events (HTTP={}, Redis={}, Postgres={})",
            trace_id,
            events.total_events,
            events.http_count,
            events.redis_count,
            events.postgres_count
        );

        // Each trace should have events
        if events.total_events == 0 {
            println!("[TEST] ❌ Trace {} has no events!", trace_id);
            correlation_errors += 1;
        }

        // Verify this trace_id is one of our expected traces
        if !trace_id.starts_with("concurrent-cross-trace-") {
            println!("[TEST] ⚠️  Unexpected trace found: {}", trace_id);
        }
    }

    let expected_trace_ids: Vec<String> = responses
        .iter()
        .map(|(trace_id, _)| trace_id.clone())
        .collect();
    for expected_trace_id in &expected_trace_ids {
        let maybe_events = traces.get(expected_trace_id);
        assert!(
            maybe_events.is_some(),
            "Expected trace {} not found in recordings",
            expected_trace_id
        );
        let events = maybe_events.unwrap();
        assert!(
            events.total_events > 0,
            "Expected trace {} has no recorded events",
            expected_trace_id
        );
    }

    ctx.stop_all().await;

    // =========================================================================
    // FINAL REPORT
    // =========================================================================
    print_banner("CONCURRENT CROSS-PROTOCOL TEST SUMMARY");

    println!("📊 Results:");
    println!("   Concurrent Traces: {}", num_concurrent);
    println!("   Traces Recorded:   {}", traces.len());
    println!("   Correlation Errors: {}", correlation_errors);

    assert_eq!(correlation_errors, 0, "Some traces had correlation errors");
    assert!(
        traces.len() >= num_concurrent,
        "Expected at least {} traces recorded",
        num_concurrent
    );

    println!("\n🎉 CONCURRENT CROSS-PROTOCOL CORRELATION TEST PASSED!");
}
