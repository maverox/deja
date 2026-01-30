use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;
use tonic::Request;

// Import the generated gRPC client
pub mod connector {
    tonic::include_proto!("connector");
}
use connector::connector_client::ConnectorClient;
use connector::{ProcessRequest, ProcessResponse};

fn get_available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to find a port");
    listener
        .local_addr()
        .expect("Failed to get local address")
        .port()
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

struct E2EContext {
    proxy_proc: Option<Child>,
    service_proc: Option<Child>,

    grpc_proxy_port: u16,
    grpc_service_port: u16,
    http_proxy_port: u16,
    echo_port: u16,
    control_port: u16,

    recordings_dir: tempfile::TempDir,
}

impl E2EContext {
    async fn setup() -> Self {
        let recordings_dir = tempfile::tempdir().unwrap();
        let grpc_proxy_port = get_available_port();
        let grpc_service_port = get_available_port();
        let http_proxy_port = get_available_port();
        let echo_port = get_available_port();
        let control_port = get_available_port();

        Self {
            proxy_proc: None,
            service_proc: None,
            grpc_proxy_port,
            grpc_service_port,
            http_proxy_port,
            echo_port,
            control_port,
            recordings_dir,
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
        let recordings_path = self.recordings_dir.path().to_str().unwrap();

        let map_grpc = format!(
            "{}:127.0.0.1:{}",
            self.grpc_proxy_port, self.grpc_service_port
        );
        let map_http = format!("{}:127.0.0.1:{}", self.http_proxy_port, self.echo_port);

        println!(
            "Starting proxy in {} mode on port {}...",
            mode, self.grpc_proxy_port
        );
        let mut child = Command::new("../target/debug/deja-proxy")
            .args([
                "--mode",
                mode,
                "--map",
                &map_grpc,
                "--map",
                &map_http,
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

    async fn start_service(&mut self, env_vars: Vec<(&str, &str)>) {
        println!(
            "Starting test-service on port {} with env {:?}...",
            self.grpc_service_port, env_vars
        );
        let mut cmd = Command::new("../target/debug/test-service");

        cmd.env(
            "GRPC_LISTEN_ADDR",
            format!("127.0.0.1:{}", self.grpc_service_port),
        )
        .env(
            "DATABASE_URL",
            "postgres://deja_user:password@localhost:5432/deja_test",
        )
        .env("REDIS_URL", "redis://localhost:6379")
        .env("DEJA_CONTROL_HOST", "127.0.0.1")
        .env("DEJA_CONTROL_PORT", self.control_port.to_string())
        .env(
            "DEJA_PROXY_URL",
            format!("http://127.0.0.1:{}", self.control_port),
        )
        .env("HTTP_ECHO_ADDR", format!("127.0.0.1:{}", self.echo_port))
        .env("DEJA_HTTP_PROXY_PORT", self.http_proxy_port.to_string());

        for (k, v) in env_vars {
            cmd.env(k, v);
        }

        let mut child = cmd
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
    }

    fn print_recordings(&self) {
        let recordings_path = self.recordings_dir.path();
        println!("\n--- RECORDINGS DISCOVERY ---");
        let sessions_dir = recordings_path.join("sessions");
        if let Ok(entries) = std::fs::read_dir(sessions_dir) {
            for entry in entries.flatten() {
                if entry.file_type().unwrap().is_dir() {
                    let session_path = entry.path();
                    let json_file = session_path.join("events.jsonl");
                    if json_file.exists() {
                        println!("Found Session: {}", entry.file_name().to_string_lossy());
                        println!("Events (JSON):");
                        if let Ok(content) = std::fs::read_to_string(json_file) {
                            for line in content.lines() {
                                println!("  {}", line);
                            }
                        }
                    }
                }
            }
        }
        println!("----------------------------\n");
    }
}

fn diff_responses(v1: &ProcessResponse, v2: &ProcessResponse) {
    use similar::{ChangeTag, TextDiff};

    println!("\n--- BEHAVIORAL DIFF REPORT ---");
    let mut diffs = 0;

    // Status Diff
    if v1.status != v2.status {
        println!("❌ FIELD MISMATCH [status]:");
        let diff = TextDiff::from_lines(&v1.status, &v2.status);
        for change in diff.iter_all_changes() {
            let sign = match change.tag() {
                ChangeTag::Delete => "-",
                ChangeTag::Insert => "+",
                ChangeTag::Equal => " ",
            };
            print!("{} {}", sign, change);
        }
        diffs += 1;
    } else {
        println!("✅ FIELD MATCH [status]: {}", v1.status);
    }

    // Postgres Result Diff
    if v1.pg_result != v2.pg_result {
        println!("❌ FIELD MISMATCH [pg_result]:");
        let diff = TextDiff::from_lines(&v1.pg_result, &v2.pg_result);
        for change in diff.iter_all_changes() {
            let sign = match change.tag() {
                ChangeTag::Delete => "-",
                ChangeTag::Insert => "+",
                ChangeTag::Equal => " ",
            };
            print!("{} {}", sign, change);
        }
        diffs += 1;
    } else {
        println!("✅ FIELD MATCH [pg_result] (Deterministic)");
    }

    // Redis Result Diff
    if v1.redis_result != v2.redis_result {
        println!("❌ FIELD MISMATCH [redis_result]:");
        let diff = TextDiff::from_lines(&v1.redis_result, &v2.redis_result);
        for change in diff.iter_all_changes() {
            let sign = match change.tag() {
                ChangeTag::Delete => "-",
                ChangeTag::Insert => "+",
                ChangeTag::Equal => " ",
            };
            print!("{} {}", sign, change);
        }
        diffs += 1;
    } else {
        println!("✅ FIELD MATCH [redis_result] (Deterministic)");
    }

    // HTTP Result Diff
    if v1.http_result != v2.http_result {
        println!("❌ FIELD MISMATCH [http_result]:");
        let diff = TextDiff::from_lines(&v1.http_result, &v2.http_result);
        for change in diff.iter_all_changes() {
            let sign = match change.tag() {
                ChangeTag::Delete => "-",
                ChangeTag::Insert => "+",
                ChangeTag::Equal => " ",
            };
            print!("{} {}", sign, change);
        }
        diffs += 1;
    } else {
        println!("✅ FIELD MATCH [http_result] (Deterministic)");
    }

    if diffs > 0 {
        println!("🚨 {} REGRESSIONS DETECTED", diffs);
    } else {
        println!("✨ NO REGRESSIONS DETECTED - PERFECT FIDELITY");
    }
    println!("------------------------------\n");
}

#[tokio::test]
async fn test_service_e2e_record_replay_grpc_fidelity() {
    let mut ctx = E2EContext::setup().await;

    print_banner("PHASE 1: RECORDING");
    ctx.start_proxy("record").await;
    ctx.start_service(vec![("SERVICE_VARIANT", "A"), ("DEJA_MODE", "record")])
        .await;

    let channel =
        tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{}", ctx.grpc_proxy_port))
            .unwrap()
            .connect()
            .await
            .unwrap();

    let mut client = ConnectorClient::new(channel);
    let request = Request::new(ProcessRequest {
        id: "e2e-grpc-1".into(),
        data: "advanced fidelity test".into(),
    });

    println!("[TEST-SCRIPT] Sending record request...");
    let response = client.process(request).await.unwrap().into_inner();
    println!("[TEST-SCRIPT] Record Response: {:?}", response);

    // Give background tasks time to record
    sleep(Duration::from_millis(500)).await;

    ctx.stop_all().await;
    sleep(Duration::from_secs(1)).await;

    ctx.print_recordings();

    // Verify background task was recorded (Epic 5)
    let sessions_dir = ctx.recordings_dir.path().join("sessions");
    let mut found_task_spawn = false;
    if let Ok(entries) = std::fs::read_dir(sessions_dir) {
        for entry in entries.flatten() {
            if entry.file_type().unwrap().is_dir() {
                let events_path = entry.path().join("events.jsonl");
                if events_path.exists() {
                    let content = std::fs::read_to_string(events_path).unwrap();
                    if content.contains("TaskSpawnCapture") && content.contains("bg_uuid_gen") {
                        found_task_spawn = true;
                        break;
                    }
                }
            }
        }
    }
    assert!(
        found_task_spawn,
        "Background task 'bg_uuid_gen' was not found in recordings!"
    );
    println!("[TEST-SCRIPT] Verified background task recording.");

    print_banner("PHASE 2: REPLAY");
    ctx.start_proxy("replay").await;
    ctx.start_service(vec![("SERVICE_VARIANT", "A"), ("DEJA_MODE", "replay")])
        .await;

    let channel =
        tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{}", ctx.grpc_proxy_port))
            .unwrap()
            .connect()
            .await
            .unwrap();

    let mut client = ConnectorClient::new(channel);
    let request = Request::new(ProcessRequest {
        id: "e2e-grpc-1".into(),
        data: "advanced fidelity test".into(),
    });

    println!("[TEST-SCRIPT] Sending replay request...");
    let replay_response = client.process(request).await.unwrap().into_inner();
    println!("[TEST-SCRIPT] Replay Response: {:?}", replay_response);

    // Verify deterministic fields (including instrumented non-deterministic values!)
    assert_eq!(response.status, replay_response.status);
    assert_eq!(response.pg_result, replay_response.pg_result);
    assert_eq!(response.redis_result, replay_response.redis_result);
    // Also verify runtime captured values
    assert_eq!(response.generated_uuid, replay_response.generated_uuid);
    assert_eq!(response.timestamp_ms, replay_response.timestamp_ms);

    println!("SUCCESS: Non-deterministic events replayed identically.");
    println!("  Time: {}", response.pg_result);
    println!("  UUID: {}", response.redis_result);

    ctx.stop_all().await;
}

#[tokio::test]
async fn test_service_regression_diff() {
    let mut ctx = E2EContext::setup().await;

    print_banner("PHASE 1: RECORDING (V1 - Variant A)");
    ctx.start_proxy("record").await;
    ctx.start_service(vec![("SERVICE_VARIANT", "A")]).await;

    let channel =
        tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{}", ctx.grpc_proxy_port))
            .unwrap()
            .connect()
            .await
            .unwrap();

    let mut client = ConnectorClient::new(channel);
    let request = Request::new(ProcessRequest {
        id: "diff-test".into(),
        data: "regression test".into(),
    });

    println!("[TEST-SCRIPT] Sending record (V1) request...");
    let response_v1 = client.process(request).await.unwrap().into_inner();
    println!("[TEST-SCRIPT] V1 Response: {:?}", response_v1);
    assert_eq!(response_v1.status, "Success");

    ctx.stop_all().await;
    sleep(Duration::from_secs(1)).await;

    ctx.print_recordings();

    print_banner("PHASE 2: REPLAY (V2 - Variant B)");
    ctx.start_proxy("replay").await;
    ctx.start_service(vec![("SERVICE_VARIANT", "B")]).await;

    let channel = tonic::transport::Channel::from_shared(format!(
        "http://127.0.0.1:{}",
        ctx.grpc_service_port
    ))
    .unwrap()
    .connect()
    .await
    .unwrap();

    let mut client = ConnectorClient::new(channel);
    let request = Request::new(ProcessRequest {
        id: "diff-test".into(),
        data: "regression test".into(),
    });

    println!("[TEST-SCRIPT] Sending replay (against V2) request...");
    let response_v2 = client.process(request).await.unwrap().into_inner();
    println!("[TEST-SCRIPT] V2 Response (Replayed): {:?}", response_v2);

    // Print behavioral diff
    diff_responses(&response_v1, &response_v2);

    // Verify Diff Detection
    assert_ne!(response_v1.status, response_v2.status);
    assert_eq!(response_v2.status, "Success (v2)");

    // Side effects should MATCH
    assert_eq!(response_v1.pg_result, response_v2.pg_result);
    assert_eq!(response_v1.redis_result, response_v2.redis_result);
    assert_eq!(response_v1.http_result, response_v2.http_result);

    ctx.stop_all().await;
}

#[tokio::test]
async fn test_service_concurrent_load() {
    let mut ctx = E2EContext::setup().await;

    // --- PHASE 1: RECORDING ---
    print_banner("PHASE 1: RECORDING (V1 - Variant A)");
    ctx.start_proxy("record").await;
    ctx.start_service(vec![("SERVICE_VARIANT", "A")]).await;

    let channel =
        tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{}", ctx.grpc_proxy_port))
            .unwrap()
            .connect()
            .await
            .unwrap();

    let client = ConnectorClient::new(channel);

    // Spawn 5 concurrent requests with UNIQUE IDs
    let handles: Vec<_> = (0..5)
        .map(|i| {
            let mut client = client.clone();
            tokio::spawn(async move {
                let id = format!("concurrent-test-{}", i);
                let request = Request::new(ProcessRequest {
                    id: id.clone(),
                    data: format!("load test data {}", i),
                });
                let response = client.process(request).await.unwrap().into_inner();
                // Basic checks
                assert_eq!(response.trace_id, id); // We passed ID, so it should be used as trace_id
                assert_eq!(response.status, "Success");
                response
            })
        })
        .collect();

    println!("[TEST-SCRIPT] Sending 5 concurrent requests...");
    let mut recorded_responses = Vec::new();
    for handle in handles {
        recorded_responses.push(handle.await.unwrap());
    }
    println!("[TEST-SCRIPT] All 5 recording requests completed.");

    // Allow time for async control messages (NonDeterministic capture) to flush
    sleep(Duration::from_secs(2)).await;

    ctx.stop_all().await;
    sleep(Duration::from_secs(1)).await;

    ctx.print_recordings();

    // Iterate over sessions and ensure EACH trace has captured UUID and Time
    let sessions_dir = ctx.recordings_dir.path().join("sessions");
    let mut captured_traces = std::collections::HashSet::new();

    // Debug: List directory contents
    if let Ok(entries) = std::fs::read_dir(&sessions_dir) {
        println!("[TEST-SCRIPT] Scanning sessions dir: {:?}", sessions_dir);
        for entry in entries.flatten() {
            println!("[TEST-SCRIPT] Entry: {:?}", entry.path());
            if entry.file_type().unwrap().is_dir() {
                let events_path = entry.path().join("events.jsonl");
                if events_path.exists() {
                    let content = std::fs::read_to_string(&events_path).unwrap();
                    println!(
                        "[TEST-SCRIPT] Content of {}:\n{}",
                        events_path.display(),
                        content
                    );

                    // Allow for partial flushes by checking strict line containment
                    for line in content.lines() {
                        for i in 0..5 {
                            let trace_id = format!("concurrent-test-{}", i);
                            if line.contains(&trace_id) {
                                if line.contains("UuidCapture") {
                                    println!("[TEST-SCRIPT] Found UUID for {}", trace_id);
                                    captured_traces.insert(format!("{}:uuid", trace_id));
                                }
                                if line.contains("TimeCaptureNs") {
                                    println!("[TEST-SCRIPT] Found Time for {}", trace_id);
                                    captured_traces.insert(format!("{}:time", trace_id));
                                }
                            }
                        }
                    }
                } else {
                    println!("[TEST-SCRIPT] No events.jsonl in {:?}", entry.path());
                }
            }
        }
    } else {
        println!(
            "[TEST-SCRIPT] Failed to read sessions dir: {:?}",
            sessions_dir
        );
    }

    println!(
        "[TEST-SCRIPT] Verified NonDeterministic captures: {:?}",
        captured_traces
    );
    for i in 0..5 {
        let trace_id = format!("concurrent-test-{}", i);
        assert!(
            captured_traces.contains(&format!("{}:uuid", trace_id)),
            "Missing UUID capture for {}",
            trace_id
        );
        assert!(
            captured_traces.contains(&format!("{}:time", trace_id)),
            "Missing Time capture for {}",
            trace_id
        );
    }

    // --- PHASE 2: REPLAY ---
    print_banner("PHASE 2: REPLAY (Concurrent)");
    ctx.start_proxy("replay").await;
    ctx.start_service(vec![("SERVICE_VARIANT", "A"), ("DEJA_MODE", "replay")])
        .await;

    let replay_channel =
        tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{}", ctx.grpc_proxy_port))
            .unwrap()
            .connect()
            .await
            .unwrap();
    let replay_client = ConnectorClient::new(replay_channel);

    // Replay the SAME concurrent pattern
    // Replay Engine should match each request by its content (ID)
    let handles: Vec<_> = (0..5)
        .map(|i| {
            let mut client = replay_client.clone();
            let original_resp = recorded_responses[i].clone();
            tokio::spawn(async move {
                let id = format!("concurrent-test-{}", i);
                let request = Request::new(ProcessRequest {
                    id: id.clone(),
                    data: format!("load test data {}", i),
                });
                let response = client.process(request).await.unwrap().into_inner();

                // Compare with original
                assert_eq!(response.trace_id, original_resp.trace_id);
                assert_eq!(response.status, original_resp.status);
                assert_eq!(response.generated_uuid, original_resp.generated_uuid); // Determinism
                assert_eq!(response.timestamp_ms, original_resp.timestamp_ms); // Determinism

                // Verify DB results are identical
                assert_eq!(response.pg_result, original_resp.pg_result);
                assert_eq!(response.redis_result, original_resp.redis_result);

                println!("[TEST-SCRIPT] Verified replay for {}", id);
            })
        })
        .collect();

    println!("[TEST-SCRIPT] Sending 5 concurrent replay requests...");
    for handle in handles {
        handle.await.unwrap();
    }
    println!("[TEST-SCRIPT] All 5 replay requests verified.");

    ctx.stop_all().await;
}
