//! Full E2E Record/Replay Test
//!
//! Covers:
//! - Multiple mappings (Postgres, Generic TCP)
//! - Generic TCP Fallback
//! - Redis Protocol
//! - SDK Integration(UUID, etc.)
//! - Record -> Replay transition
//! - Mock lifecycle management
//!
//! Covers:
//! - Multiple mappings (Postgres, Generic TCP)
//! - Generic TCP Fallback
//! - Redis Protocol
//! - SDK Integration (UUID, etc.)
//! - Record -> Replay transition
//! - Mock lifecycle management

mod integration;

use integration::harness::{get_available_port, TestHarness};
use integration::mocks::MockServer;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

// Use path-dependency sdk
use deja_sdk::{DejaMode, DejaRuntime, Runtime};

async fn run_client_scenario(
    proxy_pg_port: u16,
    proxy_redis_port: u16,
    proxy_tcp_port: u16,
    sdk: &Runtime,
) -> (String, String, String, String) {
    let trace_id = deja_sdk::current_trace_id().expect("must be called within with_trace_id");
    let control = sdk.control_client();

    // Notify the proxy that this trace has started — without this,
    // the proxy cannot allocate connection scopes for the trace.
    control
        .send_best_effort(deja_sdk::ControlMessage::start_trace(trace_id.clone()))
        .await;
    sleep(Duration::from_millis(50)).await;

    // 1. Get UUID from SDK - use run() with user's own uuid generator
    let uuid: uuid::Uuid = deja_sdk::deja_run(sdk, "uuid", || uuid::Uuid::new_v4()).await;

    // 2. Generic TCP (simulating MySQL or whatever)
    // Write 0xDEADBEEF, expect reversed 0xEFBEADDE (by mock)
    // using binary data prevents accidental Redis detection ("HELLO")
    let tcp_resp_str;
    {
        // Retry connection logic
        let mut attempts = 0;
        let mut stream = loop {
            match TcpStream::connect(format!("127.0.0.1:{}", proxy_tcp_port)).await {
                Ok(s) => break s,
                Err(e) => {
                    attempts += 1;
                    if attempts > 5 {
                        panic!("Failed to connect to Generic TCP proxy port: {}", e);
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            }
        };

        // Associate BEFORE sending any data — the proxy must know the mapping
        // before it processes the first bytes, otherwise they become orphan events.
        let source_port = stream.local_addr().unwrap().port();
        control
            .send_best_effort(deja_sdk::ControlMessage::associate_by_source_port(
                trace_id.clone(),
                source_port,
                deja_common::Protocol::Unknown,
            ))
            .await;
        sleep(Duration::from_millis(50)).await;

        stream
            .write_all(b"\xDE\xAD\xBE\xEF")
            .await
            .expect("Failed to write to TCP port");

        let mut buf = [0u8; 1024];
        let n = stream
            .read(&mut buf)
            .await
            .expect("Failed to read from TCP port");

        // Hex encode bytes
        tcp_resp_str = buf[..n]
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<String>();
    }

    // 3. Postgres
    // Connect, expecting Auth+Ready
    let mut pg_resp_summary = String::new();
    {
        let mut attempts = 0;
        let mut stream = loop {
            match TcpStream::connect(format!("127.0.0.1:{}", proxy_pg_port)).await {
                Ok(s) => break s,
                Err(e) => {
                    attempts += 1;
                    if attempts > 5 {
                        panic!("Failed to connect to Postgres proxy port: {}", e);
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            }
        };

        // Associate BEFORE sending startup message
        let source_port = stream.local_addr().unwrap().port();
        control
            .send_best_effort(deja_sdk::ControlMessage::associate_by_source_port(
                trace_id.clone(),
                source_port,
                deja_common::Protocol::Postgres,
            ))
            .await;
        sleep(Duration::from_millis(50)).await;

        // Startup: len=8, proto=3.0 (0,3,0,0)
        let startup = [0, 0, 0, 8, 0, 3, 0, 0];
        stream
            .write_all(&startup)
            .await
            .expect("Failed to write PG startup");

        // Read Auth + Ready
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.expect("Failed to read PG auth");
        if n > 0 {
            pg_resp_summary.push_str("AuthReady");
        }

        // Send Query in proper PG wire format:
        // 'Q' (tag) + int32 length (includes self) + query string + null terminator
        // "SELECT 1\0" is 9 bytes, length = 4 + 9 = 13
        let mut query = Vec::new();
        query.push(b'Q');
        query.extend_from_slice(&13u32.to_be_bytes()); // length = 4 (self) + 8 ("SELECT 1") + 1 (\0)
        query.extend_from_slice(b"SELECT 1\0");
        stream
            .write_all(&query)
            .await
            .expect("Failed to write PG query");

        let n = stream
            .read(&mut buf)
            .await
            .expect("Failed to read PG query result");
        if n > 0 {
            pg_resp_summary.push_str("+QueryResult");
        }
    }

    // 4. Redis
    // Connect, send PING
    let redis_resp_str;
    {
        // Simple retry for Redis too
        let mut attempts = 0;
        let mut stream = loop {
            match TcpStream::connect(format!("127.0.0.1:{}", proxy_redis_port)).await {
                Ok(s) => break s,
                Err(e) => {
                    attempts += 1;
                    if attempts > 5 {
                        panic!("Failed to connect to Redis proxy port: {}", e);
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            }
        };

        // Associate BEFORE sending PING
        let source_port = stream.local_addr().unwrap().port();
        control
            .send_best_effort(deja_sdk::ControlMessage::associate_by_source_port(
                trace_id.clone(),
                source_port,
                deja_common::Protocol::Redis,
            ))
            .await;
        sleep(Duration::from_millis(50)).await;

        // Send PING in RESP array format: *1\r\n$4\r\nPING\r\n
        stream
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .expect("Failed to write Redis PING");

        let mut buf = [0u8; 1024];
        let n = stream
            .read(&mut buf)
            .await
            .expect("Failed to read Redis response");
        redis_resp_str = String::from_utf8_lossy(&buf[..n]).trim().to_string();
    }

    // Notify the proxy that this trace has ended
    control
        .send_best_effort(deja_sdk::ControlMessage::end_trace(trace_id))
        .await;

    (
        uuid.to_string(),
        pg_resp_summary,
        redis_resp_str,
        tcp_resp_str,
    )
}

#[tokio::test]
async fn test_full_e2e_flow() {
    let mut harness = TestHarness::new();

    // 1. Start Mocks
    let mock_pg = MockServer::start_postgres().await;
    let mock_redis = MockServer::start_redis().await;
    let mock_tcp = MockServer::start_generic_tcp().await;

    println!(
        "✅ Mocks started: PG={}, Redis={}, TCP={}",
        mock_pg.port, mock_redis.port, mock_tcp.port
    );

    // 2. Allocate Proxy Ports
    let proxy_pg_port = get_available_port();
    let proxy_redis_port = get_available_port();
    let proxy_tcp_port = get_available_port();

    // 3. Define Mappings
    let maps = vec![
        format!("{}:127.0.0.1:{}", proxy_pg_port, mock_pg.port),
        format!("{}:127.0.0.1:{}", proxy_redis_port, mock_redis.port),
        format!("{}:127.0.0.1:{}", proxy_tcp_port, mock_tcp.port),
    ];

    // 4. Start Proxy (Record)
    harness
        .start_proxy_record_with_maps(maps.clone())
        .await
        .expect("Failed to start proxy");
    println!("✅ Proxy started in RECORD mode");

    // 5. Initialize SDK (Record)
    let sdk = Runtime::new(harness.control_api_url(), DejaMode::Record);

    let trace_id = "test-trace-1";

    // Use to_string() for trace_id to fix E0308
    let results_record = deja_sdk::with_trace_id(trace_id.to_string(), async {
        run_client_scenario(proxy_pg_port, proxy_redis_port, proxy_tcp_port, &sdk).await
    })
    .await;

    // Flush captured deterministic values (UUIDs, etc.) to the proxy before shutdown
    sdk.flush().await;

    println!("📥 Record Results: {:?}", results_record);

    // assertions
    assert_eq!(
        results_record.1, "AuthReady+QueryResult",
        "PG mocked response mismatch"
    );
    assert_eq!(results_record.2, "+PONG", "Redis mocked response mismatch");
    // Expect hex string of reversed "DEADBEEF" -> "EFBEADDE"
    assert_eq!(
        results_record.3, "EFBEADDE",
        "TCP generic response mismatch"
    );

    // 6. Stop everything
    harness.stop_proxy();
    mock_pg.stop();
    mock_redis.stop();
    mock_tcp.stop();

    println!("🛑 Mocks and Proxy stopped. Starting Replay...");

    // 7. Start Proxy (Replay)
    harness
        .start_proxy_replay_with_maps(maps)
        .await
        .expect("Failed to start replay proxy");

    // 8. Initialize SDK (Replay)
    let sdk_replay = Runtime::new(harness.control_api_url(), DejaMode::Replay);

    // Use to_string() for trace_id to fix E0308
    let replay_task = tokio::spawn(deja_sdk::with_trace_id(trace_id.to_string(), async move {
        run_client_scenario(proxy_pg_port, proxy_redis_port, proxy_tcp_port, &sdk_replay).await
    }));

    match timeout(Duration::from_secs(10), replay_task).await {
        Ok(Ok(results_replay)) => {
            println!("♻️ Replay Results: {:?}", results_replay);

            assert_eq!(results_record.0, results_replay.0, "UUID mismatch");
            assert_eq!(results_record.1, results_replay.1, "PG Replay mismatch");
            assert_eq!(results_record.2, results_replay.2, "Redis Replay mismatch");
            assert_eq!(results_record.3, results_replay.3, "TCP Replay mismatch");
        }
        Ok(Err(e)) => {
            panic!(
                "Replay scenario failed ({}) — strict replay requires control-bind correlation (no orphan fallback)",
                e
            );
        }
        Err(_) => {
            panic!(
                "Replay timed out — strict replay requires control-bind correlation (no orphan fallback)"
            );
        }
    }

    println!("✅ Full E2E Flow Passed!");
}
