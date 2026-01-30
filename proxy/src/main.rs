mod config;
mod connection;
mod control_api;
mod correlation;
mod forward_proxy;

use clap::Parser;
use deja_core::protocols::grpc::GrpcParser;
use deja_core::protocols::http::HttpParser;
use deja_core::protocols::postgres::PostgresParser;
use deja_core::protocols::redis::RedisParser;
use deja_core::protocols::ProtocolParser;
use deja_core::tls_mitm::TlsMitmManager;

use deja_core::recording::Recorder;
use deja_core::replay::{ReplayEngine, ReplayMode};
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing with EnvFilter to support RUST_LOG
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_thread_ids(true)
        .with_target(false)
        .init();

    // Install default crypto provider for rustls
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();

    let args = config::Args::parse();

    // Support env vars as fallback
    let mode_str = if args.mode != "record" {
        args.mode.clone()
    } else {
        std::env::var("DEJA_MODE").unwrap_or_else(|_| args.mode.clone())
    };

    let record_dir =
        std::env::var("DEJA_RECORDING_PATH").unwrap_or_else(|_| args.record_dir.clone());

    let mut maps = args.maps.clone();
    if maps.is_empty() {
        // Try env var
        if let Ok(env_maps) = std::env::var("DEJA_PORT_MAPS") {
            maps = env_maps.split(',').map(String::from).collect();
        }
    }

    // Get forward proxy port from args or env
    let forward_proxy_port = args.forward_proxy_port.or_else(|| {
        std::env::var("DEJA_FORWARD_PROXY_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
    });

    // At least one of maps or forward_proxy_port must be provided
    if maps.is_empty() && forward_proxy_port.is_none() {
        tracing::error!("No mappings or forward proxy port provided. Use --map PORT:TARGET_HOST:TARGET_PORT, --forward-proxy-port PORT, or env vars");
        std::process::exit(1);
    }

    let mode = ReplayMode::from_str(&mode_str);
    tracing::info!("Starting Deja Proxy in {:?} mode", mode);
    tracing::info!("Recording Directory: {}", record_dir);

    // Initialize Shared State based on mode
    let recorder = match mode {
        ReplayMode::Recording => Some(Arc::new(Recorder::new(&record_dir).await)),
        _ => None,
    };

    let replay_engine = match mode {
        ReplayMode::FullMock | ReplayMode::Orchestrated => {
            tracing::info!("Loading replay engine from {}", record_dir);
            let engine = ReplayEngine::new(&record_dir).await?;
            tracing::info!(
                "Loaded {} recordings, {} remaining",
                engine.total_count(),
                engine.remaining_count()
            );
            Some(Arc::new(tokio::sync::Mutex::new(engine)))
        }
        _ => None,
    };

    // Initialize trace correlator for connection tracking
    let correlator = Arc::new(correlation::TraceCorrelator::new());
    tracing::info!("[Correlation] Initialized trace correlator");

    // Start Control API for SDK communication
    let control_port = std::env::var("DEJA_CONTROL_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(args.control_port);

    let control_state = Arc::new(control_api::ControlApiState {
        recorder: recorder.clone(),
        replay_engine: replay_engine.clone(),
        correlator: correlator.clone(),
    });

    tokio::spawn(async move {
        if let Err(e) = control_api::start_control_api(control_port, control_state).await {
            tracing::error!("[ControlAPI] Error: {}", e);
        }
    });

    // Initialize TLS MITM if CA cert/key provided
    let tls_manager = {
        let ca_cert_path = args
            .ca_cert
            .clone()
            .or_else(|| std::env::var("DEJA_CA_CERT").ok());
        let ca_key_path = args
            .ca_key
            .clone()
            .or_else(|| std::env::var("DEJA_CA_KEY").ok());

        match (ca_cert_path, ca_key_path) {
            (Some(cert_path), Some(key_path)) => {
                match (
                    std::fs::read_to_string(&cert_path),
                    std::fs::read_to_string(&key_path),
                ) {
                    (Ok(cert_pem), Ok(key_pem)) => match TlsMitmManager::new(&cert_pem, &key_pem) {
                        Ok(manager) => {
                            tracing::info!(
                                "[TLS] MITM manager initialized with CA from {}",
                                cert_path
                            );
                            Some(Arc::new(manager))
                        }
                        Err(e) => {
                            tracing::error!("[TLS] Failed to initialize MITM manager: {}", e);
                            None
                        }
                    },
                    (Err(e), _) => {
                        tracing::error!("[TLS] Failed to read CA cert: {}", e);
                        None
                    }
                    (_, Err(e)) => {
                        tracing::error!("[TLS] Failed to read CA key: {}", e);
                        None
                    }
                }
            }
            _ => {
                tracing::info!("[TLS] No CA cert/key provided, TLS MITM disabled");
                None
            }
        }
    };

    let parsers: Vec<Arc<dyn ProtocolParser>> = vec![
        Arc::new(GrpcParser), // Check for gRPC (HTTP/2) first
        Arc::new(HttpParser),
        Arc::new(PostgresParser),
        Arc::new(RedisParser),
    ];
    let parsers_arc = Arc::new(parsers);

    // Spawn listeners
    let mut tasks = Vec::new();

    // Start forward proxy if configured
    if let Some(fwd_port) = forward_proxy_port {
        if let Some(tls_mgr) = &tls_manager {
            let tls_mgr_clone = tls_mgr.clone();
            let recorder_clone = recorder.clone();
            let replay_engine_clone = replay_engine.clone();
            let correlator_clone = correlator.clone();

            tracing::info!(
                "[ForwardProxy] Starting forward proxy on port {} (HTTP CONNECT tunneling)",
                fwd_port
            );

            tasks.push(tokio::spawn(async move {
                if let Err(e) = forward_proxy::start_forward_proxy(
                    fwd_port,
                    tls_mgr_clone,
                    recorder_clone,
                    replay_engine_clone,
                    correlator_clone,
                )
                .await
                {
                    tracing::error!("[ForwardProxy] Error: {}", e);
                }
            }));
        } else {
            tracing::error!(
                "[ForwardProxy] Forward proxy requires TLS MITM (--ca-cert and --ca-key). Skipping."
            );
        }
    }

    for map in maps {
        let parts: Vec<&str> = map.split(':').collect();
        if parts.len() != 3 {
            tracing::error!(
                "Invalid map format '{}'. Expected PORT:TARGET_HOST:TARGET_PORT",
                map
            );
            continue;
        }

        let listen_port = parts[0].to_string();
        let target_host = parts[1].to_string(); // Own it for 'static lifetime
        let target_port = parts[2];

        let listen_addr = format!("0.0.0.0:{}", listen_port);
        let target_addr = format!("{}:{}", target_host, target_port);

        tracing::info!("Setting up listener on {} -> {}", listen_addr, target_addr);

        let listener = TcpListener::bind(&listen_addr).await?;
        let recorder_clone = recorder.clone();
        let replay_engine_clone = replay_engine.clone();
        let parsers_clone = parsers_arc.clone();
        let tls_manager_clone = tls_manager.clone();
        let correlator_clone = correlator.clone();

        // Task for this listener
        tasks.push(tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((client_socket, addr)) => {
                        tracing::info!("[{}] Accepted connection from {}", listen_port, addr);
                        let target = target_addr.clone();
                        let rec = recorder_clone.clone();
                        let rep = replay_engine_clone.clone();
                        let ps = parsers_clone.clone();
                        let lp = listen_port.clone();
                        let tls_mgr = tls_manager_clone.clone();
                        let th = target_host.clone();
                        let corr = correlator_clone.clone();

                        // Spawn connection handler
                        tokio::spawn(async move {
                            // Check for TLS handshake
                            if let Some(tls_manager) = &tls_mgr {
                                if connection::is_tls_handshake(&client_socket).await {
                                    tracing::info!(
                                        "[{}] TLS handshake detected, initiating MITM",
                                        lp
                                    );
                                    if let Err(e) = connection::handle_tls_connection(
                                        client_socket,
                                        target,
                                        &th,
                                        tls_manager.clone(),
                                        ps,
                                        rec,
                                        rep,
                                        corr,
                                    )
                                    .await
                                    {
                                        tracing::error!("[{}] TLS connection error: {}", lp, e);
                                    }
                                    return;
                                }
                            }

                            // Plain connection
                            let detected_parser =
                                connection::detect_protocol(&client_socket, &ps).await;
                            if let Err(e) = connection::handle_connection(
                                client_socket,
                                target,
                                detected_parser,
                                rec,
                                rep,
                                corr,
                            )
                            .await
                            {
                                tracing::error!("[{}] Connection error: {}", lp, e);
                            }
                        });
                    }
                    Err(e) => tracing::error!("[{}] Accept error: {}", listen_port, e),
                }
            }
        }));
    }

    tracing::info!("Deja Proxy Running with {} listeners...", tasks.len());
    futures::future::join_all(tasks).await;

    Ok(())
}
