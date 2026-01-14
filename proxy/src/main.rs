mod control_api;

use clap::Parser;
use deja_core::protocols::http::HttpParser;
use deja_core::protocols::postgres::PostgresParser;
use deja_core::protocols::redis::RedisParser;
use deja_core::protocols::ProtocolParser;
use deja_core::tls_mitm::TlsMitmManager;

use deja_core::recording::Recorder;
use deja_core::replay::{ReplayEngine, ReplayMode};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port mappings in format PORT:TARGET_HOST:TARGET_PORT (e.g., 5433:127.0.0.1:5432)
    /// Can also use DEJA_PORT_MAPS env var (comma-separated)
    #[arg(long = "map")]
    maps: Vec<String>,

    /// Directory for recordings
    /// Can also use DEJA_RECORDING_PATH env var
    #[arg(long, default_value = "recordings")]
    record_dir: String,

    /// Mode: record, replay, or orchestrated
    /// Can also use DEJA_MODE env var
    #[arg(long, default_value = "record")]
    mode: String,

    /// Control API port for SDK communication
    /// Can also use DEJA_CONTROL_PORT env var
    #[arg(long, default_value = "9999")]
    control_port: u16,

    /// Path to CA certificate for TLS MITM (PEM format)
    /// Can also use DEJA_CA_CERT env var
    #[arg(long)]
    ca_cert: Option<String>,

    /// Path to CA private key for TLS MITM (PEM format)
    /// Can also use DEJA_CA_KEY env var
    #[arg(long)]
    ca_key: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    // Support env vars as fallback
    let mode_str = if args.mode != "record" {
        args.mode.clone()
    } else {
        std::env::var("DEJA_MODE").unwrap_or_else(|_| args.mode.clone())
    };

    let record_dir = std::env::var("DEJA_RECORDING_PATH").unwrap_or_else(|_| args.record_dir.clone());

    let mut maps = args.maps.clone();
    if maps.is_empty() {
        // Try env var
        if let Ok(env_maps) = std::env::var("DEJA_PORT_MAPS") {
            maps = env_maps.split(',').map(String::from).collect();
        }
    }

    if maps.is_empty() {
        eprintln!("Error: No mappings provided. Use --map PORT:TARGET_HOST:TARGET_PORT or DEJA_PORT_MAPS env var");
        std::process::exit(1);
    }

    let mode = ReplayMode::from_str(&mode_str);
    println!("Starting Deja Proxy in {:?} mode", mode);
    println!("Recording Directory: {}", record_dir);

    // Initialize Shared State based on mode
    let recorder = match mode {
        ReplayMode::Recording => Some(Arc::new(Recorder::new(&record_dir).await)),
        _ => None,
    };

    let replay_engine = match mode {
        ReplayMode::FullMock | ReplayMode::Orchestrated => {
            println!("Loading replay engine from {}", record_dir);
            let engine = ReplayEngine::new(&record_dir).await?;
            println!("Loaded {} recordings, {} remaining", engine.total_count(), engine.remaining_count());
            Some(Arc::new(tokio::sync::Mutex::new(engine)))
        }
        _ => None,
    };

    // Start Control API for SDK communication
    let control_port = std::env::var("DEJA_CONTROL_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(args.control_port);

    let control_state = Arc::new(control_api::ControlApiState {
        recorder: recorder.clone(),
        replay_engine: replay_engine.clone(),
    });

    tokio::spawn(async move {
        if let Err(e) = control_api::start_control_api(control_port, control_state).await {
            eprintln!("[ControlAPI] Error: {}", e);
        }
    });

    // Initialize TLS MITM if CA cert/key provided
    let tls_manager = {
        let ca_cert_path = args.ca_cert.clone()
            .or_else(|| std::env::var("DEJA_CA_CERT").ok());
        let ca_key_path = args.ca_key.clone()
            .or_else(|| std::env::var("DEJA_CA_KEY").ok());

        match (ca_cert_path, ca_key_path) {
            (Some(cert_path), Some(key_path)) => {
                match (std::fs::read_to_string(&cert_path), std::fs::read_to_string(&key_path)) {
                    (Ok(cert_pem), Ok(key_pem)) => {
                        match deja_core::tls_mitm::TlsMitmManager::new(&cert_pem, &key_pem) {
                            Ok(manager) => {
                                println!("[TLS] MITM manager initialized with CA from {}", cert_path);
                                Some(Arc::new(manager))
                            }
                            Err(e) => {
                                eprintln!("[TLS] Failed to initialize MITM manager: {}", e);
                                None
                            }
                        }
                    }
                    (Err(e), _) => {
                        eprintln!("[TLS] Failed to read CA cert: {}", e);
                        None
                    }
                    (_, Err(e)) => {
                        eprintln!("[TLS] Failed to read CA key: {}", e);
                        None
                    }
                }
            }
            _ => {
                println!("[TLS] No CA cert/key provided, TLS MITM disabled");
                None
            }
        }
    };

    let parsers: Vec<Arc<dyn ProtocolParser>> = vec![
        Arc::new(HttpParser),
        Arc::new(PostgresParser),
        Arc::new(RedisParser),
    ];
    let parsers_arc = Arc::new(parsers);

    // Spawn listeners
    let mut tasks = Vec::new();

    for map in maps {
        let parts: Vec<&str> = map.split(':').collect();
        if parts.len() != 3 {
            eprintln!(
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

        println!("Setting up listener on {} -> {}", listen_addr, target_addr);

        let listener = TcpListener::bind(&listen_addr).await?;
        let recorder_clone = recorder.clone();
        let replay_engine_clone = replay_engine.clone();
        let parsers_clone = parsers_arc.clone();
        let tls_manager_clone = tls_manager.clone();

        // Task for this listener
        tasks.push(tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((client_socket, addr)) => {
                        println!("[{}] Accepted connection from {}", listen_port, addr);
                        let target = target_addr.clone();
                        let rec = recorder_clone.clone();
                        let rep = replay_engine_clone.clone();
                        let ps = parsers_clone.clone();
                        let lp = listen_port.clone();
                        let tls_mgr = tls_manager_clone.clone();
                        let th = target_host.clone();

                        // Spawn connection handler
                        tokio::spawn(async move {
                            // Check for TLS handshake
                            if let Some(tls_manager) = &tls_mgr {
                                if is_tls_handshake(&client_socket).await {
                                    println!("[{}] TLS handshake detected, initiating MITM", lp);
                                    if let Err(e) = handle_tls_connection(
                                        client_socket,
                                        target,
                                        &th,
                                        tls_manager.clone(),
                                        ps,
                                        rec,
                                        rep,
                                    ).await {
                                        eprintln!("[{}] TLS connection error: {}", lp, e);
                                    }
                                    return;
                                }
                            }

                            // Plain connection
                            let detected_parser = detect_protocol(&client_socket, &ps).await;
                            if let Err(e) =
                                handle_connection(client_socket, target, detected_parser, rec, rep)
                                    .await
                            {
                                eprintln!("[{}] Connection error: {}", lp, e);
                            }
                        });
                    }
                    Err(e) => eprintln!("[{}] Accept error: {}", listen_port, e),
                }
            }
        }));
    }

    println!("Deja Proxy Running with {} listeners...", tasks.len());
    futures::future::join_all(tasks).await;

    Ok(())
}

/// Detect if the connection starts with a TLS handshake
/// TLS record format: 0x16 (handshake) 0x03 0x0X (version)
async fn is_tls_handshake(stream: &TcpStream) -> bool {
    let mut buf = [0u8; 3];
    match stream.peek(&mut buf).await {
        Ok(n) if n >= 3 => {
            // TLS handshake starts with: 0x16 (ContentType: Handshake) 0x03 0x0X (Version)
            buf[0] == 0x16 && buf[1] == 0x03 && buf[2] <= 0x03
        }
        _ => false,
    }
}

/// Handle a TLS connection with MITM interception
async fn handle_tls_connection(
    client_stream: TcpStream,
    target_addr: String,
    target_host: &str,
    tls_manager: Arc<TlsMitmManager>,
    parsers: Arc<Vec<Arc<dyn ProtocolParser>>>,
    recorder: Option<Arc<Recorder>>,
    replay_engine: Option<Arc<tokio::sync::Mutex<ReplayEngine>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Generate server config for this host (dynamically signed cert)
    let server_config = tls_manager.generate_server_config(target_host)?;
    let acceptor = TlsAcceptor::from(server_config);

    // Accept TLS from client
    let tls_client_stream = acceptor.accept(client_stream).await?;
    println!("[TLS] Accepted TLS connection for host: {}", target_host);

    // For protocol detection, we need to peek the decrypted data
    // Split the TLS stream for bidirectional handling
    let (mut c_read, c_write) = tokio::io::split(tls_client_stream);

    // In replay mode, we don't connect to the real target
    let target_stream = if replay_engine.is_none() {
        // Connect to target with TLS
        let target_tcp = TcpStream::connect(&target_addr).await?;
        let client_config = tls_manager.client_config();
        let connector = tokio_rustls::TlsConnector::from(client_config);
        let server_name = rustls::pki_types::ServerName::try_from(target_host.to_string())?;
        let tls_target = connector.connect(server_name, target_tcp).await?;
        Some(tls_target)
    } else {
        None
    };

    // Detect protocol from first decrypted bytes
    let mut peek_buf = [0u8; 1024];
    let peeked = {
        // We need to actually read since we can't peek TLS
        // Use a buffered approach - read and then prepend to further processing
        let n = c_read.read(&mut peek_buf).await?;
        if n == 0 {
            return Ok(());
        }
        n
    };

    let detected_parser = detect_protocol_from_bytes(&peek_buf[..peeked], &parsers);
    println!("[TLS] Detected protocol: {}", detected_parser.protocol_id());

    // Handle the connection with decrypted streams
    // Note: We already consumed some bytes, need to handle them
    let c_write = Arc::new(tokio::sync::Mutex::new(c_write));

    let (t_read, t_write) = if let Some(ts) = target_stream {
        let (tr, tw) = tokio::io::split(ts);
        (Some(tr), Some(Arc::new(tokio::sync::Mutex::new(tw))))
    } else {
        (None, None)
    };

    let connection_id = uuid::Uuid::new_v4().to_string();
    let connection_parser = detected_parser.new_connection(connection_id);
    let parser_lock = Arc::new(tokio::sync::Mutex::new(connection_parser));

    // Process the initial peeked data first
    {
        let mut p = parser_lock.lock().await;
        match p.parse_client_data(&peek_buf[..peeked]) {
            Ok(res) => {
                for event in res.events {
                    if let Some(rec) = &recorder {
                        let _ = rec.save_event(&event).await;
                    }

                    if let Some(engine_lock) = &replay_engine {
                        let mut engine = engine_lock.lock().await;
                        if let Some((_, responses)) = engine.find_match_with_responses(&event) {
                            let mut cw = c_write.lock().await;
                            for resp in responses {
                                cw.write_all(&resp).await?;
                            }
                        }
                    }
                }

                if let Some(tw) = &t_write {
                    if !res.forward.is_empty() {
                        let mut tw = tw.lock().await;
                        tw.write_all(&res.forward).await?;
                    }
                }

                if let Some(reply) = res.reply {
                    let mut cw = c_write.lock().await;
                    cw.write_all(&reply).await?;
                }
            }
            Err(_) => {
                if let Some(tw) = &t_write {
                    let mut tw = tw.lock().await;
                    tw.write_all(&peek_buf[..peeked]).await?;
                }
            }
        }
    }

    // Continue with bidirectional proxying using the generic handler
    let c_write_clone = c_write.clone();
    let t_write_clone = t_write.clone();
    let parser_lock_clone = parser_lock.clone();
    let recorder_clone = recorder.clone();
    let replay_engine_clone = replay_engine.clone();
    let parser_arc_clone = detected_parser.clone();

    // Client -> Target handler
    let c_handle = async move {
        let mut buf = [0u8; 8192];
        loop {
            let n = c_read.read(&mut buf).await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            if n == 0 {
                break;
            }

            let mut p = parser_lock_clone.lock().await;
            match p.parse_client_data(&buf[..n]) {
                Ok(res) => {
                    for event in res.events {
                        if let Some(rec) = &recorder_clone {
                            let _ = rec.save_event(&event).await;
                        }

                        if let Some(engine_lock) = &replay_engine_clone {
                            let mut engine = engine_lock.lock().await;
                            if let Some((_, responses)) = engine.find_match_with_responses(&event) {
                                let mut cw = c_write_clone.lock().await;
                                for resp in responses {
                                    cw.write_all(&resp).await.map_err(|e| {
                                        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                                    })?;
                                }
                            } else {
                                let protocol = parser_arc_clone.protocol_id();
                                let error_bytes = match protocol {
                                    "redis" => b"-ERR No replay match found\r\n".to_vec(),
                                    "http" => b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n".to_vec(),
                                    "postgres" => {
                                        deja_core::protocols::postgres::PgSerializer::serialize_error("No Replay Match").to_vec()
                                    }
                                    _ => vec![],
                                };
                                if !error_bytes.is_empty() {
                                    let mut cw = c_write_clone.lock().await;
                                    let _ = cw.write_all(&error_bytes).await;
                                }
                            }
                        }
                    }

                    if let Some(tw) = &t_write_clone {
                        if !res.forward.is_empty() {
                            let mut tw = tw.lock().await;
                            tw.write_all(&res.forward).await.map_err(|e| {
                                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                            })?;
                        }
                    }

                    if let Some(reply) = res.reply {
                        let mut cw = c_write_clone.lock().await;
                        cw.write_all(&reply).await.map_err(|e| {
                            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                        })?;
                    }
                }
                Err(_) => {
                    if let Some(tw) = &t_write_clone {
                        let mut tw = tw.lock().await;
                        tw.write_all(&buf[..n]).await.map_err(|e| {
                            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                        })?;
                    }
                }
            }
        }
        if let Some(tw) = &t_write_clone {
            let mut tw = tw.lock().await;
            let _ = tw.shutdown().await;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    // Target -> Client handler
    let c_write_clone2 = c_write.clone();
    let parser_lock_clone2 = parser_lock.clone();
    let recorder_clone2 = recorder.clone();

    let t_handle = async move {
        if let Some(mut tr) = t_read {
            let mut buf = [0u8; 8192];
            loop {
                let n = tr.read(&mut buf).await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                if n == 0 {
                    break;
                }

                let slice = &buf[..n];
                let mut p = parser_lock_clone2.lock().await;
                if let Ok(res) = p.parse_server_data(slice) {
                    for event in res.events {
                        if let Some(rec) = &recorder_clone2 {
                            let _ = rec.save_event(&event).await;
                        }
                    }
                    let mut cw = c_write_clone2.lock().await;
                    cw.write_all(&res.forward).await
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                } else {
                    let mut cw = c_write_clone2.lock().await;
                    cw.write_all(slice).await
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                }
            }
            let mut cw = c_write_clone2.lock().await;
            let _ = cw.shutdown().await;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    tokio::try_join!(c_handle, t_handle)?;
    Ok(())
}

/// Detect protocol from bytes (for TLS where we can't peek)
fn detect_protocol_from_bytes(
    data: &[u8],
    parsers: &[Arc<dyn ProtocolParser>],
) -> Arc<dyn ProtocolParser> {
    let mut best_score = 0.0;
    let mut best_idx = 0;
    for (i, p) in parsers.iter().enumerate() {
        let score = p.detect(data);
        if score > best_score {
            best_score = score;
            best_idx = i;
        }
    }
    if best_score > 0.0 {
        return parsers[best_idx].clone();
    }
    Arc::new(RedisParser)
}

async fn detect_protocol(
    stream: &TcpStream,
    parsers: &[Arc<dyn ProtocolParser>],
) -> Arc<dyn ProtocolParser> {
    let mut buf = [0u8; 1024];
    // peek
    match stream.peek(&mut buf).await {
        Ok(n) if n > 0 => {
            let mut best_score = 0.0;
            let mut best_idx = 0;
            for (i, p) in parsers.iter().enumerate() {
                let score = p.detect(&buf[0..n]);
                if score > best_score {
                    best_score = score;
                    best_idx = i;
                }
            }
            if best_score > 0.0 {
                // Low verbosity log
                println!(
                    "Detected protocol: {} (score: {})",
                    parsers[best_idx].protocol_id(),
                    best_score
                );
                return parsers[best_idx].clone();
            } else {
                println!("No protocol detected for {} bytes: {:02x?}", n, &buf[0..n]);
            }
        }
        _ => {}
    }
    // Default to Redis if uncertain (safe fallback for simple text protocols or actually Redis)
    Arc::new(RedisParser)
}

async fn handle_connection(
    client_stream: TcpStream,
    target_addr: String,
    parser: Arc<dyn ProtocolParser>,
    recorder: Option<Arc<Recorder>>,
    replay_engine: Option<Arc<tokio::sync::Mutex<ReplayEngine>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Connection Logic - 1:1 Mapping for Transparency
    // NOTE: Connection pooling is not compatible with transparent recording of stateful L4/L7 protocols (like Postgres startup)
    // unless we implement a full protocol-aware pooler. For Deja's purpose (fidelity), we must proxy 1:1.
    // Efficiency is achieved via Tokio's lightweight tasks and zero-copy splices where possible (though we sniff, so we read userspace).

    let target_stream = if replay_engine.is_none() {
        Some(
            TcpStream::connect(&target_addr)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        )
    } else {
        None
    };

    let (mut c_read, c_write) = client_stream.into_split();
    let c_write = Arc::new(tokio::sync::Mutex::new(c_write));

    let (t_read, t_write) = if let Some(ts) = target_stream {
        let (tr, tw) = ts.into_split();
        (Some(tr), Some(Arc::new(tokio::sync::Mutex::new(tw))))
    } else {
        (None, None)
    };

    let connection_id = uuid::Uuid::new_v4().to_string();
    let connection_parser = parser.new_connection(connection_id);
    let parser_lock = Arc::new(tokio::sync::Mutex::new(connection_parser));

    let c_write_clone = c_write.clone();
    let t_write_clone = t_write.clone();
    let parser_lock_clone = parser_lock.clone();
    let recorder_clone = recorder.clone();
    let replay_engine_clone = replay_engine.clone();
    let parser_arc_clone = parser.clone();

    // Spawn Client->Target Handler
    let c_handle = async move {
        let mut buf = [0u8; 8192];
        loop {
            let n = c_read
                .read(&mut buf)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            if n == 0 {
                break;
            }

            // Logic: Parse -> Save -> (Forward OR Reply)
            let mut p = parser_lock_clone.lock().await;
            match p.parse_client_data(&buf[0..n]) {
                Ok(res) => {
                    for event in res.events {
                        if let Some(rec) = &recorder_clone {
                            let _ = rec.save_event(&event).await;
                        }

                        if let Some(engine_lock) = &replay_engine_clone {
                            let mut engine = engine_lock.lock().await;
                            if let Some((_, responses)) = engine.find_match_with_responses(&event) {
                                println!("Replay Match for trace: {}", event.trace_id);
                                let mut cw = c_write_clone.lock().await;
                                for resp in responses {
                                    cw.write_all(&resp).await.map_err(|e| {
                                        Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                                    })?;
                                }
                            } else {
                                println!(
                                    "Replay Match FAILED for trace: {} (event type: {:?})",
                                    event.trace_id, event.event
                                );
                                // No Match - Inject Error
                                println!("No Match for trace: {}", event.trace_id);
                                // ... (Error injection logic same as before, abbreviated here for brevity if needed, but keeping logic)
                                let protocol = parser_arc_clone.protocol_id();
                                let error_bytes = match protocol {
                                        "redis" => b"-ERR No replay match found\r\n".to_vec(),
                                        "http" => b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n".to_vec(),
                                         "postgres" => {
                                             deja_core::protocols::postgres::PgSerializer::serialize_error("No Replay Match").to_vec()
                                         },
                                        _ => vec![],
                                    };
                                if !error_bytes.is_empty() {
                                    let mut cw = c_write_clone.lock().await;
                                    let _ = cw.write_all(&error_bytes).await;
                                }
                            }
                        }
                    }

                    // Forwarding (If not Replay Mode)
                    if let Some(tw) = &t_write_clone {
                        if !res.forward.is_empty() {
                            let mut tw = tw.lock().await;
                            tw.write_all(&res.forward).await.map_err(|e| {
                                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                            })?;
                        }
                    }

                    if let Some(reply) = res.reply {
                        // Injection logic (e.g. SSL denial)
                        let mut cw = c_write_clone.lock().await;
                        cw.write_all(&reply)
                            .await
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    }
                }
                Err(_e) => {
                    // Fallback Forward
                    if let Some(tw) = &t_write_clone {
                        let mut tw = tw.lock().await;
                        tw.write_all(&buf[0..n])
                            .await
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    }
                }
            }
        }
        if let Some(tw) = &t_write_clone {
            let mut tw = tw.lock().await;
            let _ = tw.shutdown().await;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    // Spawn Target->Client Handler
    let c_write_clone2 = c_write.clone();
    let parser_lock_clone2 = parser_lock.clone();
    let recorder_clone2 = recorder.clone();

    let t_handle = async move {
        if let Some(mut tr) = t_read {
            let mut buf = [0u8; 8192];
            loop {
                let n = tr
                    .read(&mut buf)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                if n == 0 {
                    break;
                }

                let str_slice = &buf[0..n];
                // Sniff
                {
                    let mut p = parser_lock_clone2.lock().await;
                    if let Ok(res) = p.parse_server_data(str_slice) {
                        for event in res.events {
                            if let Some(rec) = &recorder_clone2 {
                                let _ = rec.save_event(&event).await;
                            }
                        }
                        // Forward
                        let mut cw = c_write_clone2.lock().await;
                        cw.write_all(&res.forward)
                            .await
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    } else {
                        // Fallback Forward
                        let mut cw = c_write_clone2.lock().await;
                        cw.write_all(str_slice)
                            .await
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    }
                }
            }
            let mut cw = c_write_clone2.lock().await;
            let _ = cw.shutdown().await;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    tokio::try_join!(c_handle, t_handle)?;
    Ok(())
}
