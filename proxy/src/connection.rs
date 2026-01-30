use crate::correlation;
use crate::correlation::TraceCorrelator;
use deja_common::Protocol;
use deja_core::events::RecordedEvent;
use deja_core::protocols::redis::RedisParser;
use deja_core::protocols::ProtocolParser;
use deja_core::recording::Recorder;
use deja_core::replay::ReplayEngine;
use deja_core::tls_mitm::TlsMitmManager;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tracing::{info, instrument, warn};

/// Enrich an event with trace_id and sequence from the correlator
///
/// If the correlator has a mapping for this connection, the event's trace_id
/// and sequence will be updated. Otherwise, the event is returned unchanged.
async fn enrich_event(
    mut event: RecordedEvent,
    correlator: &TraceCorrelator,
    protocol: Protocol,
) -> RecordedEvent {
    let connection_id = &event.connection_id;

    // Look up trace_id from correlator's connection mapping
    if let Some(trace_id) = correlator.get_trace_id(connection_id).await {
        event.trace_id = trace_id.clone();

        // Get next sequence for this (trace_id, protocol) pair
        let sequence = correlator.next_sequence(&trace_id, protocol).await;
        event.sequence = sequence;
    }

    event
}

/// Detect if the connection starts with a TLS handshake
/// TLS record format: 0x16 (handshake) 0x03 0x0X (version)
pub async fn is_tls_handshake(stream: &TcpStream) -> bool {
    let mut buf = [0u8; 3];
    match stream.peek(&mut buf).await {
        Ok(n) if n >= 3 => {
            // TLS handshake starts with: 0x16 (ContentType: Handshake) 0x03 0x0X (Version)
            buf[0] == 0x16 && buf[1] == 0x03 && buf[2] <= 0x03
        }
        _ => false,
    }
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

pub async fn detect_protocol(
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
                info!(
                    "Detected protocol: {} (score: {})",
                    parsers[best_idx].protocol_id(),
                    best_score
                );
                return parsers[best_idx].clone();
            } else {
                info!("No protocol detected for {} bytes: {:02x?}", n, &buf[0..n]);
            }
        }
        _ => {}
    }
    // Default to Redis if uncertain (safe fallback for simple text protocols or actually Redis)
    Arc::new(RedisParser)
}

/// Handle a TLS connection with MITM interception
#[instrument(
    skip(
        client_stream,
        tls_manager,
        parsers,
        recorder,
        replay_engine,
        correlator
    ),
    fields(connection_id)
)]
pub async fn handle_tls_connection(
    client_stream: TcpStream,
    target_addr: String,
    target_host: &str,
    tls_manager: Arc<TlsMitmManager>,
    parsers: Arc<Vec<Arc<dyn ProtocolParser>>>,
    recorder: Option<Arc<Recorder>>,
    replay_engine: Option<Arc<tokio::sync::Mutex<ReplayEngine>>>,
    correlator: Arc<correlation::TraceCorrelator>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Generate server config for this host (dynamically signed cert)
    let server_config = tls_manager.generate_server_config(target_host)?;
    let acceptor = TlsAcceptor::from(server_config);

    // Accept TLS from client
    let tls_client_stream = acceptor.accept(client_stream).await?;
    info!("[TLS] Accepted TLS connection for host: {}", target_host);

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
    info!("[TLS] Detected protocol: {}", detected_parser.protocol_id());

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
    tracing::Span::current().record("connection_id", &connection_id);
    let connection_parser = detected_parser.new_connection(connection_id);
    let parser_lock = Arc::new(tokio::sync::Mutex::new(connection_parser));

    // Process the initial peeked data first
    {
        let mut p = parser_lock.lock().await;
        match p.parse_client_data(&peek_buf[..peeked]) {
            Ok(res) => {
                // Determine protocol for correlator
                let protocol = detected_parser
                    .protocol_id()
                    .parse()
                    .unwrap_or(Protocol::Unknown);

                for event in res.events {
                    // Enrich event with trace_id and sequence from correlator
                    let enriched_event = enrich_event(event, &correlator, protocol).await;

                    if let Some(rec) = &recorder {
                        let _ = rec.save_event(&enriched_event).await;
                    }

                    if let Some(engine_lock) = &replay_engine {
                        let mut engine = engine_lock.lock().await;
                        if let Some((_, response_events, response_bytes)) =
                            engine.find_match_with_responses(&enriched_event)
                        {
                            let mut cw = c_write.lock().await;
                            let protocol = detected_parser.protocol_id();
                            if protocol == "grpc" {
                                for resp_event in response_events {
                                    if let Some(
                                        deja_core::events::recorded_event::Event::GrpcResponse(
                                            grpc_resp,
                                        ),
                                    ) = &resp_event.event
                                    {
                                        let bytes = deja_core::protocols::grpc::GrpcSerializer::serialize_response(grpc_resp);
                                        cw.write_all(&bytes).await?;
                                    }
                                }
                            } else {
                                for resp in response_bytes {
                                    cw.write_all(&resp).await?;
                                }
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
    let correlator_clone = correlator.clone();
    let protocol = detected_parser
        .protocol_id()
        .parse()
        .unwrap_or(Protocol::Unknown);

    // Client -> Target handler
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

            let mut p = parser_lock_clone.lock().await;
            match p.parse_client_data(&buf[..n]) {
                Ok(res) => {
                    for event in res.events {
                        // Enrich event with trace_id and sequence from correlator
                        let enriched = enrich_event(event, &correlator_clone, protocol).await;

                        if let Some(rec) = &recorder_clone {
                            let _ = rec.save_event(&enriched).await;
                        }

                        if let Some(engine_lock) = &replay_engine_clone {
                            let mut engine = engine_lock.lock().await;
                            if let Some((_, response_events, response_bytes)) =
                                engine.find_match_with_responses(&enriched)
                            {
                                let mut cw = c_write_clone.lock().await;
                                let protocol = parser_arc_clone.protocol_id();
                                if protocol == "grpc" {
                                    for resp_event in response_events {
                                        if let Some(
                                            deja_core::events::recorded_event::Event::GrpcResponse(
                                                grpc_resp,
                                            ),
                                        ) = &resp_event.event
                                        {
                                            let bytes = deja_core::protocols::grpc::GrpcSerializer::serialize_response(grpc_resp);
                                            cw.write_all(&bytes).await.map_err(|e| {
                                                Box::new(e)
                                                    as Box<dyn std::error::Error + Send + Sync>
                                            })?;
                                        }
                                    }
                                } else {
                                    for resp in response_bytes {
                                        cw.write_all(&resp).await.map_err(|e| {
                                            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                                        })?;
                                    }
                                }
                            } else {
                                let protocol = parser_arc_clone.protocol_id();
                                let error_bytes = match protocol {
                                    "redis" => b"-ERR No replay match found\r\n".to_vec(),
                                    "http" => b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n".to_vec(),
                                    "postgres" => {
                                        deja_core::protocols::postgres::PgSerializer::serialize_error("No Replay Match").to_vec()
                                    }
                                    "grpc" => {
                                        // gRPC error: return empty frame with trailers indicating error
                                        // For simplicity, return an empty response with error status
                                        use deja_core::events::GrpcStatusCode;
                                        deja_core::protocols::grpc::GrpcSerializer::serialize_error(
                                            GrpcStatusCode::GrpcStatusInternal,
                                            "No replay match found"
                                        ).to_vec()
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
                        cw.write_all(&reply)
                            .await
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    }
                }
                Err(_) => {
                    if let Some(tw) = &t_write_clone {
                        let mut tw = tw.lock().await;
                        tw.write_all(&buf[..n])
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

    // Target -> Client handler
    let c_write_clone2 = c_write.clone();
    let parser_lock_clone2 = parser_lock.clone();
    let recorder_clone2 = recorder.clone();
    let correlator_clone2 = correlator.clone();

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

                let slice = &buf[..n];
                let mut p = parser_lock_clone2.lock().await;
                if let Ok(res) = p.parse_server_data(slice) {
                    for event in res.events {
                        // Enrich event with trace_id and sequence from correlator
                        let enriched = enrich_event(event, &correlator_clone2, protocol).await;

                        if let Some(rec) = &recorder_clone2 {
                            let _ = rec.save_event(&enriched).await;
                        }
                    }
                    let mut cw = c_write_clone2.lock().await;
                    cw.write_all(&res.forward)
                        .await
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                } else {
                    let mut cw = c_write_clone2.lock().await;
                    cw.write_all(slice)
                        .await
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

#[instrument(
    skip(client_stream, parser, recorder, replay_engine, correlator),
    fields(connection_id)
)]
pub async fn handle_connection(
    client_stream: TcpStream,
    target_addr: String,
    parser: Arc<dyn ProtocolParser>,
    recorder: Option<Arc<Recorder>>,
    replay_engine: Option<Arc<tokio::sync::Mutex<ReplayEngine>>>,
    correlator: Arc<correlation::TraceCorrelator>,
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
    tracing::Span::current().record("connection_id", &connection_id);
    let mut connection_parser = parser.new_connection(connection_id);
    connection_parser.set_mode(replay_engine.is_some());
    let parser_lock = Arc::new(tokio::sync::Mutex::new(connection_parser));

    // Replay Initialization
    if replay_engine.is_some() {
        if let Some(init_bytes) = parser.on_replay_init() {
            let mut cw = c_write.lock().await;
            let _ = cw.write_all(&init_bytes).await;
        }
    }

    let c_write_clone = c_write.clone();
    let t_write_clone = t_write.clone();
    let parser_lock_clone = parser_lock.clone();
    let recorder_clone = recorder.clone();
    let replay_engine_clone = replay_engine.clone();
    let parser_arc_clone = parser.clone();
    let correlator_clone = correlator.clone();
    let protocol = parser.protocol_id().parse().unwrap_or(Protocol::Unknown);

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
                        // Enrich event with trace_id and sequence from correlator
                        let enriched = enrich_event(event, &correlator_clone, protocol).await;

                        if let Some(rec) = &recorder_clone {
                            let _ = rec.save_event(&enriched).await;
                        }

                        if let Some(engine_lock) = &replay_engine_clone {
                            let mut engine = engine_lock.lock().await;
                            if let Some((_, response_events, response_bytes)) =
                                engine.find_match_with_responses(&enriched)
                            {
                                info!("Replay Match for trace: {}", enriched.trace_id);
                                let mut cw = c_write_clone.lock().await;

                                let protocol = parser_arc_clone.protocol_id();
                                if protocol == "grpc" {
                                    for resp_event in response_events {
                                        if let Some(
                                            deja_core::events::recorded_event::Event::GrpcResponse(
                                                grpc_resp,
                                            ),
                                        ) = &resp_event.event
                                        {
                                            let bytes = deja_core::protocols::grpc::GrpcSerializer::serialize_response(grpc_resp);
                                            cw.write_all(&bytes).await.map_err(|e| {
                                                Box::new(e)
                                                    as Box<dyn std::error::Error + Send + Sync>
                                            })?;
                                        }
                                    }
                                } else {
                                    for resp in response_bytes {
                                        cw.write_all(&resp).await.map_err(|e| {
                                            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                                        })?;
                                    }
                                }
                            } else {
                                warn!(
                                    "Replay Match FAILED for trace: {} (event type: {:?})",
                                    enriched.trace_id, enriched.event
                                );
                                // No Match - Inject Error
                                warn!("No Match for trace: {}", enriched.trace_id);
                                // Error injection based on protocol
                                let protocol = parser_arc_clone.protocol_id();
                                let error_bytes = match protocol {
                                        "redis" => b"-ERR No replay match found\r\n".to_vec(),
                                        "http" => b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n".to_vec(),
                                         "postgres" => {
                                             deja_core::protocols::postgres::PgSerializer::serialize_error("No Replay Match").to_vec()
                                         },
                                         "grpc" => {
                                             use deja_core::events::GrpcStatusCode;
                                             deja_core::protocols::grpc::GrpcSerializer::serialize_error(
                                                 GrpcStatusCode::GrpcStatusInternal,
                                                 "No replay match found"
                                             ).to_vec()
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
    let correlator_clone2 = correlator.clone();

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
                            // Enrich event with trace_id and sequence from correlator
                            let enriched = enrich_event(event, &correlator_clone2, protocol).await;

                            if let Some(rec) = &recorder_clone2 {
                                let _ = rec.save_event(&enriched).await;
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
