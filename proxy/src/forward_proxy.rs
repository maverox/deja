//! Forward Proxy - HTTP CONNECT Tunneling for HTTPS Interception
//!
//! This module implements an HTTP forward proxy that handles CONNECT requests,
//! enabling HTTPS interception via TLS MITM. Services configure their HTTP client
//! to use this proxy (via HTTP_PROXY/HTTPS_PROXY env vars), and the proxy:
//!
//! 1. Accepts HTTP CONNECT requests (e.g., "CONNECT api.stripe.com:443 HTTP/1.1")
//! 2. Responds with "200 Connection Established"
//! 3. Performs TLS MITM using dynamically-generated certificates
//! 4. Parses and records/replays the decrypted HTTP traffic
//! 5. Extracts x-trace-id headers for correlation with the originating request

use crate::correlation::TraceCorrelator;
use deja_common::ScopeId;
use deja_core::events::{recorded_event, EventDirection, HttpRequestEvent, HttpResponseEvent, RecordedEvent};
use deja_core::recording::Recorder;
use deja_core::replay::ReplayEngine;
use deja_core::tls_mitm::TlsMitmManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, instrument, warn};

/// Start the forward proxy listener
///
/// This listens on the specified port and handles HTTP CONNECT requests
/// for HTTPS tunneling with TLS MITM interception.
pub async fn start_forward_proxy(
    listen_port: u16,
    tls_manager: Arc<TlsMitmManager>,
    recorder: Option<Arc<Recorder>>,
    replay_engine: Option<Arc<Mutex<ReplayEngine>>>,
    correlator: Arc<TraceCorrelator>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listen_addr = format!("0.0.0.0:{}", listen_port);
    let listener = TcpListener::bind(&listen_addr).await?;

    info!(
        "[ForwardProxy] Listening on {} for HTTP CONNECT requests",
        listen_addr
    );

    loop {
        match listener.accept().await {
            Ok((client_socket, peer_addr)) => {
                debug!("[ForwardProxy] Accepted connection from {}", peer_addr);

                let tls_mgr = tls_manager.clone();
                let rec = recorder.clone();
                let rep = replay_engine.clone();
                let corr = correlator.clone();
                let peer_port = peer_addr.port();

                tokio::spawn(async move {
                    if let Err(e) =
                        handle_forward_proxy_connection(client_socket, peer_port, tls_mgr, rec, rep, corr)
                            .await
                    {
                        error!("[ForwardProxy] Connection error: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("[ForwardProxy] Accept error: {}", e);
            }
        }
    }
}

/// Handle an incoming forward proxy connection
///
/// This parses the HTTP CONNECT request, establishes the tunnel,
/// and performs TLS MITM interception.
#[instrument(skip(client_socket, tls_manager, recorder, replay_engine, correlator))]
async fn handle_forward_proxy_connection(
    mut client_socket: TcpStream,
    peer_port: u16,
    tls_manager: Arc<TlsMitmManager>,
    recorder: Option<Arc<Recorder>>,
    replay_engine: Option<Arc<Mutex<ReplayEngine>>>,
    correlator: Arc<TraceCorrelator>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Read the CONNECT request
    let mut buf_reader = BufReader::new(&mut client_socket);
    let mut request_line = String::new();
    buf_reader.read_line(&mut request_line).await?;

    // Parse CONNECT host:port HTTP/1.1
    let parts: Vec<&str> = request_line.trim().split_whitespace().collect();
    if parts.len() < 3 || parts[0] != "CONNECT" {
        warn!(
            "[ForwardProxy] Invalid request (expected CONNECT): {}",
            request_line.trim()
        );
        client_socket
            .write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            .await?;
        return Ok(());
    }

    let host_port = parts[1];
    let (host, port) = parse_host_port(host_port)?;

    debug!(
        "[ForwardProxy] CONNECT request for {}:{} ({})",
        host, port, host_port
    );

    // Read and discard remaining headers (until empty line)
    let mut header_line = String::new();
    loop {
        header_line.clear();
        buf_reader.read_line(&mut header_line).await?;
        if header_line.trim().is_empty() {
            break;
        }
    }

    // Drop the BufReader to get back ownership of client_socket
    drop(buf_reader);

    // Send 200 Connection Established
    client_socket
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await?;

    info!(
        "[ForwardProxy] Tunnel established to {}:{}, starting TLS MITM",
        host, port
    );

    // Now the client will start TLS handshake - we perform MITM
    handle_tls_tunnel(
        client_socket,
        peer_port,
        &host,
        port,
        tls_manager,
        recorder,
        replay_engine,
        correlator,
    )
    .await
}

/// Parse host:port string into (host, port) tuple
fn parse_host_port(
    host_port: &str,
) -> Result<(String, u16), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(colon_pos) = host_port.rfind(':') {
        let host = &host_port[..colon_pos];
        let port_str = &host_port[colon_pos + 1..];
        let port: u16 = port_str.parse()?;
        Ok((host.to_string(), port))
    } else {
        // Default to port 443 for HTTPS
        Ok((host_port.to_string(), 443))
    }
}

/// Handle TLS tunnel with MITM interception
///
/// This performs the TLS handshake with the client using a dynamically
/// generated certificate, connects to the real target (in recording mode),
/// and proxies/records the decrypted HTTP traffic.
#[instrument(skip(client_socket, tls_manager, recorder, replay_engine, correlator))]
async fn handle_tls_tunnel(
    client_socket: TcpStream,
    peer_port: u16,
    target_host: &str,
    target_port: u16,
    tls_manager: Arc<TlsMitmManager>,
    recorder: Option<Arc<Recorder>>,
    replay_engine: Option<Arc<Mutex<ReplayEngine>>>,
    correlator: Arc<TraceCorrelator>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // === Connection-level trace correlation via source-port ===
    // The SDK's socket_interceptor reports associate_by_source_port() when connecting
    // to the forward proxy. Use that as the primary correlation mechanism.
    let connection_assoc = match correlator
        .on_new_connection(peer_port, deja_common::Protocol::Http)
        .await
    {
        Ok(assoc) => {
            debug!(
                trace_id = %assoc.trace_id,
                scope_id = %assoc.scope_id,
                peer_port = peer_port,
                "[ForwardProxy] Source-port correlation resolved immediately"
            );
            Some(assoc)
        }
        Err(rx) => {
            // SDK hasn't registered yet — wait briefly for the association
            match tokio::time::timeout(std::time::Duration::from_millis(200), rx).await {
                Ok(Ok(assoc)) => {
                    debug!(
                        trace_id = %assoc.trace_id,
                        scope_id = %assoc.scope_id,
                        "[ForwardProxy] Source-port correlation resolved after wait"
                    );
                    Some(assoc)
                }
                _ => {
                    debug!(
                        peer_port = peer_port,
                        "[ForwardProxy] Source-port correlation timed out, will use header fallback"
                    );
                    None
                }
            }
        }
    };

    // Generate server config for this host (dynamically signed certificate)
    let server_config = tls_manager.generate_server_config(target_host)?;
    let acceptor = TlsAcceptor::from(server_config);

    // Accept TLS from client
    let tls_client_stream = acceptor.accept(client_socket).await?;
    info!(
        "[ForwardProxy] TLS handshake complete for host: {}",
        target_host
    );

    // Split the client TLS stream
    let (mut client_read, client_write) = tokio::io::split(tls_client_stream);
    let client_write = Arc::new(Mutex::new(client_write));

    // Connect to target (only in recording mode)
    let target_stream = if replay_engine.is_none() {
        let target_addr = format!("{}:{}", target_host, target_port);
        let target_tcp = TcpStream::connect(&target_addr).await?;

        // TLS connect to target
        let client_config = tls_manager.client_config();
        let connector = tokio_rustls::TlsConnector::from(client_config);
        let server_name = rustls::pki_types::ServerName::try_from(target_host.to_string())?;
        let tls_target = connector.connect(server_name, target_tcp).await?;

        info!(
            "[ForwardProxy] Connected to target {}:{} with TLS",
            target_host, target_port
        );
        Some(tls_target)
    } else {
        info!(
            "[ForwardProxy] Replay mode - not connecting to {}:{}",
            target_host, target_port
        );
        None
    };

    // Split target stream if we have one
    let (target_read, target_write) = if let Some(ts) = target_stream {
        let (tr, tw) = tokio::io::split(ts);
        (Some(tr), Some(Arc::new(Mutex::new(tw))))
    } else {
        (None, None)
    };

    // Connection-level scope: resolved once, shared by all requests on this tunnel.
    // If source-port resolved it, use that. Otherwise, the first request's x-trace-id
    // header will allocate the scope (fallback for non-SDK clients).
    let conn_scope: Arc<Mutex<Option<(String, ScopeId)>>> = Arc::new(Mutex::new(
        connection_assoc.map(|a| (a.trace_id, a.scope_id)),
    ));

    // Client -> Target handler
    let client_write_clone = client_write.clone();
    let target_write_clone = target_write.clone();
    let recorder_clone = recorder.clone();
    let replay_engine_clone = replay_engine.clone();
    let correlator_clone = correlator.clone();
    let conn_scope_clone = conn_scope.clone();
    let target_host_owned = target_host.to_string();

    let client_to_target = async move {
        let mut buf = [0u8; 8192];
        let mut request_buf = Vec::new();

        loop {
            let n = client_read.read(&mut buf).await?;
            if n == 0 {
                break;
            }

            request_buf.extend_from_slice(&buf[..n]);

            // Try to parse HTTP request
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut req = httparse::Request::new(&mut headers);

            match req.parse(&request_buf) {
                Ok(httparse::Status::Complete(head_len)) => {
                    let method = req.method.unwrap_or("").to_string();
                    let path = req.path.unwrap_or("").to_string();

                    let mut header_map = HashMap::new();
                    for h in req.headers.iter() {
                        header_map.insert(
                            h.name.to_lowercase(),
                            String::from_utf8_lossy(h.value).to_string(),
                        );
                    }

                    // Resolve (trace_id, scope_id) — prefer connection-level association,
                    // fall back to x-trace-id header if source-port didn't resolve.
                    let (trace_id, scope_id) = {
                        let mut scope_guard = conn_scope_clone.lock().await;
                        let request_trace = header_map.get("x-trace-id").cloned();

                        if let Some((existing_trace, existing_scope)) = scope_guard.as_ref().cloned() {
                            if let Some(ref requested_trace) = request_trace {
                                if requested_trace != &existing_trace {
                                    let new_scope = correlator_clone
                                        .allocate_connection_id(requested_trace)
                                        .await;
                                    debug!(
                                        old_trace_id = %existing_trace,
                                        new_trace_id = %requested_trace,
                                        new_scope_id = %new_scope,
                                        "[ForwardProxy] Updating keep-alive tunnel association"
                                    );
                                    *scope_guard = Some((requested_trace.clone(), new_scope.clone()));
                                    (requested_trace.clone(), new_scope)
                                } else {
                                    (existing_trace, existing_scope)
                                }
                            } else {
                                (existing_trace, existing_scope)
                            }
                        } else {
                            let tid = request_trace.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                            let sid = correlator_clone.allocate_connection_id(&tid).await;
                            debug!(
                                trace_id = %tid,
                                scope_id = %sid,
                                "[ForwardProxy] Using header-based trace fallback"
                            );
                            *scope_guard = Some((tid.clone(), sid.clone()));
                            (tid, sid)
                        }
                    };

                    debug!(
                        "[ForwardProxy] HTTP {} {} (trace: {}, scope: {})",
                        method, path, trace_id, scope_id
                    );

                    // Get content length
                    let content_length = header_map
                        .get("content-length")
                        .and_then(|v| v.parse::<usize>().ok())
                        .unwrap_or(0);

                    // Check if we have the full request
                    let total_len = head_len + content_length;
                    if request_buf.len() < total_len {
                        // Need more data
                        continue;
                    }

                    // Extract body
                    let body = request_buf[head_len..total_len].to_vec();

                    // Get sequence number within this connection scope
                    let sequence = correlator_clone
                        .next_scope_sequence(&scope_id)
                        .await;

                    // Create event
                    let event = RecordedEvent {
                        trace_id: trace_id.clone(),
                        scope_id: scope_id.as_str().to_string(),
                        scope_sequence: sequence,
                        global_sequence: correlator_clone.next_global_sequence().await,
                        timestamp_ns: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_nanos() as u64,
                        direction: EventDirection::ClientToServer as i32,
                        event: Some(recorded_event::Event::HttpRequest(HttpRequestEvent {
                            method: method.clone(),
                            path: path.clone(),
                            headers: header_map.clone(),
                            body: body.clone(),
                            schema: "https".to_string(),
                            host: target_host_owned.clone(),
                        })),
                        metadata: Default::default(),
                    };

                    // Record or replay
                    if let Some(rec) = &recorder_clone {
                        let _ = rec.save_event(&event).await;
                    }

                    if let Some(engine_lock) = &replay_engine_clone {
                        let mut engine = engine_lock.lock().await;
                        if let Some((_, _response_events, response_bytes)) =
                            engine.find_match_with_responses(&event)
                        {
                            info!("[ForwardProxy] Replay match found for {} {}", method, path);
                            let mut cw = client_write_clone.lock().await;
                            for resp in response_bytes {
                                cw.write_all(&resp).await?;
                            }
                        } else {
                            warn!(
                                "[ForwardProxy] No replay match for {} {} (trace: {})",
                                method, path, trace_id
                            );
                            let mut cw = client_write_clone.lock().await;
                            cw.write_all(
                                b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 22\r\n\r\nNo replay match found.",
                            )
                            .await?;
                        }
                    } else if let Some(tw) = &target_write_clone {
                        // Forward to target
                        let mut tw = tw.lock().await;
                        tw.write_all(&request_buf[..total_len]).await?;
                    }

                    // Clear processed data
                    request_buf.drain(..total_len);
                }
                Ok(httparse::Status::Partial) => {
                    // Need more data
                    continue;
                }
                Err(e) => {
                    warn!("[ForwardProxy] HTTP parse error: {}", e);
                    // Forward raw data to target if in recording mode
                    if let Some(tw) = &target_write_clone {
                        let mut tw = tw.lock().await;
                        tw.write_all(&request_buf).await?;
                    }
                    request_buf.clear();
                }
            }
        }

        // Shutdown target write
        if let Some(tw) = &target_write_clone {
            let mut tw = tw.lock().await;
            let _ = tw.shutdown().await;
        }

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    // Target -> Client handler
    let recorder_clone2 = recorder.clone();
    let correlator_clone2 = correlator.clone();
    let conn_scope_clone2 = conn_scope.clone();

    let target_to_client = async move {
        if let Some(mut target_read) = target_read {
            let mut buf = [0u8; 8192];
            let mut response_buf = Vec::new();

            loop {
                let n = target_read.read(&mut buf).await?;
                if n == 0 {
                    break;
                }

                response_buf.extend_from_slice(&buf[..n]);

                // Try to parse HTTP response
                let mut headers = [httparse::EMPTY_HEADER; 64];
                let mut res = httparse::Response::new(&mut headers);

                match res.parse(&response_buf) {
                    Ok(httparse::Status::Complete(head_len)) => {
                        let status_code = res.code.unwrap_or(0) as u32;

                        let mut header_map = HashMap::new();
                        for h in res.headers.iter() {
                            header_map.insert(
                                h.name.to_lowercase(),
                                String::from_utf8_lossy(h.value).to_string(),
                            );
                        }

                        // Get content length
                        let content_length = header_map
                            .get("content-length")
                            .and_then(|v| v.parse::<usize>().ok())
                            .unwrap_or(0);

                        // Check if we have the full response
                        let total_len = head_len + content_length;
                        if response_buf.len() < total_len {
                            // Need more data
                            continue;
                        }

                        let body = response_buf[head_len..total_len].to_vec();

                        // Use the connection-level scope (shared with request handler)
                        let (trace_id, scope_id) = {
                            let guard = conn_scope_clone2.lock().await;
                            guard.clone().unwrap_or_else(|| {
                                let fallback_id = uuid::Uuid::new_v4().to_string();
                                let fallback_scope = ScopeId::connection(&fallback_id, 0);
                                (fallback_id, fallback_scope)
                            })
                        };

                        // Get sequence number
                        let sequence = correlator_clone2
                            .next_scope_sequence(&scope_id)
                            .await;

                        let event = RecordedEvent {
                            trace_id,
                            scope_id: scope_id.as_str().to_string(),
                            scope_sequence: sequence,
                            global_sequence: correlator_clone2.next_global_sequence().await,
                            timestamp_ns: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_nanos() as u64,
                            direction: EventDirection::ServerToClient as i32,
                            event: Some(recorded_event::Event::HttpResponse(HttpResponseEvent {
                                status: status_code,
                                headers: header_map,
                                body,
                                latency_ms: 0,
                            })),
                            metadata: Default::default(),
                        };

                        if let Some(rec) = &recorder_clone2 {
                            let _ = rec.save_event(&event).await;
                        }

                        // Forward response to client
                        {
                            let mut cw = client_write.lock().await;
                            cw.write_all(&response_buf[..total_len]).await?;
                        }

                        // Clear processed data
                        response_buf.drain(..total_len);
                    }
                    Ok(httparse::Status::Partial) => {
                        // Need more data, but forward what we have to reduce latency
                        // (for streaming responses)
                    }
                    Err(e) => {
                        warn!("[ForwardProxy] HTTP response parse error: {}", e);
                        // Forward raw data
                        {
                            let mut cw = client_write.lock().await;
                            cw.write_all(&response_buf).await?;
                        }
                        response_buf.clear();
                    }
                }
            }

            // Forward any remaining data
            if !response_buf.is_empty() {
                let mut cw = client_write.lock().await;
                cw.write_all(&response_buf).await?;
            }

            // Shutdown client write
            let mut cw = client_write.lock().await;
            let _ = cw.shutdown().await;
        }

        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    // Run both handlers concurrently
    tokio::try_join!(client_to_target, target_to_client)?;

    Ok(())
}
