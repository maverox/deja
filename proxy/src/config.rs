use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Port mappings in format PORT:TARGET_HOST:TARGET_PORT (e.g., 5433:127.0.0.1:5432)
    /// Can also use DEJA_PORT_MAPS env var (comma-separated)
    #[arg(long = "map")]
    pub maps: Vec<String>,

    /// Ingress port mapping: LISTEN_PORT:TARGET_HOST:TARGET_PORT
    /// This is the trigger point for replay - the client-facing endpoint.
    /// Example: --ingress 8001:127.0.0.1:8000
    /// Can also use DEJA_INGRESS env var
    #[arg(long)]
    pub ingress: Option<String>,

    /// Directory for recordings
    /// Can also use DEJA_RECORDING_PATH env var
    #[arg(long, default_value = "recordings")]
    pub record_dir: String,

    /// Mode: record, replay, or orchestrated
    /// Can also use DEJA_MODE env var
    #[arg(long, default_value = "record")]
    pub mode: String,

    /// Control API port for SDK communication
    /// Can also use DEJA_CONTROL_PORT env var
    #[arg(long, default_value = "9999")]
    pub control_port: u16,

    /// Path to CA certificate for TLS MITM (PEM format)
    /// Can also use DEJA_CA_CERT env var
    #[arg(long)]
    pub ca_cert: Option<String>,

    /// Path to CA private key for TLS MITM (PEM format)
    /// Can also use DEJA_CA_KEY env var
    #[arg(long)]
    pub ca_key: Option<String>,

    /// Port for forward proxy mode (HTTP CONNECT tunneling)
    /// When set, the proxy listens on this port and accepts HTTP CONNECT requests
    /// for HTTPS interception. Clients should set HTTP_PROXY/HTTPS_PROXY to this port.
    /// Can also use DEJA_FORWARD_PROXY_PORT env var
    #[arg(long)]
    pub forward_proxy_port: Option<u16>,

    /// Timeout in milliseconds to wait for SDK to associate a connection with a trace.
    /// Connections not associated within this window are recorded as orphans.
    /// Can also use DEJA_ASSOCIATION_TIMEOUT_MS env var
    #[arg(long, default_value = "200")]
    pub association_timeout_ms: u64,

    /// Window in milliseconds to retroactively bind buffered events when association arrives.
    /// Events buffered before association that fall within this window are attributed.
    /// Events beyond this window are quarantined.
    /// Can also use DEJA_RETRO_BIND_WINDOW_MS env var
    #[arg(long, default_value = "500")]
    pub retro_bind_window_ms: u64,

    /// Replay strictness mode: 'strict' or 'lenient'
    ///
    /// - 'strict' (fail-closed): Replay rejects ambiguous/unattributed events deterministically
    /// - 'lenient' (fail-open): Replay buffers quarantined events and attempts fallback matching
    ///
    /// Defaults to 'strict' for replay mode to ensure deterministic behavior.
    /// Can also use DEJA_REPLAY_STRICT env var
    #[arg(long, default_value = "strict")]
    pub replay_strict_mode: String,

    /// Storage backend: "local-file" (default) or "kafka"
    /// Can also use DEJA_STORAGE_BACKEND env var
    #[arg(long, default_value = "local-file")]
    pub storage_backend: String,

    /// Kafka brokers (comma-separated), used when --storage-backend=kafka
    /// Can also use DEJA_KAFKA_BROKERS env var
    #[arg(long, default_value = "localhost:9092")]
    pub kafka_brokers: String,

    /// Kafka topic prefix, used when --storage-backend=kafka
    /// Can also use DEJA_KAFKA_TOPIC_PREFIX env var
    #[arg(long, default_value = "deja")]
    pub kafka_topic_prefix: String,
}

/// Parsed ingress configuration
#[derive(Debug, Clone)]
pub struct IngressConfig {
    pub listen_port: u16,
    pub target_host: String,
    pub target_port: u16,
}

impl IngressConfig {
    /// Parse from string format: "LISTEN_PORT:TARGET_HOST:TARGET_PORT"
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 3 {
            return None;
        }
        Some(Self {
            listen_port: parts[0].parse().ok()?,
            target_host: parts[1].to_string(),
            target_port: parts[2].parse().ok()?,
        })
    }
}
