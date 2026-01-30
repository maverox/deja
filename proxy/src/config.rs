use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Port mappings in format PORT:TARGET_HOST:TARGET_PORT (e.g., 5433:127.0.0.1:5432)
    /// Can also use DEJA_PORT_MAPS env var (comma-separated)
    #[arg(long = "map")]
    pub maps: Vec<String>,

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
}
