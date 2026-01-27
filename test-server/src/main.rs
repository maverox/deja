//! Test Server Main Entry Point
//!
//! Starts the gRPC server and optionally an embedded mock backend.

use clap::Parser;
use test_server::service::TestServiceImpl;
use test_server::test_service::test_service_server::TestServiceServer;
use test_server::mock_backend;
use tonic::transport::Server;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(author, version, about = "Test server for Deja proxy integration testing")]
struct Args {
    /// Port for the gRPC server
    #[arg(short, long, default_value = "50051")]
    port: u16,

    /// Base URL for the HTTP backend (optional, defaults to mock)
    #[arg(short, long)]
    backend_url: Option<String>,

    /// Port for embedded mock backend (0 to disable)
    #[arg(short, long, default_value = "8090")]
    mock_port: u16,

    /// URL of the Deja proxy control API (for SDK)
    #[arg(long)]
    deja_control_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();
    
    info!("🚀 Starting Test Server...");
    
    // Start mock backend if enabled
    let mock_backend_url = if args.mock_port > 0 {
        let mock_addr = format!("0.0.0.0:{}", args.mock_port);
        info!("📦 Starting mock backend on {}", mock_addr);
        
        tokio::spawn(async move {
            if let Err(e) = mock_backend::start_mock_backend(&mock_addr).await {
                tracing::error!("Mock backend error: {}", e);
            }
        });
        
        // Give it a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        Some(format!("http://127.0.0.1:{}", args.mock_port))
    } else {
        None
    };
    
    // Determine backend URL
    let backend_url = args.backend_url
        .or(mock_backend_url)
        .unwrap_or_else(|| "http://127.0.0.1:8090".to_string());
    
    info!("🔗 Backend URL: {}", backend_url);
    
    // Set up Deja SDK environment if control URL provided
    if let Some(ref control_url) = args.deja_control_url {
        std::env::set_var("DEJA_CONTROL_URL", control_url);
        std::env::set_var("DEJA_MODE", "record");
        info!("📼 Deja SDK configured: {}", control_url);
    }
    
    // Create service
    let service = TestServiceImpl::new(backend_url);
    
    // Start gRPC server
    let addr = format!("0.0.0.0:{}", args.port).parse()?;
    info!("🌐 gRPC server listening on {}", addr);
    
    Server::builder()
        .add_service(TestServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
