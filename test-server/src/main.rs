use actix_web::{web, App, HttpServer};
use clap::Parser;
use deja_sdk::{reqwest::DejaClient, tonic::DejaTonicLayer};
use std::net::SocketAddr;
use test_server::{
    connector::connector_server::ConnectorServer,
    connector_service::ConnectorService,
    db_pools::DatabasePools,
    http_handlers::{
        correlation_torture_handler, health_handler, process_handler, HttpServiceState,
    },
    mock_backend,
    service::TestServiceImpl,
    test_service::test_service_server::TestServiceServer,
};
use tonic::transport::Server as TonicServer;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Unified Test Server for Deja proxy integration testing"
)]
struct Args {
    #[arg(short, long, default_value = "50051")]
    grpc_port: u16,

    #[arg(long, default_value = "50052")]
    connector_port: u16,

    #[arg(long, default_value = "8080")]
    http_port: u16,

    #[arg(short, long, default_value = "8090")]
    mock_port: u16,

    #[arg(long)]
    backend_url: Option<String>,

    #[arg(long)]
    deja_control_url: Option<String>,

    #[arg(long, default_value = "./recordings")]
    recording_path: String,

    #[arg(
        long,
        default_value = "postgres://deja_user:password@localhost:5433/deja_test"
    )]
    pg_url: String,

    #[arg(long, default_value = "redis://localhost:6390")]
    redis_url: String,

    #[arg(long, default_value = "./test_output.jsonl")]
    jsonl_path: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("🚀 Starting Unified Test Server...");

    let args = Args::parse();

    std::env::set_var("DEJA_RECORDING_PATH", &args.recording_path);
    if let Some(ref control_url) = args.deja_control_url {
        std::env::set_var("DEJA_CONTROL_URL", control_url);
        std::env::set_var("DEJA_MODE", "record");
        info!("📼 Deja SDK configured: {}", control_url);
    }

    let pools = DatabasePools::new(&args.pg_url, &args.redis_url).await?;
    let pools_clone = pools.clone();

    let http_client = DejaClient::new(reqwest::Client::new());
    let proxy_http_port = args.http_port;
    let pg_url = args.pg_url.clone();

    let mock_backend_url = if args.mock_port > 0 {
        let mock_addr = format!("0.0.0.0:{}", args.mock_port);
        info!("📦 Starting mock backend on {}", mock_addr);
        tokio::spawn(async move {
            if let Err(e) = mock_backend::start_mock_backend(&mock_addr).await {
                tracing::error!("Mock backend error: {}", e);
            }
        });
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        Some(format!("http://127.0.0.1:{}", args.mock_port))
    } else {
        None
    };

    let backend_url = args
        .backend_url
        .or(mock_backend_url)
        .unwrap_or_else(|| "http://127.0.0.1:8090".to_string());
    info!("🔗 Backend URL: {}", backend_url);

    let http_service_state = HttpServiceState {
        pools: pools_clone,
        http_client,
        proxy_http_port,
        pg_url: pg_url.clone(),
    };

    let http_state = web::Data::new(http_service_state);

    let http_handle = tokio::spawn(async move {
        let addr: SocketAddr = format!("0.0.0.0:{}", args.http_port).parse().unwrap();
        info!("🌐 Actix HTTP server listening on {}", addr);

        HttpServer::new(move || {
            App::new()
                .app_data(http_state.clone())
                .route("/api/process", web::post().to(process_handler))
                .route(
                    "/api/correlation/torture",
                    web::post().to(correlation_torture_handler),
                )
                .route("/api/health", web::get().to(health_handler))
        })
        .bind(addr)
        .unwrap()
        .run()
        .await
    });

    let connector_service = ConnectorService {
        pools,
        http_client: DejaClient::new(reqwest::Client::new()),
        proxy_http_port,
        pg_url: pg_url.clone(),
    };

    let connector_handle = tokio::spawn(async move {
        let addr: SocketAddr = format!("0.0.0.0:{}", args.connector_port).parse().unwrap();
        info!("🌐 Connector gRPC server listening on {}", addr);

        TonicServer::builder()
            .layer(DejaTonicLayer::default())
            .add_service(ConnectorServer::new(connector_service))
            .serve(addr)
            .await
    });

    let test_service = TestServiceImpl::new(backend_url);
    let test_service_addr: SocketAddr = format!("0.0.0.0:{}", args.grpc_port).parse()?;
    info!(
        "🌐 TestService gRPC server listening on {}",
        test_service_addr
    );

    tokio::select! {
        result = http_handle => {
            if let Err(e) = result {
                tracing::error!("HTTP server error: {:?}", e);
            }
        }
        result = connector_handle => {
            if let Err(e) = result {
                tracing::error!("Connector gRPC server error: {:?}", e);
            }
        }
        result = TonicServer::builder()
            .layer(DejaTonicLayer::default())
            .add_service(TestServiceServer::new(test_service))
            .serve(test_service_addr) => {
            if let Err(e) = result {
                tracing::error!("TestService gRPC server error: {:?}", e);
            }
        }
    }

    Ok(())
}
