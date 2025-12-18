use clap::{Parser, Subcommand};
use deja_core::protocols::http::HttpParser;
use deja_core::protocols::redis::RedisParser;
use deja_core::protocols::{ConnectionParser, ProtocolParser};
use deja_core::replay::ReplayEngine;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // Import AsyncWriteExt
use tokio::net::TcpListener;
use tokio::sync::Mutex;

#[derive(Parser)]
#[command(name = "deja")]
#[command(about = "Production Traffic Replay & Regression Testing Framework", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run replay engine
    Run {
        #[arg(long)]
        recordings: PathBuf,
        #[arg(long, default_value = "8080")]
        port: u16,
    },
    /// Compare two recordings for regressions
    Diff {
        #[arg(long)]
        baseline: PathBuf,
        #[arg(long)]
        candidate: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Run { recordings, port } => {
            println!("Loading recordings from {:?}...", recordings);
            // Wrap in Mutex for mutable access
            let engine = Arc::new(Mutex::new(ReplayEngine::new(recordings).await?));
            println!("Recordings loaded.");

            let addr = format!("0.0.0.0:{}", port);
            println!("Starting Replay Server on {}", addr);
            let listener = TcpListener::bind(addr).await?;

            loop {
                let (mut socket, _) = listener.accept().await?;
                let engine = engine.clone();

                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    // Peek to detect protocol
                    // In real impl, we'd use MSG_PEEK, but here we read and feed
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }

                    let data = &buf[0..n];

                    // Simple detection
                    let http = HttpParser;
                    let redis = RedisParser;

                    let mut parser: Box<dyn ConnectionParser> = if http.detect(data) > 0.5 {
                        println!("Detected HTTP");
                        http.new_connection("cli-inspect".into())
                    } else if redis.detect(data) > 0.5 {
                        println!("Detected Redis");
                        redis.new_connection("cli-inspect".into())
                    } else {
                        println!("Unknown protocol");
                        return;
                    };

                    // Parse
                    match parser.parse_client_data(data) {
                        Ok(result) => {
                            if result.events.is_empty() {
                                println!("Parsed 0 events from {} bytes", data.len());
                            }
                            for event in result.events {
                                println!("Processing event: {:?}", event.event);

                                // Lock the engine to find a match
                                let mut engine_guard = engine.lock().await;
                                if let Some(matched) = engine_guard.find_match(&event) {
                                    println!("Found match for trace: {}", matched.trace_id);
                                    let response = "HTTP/1.1 200 OK\r\nContent-Length: 15\r\n\r\nMOCKED RESPONSE";
                                    let _ = socket.write_all(response.as_bytes()).await;
                                } else {
                                    println!("No match found");
                                    let response =
                                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
                                    let _ = socket.write_all(response.as_bytes()).await;
                                }
                            }
                        }
                        Err(e) => eprintln!("Parse error: {}", e),
                    }
                });
            }
        }
        Commands::Diff {
            baseline,
            candidate,
        } => {
            use deja_core::diff::{calculate_diff, DiffReport, NoiseConfig};

            println!(
                "Diffing baseline {:?} vs candidate {:?}",
                baseline, candidate
            );

            let baseline_events = deja_core::replay::load_recordings(baseline.clone()).await?;
            let candidate_events = deja_core::replay::load_recordings(candidate.clone()).await?;

            println!(
                "Loaded {} baseline events, {} candidate events.",
                baseline_events.len(),
                candidate_events.len()
            );

            let mut report = DiffReport::new();
            let config = NoiseConfig::default(); // TODO: Load from file if needed

            let max_len = std::cmp::max(baseline_events.len(), candidate_events.len());
            for i in 0..max_len {
                let b = baseline_events.get(i);
                let c = candidate_events.get(i);

                match (b, c) {
                    (Some(b_ev), Some(c_ev)) => {
                        let mismatch = calculate_diff(b_ev, c_ev, &config);
                        report.add_result(mismatch);
                    }
                    (Some(_), None) => {
                        println!("Event missing in candidate at index {}", i);
                    }
                    (None, Some(_)) => {
                        println!("Extra event in candidate at index {}", i);
                    }
                    _ => {}
                }
            }

            if report.total_mismatched == 0 {
                println!("No regressions found!");
            } else {
                println!(
                    "Found {} mismatches out of {} compared events.",
                    report.total_mismatched, report.total_compared
                );
                for m in report.mismatches {
                    println!("\n[MISMATCH] Trace: {}", m.trace_id);
                    for d in m.diffs {
                        println!("  - {}:", d.path);
                        println!("    Baseline:  {}", d.baseline);
                        println!("    Candidate: {}", d.candidate);
                    }
                }
            }
        }
    }
    Ok(())
}
