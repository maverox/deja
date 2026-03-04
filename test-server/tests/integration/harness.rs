//! Integration Test Framework for Deja Proxy
//!
//! Provides test harness utilities for orchestrating:
//! - Mock HTTP backends
//! - Deja proxy (record/replay modes)
//!
//! Each test scenario spins up fresh instances for isolation.

use std::net::TcpListener as StdTcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;

/// Find an available port
pub fn get_available_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Test harness for integration tests
pub struct TestHarness {
    /// Mock backend port
    pub mock_backend_port: u16,
    /// Proxy listen port
    pub proxy_port: u16,
    /// Proxy control API port
    pub control_api_port: u16,
    /// Recordings directory
    pub recordings_dir: tempfile::TempDir,
    /// Proxy process (if started)
    proxy_process: Option<Child>,
}

impl TestHarness {
    /// Create a new test harness with allocated ports
    pub fn new() -> Self {
        Self {
            mock_backend_port: get_available_port(),
            proxy_port: get_available_port(),
            control_api_port: get_available_port(),
            recordings_dir: tempfile::tempdir().unwrap(),
            proxy_process: None,
        }
    }

    /// Start the mock HTTP backend (embedded)
    pub async fn start_mock_backend(&self) {
        let addr = format!("127.0.0.1:{}", self.mock_backend_port);
        tokio::spawn(async move {
            if let Err(e) = test_server::mock_backend::start_mock_backend(&addr).await {
                eprintln!("Mock backend error: {}", e);
            }
        });
        // Give it time to start
        sleep(Duration::from_millis(100)).await;
    }

    /// Start the proxy in recording mode with custom mappings
    /// map format: "LISTEN_PORT:TARGET_HOST:TARGET_PORT"
    pub async fn start_proxy_record_with_maps(
        &mut self,
        maps: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let recordings_path = self.recordings_dir.path().to_str().unwrap();

        let mut args = Vec::new();

        for map in &maps {
            args.push("--map".to_string());
            args.push(map.clone());
        }

        args.extend_from_slice(&[
            "--mode".to_string(),
            "record".to_string(),
            "--record-dir".to_string(),
            recordings_path.to_string(),
            "--control-port".to_string(),
            self.control_api_port.to_string(),
        ]);

        let cwd = std::env::current_dir().unwrap();
        let mut binary_path = cwd.join("target/debug/deja-proxy");
        if !binary_path.exists() {
            binary_path = cwd.join("../target/debug/deja-proxy");
        }

        let child = Command::new(binary_path)
            .args(&args)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;

        self.proxy_process = Some(child);

        // Wait for proxy to be ready
        sleep(Duration::from_secs(5)).await;

        // Wait for first port
        if let Some(first_map) = maps.first() {
            let parts: Vec<&str> = first_map.split(':').collect();
            let port = parts[0];
            self.wait_for_port(port.parse().unwrap()).await?;
        }

        Ok(())
    }

    /// Start the proxy in replay mode with custom mappings
    pub async fn start_proxy_replay_with_maps(
        &mut self,
        maps: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let recordings_path = self.recordings_dir.path().to_str().unwrap();

        let mut args = Vec::new();

        for map in &maps {
            args.push("--map".to_string());
            args.push(map.clone());
        }

        args.extend_from_slice(&[
            "--mode".to_string(),
            "replay".to_string(),
            "--record-dir".to_string(),
            recordings_path.to_string(),
            "--control-port".to_string(),
            self.control_api_port.to_string(),
            "--replay-strict-mode".to_string(),
            "lenient".to_string(),
        ]);

        let cwd = std::env::current_dir().unwrap();
        let mut binary_path = cwd.join("target/debug/deja-proxy");
        if !binary_path.exists() {
            binary_path = cwd.join("../target/debug/deja-proxy");
        }

        let child = Command::new(binary_path)
            .args(&args)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;

        self.proxy_process = Some(child);

        // Wait for proxy to be ready
        sleep(Duration::from_secs(5)).await;

        // Wait for first port
        if let Some(first_map) = maps.first() {
            let parts: Vec<&str> = first_map.split(':').collect();
            let port = parts[0];
            self.wait_for_port(port.parse().unwrap()).await?;
        }

        Ok(())
    }

    /// Wait for a specific port to be open
    async fn wait_for_port(&self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..20 {
            if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
                return Ok(());
            }
            sleep(Duration::from_millis(500)).await;
        }
        Err(format!("Port {} did not open in time", port).into())
    }

    /// Start the proxy in recording mode
    /// Uses --map format: PROXY_PORT:TARGET_HOST:TARGET_PORT
    pub async fn start_proxy_record(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let map = format!("{}:127.0.0.1:{}", self.proxy_port, self.mock_backend_port);
        self.start_proxy_record_with_maps(vec![map]).await
    }

    /// Start the proxy in replay mode
    pub async fn start_proxy_replay(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let map = format!("{}:127.0.0.1:{}", self.proxy_port, self.mock_backend_port);
        self.start_proxy_replay_with_maps(vec![map]).await
    }

    /// Wait for proxy to be ready by checking if the port is open
    async fn wait_for_proxy(&self) -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..20 {
            if std::net::TcpStream::connect(format!("127.0.0.1:{}", self.proxy_port)).is_ok() {
                return Ok(());
            }
            sleep(Duration::from_millis(500)).await;
        }
        Err("Proxy did not start in time".into())
    }

    /// Stop the proxy
    pub fn stop_proxy(&mut self) {
        if let Some(mut child) = self.proxy_process.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Get recordings directory path
    pub fn recordings_path(&self) -> &std::path::Path {
        self.recordings_dir.path()
    }

    /// URL for the mock backend (direct, bypassing proxy)
    pub fn mock_backend_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.mock_backend_port)
    }

    /// URL for the proxy (requests go through here)
    pub fn proxy_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.proxy_port)
    }

    /// URL for the control API
    pub fn control_api_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.control_api_port)
    }

    /// Count recording files in the recordings directory
    pub fn count_recordings(&self) -> usize {
        std::fs::read_dir(self.recordings_path())
            .map(|entries| entries.filter(|e| e.is_ok()).count())
            .unwrap_or(0)
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.stop_proxy();
    }
}

impl Default for TestHarness {
    fn default() -> Self {
        Self::new()
    }
}
