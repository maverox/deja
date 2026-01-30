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

    /// Start the proxy in recording mode
    /// Uses --map format: PROXY_PORT:TARGET_HOST:TARGET_PORT
    pub async fn start_proxy_record(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let recordings_path = self.recordings_dir.path().to_str().unwrap();

        // Format: PROXY_PORT:TARGET_HOST:TARGET_PORT
        let map_arg = format!("{}:127.0.0.1:{}", self.proxy_port, self.mock_backend_port);

        let child = Command::new("cargo")
            .args([
                "run",
                "-p",
                "deja-proxy",
                "--",
                "--map",
                &map_arg,
                "--mode",
                "record",
                "--record-dir",
                recordings_path,
                "--control-port",
                &self.control_api_port.to_string(),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        self.proxy_process = Some(child);

        // Wait for proxy to be ready (compile + start)
        sleep(Duration::from_secs(3)).await;

        // Verify proxy is listening
        self.wait_for_proxy().await?;

        Ok(())
    }

    /// Start the proxy in replay mode
    pub async fn start_proxy_replay(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let recordings_path = self.recordings_dir.path().to_str().unwrap();

        // In replay mode, target is ignored but we still need to provide the map
        let map_arg = format!("{}:127.0.0.1:{}", self.proxy_port, self.mock_backend_port);

        let child = Command::new("cargo")
            .args([
                "run",
                "-p",
                "deja-proxy",
                "--",
                "--map",
                &map_arg,
                "--mode",
                "replay",
                "--record-dir",
                recordings_path,
                "--control-port",
                &self.control_api_port.to_string(),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        self.proxy_process = Some(child);

        // Wait for proxy to be ready
        sleep(Duration::from_secs(3)).await;

        self.wait_for_proxy().await?;

        Ok(())
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
