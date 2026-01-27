//! SyncDejaRuntime - Synchronous Abstraction for non-deterministic operations
//!
//! This module provides the `SyncDejaRuntime` trait that services integrate with
//! to enable deterministic replay of non-deterministic operations in SYNCHRONOUS contexts.
//!
//! Mirrors `DejaRuntime` but uses blocking I/O.

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::SystemTime;
use uuid::Uuid;

/// Trait for handling non-deterministic operations in a replay-friendly way (Synchronous).
pub trait SyncDejaRuntime: Send + Sync {
    /// Generate a UUID v4
    fn uuid(&self) -> Uuid;

    /// Generate a UUID v7 (time-ordered)
    fn uuid_v7(&self) -> Uuid;

    /// Get current system time
    fn now(&self) -> SystemTime;

    /// Get current time as milliseconds since epoch
    fn now_millis(&self) -> u64;

    /// Generate a random u64
    fn random_u64(&self) -> u64;

    /// Generate random bytes
    fn random_bytes(&self, len: usize) -> Vec<u8>;

    /// Generate a nanoid (default 21 chars, alphanumeric)
    fn nanoid(&self) -> String;

    /// Record a non-deterministic value with a tag.
    fn capture(&self, tag: &str, value: &str);

    /// Replay a recorded value by tag.
    fn replay(&self, tag: &str) -> Option<String>;
}

// ============================================================================
// SyncProductionRuntime - Pass-through implementation
// ============================================================================

pub struct SyncProductionRuntime;

impl SyncProductionRuntime {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SyncProductionRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncDejaRuntime for SyncProductionRuntime {
    fn uuid(&self) -> Uuid {
        Uuid::new_v4()
    }

    fn uuid_v7(&self) -> Uuid {
        Uuid::now_v7()
    }

    fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn random_u64(&self) -> u64 {
        rand::random()
    }

    fn random_bytes(&self, len: usize) -> Vec<u8> {
        use rand::RngCore;
        let mut bytes = vec![0u8; len];
        rand::thread_rng().fill_bytes(&mut bytes);
        bytes
    }

    fn nanoid(&self) -> String {
        use rand::Rng;
        const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        let mut rng = rand::thread_rng();
        (0..21)
            .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
            .collect()
    }

    fn capture(&self, _tag: &str, _value: &str) {}

    fn replay(&self, _tag: &str) -> Option<String> {
        None
    }
}

// ============================================================================
// SyncNetworkRuntime - Communicates with Deja proxy (Blocking)
// ============================================================================

/// Request body for /capture endpoint
#[derive(serde::Serialize)]
struct CaptureRequest {
    trace_id: String,
    kind: String,
    value: String,
}

/// Response from /replay endpoint
#[derive(serde::Deserialize)]
struct ReplayResponse {
    value: String,
    found: bool,
}

pub struct SyncNetworkRuntime {
    client: reqwest::blocking::Client,
    proxy_url: String,
    trace_id: String, // Immutable in this context, passed on creation
    mode: String,
    // task_sequence: Arc<AtomicU64>, // Not needed for sync yet?
}

impl SyncNetworkRuntime {
    pub fn new(trace_id: String) -> Self {
        let proxy_url =
            env::var("DEJA_PROXY_URL").unwrap_or_else(|_| "http://localhost:9999".into());
        let mode = env::var("DEJA_MODE").unwrap_or_else(|_| "production".into());

        Self {
            client: reqwest::blocking::Client::new(),
            proxy_url,
            trace_id,
            mode,
        }
    }

    fn capture_internal(&self, kind: &str, value: &str) {
        let req = CaptureRequest {
            trace_id: self.trace_id.clone(),
            kind: kind.to_string(),
            value: value.to_string(),
        };
        let _ = self
            .client
            .post(format!("{}/capture", self.proxy_url))
            .json(&req)
            .send();
    }

    fn replay_internal(&self, kind: &str) -> Option<String> {
        let resp = self
            .client
            .get(format!("{}/replay", self.proxy_url))
            .query(&[("trace_id", &self.trace_id), ("kind", &kind.to_string())])
            .send()
            .ok()?;

        let replay_resp: ReplayResponse = resp.json().ok()?;
        if replay_resp.found {
            Some(replay_resp.value)
        } else {
            None
        }
    }
}

impl SyncDejaRuntime for SyncNetworkRuntime {
    fn uuid(&self) -> Uuid {
        match self.mode.as_str() {
            "record" => {
                let id = Uuid::new_v4();
                self.capture_internal("uuid", &id.to_string());
                id
            }
            "replay" => {
                if let Some(val) = self.replay_internal("uuid") {
                    if let Ok(id) = Uuid::parse_str(&val) {
                        return id;
                    }
                }
                Uuid::new_v4()
            }
            _ => Uuid::new_v4(),
        }
    }

    fn uuid_v7(&self) -> Uuid {
        match self.mode.as_str() {
            "record" => {
                let id = Uuid::now_v7();
                self.capture_internal("uuid_v7", &id.to_string());
                id
            }
            "replay" => {
                if let Some(val) = self.replay_internal("uuid_v7") {
                    if let Ok(id) = Uuid::parse_str(&val) {
                        return id;
                    }
                }
                Uuid::now_v7()
            }
            _ => Uuid::now_v7(),
        }
    }

    fn now(&self) -> SystemTime {
        match self.mode.as_str() {
            "record" => {
                let now = SystemTime::now();
                let ns = now
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                self.capture_internal("time", &ns.to_string());
                now
            }
            "replay" => {
                if let Some(val) = self.replay_internal("time") {
                    if let Ok(ns) = val.parse::<u64>() {
                        return SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(ns);
                    }
                }
                SystemTime::now()
            }
            _ => SystemTime::now(),
        }
    }

    fn now_millis(&self) -> u64 {
        let time = self.now();
        time.duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn random_u64(&self) -> u64 {
        match self.mode.as_str() {
            "record" => {
                let val: u64 = rand::random();
                self.capture_internal("random", &val.to_string());
                val
            }
            "replay" => {
                if let Some(val) = self.replay_internal("random") {
                    if let Ok(v) = val.parse::<u64>() {
                        return v;
                    }
                }
                rand::random()
            }
            _ => rand::random(),
        }
    }

    fn random_bytes(&self, len: usize) -> Vec<u8> {
        use rand::RngCore;
        let mut bytes = vec![0u8; len];
        rand::thread_rng().fill_bytes(&mut bytes);
        bytes
    }

    fn nanoid(&self) -> String {
        use rand::Rng;
        const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        let mut rng = rand::thread_rng();
        (0..21)
            .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
            .collect()
    }

    fn capture(&self, tag: &str, value: &str) {
        if self.mode == "record" {
            self.capture_internal(tag, value);
        }
    }

    fn replay(&self, tag: &str) -> Option<String> {
        if self.mode == "replay" {
            self.replay_internal(tag)
        } else {
            None
        }
    }
}

pub fn get_sync_runtime(trace_id: &str) -> Box<dyn SyncDejaRuntime> {
    let mode = env::var("DEJA_MODE").unwrap_or_else(|_| "production".into());
    match mode.as_str() {
        "record" | "replay" => Box::new(SyncNetworkRuntime::new(trace_id.to_string())),
        _ => Box::new(SyncProductionRuntime::new()),
    }
}
