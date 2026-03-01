use deja_common::{DejaMode, SyncDejaRuntime};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, RwLock};

/// Extension trait for SyncDejaRuntime providing a simplified "run" method.
pub trait SyncDejaRuntimeExt: SyncDejaRuntime {
    fn run<T, F>(&self, kind: &str, generator: F) -> T
    where
        T: Serialize + DeserializeOwned + Send,
        F: FnOnce() -> T + Send,
        Self: Sized,
    {
        deja_common::deja_run_sync(self, kind, generator)
    }
}

// Blanket implementation
impl<R: SyncDejaRuntime + ?Sized> SyncDejaRuntimeExt for R {}

/// Synchronous runtime with per-kind sequence tracking.
pub struct SyncRuntime {
    client: reqwest::blocking::Client,
    proxy_url: String,
    trace_id: String,
    task_id: String,
    mode: DejaMode,
    sequences: Arc<RwLock<HashMap<String, u64>>>,
}

impl SyncRuntime {
    pub fn new(trace_id: String) -> Self {
        let proxy_url =
            env::var("DEJA_PROXY_URL").unwrap_or_else(|_| "http://localhost:9999".into());
        let mode_str = env::var("DEJA_MODE").unwrap_or_else(|_| "record".into());
        let mode = mode_str.parse().unwrap_or(DejaMode::Record);
        Self {
            client: reqwest::blocking::Client::new(),
            proxy_url,
            trace_id,
            task_id: "0".to_string(),
            mode,
            sequences: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn next_seq(&self, kind: &str) -> u64 {
        match self.sequences.write() {
            Ok(mut seqs) => {
                let seq = seqs.entry(kind.to_string()).or_insert(0);
                let current = *seq;
                *seq += 1;
                current
            }
            Err(e) => {
                let mut seqs = e.into_inner();
                let seq = seqs.entry(kind.to_string()).or_insert(0);
                let current = *seq;
                *seq += 1;
                current
            }
        }
    }
}

impl SyncDejaRuntime for SyncRuntime {
    fn capture_value(&self, kind: &str, value: String) {
        if self.mode != DejaMode::Record {
            return;
        }

        let seq = self.next_seq(kind);
        let req = CaptureRequest {
            trace_id: self.trace_id.clone(),
            task_id: self.task_id.clone(),
            kind: kind.to_string(),
            seq,
            value,
        };
        let _ = self
            .client
            .post(format!("{}/capture", self.proxy_url))
            .json(&req)
            .send();
    }

    fn replay_value(&self, kind: &str) -> Option<String> {
        if self.mode != DejaMode::Replay {
            return None;
        }

        let seq = self.next_seq(kind);
        let url = format!(
            "{}/replay?trace_id={}&task_id={}&kind={}&seq={}",
            self.proxy_url, self.trace_id, self.task_id, kind, seq
        );
        let resp = self.client.get(url).send().ok()?;
        let replay_resp: ReplayResponse = resp.json().ok()?;
        if replay_resp.found {
            Some(replay_resp.value)
        } else {
            tracing::warn!(
                trace_id = %self.trace_id,
                kind = %kind,
                seq = %seq,
                "No recorded value found (sync)"
            );
            None
        }
    }

    fn mode(&self) -> DejaMode {
        self.mode
    }
}

pub fn get_sync_runtime(trace_id: &str) -> Box<dyn SyncDejaRuntime> {
    Box::new(SyncRuntime::new(trace_id.to_string())) as Box<dyn SyncDejaRuntime>
}

#[derive(serde::Serialize)]
struct CaptureRequest {
    trace_id: String,
    task_id: String,
    kind: String,
    seq: u64,
    value: String,
}

#[derive(serde::Deserialize)]
struct ReplayResponse {
    value: String,
    found: bool,
}
