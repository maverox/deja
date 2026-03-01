use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonlEvent {
    pub trace_id: String,
    pub timestamp_ms: u64,
    pub event_type: String,
    pub protocol: String,
    pub payload: serde_json::Value,
}

pub struct JsonlExporter {
    file_path: PathBuf,
    file: Arc<Mutex<std::fs::File>>,
}

impl JsonlExporter {
    pub fn new<P: Into<PathBuf>>(path: P) -> io::Result<Self> {
        let path = path.into();

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            file_path: path,
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn export_event(&self, event: &JsonlEvent) -> io::Result<()> {
        let line = serde_json::to_string(event)?;
        let mut file = self.file.lock().unwrap();
        writeln!(file, "{}", line)?;
        file.flush()?;
        Ok(())
    }

    pub fn get_path(&self) -> &PathBuf {
        &self.file_path
    }
}
