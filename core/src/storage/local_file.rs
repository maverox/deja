use async_trait::async_trait;
use deja_common::storage::{IndexStore, RecordingStore, StorageError};
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncWriteExt;

pub struct LocalFileStore {
    base_path: PathBuf,
    format: String,
}

impl LocalFileStore {
    pub fn new(base_path: impl Into<PathBuf>, format: Option<String>) -> Self {
        Self {
            base_path: base_path.into(),
            format: format.unwrap_or_else(|| "binary".to_string()),
        }
    }

    fn session_dir(&self, session_id: &str) -> PathBuf {
        self.base_path.join("sessions").join(session_id)
    }

    fn events_file(&self, session_id: &str) -> PathBuf {
        let dir = self.session_dir(session_id);
        if self.format == "json" {
            dir.join("events.jsonl")
        } else {
            dir.join("events.bin")
        }
    }

    fn index_file(&self, session_id: &str) -> PathBuf {
        self.session_dir(session_id).join("index.json")
    }

    async fn ensure_session_dir(&self, session_id: &str) -> std::io::Result<()> {
        fs::create_dir_all(self.session_dir(session_id)).await
    }

    fn encode_event(&self, event: &[u8]) -> Vec<u8> {
        if self.format == "json" {
            event.to_vec()
        } else {
            // [u32 big-endian length][event bytes]
            let length = event.len() as u32;
            let mut buf = Vec::with_capacity(4 + event.len());
            buf.extend_from_slice(&length.to_be_bytes());
            buf.extend_from_slice(event);
            buf
        }
    }
}

#[async_trait]
impl RecordingStore for LocalFileStore {
    async fn save_event(&self, session_id: &str, event: Vec<u8>) -> Result<(), StorageError> {
        self.ensure_session_dir(session_id).await?;
        let payload = self.encode_event(&event);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.events_file(session_id))
            .await?;
        file.write_all(&payload).await?;
        file.flush().await?;
        Ok(())
    }

    async fn save_batch(&self, session_id: &str, events: Vec<Vec<u8>>) -> Result<(), StorageError> {
        self.ensure_session_dir(session_id).await?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.events_file(session_id))
            .await?;
        for event in events {
            let payload = self.encode_event(&event);
            file.write_all(&payload).await?;
        }
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn flush(&self, session_id: &str) -> Result<(), StorageError> {
        // Open without `create(true)` so we get NotFound if nothing was written yet.
        match fs::OpenOptions::new()
            .write(true)
            .open(self.events_file(session_id))
            .await
        {
            Ok(file) => {
                file.sync_all().await?;
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    async fn load_events(&self, session_id: &str) -> Result<Vec<Vec<u8>>, StorageError> {
        let contents = match fs::read(self.events_file(session_id)).await {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(StorageError::Io(e)),
        };

        if self.format == "json" {
            let events = contents
                .split(|&b| b == b'\n')
                .filter(|line| !line.is_empty())
                .map(|line| line.to_vec())
                .collect();
            Ok(events)
        } else {
            // Binary: [u32 BE len][data] repeated
            let mut events = Vec::new();
            let mut cursor = 0usize;
            while cursor + 4 <= contents.len() {
                let len = u32::from_be_bytes([
                    contents[cursor],
                    contents[cursor + 1],
                    contents[cursor + 2],
                    contents[cursor + 3],
                ]) as usize;
                cursor += 4;
                if cursor + len > contents.len() {
                    return Err(StorageError::Other(format!(
                        "Corrupt binary events file for session '{}': \
                         expected {} bytes at offset {}, only {} remaining",
                        session_id,
                        len,
                        cursor,
                        contents.len() - cursor,
                    )));
                }
                events.push(contents[cursor..cursor + len].to_vec());
                cursor += len;
            }
            Ok(events)
        }
    }
}

#[async_trait]
impl IndexStore for LocalFileStore {
    async fn save_index(&self, session_id: &str, data: Vec<u8>) -> Result<(), StorageError> {
        self.ensure_session_dir(session_id).await?;
        fs::write(self.index_file(session_id), data).await?;
        Ok(())
    }

    async fn load_index(&self, session_id: &str) -> Result<Option<Vec<u8>>, StorageError> {
        match fs::read(self.index_file(session_id)).await {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Io(e)),
        }
    }
}
