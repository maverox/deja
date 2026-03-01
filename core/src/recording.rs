use crate::events::RecordedEvent;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncWriteExt;

pub struct Recorder {
    base_path: PathBuf,
    session_id: String,
}

impl Recorder {
    pub async fn new(base_path: impl Into<PathBuf>) -> Self {
        let path = base_path.into();
        let session_id = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();

        let session_dir = path.join("sessions").join(&session_id);
        fs::create_dir_all(&session_dir).await.unwrap_or_default();

        println!("Recorder initialized for session: {}", session_id);
        println!("Storage path: {:?}", session_dir);

        Self {
            base_path: path,
            session_id,
        }
    }

    pub fn get_session_id(&self) -> &str {
        &self.session_id
    }

    pub async fn save_event(&self, event: &RecordedEvent) -> std::io::Result<()> {
        let format = std::env::var("DEJA_STORAGE_FORMAT").unwrap_or_else(|_| "binary".to_string());
        let session_dir = self.base_path.join("sessions").join(&self.session_id);

        let (filename, payload) = if format == "json" {
            let f = session_dir.join("events.jsonl");
            let j = serde_json::to_string(event)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            (f, format!("{}\n", j).into_bytes())
        } else {
            let f = session_dir.join("events.bin");
            let bytes = bincode::serialize(event)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let length = bytes.len() as u32;
            let mut buf = Vec::with_capacity(4 + bytes.len());
            buf.extend_from_slice(&length.to_be_bytes());
            buf.extend_from_slice(&bytes);
            (f, buf)
        };

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&filename)
            .await?;

        file.write_all(&payload).await?;
        file.flush().await?;
        file.sync_all().await?;

        Ok(())
    }
}
