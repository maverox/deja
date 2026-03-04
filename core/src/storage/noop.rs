use async_trait::async_trait;
use deja_common::storage::{IndexStore, RecordingStore, StorageError};

/// A no-op storage backend that discards all data.
///
/// Used as a placeholder between PR1 and PR3 while real backends are built.
/// All write operations succeed silently; reads return empty results.
pub struct NoopStore;

#[async_trait]
impl RecordingStore for NoopStore {
    async fn save_event(&self, _session_id: &str, _event: Vec<u8>) -> Result<(), StorageError> {
        Ok(())
    }

    async fn save_batch(
        &self,
        _session_id: &str,
        _events: Vec<Vec<u8>>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn flush(&self, _session_id: &str) -> Result<(), StorageError> {
        Ok(())
    }

    async fn load_events(&self, _session_id: &str) -> Result<Vec<Vec<u8>>, StorageError> {
        Ok(vec![])
    }
}

#[async_trait]
impl IndexStore for NoopStore {
    async fn save_index(&self, _session_id: &str, _data: Vec<u8>) -> Result<(), StorageError> {
        Ok(())
    }

    async fn load_index(&self, _session_id: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(None)
    }
}
