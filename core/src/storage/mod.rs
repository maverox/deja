pub mod local_file;
pub mod noop;

pub use local_file::LocalFileStore;
pub use noop::NoopStore;

use deja_common::storage::{IndexStore, RecordingStore, StorageConfig};

pub fn create_store(config: &StorageConfig) -> Box<dyn RecordingStore> {
    match config {
        StorageConfig::LocalFile { base_path, format } => {
            Box::new(LocalFileStore::new(base_path, format.clone()))
        }
        StorageConfig::Kafka { .. } => Box::new(NoopStore),
    }
}

pub fn create_index_store(config: &StorageConfig) -> Box<dyn IndexStore> {
    match config {
        StorageConfig::LocalFile { base_path, format } => {
            Box::new(LocalFileStore::new(base_path, format.clone()))
        }
        StorageConfig::Kafka { .. } => Box::new(NoopStore),
    }
}
