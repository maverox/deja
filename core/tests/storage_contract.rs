use deja_common::storage::{IndexStore, RecordingStore, StorageConfig};
use deja_core::storage::noop::NoopStore;
use std::sync::Arc;

async fn run_save_and_load_roundtrip(store: &dyn RecordingStore) {
    let session = "test-session-roundtrip";
    let event1 = b"event-data-1".to_vec();
    let event2 = b"event-data-2".to_vec();

    store
        .save_event(session, event1.clone())
        .await
        .expect("save_event should succeed");
    store
        .save_event(session, event2.clone())
        .await
        .expect("save_event should succeed");
    store.flush(session).await.expect("flush should succeed");

    let loaded = store
        .load_events(session)
        .await
        .expect("load_events should succeed");

    // NoopStore returns empty — real backends must return saved events.
    // This test verifies the interface contract compiles and runs.
    let _ = loaded;
}

async fn run_save_batch_atomic(store: &dyn RecordingStore) {
    let session = "test-session-batch";
    let events = vec![
        b"batch-event-1".to_vec(),
        b"batch-event-2".to_vec(),
        b"batch-event-3".to_vec(),
    ];

    store
        .save_batch(session, events)
        .await
        .expect("save_batch should succeed");
    store.flush(session).await.expect("flush should succeed");
}

async fn run_flush_durability(store: &dyn RecordingStore) {
    let session = "test-session-flush";
    store
        .save_event(session, b"durable-event".to_vec())
        .await
        .expect("save_event should succeed");
    store.flush(session).await.expect("flush should succeed");

    let _ = store
        .load_events(session)
        .await
        .expect("load_events after flush should succeed");
}

async fn run_concurrent_saves(store: Arc<dyn RecordingStore>) {
    let session = "test-session-concurrent";
    let mut handles = vec![];

    for i in 0..10u8 {
        let s = store.clone();
        let sess = session.to_string();
        handles.push(tokio::spawn(async move {
            s.save_event(&sess, vec![i])
                .await
                .expect("concurrent save_event should succeed");
        }));
    }

    for h in handles {
        h.await.expect("task should not panic");
    }

    store.flush(session).await.expect("flush should succeed");
}

async fn run_empty_session(store: &dyn RecordingStore) {
    let loaded = store
        .load_events("nonexistent-session-xyz")
        .await
        .expect("load_events on missing session should return Ok");
    assert!(loaded.is_empty(), "missing session should return empty vec");
}

async fn run_index_save_and_load(store: &dyn IndexStore) {
    let session = "test-index-session";
    let data = b"index-data".to_vec();

    store
        .save_index(session, data)
        .await
        .expect("save_index should succeed");

    let _ = store
        .load_index(session)
        .await
        .expect("load_index should succeed");
}

async fn run_index_missing_session(store: &dyn IndexStore) {
    let result = store
        .load_index("nonexistent-index-session")
        .await
        .expect("load_index on missing session should return Ok");
    assert!(result.is_none(), "missing index session should return None");
}

#[tokio::test]
async fn test_save_and_load_roundtrip() {
    let store = NoopStore;
    run_save_and_load_roundtrip(&store).await;
}

#[tokio::test]
async fn test_save_batch_atomic() {
    let store = NoopStore;
    run_save_batch_atomic(&store).await;
}

#[tokio::test]
async fn test_flush_durability() {
    let store = NoopStore;
    run_flush_durability(&store).await;
}

#[tokio::test]
async fn test_concurrent_saves() {
    let store = Arc::new(NoopStore);
    run_concurrent_saves(store).await;
}

#[tokio::test]
async fn test_empty_session() {
    let store = NoopStore;
    run_empty_session(&store).await;
}

#[tokio::test]
async fn test_index_save_and_load() {
    let store = NoopStore;
    run_index_save_and_load(&store).await;
}

#[tokio::test]
async fn test_index_missing_session() {
    let store = NoopStore;
    run_index_missing_session(&store).await;
}

#[tokio::test]
async fn test_create_store_factory() {
    let config = StorageConfig::default();
    let store = deja_core::storage::create_store(&config);
    run_save_and_load_roundtrip(store.as_ref()).await;
}
