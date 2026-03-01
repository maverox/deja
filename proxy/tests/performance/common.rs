//! Common types and utilities for performance tests

use crate::lib::buffer::PendingEventBuffer;
use crate::lib::common::ScopeId;
use crate::lib::core::events::{EventDirection, RecordedEvent};
use std::time::Instant;

/// Create test event for buffering tests
pub fn create_test_event() -> RecordedEvent {
    RecordedEvent {
        trace_id: String::new(),
        scope_id: String::new(),
        scope_sequence: 0,
        global_sequence: 0,
        timestamp_ns: 0,
        direction: EventDirection::ClientToServer as i32,
        metadata: std::collections::HashMap::new(),
        event: None,
    }
}
