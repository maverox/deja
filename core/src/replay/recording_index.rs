//! Scope-Tree Recording Index
//!
//! Indexes recorded events by (trace_id, scope_id, scope_sequence) for deterministic replay.
//! Uses the EventDirection field to classify request vs response events.
//!
//! # Structure
//!
//! ```text
//! RecordingIndex
//! └── traces: HashMap<trace_id, RecordedTrace>
//!     └── RecordedTrace
//!         └── scopes: HashMap<ScopeId, Vec<MessageExchange>>
//!             └── MessageExchange
//!                 ├── scope_sequence: u64
//!                 ├── client_message: RecordedEvent
//!                 ├── response_events: Vec<RecordedEvent>
//!                 └── server_responses: Vec<Bytes>
//! ```

use crate::events::{recorded_event, EventDirection, RecordedEvent};
use bytes::Bytes;
use deja_common::{Protocol, ScopeId};
use std::collections::HashMap;

/// A recorded message exchange: client request + server responses
#[derive(Debug, Clone)]
pub struct MessageExchange {
    /// Position within this scope
    pub scope_sequence: u64,
    /// The recorded client message (request)
    pub client_message: RecordedEvent,
    /// The original response events
    pub response_events: Vec<RecordedEvent>,
    /// The server's response(s) as serialized bytes
    pub server_responses: Vec<Bytes>,
}

/// All recorded data within a single scope
#[derive(Debug, Clone, Default)]
pub struct ScopeRecording {
    pub scope_id: ScopeId,
    pub protocol: Protocol,
    pub exchanges: Vec<MessageExchange>,
}

/// A recorded trace with all its scopes
#[derive(Debug, Default)]
pub struct RecordedTrace {
    pub trace_id: String,
    /// scope_id → ordered exchanges within that scope
    pub scopes: HashMap<ScopeId, ScopeRecording>,
}

/// Index recordings by (trace_id, scope_id, scope_sequence) for efficient replay lookup
#[derive(Debug, Default)]
pub struct RecordingIndex {
    traces: HashMap<String, RecordedTrace>,
}

impl RecordingIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build index from a flat list of recorded events.
    ///
    /// Groups by (trace_id, scope_id), sorts by scope_sequence,
    /// pairs client events with subsequent server events using the direction field.
    pub fn from_recordings(recordings: &[RecordedEvent]) -> Self {
        let mut index = Self::new();

        // Group events by (trace_id, scope_id)
        let mut grouped: HashMap<(String, String), Vec<RecordedEvent>> = HashMap::new();

        for event in recordings {
            if event.trace_id.is_empty() {
                continue;
            }
            let key = (event.trace_id.clone(), event.scope_id.clone());
            grouped.entry(key).or_default().push(event.clone());
        }

        // Process each group
        for ((trace_id, scope_id_str), mut events) in grouped {
            // Sort by scope_sequence
            events.sort_by_key(|e| e.scope_sequence);

            let scope_id = ScopeId::from_raw(&scope_id_str);
            let protocol = Self::detect_scope_protocol(&events);

            let mut exchanges = Vec::new();
            let mut i = 0;

            while i < events.len() {
                let event = &events[i];

                // Use direction field to classify
                if Self::is_client_event(event) {
                    let client_message = event.clone();
                    let scope_seq = event.scope_sequence;

                    // Collect subsequent server responses in same scope
                    let mut response_events = Vec::new();
                    let mut response_bytes = Vec::new();
                    let mut j = i + 1;

                    while j < events.len() {
                        let next = &events[j];
                        if Self::is_client_event(next) {
                            break; // Next client request
                        }
                        response_events.push(next.clone());
                        if let Some(bytes) = Self::serialize_response(next) {
                            response_bytes.push(bytes);
                        }
                        j += 1;
                    }

                    exchanges.push(MessageExchange {
                        scope_sequence: scope_seq,
                        client_message,
                        response_events,
                        server_responses: response_bytes,
                    });

                    i = j;
                } else {
                    // Orphan response — skip
                    i += 1;
                }
            }

            let scope_recording = ScopeRecording {
                scope_id: scope_id.clone(),
                protocol,
                exchanges,
            };

            let trace = index
                .traces
                .entry(trace_id.clone())
                .or_insert_with(|| RecordedTrace {
                    trace_id: trace_id.clone(),
                    scopes: HashMap::new(),
                });
            trace.scopes.insert(scope_id, scope_recording);
        }

        index
    }

    /// Get a specific exchange by (trace_id, scope_id, scope_sequence)
    pub fn get_exchange(
        &self,
        trace_id: &str,
        scope_id: &ScopeId,
        scope_seq: u64,
    ) -> Option<&MessageExchange> {
        let trace = self.traces.get(trace_id)?;
        let scope = trace.scopes.get(scope_id)?;
        scope
            .exchanges
            .iter()
            .find(|ex| ex.scope_sequence == scope_seq)
    }

    /// Legacy compatibility: get by (trace_id, protocol, sequence)
    /// Searches all scopes of matching protocol
    pub fn get(
        &self,
        trace_id: &str,
        protocol: Protocol,
        sequence: u64,
    ) -> Option<&MessageExchange> {
        let trace = self.traces.get(trace_id)?;
        let mut seq_counter = 0u64;
        for scope in trace.scopes.values() {
            if scope.protocol == protocol {
                for ex in &scope.exchanges {
                    if seq_counter == sequence {
                        return Some(ex);
                    }
                    seq_counter += 1;
                }
            }
        }
        None
    }

    pub fn trace_ids(&self) -> impl Iterator<Item = &String> {
        self.traces.keys()
    }

    pub fn get_trace(&self, trace_id: &str) -> Option<&RecordedTrace> {
        self.traces.get(trace_id)
    }

    pub fn trace_count(&self) -> usize {
        self.traces.len()
    }

    /// Detect protocol from a set of events in a scope
    fn detect_scope_protocol(events: &[RecordedEvent]) -> Protocol {
        for event in events {
            let proto = Self::detect_event_protocol(event);
            if proto != Protocol::Unknown {
                return proto;
            }
        }
        Protocol::Unknown
    }

    /// Detect the protocol of an event
    pub fn detect_event_protocol(event: &RecordedEvent) -> Protocol {
        match &event.event {
            Some(recorded_event::Event::PgMessage(_)) => Protocol::Postgres,
            Some(recorded_event::Event::RedisCommand(_))
            | Some(recorded_event::Event::RedisResponse(_)) => Protocol::Redis,
            Some(recorded_event::Event::HttpRequest(_))
            | Some(recorded_event::Event::HttpResponse(_)) => Protocol::Http,
            Some(recorded_event::Event::GrpcRequest(_))
            | Some(recorded_event::Event::GrpcResponse(_)) => Protocol::Grpc,
            _ => Protocol::Unknown,
        }
    }

    /// Check if event is a client-side event using direction field,
    /// falling back to event type heuristic
    fn is_client_event(event: &RecordedEvent) -> bool {
        // Primary: use direction field
        if event.direction == EventDirection::ClientToServer as i32 {
            return true;
        }
        if event.direction == EventDirection::ServerToClient as i32 {
            return false;
        }

        // Fallback: classify by event type
        Self::is_client_request_by_type(event)
    }

    /// Classify by event type (for legacy recordings without direction field)
    fn is_client_request_by_type(event: &RecordedEvent) -> bool {
        use crate::events::pg_message_event;

        match &event.event {
            Some(recorded_event::Event::PgMessage(msg)) => {
                matches!(
                    &msg.message,
                    Some(pg_message_event::Message::Query(_))
                        | Some(pg_message_event::Message::Parse(_))
                        | Some(pg_message_event::Message::Bind(_))
                        | Some(pg_message_event::Message::Execute(_))
                        | Some(pg_message_event::Message::Sync(_))
                        | Some(pg_message_event::Message::Startup(_))
                )
            }
            Some(recorded_event::Event::RedisCommand(_)) => true,
            Some(recorded_event::Event::HttpRequest(_)) => true,
            Some(recorded_event::Event::GrpcRequest(_)) => true,
            Some(recorded_event::Event::TcpData(data)) => data.is_client,
            _ => false,
        }
    }

    /// Serialize a response event to bytes
    fn serialize_response(event: &RecordedEvent) -> Option<Bytes> {
        match &event.event {
            Some(recorded_event::Event::PgMessage(msg)) => {
                if let Some(m) = &msg.message {
                    crate::protocols::postgres::PgSerializer::serialize_message(m)
                } else {
                    None
                }
            }
            Some(inner @ recorded_event::Event::RedisResponse(_)) => {
                crate::protocols::redis::RedisSerializer::serialize_message(inner)
            }
            Some(inner @ recorded_event::Event::HttpResponse(_)) => {
                crate::protocols::http::HttpSerializer::serialize_message(inner)
            }
            Some(inner @ recorded_event::Event::GrpcResponse(_)) => {
                crate::protocols::grpc::GrpcSerializer::serialize_message(inner)
            }
            Some(recorded_event::Event::TcpData(data)) if !data.is_client => {
                Some(Bytes::copy_from_slice(&data.data))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{HttpRequestEvent, HttpResponseEvent};

    fn create_http_request(trace_id: &str, scope_id: &str, path: &str, seq: u64) -> RecordedEvent {
        RecordedEvent {
            trace_id: trace_id.to_string(),
            scope_id: scope_id.to_string(),
            scope_sequence: seq,
            global_sequence: seq,
            timestamp_ns: seq * 1000,
            direction: EventDirection::ClientToServer as i32,
            event: Some(recorded_event::Event::HttpRequest(HttpRequestEvent {
                method: "GET".to_string(),
                path: path.to_string(),
                headers: Default::default(),
                body: vec![],
                schema: "http".to_string(),
                host: "localhost".to_string(),
            })),
            metadata: Default::default(),
        }
    }

    fn create_http_response(
        trace_id: &str,
        scope_id: &str,
        status: u32,
        seq: u64,
    ) -> RecordedEvent {
        RecordedEvent {
            trace_id: trace_id.to_string(),
            scope_id: scope_id.to_string(),
            scope_sequence: seq,
            global_sequence: seq,
            timestamp_ns: seq * 1000,
            direction: EventDirection::ServerToClient as i32,
            event: Some(recorded_event::Event::HttpResponse(HttpResponseEvent {
                status,
                headers: Default::default(),
                body: vec![],
                latency_ms: 10,
            })),
            metadata: Default::default(),
        }
    }

    #[test]
    fn test_build_index_from_recordings() {
        let scope = "trace:trace-1:conn:0";
        let recordings = vec![
            create_http_request("trace-1", scope, "/api/users", 0),
            create_http_response("trace-1", scope, 200, 1),
            create_http_request("trace-1", scope, "/api/posts", 2),
            create_http_response("trace-1", scope, 201, 3),
        ];

        let index = RecordingIndex::from_recordings(&recordings);
        assert_eq!(index.trace_count(), 1);

        let scope_id = ScopeId::from_raw(scope);
        let ex0 = index.get_exchange("trace-1", &scope_id, 0).unwrap();
        assert_eq!(ex0.scope_sequence, 0);
        if let Some(recorded_event::Event::HttpRequest(req)) = &ex0.client_message.event {
            assert_eq!(req.path, "/api/users");
        } else {
            panic!("Expected HTTP request");
        }

        let ex1 = index.get_exchange("trace-1", &scope_id, 2).unwrap();
        if let Some(recorded_event::Event::HttpRequest(req)) = &ex1.client_message.event {
            assert_eq!(req.path, "/api/posts");
        } else {
            panic!("Expected HTTP request");
        }
    }

    #[test]
    fn test_multi_scope_index() {
        let scope_a = "trace:t1:conn:0";
        let scope_b = "trace:t1:conn:1";

        let recordings = vec![
            create_http_request("t1", scope_a, "/a", 0),
            create_http_response("t1", scope_a, 200, 1),
            create_http_request("t1", scope_b, "/b", 0),
            create_http_response("t1", scope_b, 200, 1),
        ];

        let index = RecordingIndex::from_recordings(&recordings);
        assert_eq!(index.trace_count(), 1);

        let trace = index.get_trace("t1").unwrap();
        assert_eq!(trace.scopes.len(), 2);
    }
}
