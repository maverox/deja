//! Recording Index for Protocol-Scoped Replay
//!
//! This module provides efficient lookup of recorded messages by
//! (trace_id, protocol, sequence) for deterministic replay.
//!
//! # Why Protocol-Scoped?
//!
//! Connection IDs are ephemeral - they change between recording and replay.
//! By organizing recordings by (trace_id, protocol) pairs and using sequence
//! numbers, we get stable identifiers that work across runs.
//!
//! # Structure
//!
//! ```text
//! RecordingIndex
//! └── traces: HashMap<trace_id, RecordedTrace>
//!     └── RecordedTrace
//!         ├── postgres_messages: Vec<MessageExchange>
//!         ├── redis_messages: Vec<MessageExchange>
//!         ├── http_messages: Vec<MessageExchange>
//!         └── grpc_messages: Vec<MessageExchange>
//!             └── MessageExchange
//!                 ├── sequence: u64
//!                 ├── client_message: RecordedEvent
//!                 └── server_responses: Vec<bytes::Bytes>
//! ```

use crate::events::{recorded_event, RecordedEvent};
use bytes::Bytes;
use deja_common::Protocol;
use std::collections::HashMap;

/// A recorded message exchange: client request + server responses
#[derive(Debug, Clone)]
pub struct MessageExchange {
    /// Sequence number within (trace, protocol) scope
    pub sequence: u64,
    /// The recorded client message (request)
    pub client_message: RecordedEvent,
    /// The original response events
    pub response_events: Vec<RecordedEvent>,
    /// The server's response(s) as serialized bytes (optimized for non-complex protocols)
    pub server_responses: Vec<Bytes>,
}

/// Messages grouped by protocol for a single trace
#[derive(Debug, Default)]
pub struct RecordedTrace {
    /// PostgreSQL messages in sequence order
    pub postgres_messages: Vec<MessageExchange>,
    /// Redis messages in sequence order
    pub redis_messages: Vec<MessageExchange>,
    /// HTTP messages in sequence order
    pub http_messages: Vec<MessageExchange>,
    /// gRPC messages in sequence order
    pub grpc_messages: Vec<MessageExchange>,
}

impl RecordedTrace {
    /// Get messages for a specific protocol
    pub fn get_messages(&self, protocol: Protocol) -> &[MessageExchange] {
        match protocol {
            Protocol::Postgres => &self.postgres_messages,
            Protocol::Redis => &self.redis_messages,
            Protocol::Http => &self.http_messages,
            Protocol::Grpc => &self.grpc_messages,
            Protocol::Unknown => &[],
        }
    }

    /// Get mutable messages for a specific protocol
    fn get_messages_mut(&mut self, protocol: Protocol) -> &mut Vec<MessageExchange> {
        match protocol {
            Protocol::Postgres => &mut self.postgres_messages,
            Protocol::Redis => &mut self.redis_messages,
            Protocol::Http => &mut self.http_messages,
            Protocol::Grpc => &mut self.grpc_messages,
            Protocol::Unknown => &mut self.postgres_messages, // Fallback, won't be used
        }
    }

    /// Get a message by sequence number
    pub fn get_by_sequence(&self, protocol: Protocol, sequence: u64) -> Option<&MessageExchange> {
        self.get_messages(protocol).get(sequence as usize)
    }

    /// Count messages for a protocol
    pub fn message_count(&self, protocol: Protocol) -> usize {
        self.get_messages(protocol).len()
    }
}

/// Index recordings by (trace_id, protocol, sequence) for efficient replay lookup
#[derive(Debug, Default)]
pub struct RecordingIndex {
    /// trace_id -> RecordedTrace
    traces: HashMap<String, RecordedTrace>,
}

impl RecordingIndex {
    /// Create a new empty recording index
    pub fn new() -> Self {
        Self::default()
    }

    /// Build index from a flat list of recorded events
    ///
    /// This groups events by (trace_id, protocol) and assigns sequence numbers
    /// based on the order they appear in the recording.
    pub fn from_recordings(recordings: &[RecordedEvent]) -> Self {
        let mut index = Self::new();

        // Group events by (trace_id, connection_id) first, then by protocol
        // We need to track which events are requests vs responses
        let mut current_request: Option<(String, Protocol, RecordedEvent)> = None;
        let mut pending_response_bytes: Vec<Bytes> = Vec::new();
        let mut pending_response_events: Vec<RecordedEvent> = Vec::new();
        let mut sequence_counters: HashMap<(String, Protocol), u64> = HashMap::new();

        for event in recordings {
            let trace_id = &event.trace_id;
            if trace_id.is_empty() {
                continue;
            }

            let protocol = Self::detect_event_protocol(event);
            if matches!(protocol, Protocol::Unknown) {
                continue;
            }

            let is_request = Self::is_client_request(event);

            if is_request {
                // Flush any pending request/responses
                if let Some((req_trace, req_proto, req_event)) = current_request.take() {
                    let seq_key = (req_trace.clone(), req_proto);
                    let seq = *sequence_counters.get(&seq_key).unwrap_or(&0);
                    sequence_counters.insert(seq_key, seq + 1);

                    let exchange = MessageExchange {
                        sequence: seq,
                        client_message: req_event,
                        response_events: std::mem::take(&mut pending_response_events),
                        server_responses: std::mem::take(&mut pending_response_bytes),
                    };

                    index.add_exchange(&req_trace, req_proto, exchange);
                }

                // Start new request
                current_request = Some((trace_id.clone(), protocol, event.clone()));
                pending_response_bytes.clear();
                pending_response_events.clear();
            } else {
                // This is a response, serialize and add to pending
                pending_response_events.push(event.clone());
                if let Some(bytes) = Self::serialize_response(event) {
                    pending_response_bytes.push(bytes);
                }
            }
        }

        // Flush final request
        if let Some((req_trace, req_proto, req_event)) = current_request {
            let seq_key = (req_trace.clone(), req_proto);
            let seq = *sequence_counters.get(&seq_key).unwrap_or(&0);

            let exchange = MessageExchange {
                sequence: seq,
                client_message: req_event,
                response_events: pending_response_events,
                server_responses: pending_response_bytes,
            };

            index.add_exchange(&req_trace, req_proto, exchange);
        }

        index
    }

    /// Add a message exchange to the index
    fn add_exchange(&mut self, trace_id: &str, protocol: Protocol, exchange: MessageExchange) {
        let trace = self.traces.entry(trace_id.to_string()).or_default();
        trace.get_messages_mut(protocol).push(exchange);
    }

    /// Get a specific message exchange by (trace, protocol, sequence)
    pub fn get(
        &self,
        trace_id: &str,
        protocol: Protocol,
        sequence: u64,
    ) -> Option<&MessageExchange> {
        self.traces
            .get(trace_id)?
            .get_by_sequence(protocol, sequence)
    }

    /// Get all trace IDs in the index
    pub fn trace_ids(&self) -> impl Iterator<Item = &String> {
        self.traces.keys()
    }

    /// Get a trace by ID
    pub fn get_trace(&self, trace_id: &str) -> Option<&RecordedTrace> {
        self.traces.get(trace_id)
    }

    /// Get total number of traces
    pub fn trace_count(&self) -> usize {
        self.traces.len()
    }

    /// Detect the protocol of an event
    fn detect_event_protocol(event: &RecordedEvent) -> Protocol {
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

    /// Check if event is a client request (vs server response)
    fn is_client_request(event: &RecordedEvent) -> bool {
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
            _ => false,
        }
    }

    /// Serialize a response event to bytes
    fn serialize_response(event: &RecordedEvent) -> Option<Bytes> {
        // use crate::events::pg_message_event;

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
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{HttpRequestEvent, HttpResponseEvent};

    fn create_http_request(trace_id: &str, path: &str, seq: u64) -> RecordedEvent {
        RecordedEvent {
            trace_id: trace_id.to_string(),
            span_id: "span".to_string(),
            parent_span_id: None,
            sequence: seq,
            timestamp_ns: seq * 1000,
            connection_id: "conn-1".to_string(),
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

    fn create_http_response(trace_id: &str, status: u32, seq: u64) -> RecordedEvent {
        RecordedEvent {
            trace_id: trace_id.to_string(),
            span_id: "span".to_string(),
            parent_span_id: None,
            sequence: seq,
            timestamp_ns: seq * 1000,
            connection_id: "conn-1".to_string(),
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
        let recordings = vec![
            create_http_request("trace-1", "/api/users", 0),
            create_http_response("trace-1", 200, 1),
            create_http_request("trace-1", "/api/posts", 2),
            create_http_response("trace-1", 201, 3),
        ];

        let index = RecordingIndex::from_recordings(&recordings);

        // Should have one trace
        assert_eq!(index.trace_count(), 1);

        // Should have two HTTP message exchanges
        let trace = index.get_trace("trace-1").unwrap();
        assert_eq!(trace.message_count(Protocol::Http), 2);

        // First exchange
        let ex0 = index.get("trace-1", Protocol::Http, 0).unwrap();
        assert_eq!(ex0.sequence, 0);
        if let Some(recorded_event::Event::HttpRequest(req)) = &ex0.client_message.event {
            assert_eq!(req.path, "/api/users");
        } else {
            panic!("Expected HTTP request");
        }

        // Second exchange
        let ex1 = index.get("trace-1", Protocol::Http, 1).unwrap();
        assert_eq!(ex1.sequence, 1);
        if let Some(recorded_event::Event::HttpRequest(req)) = &ex1.client_message.event {
            assert_eq!(req.path, "/api/posts");
        } else {
            panic!("Expected HTTP request");
        }
    }

    #[test]
    fn test_multi_trace_index() {
        let recordings = vec![
            create_http_request("trace-A", "/a", 0),
            create_http_response("trace-A", 200, 1),
            create_http_request("trace-B", "/b", 0),
            create_http_response("trace-B", 200, 1),
        ];

        let index = RecordingIndex::from_recordings(&recordings);

        assert_eq!(index.trace_count(), 2);

        let trace_a = index.get("trace-A", Protocol::Http, 0).unwrap();
        let trace_b = index.get("trace-B", Protocol::Http, 0).unwrap();

        if let Some(recorded_event::Event::HttpRequest(req)) = &trace_a.client_message.event {
            assert_eq!(req.path, "/a");
        }
        if let Some(recorded_event::Event::HttpRequest(req)) = &trace_b.client_message.event {
            assert_eq!(req.path, "/b");
        }
    }

    #[test]
    fn test_protocol_detection() {
        let http_req = create_http_request("t", "/", 0);
        assert_eq!(
            RecordingIndex::detect_event_protocol(&http_req),
            Protocol::Http
        );

        let http_resp = create_http_response("t", 200, 0);
        assert_eq!(
            RecordingIndex::detect_event_protocol(&http_resp),
            Protocol::Http
        );
    }
}
