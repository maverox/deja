use super::{ConnectionParser, ParseError, ParseResult, ProtocolParser};
use crate::events::{recorded_event, RecordedEvent, TcpDataEvent};
use async_trait::async_trait;
use bytes::Bytes;

pub struct GenericTcpParser;

#[async_trait]
impl ProtocolParser for GenericTcpParser {
    fn protocol_id(&self) -> &'static str {
        "tcp"
    }

    fn detect(&self, _peek: &[u8]) -> f32 {
        // Very low confidence as a fallback
        0.1
    }

    fn new_connection(&self, connection_id: String) -> Box<dyn ConnectionParser> {
        Box::new(GenericTcpConnectionParser::new(connection_id))
    }
}

pub struct GenericTcpConnectionParser {
    connection_id: String,
    is_replay: bool,
}

impl GenericTcpConnectionParser {
    pub fn new(connection_id: String) -> Self {
        Self {
            connection_id,
            is_replay: false,
        }
    }
}

#[async_trait]
impl ConnectionParser for GenericTcpConnectionParser {
    fn parse_client_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        let event = RecordedEvent {
            trace_id: String::new(),
            scope_id: String::new(),
            scope_sequence: 0,
            global_sequence: 0,
            timestamp_ns: 0,
            direction: 0,
            event: Some(recorded_event::Event::TcpData(TcpDataEvent {
                data: data.to_vec(),
                is_client: true,
            })),
            metadata: Default::default(),
        };

        Ok(ParseResult {
            events: vec![event],
            forward: Bytes::copy_from_slice(data),
            needs_more: false,
            reply: None,
        })
    }

    fn parse_server_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        let event = RecordedEvent {
            trace_id: String::new(),
            scope_id: String::new(),
            scope_sequence: 0,
            global_sequence: 0,
            timestamp_ns: 0,
            direction: 0,
            event: Some(recorded_event::Event::TcpData(TcpDataEvent {
                data: data.to_vec(),
                is_client: false,
            })),
            metadata: Default::default(),
        };

        Ok(ParseResult {
            events: vec![event],
            forward: Bytes::copy_from_slice(data),
            needs_more: false,
            reply: None,
        })
    }

    fn connection_id(&self) -> &str {
        &self.connection_id
    }

    fn reset(&mut self) {}

    fn set_mode(&mut self, is_replay: bool) {
        self.is_replay = is_replay;
    }
}
