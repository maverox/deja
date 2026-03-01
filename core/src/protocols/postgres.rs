use super::{ConnectionParser, ParseError, ParseResult, ProtocolParser};
use crate::events::{
    pg_message_event, recorded_event, PgBackendKeyData, PgBind, PgCommandComplete, PgDataRow,
    PgExecute, PgMessageEvent, PgParameterStatus, PgParse, PgQuery, PgReadyForQuery,
    PgSimpleResponse, PgStartup, RecordedEvent,
};
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend;
use std::str;

pub struct PostgresParser;

#[async_trait]
impl ProtocolParser for PostgresParser {
    fn protocol_id(&self) -> &'static str {
        "postgres"
    }

    fn detect(&self, peek: &[u8]) -> f32 {
        if peek.len() < 8 {
            return 0.0;
        }
        // PG Startup Packet: Length (4 bytes) + Protocol Version (4 bytes, 196608 for 3.0)
        let mut buf = &peek[0..8];
        let _len = buf.get_u32(); // Big Endian
        let version = buf.get_u32();

        if version == 196608 {
            // 3.0
            return 1.0;
        }
        // SSL Request: 80877103 or GSSENC Request: 80877104
        if version == 80877103 || version == 80877104 {
            return 0.9;
        }

        // Query packet 'Q' + len
        if peek[0] == b'Q' {
            return 0.5; // Weak heuristic for mid-stream
        }
        0.0
    }

    fn new_connection(&self, connection_id: String) -> Box<dyn ConnectionParser> {
        Box::new(PostgresConnectionParser::new(connection_id))
    }
}

pub struct PostgresConnectionParser {
    client_buf: BytesMut,
    server_buf: BytesMut,
    state: PgState,
    connection_id: String,
    is_replay: bool,
}

enum PgState {
    Startup,
    Normal,
}

impl PostgresConnectionParser {
    pub fn new(connection_id: String) -> Self {
        Self {
            client_buf: BytesMut::new(),
            server_buf: BytesMut::new(),
            state: PgState::Startup,
            connection_id,
            is_replay: false,
        }
    }

    fn parse_startup(buf: &mut BytesMut) -> Option<PgStartup> {
        if buf.len() < 4 {
            return None;
        }
        let len = (&buf[0..4]).get_u32() as usize; // peek length
        if buf.len() < len {
            return None;
        }

        let _ = buf.split_to(4); // consume length
        let _version = buf.get_u32(); // consume version

        // Parse Params: null terminated key/value pairs
        let mut params = std::collections::HashMap::new();
        let body_len = len.saturating_sub(8);
        let mut body = buf.split_to(body_len);

        while body.has_remaining() {
            let key = read_null_term_string(&mut body);
            if key.is_empty() {
                break;
            }
            let val = read_null_term_string(&mut body);
            params.insert(key, val);
        }

        Some(PgStartup {
            user: params.get("user").cloned().unwrap_or_default(),
            database: params.get("database").cloned().unwrap_or_default(),
        })
    }

    fn synthesize_startup_response() -> Bytes {
        let mut buf = BytesMut::new();
        // AuthOk
        buf.extend_from_slice(&PgSerializer::serialize_authentication_ok());
        // ParameterStatus
        buf.extend_from_slice(&PgSerializer::serialize_parameter_status(
            "server_version",
            "14.0",
        ));
        buf.extend_from_slice(&PgSerializer::serialize_parameter_status(
            "client_encoding",
            "UTF8",
        ));
        buf.extend_from_slice(&PgSerializer::serialize_parameter_status(
            "DateStyle",
            "ISO, MDY",
        ));
        buf.extend_from_slice(&PgSerializer::serialize_parameter_status(
            "standard_conforming_strings",
            "on",
        ));
        buf.extend_from_slice(&PgSerializer::serialize_parameter_status("TimeZone", "UTC"));
        // BackendKeyData (dummy)
        buf.extend_from_slice(&PgSerializer::serialize_backend_key_data(1234, 5678));
        // ReadyForQuery
        buf.extend_from_slice(&PgSerializer::serialize_ready_for_query("I"));
        buf.freeze()
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Extract a Deja trace ID from a PG SET application_name query.
///
/// Matches:
///   SET application_name = 'deja:<trace_id>'
///   SET LOCAL application_name = 'deja:<trace_id>'
/// Returns the trace_id portion if found, None otherwise.
pub fn extract_pg_trace_id(query: &str) -> Option<String> {
    let q = query.trim();
    let lower = q.to_lowercase();

    // Must be a SET statement targeting application_name
    let after_set = if lower.starts_with("set local ") {
        &lower["set local ".len()..]
    } else if lower.starts_with("set ") {
        &lower["set ".len()..]
    } else {
        return None;
    };

    let after_set = after_set.trim();
    if !after_set.starts_with("application_name") {
        return None;
    }

    // Find the value after '='
    let after_name = after_set["application_name".len()..].trim();
    let after_eq = after_name.strip_prefix('=')?.trim();

    // Strip surrounding quotes (single or double)
    let value = if (after_eq.starts_with('\'') && after_eq.ends_with('\''))
        || (after_eq.starts_with('"') && after_eq.ends_with('"'))
    {
        &after_eq[1..after_eq.len() - 1]
    } else {
        after_eq
    };

    // Must start with "deja:" prefix
    value.strip_prefix("deja:").map(|id| id.to_string())
}

fn read_null_term_string(buf: &mut BytesMut) -> String {
    if let Some(pos) = buf.iter().position(|&b| b == 0) {
        let bytes = buf.split_to(pos);
        let _ = buf.get_u8(); // consume null
        String::from_utf8_lossy(&bytes).to_string()
    } else {
        // Handle EOF/Incomplete case? For now, consume all
        let bytes = buf.split_to(buf.len());
        String::from_utf8_lossy(&bytes).to_string()
    }
}

#[async_trait]
impl ConnectionParser for PostgresConnectionParser {
    fn parse_client_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.client_buf.extend_from_slice(data);
        let mut events = Vec::new();
        let mut reply = None;

        loop {
            if self.client_buf.is_empty() {
                break;
            }

            match self.state {
                PgState::Startup => {
                    if self.client_buf.len() < 4 {
                        break;
                    }
                    if self.client_buf.len() >= 8 {
                        let len_peek =
                            u32::from_be_bytes(self.client_buf[0..4].try_into().unwrap());
                        let ver_peek =
                            u32::from_be_bytes(self.client_buf[4..8].try_into().unwrap());
                        if len_peek == 8 && (ver_peek == 80877103 || ver_peek == 80877104) {
                            // SSL/GSSENC -> Cancel
                            let _ = self.client_buf.split_to(8);
                            return Ok(ParseResult {
                                events: vec![],
                                forward: Bytes::new(),
                                needs_more: false,
                                reply: Some(Bytes::from_static(b"N")),
                            });
                        }
                    }

                    if let Some(startup) = Self::parse_startup(&mut self.client_buf) {
                        events.push(RecordedEvent {
                            trace_id: String::new(),
                            scope_id: String::new(),
                            scope_sequence: 0,
                            global_sequence: 0,
                            timestamp_ns: now_ns(),
                            direction: 0,
                            event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                                message: Some(pg_message_event::Message::Startup(startup)),
                            })),
                            metadata: Default::default(),
                        });
                        self.state = PgState::Normal;

                        // SYNTHETIC RESPONSE
                        if self.is_replay {
                            reply = Some(Self::synthesize_startup_response());
                        }
                    } else {
                        let len = (&self.client_buf[0..4]).get_u32() as usize;
                        if self.client_buf.len() + 4 < len {
                            break;
                        }
                        break;
                    }
                }
                PgState::Normal => {
                    // Manual Frontend Parsing
                    if self.client_buf.len() < 5 {
                        break;
                    }
                    let tag = self.client_buf[0];
                    let len_total = (&self.client_buf[1..5]).get_u32() as usize;
                    // Length includes self (4 bytes), so total frame is 1 + len_total.
                    // But wait, get_u32 advances? No, &self.client_buf[1..5] is a slice.
                    let required = 1 + len_total;
                    if self.client_buf.len() < required {
                        break;
                    }

                    // Consume frame
                    let mut frame = self.client_buf.split_to(required);
                    frame.advance(5); // skip tag (1) and len (4)
                    let mut body = frame;

                    let message = match tag {
                        b'Q' => {
                            let query = read_null_term_string(&mut body);

                            // Check for SELECT 1 health check in Replay Mode
                            if self.is_replay
                                && (query.trim().eq_ignore_ascii_case("SELECT 1")
                                    || query.trim().eq_ignore_ascii_case("SELECT 1;"))
                            {
                                let mut buf = BytesMut::new();

                                // RowDescription
                                let fields = vec![crate::events::PgColumn {
                                    name: "?column?".to_string(),
                                    table_oid: 0,
                                    type_oid: 23, // int4/integer
                                }];
                                let row_desc = crate::events::PgRowDescription { fields };
                                buf.extend_from_slice(&PgSerializer::serialize_row_desc(&row_desc));

                                // DataRow
                                let values = vec![b"1".to_vec()];
                                let row = crate::events::PgDataRow { values };
                                buf.extend_from_slice(&PgSerializer::serialize_data_row(&row));

                                // CommandComplete
                                let cc = crate::events::PgCommandComplete {
                                    tag: "SELECT 1".to_string(),
                                };
                                buf.extend_from_slice(&PgSerializer::serialize_command_complete(
                                    &cc,
                                ));

                                // ReadyForQuery
                                buf.extend_from_slice(&PgSerializer::serialize_ready_for_query(
                                    "I",
                                ));

                                reply = Some(buf.freeze());
                            }

                            Some(pg_message_event::Message::Query(PgQuery { query }))
                        }
                        b'P' => {
                            // Parse
                            let name = read_null_term_string(&mut body);
                            let query = read_null_term_string(&mut body);
                            let num_params = body.get_u16();
                            let mut param_types = Vec::new();
                            for _ in 0..num_params {
                                param_types.push(body.get_u32());
                            }
                            Some(pg_message_event::Message::Parse(PgParse {
                                name,
                                query,
                                param_types,
                            }))
                        }
                        b'B' => {
                            // Bind
                            let portal = read_null_term_string(&mut body);
                            let statement = read_null_term_string(&mut body);
                            // Skip formats, values, result formats for now (complex)
                            Some(pg_message_event::Message::Bind(PgBind {
                                portal,
                                statement,
                            }))
                        }
                        b'E' => {
                            // Execute
                            let portal = read_null_term_string(&mut body);
                            let max_rows = body.get_u32();
                            Some(pg_message_event::Message::Execute(PgExecute {
                                portal,
                                max_rows,
                            }))
                        }
                        b'S' => Some(pg_message_event::Message::Sync(PgSimpleResponse {})),
                        b'X' => None, // Terminate
                        _ => None,    // Unknown
                    };

                    if let Some(msg) = message {
                        events.push(RecordedEvent {
                            trace_id: String::new(),
                            scope_id: String::new(),
                            scope_sequence: 0,
                            global_sequence: 0,
                            timestamp_ns: now_ns(),
                            direction: 0,
                            event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                                message: Some(msg),
                            })),
                            metadata: Default::default(),
                        });
                    }
                }
            }
        }

        // If we synthesized a reply, we shouldn't forward the data if there is no upstream
        // However, in mock mode, there IS no upstream.
        // In full protocol simulation, we rely on the Replay Engine matching events.
        // BUT, the Startup Packet is technically "matched" by our synthetic response logic here.
        // So we should NOT include the data in `forward` if we synthesized a response.
        let forward = if reply.is_some() {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(data)
        };

        Ok(ParseResult {
            events,
            forward,
            needs_more: false,
            reply,
        })
    }

    fn parse_server_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.server_buf.extend_from_slice(data);
        let mut events = Vec::new();

        loop {
            if self.server_buf.is_empty() {
                break;
            }

            // Using backend::Message::parse
            match backend::Message::parse(&mut self.server_buf) {
                Ok(Some(msg)) => {
                    if let Some(event_msg) = map_backend_message(msg) {
                        events.push(RecordedEvent {
                            trace_id: String::new(),
                            scope_id: String::new(),
                            scope_sequence: 0,
                            global_sequence: 0,
                            timestamp_ns: now_ns(),
                            direction: 0,
                            event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                                message: Some(event_msg),
                            })),
                            metadata: Default::default(),
                        });
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    println!("[Postgres] Backend Parse error: {:?}", e);
                    break;
                }
            }
        }

        Ok(ParseResult {
            events,
            forward: Bytes::copy_from_slice(data),
            needs_more: false,
            reply: None,
        })
    }

    fn connection_id(&self) -> &str {
        &self.connection_id
    }

    fn reset(&mut self) {
        self.client_buf.clear();
        self.server_buf.clear();
        self.state = PgState::Startup;
    }

    fn set_mode(&mut self, is_replay: bool) {
        self.is_replay = is_replay;
    }
}

// Map only backend messages as we manually parse frontend
fn map_backend_message(msg: backend::Message) -> Option<pg_message_event::Message> {
    match msg {
        backend::Message::AuthenticationOk => Some(pg_message_event::Message::AuthenticationOk(
            PgSimpleResponse {},
        )),
        backend::Message::ParameterStatus(b) => Some(pg_message_event::Message::ParameterStatus(
            PgParameterStatus {
                name: b.name().ok()?.to_string(),
                value: b.value().ok()?.to_string(),
            },
        )),
        backend::Message::BackendKeyData(b) => Some(pg_message_event::Message::BackendKeyData(
            PgBackendKeyData {
                process_id: b.process_id() as u32,
                secret_key: b.secret_key() as u32,
            },
        )),
        backend::Message::ReadyForQuery(b) => {
            Some(pg_message_event::Message::ReadyForQuery(PgReadyForQuery {
                status: (b.status() as char).to_string(),
            }))
        }
        backend::Message::ParseComplete => Some(pg_message_event::Message::ParseComplete(
            PgSimpleResponse {},
        )),
        backend::Message::BindComplete => {
            Some(pg_message_event::Message::BindComplete(PgSimpleResponse {}))
        }
        backend::Message::RowDescription(b) => {
            let mut fields = Vec::new();
            let mut iter = b.fields();
            while let Ok(Some(f)) = iter.next() {
                fields.push(crate::events::PgColumn {
                    name: f.name().to_string(),
                    table_oid: f.table_oid(),
                    type_oid: f.type_oid(),
                });
            }
            Some(pg_message_event::Message::RowDesc(
                crate::events::PgRowDescription { fields },
            ))
        }
        backend::Message::DataRow(b) => {
            let mut values = Vec::new();
            // Use ranges() + buffer() for data row access
            let mut iter = b.ranges();
            while let Ok(Some(range)) = iter.next() {
                if let Some(r) = range {
                    values.push(b.buffer()[r].to_vec());
                } else {
                    values.push(vec![]);
                }
            }
            Some(pg_message_event::Message::DataRow(PgDataRow { values }))
        }
        backend::Message::CommandComplete(b) => Some(pg_message_event::Message::CommandComplete(
            PgCommandComplete {
                tag: b.tag().ok()?.to_string(),
            },
        )),
        _ => None,
    }
}

pub struct PgSerializer;

impl PgSerializer {
    pub fn serialize_message(msg: &pg_message_event::Message) -> Option<Bytes> {
        match msg {
            pg_message_event::Message::RowDesc(desc) => Some(Self::serialize_row_desc(desc)),
            pg_message_event::Message::DataRow(row) => Some(Self::serialize_data_row(row)),
            pg_message_event::Message::CommandComplete(cc) => {
                Some(Self::serialize_command_complete(cc))
            }
            pg_message_event::Message::AuthenticationOk(_) => {
                Some(Self::serialize_authentication_ok())
            }
            pg_message_event::Message::ParameterStatus(ps) => {
                Some(Self::serialize_parameter_status(&ps.name, &ps.value))
            }
            pg_message_event::Message::BackendKeyData(bk) => Some(
                Self::serialize_backend_key_data(bk.process_id, bk.secret_key),
            ),
            pg_message_event::Message::ReadyForQuery(rfq) => {
                Some(Self::serialize_ready_for_query(&rfq.status))
            }

            pg_message_event::Message::ParseComplete(_) => Some(Self::serialize_simple_tag(b'1')),
            pg_message_event::Message::BindComplete(_) => Some(Self::serialize_simple_tag(b'2')),
            pg_message_event::Message::NoData(_) => Some(Self::serialize_simple_tag(b'n')),
            // Requests (for debug or other replay modes)
            pg_message_event::Message::Query(q) => Some(Self::serialize_simple_query(q)),
            pg_message_event::Message::Parse(p) => Some(Self::serialize_parse(p)),
            pg_message_event::Message::Bind(b) => Some(Self::serialize_bind(b)),
            pg_message_event::Message::Execute(e) => Some(Self::serialize_execute(e)),
            pg_message_event::Message::Sync(_) => Some(Self::serialize_simple_tag(b'S')),
            _ => None,
        }
    }

    fn serialize_simple_tag(tag: u8) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(tag);
        buf.put_u32(4); // Length 4 (just the length field)
        buf.freeze()
    }

    fn serialize_simple_query(q: &PgQuery) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_u32((q.query.len() + 5) as u32);
        buf.put_slice(q.query.as_bytes());
        buf.put_u8(0);
        buf.freeze()
    }

    fn serialize_parse(p: &PgParse) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'P');
        let len = 4 + p.name.len() + 1 + p.query.len() + 1 + 2 + p.param_types.len() * 4;
        buf.put_u32(len as u32);
        buf.put_slice(p.name.as_bytes());
        buf.put_u8(0);
        buf.put_slice(p.query.as_bytes());
        buf.put_u8(0);
        buf.put_u16(p.param_types.len() as u16);
        for &t in &p.param_types {
            buf.put_u32(t);
        }
        buf.freeze()
    }

    fn serialize_bind(b: &PgBind) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'B');
        // Simplified bind serialization (missing param formats/values)
        let len = 4 + b.portal.len() + 1 + b.statement.len() + 1 + 2 + 2 + 2;
        buf.put_u32(len as u32);
        buf.put_slice(b.portal.as_bytes());
        buf.put_u8(0);
        buf.put_slice(b.statement.as_bytes());
        buf.put_u8(0);
        buf.put_u16(0); // param formats
        buf.put_u16(0); // param values
        buf.put_u16(0); // result formats
        buf.freeze()
    }

    fn serialize_execute(e: &PgExecute) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'E');
        let len = 4 + e.portal.len() + 1 + 4;
        buf.put_u32(len as u32);
        buf.put_slice(e.portal.as_bytes());
        buf.put_u8(0);
        buf.put_u32(e.max_rows);
        buf.freeze()
    }

    fn serialize_row_desc(desc: &crate::events::PgRowDescription) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'T');
        // Reserve space for length (4 bytes)
        buf.put_i32(0);

        buf.put_u16(desc.fields.len() as u16);
        for field in &desc.fields {
            buf.put_slice(field.name.as_bytes());
            buf.put_u8(0); // null term
            buf.put_u32(field.table_oid); // table_oid
            buf.put_u16(0); // col_attr (default 0)
            buf.put_u32(field.type_oid); // type_oid
            buf.put_i16(-1); // type_len (var len -1)
            buf.put_u32(0); // type_mod
            buf.put_i16(0); // format (text 0)
        }

        let len = buf.len() as i32;
        // Write length at index 1
        let mut slice = &mut buf[1..5];
        slice.put_i32(len - 1); // length includes self but not tag? No, PG packet length includes length field itself (4 bytes) but not tag.
                                // Wait, pg protocol: "The length count includes the size of the length field itself"
                                // So len is buf.len() - 1 (exclude tag).

        buf.freeze()
    }

    fn serialize_data_row(row: &PgDataRow) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'D');
        buf.put_i32(0); // len placeholder

        buf.put_u16(row.values.len() as u16);
        for val in &row.values {
            if val.is_empty() {
                buf.put_i32(-1);
            } else {
                buf.put_i32(val.len() as i32);
                buf.put_slice(val);
            }
        }

        let len = (buf.len() - 1) as i32;
        let mut slice = &mut buf[1..5];
        slice.put_i32(len);

        buf.freeze()
    }

    fn serialize_command_complete(cc: &PgCommandComplete) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'C');
        buf.put_i32(0);

        buf.put_slice(cc.tag.as_bytes());
        buf.put_u8(0);

        let len = (buf.len() - 1) as i32;
        let mut slice = &mut buf[1..5];
        slice.put_i32(len);

        buf.freeze()
    }

    pub fn serialize_authentication_ok() -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'R');
        buf.put_u32(8);
        buf.put_u32(0); // Auth Type 0 (Success)
        buf.freeze()
    }

    pub fn serialize_parameter_status(name: &str, value: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'S');
        let len = 4 + name.len() + 1 + value.len() + 1;
        buf.put_u32(len as u32);
        buf.put_slice(name.as_bytes());
        buf.put_u8(0);
        buf.put_slice(value.as_bytes());
        buf.put_u8(0);
        buf.freeze()
    }

    pub fn serialize_backend_key_data(process_id: u32, secret_key: u32) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'K');
        buf.put_u32(12);
        buf.put_u32(process_id);
        buf.put_u32(secret_key);
        buf.freeze()
    }

    pub fn serialize_ready_for_query(status: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Z');
        buf.put_u32(5);
        buf.put_u8(status.as_bytes().first().cloned().unwrap_or(b'I'));
        buf.freeze()
    }

    pub fn serialize_error(message: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(b'E');
        buf.put_i32(0); // placeholder

        // Severity: ERROR
        buf.put_u8(b'S');
        buf.put_slice(b"ERROR\0");

        // Code: XX000 (Internal Error)
        buf.put_u8(b'C');
        buf.put_slice(b"XX000\0");

        // Message
        buf.put_u8(b'M');
        buf.put_slice(message.as_bytes());
        buf.put_u8(0);

        // Terminator
        buf.put_u8(0);

        let len = (buf.len() - 1) as i32;
        let mut slice = &mut buf[1..5];
        slice.put_i32(len);

        buf.freeze()
    }
}
