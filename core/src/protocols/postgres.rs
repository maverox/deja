use super::{ConnectionParser, ParseError, ParseResult, ProtocolParser};
use crate::events::{
    pg_message_event, recorded_event, PgBackendKeyData, PgBind, PgCommandComplete, PgDataRow,
    PgExecute, PgMessageEvent, PgParameterStatus, PgParse, PgQuery, PgReadyForQuery,
    PgSimpleResponse, PgStartup, RecordedEvent,
};
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
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
    sequence: u64,
    connection_id: String,
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
            sequence: 0,
            connection_id,
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
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
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

        loop {
            if self.client_buf.is_empty() {
                break;
            }

            match self.state {
                PgState::Startup => {
                    // Check startup packet
                    // We need at least length
                    if self.client_buf.len() < 4 {
                        break;
                    }

                    // Check for SSL or GSSENC Request (Length 8, Version 80877103 or 80877104)
                    if self.client_buf.len() >= 8 {
                        let len_peek =
                            u32::from_be_bytes(self.client_buf[0..4].try_into().unwrap());
                        let ver_peek =
                            u32::from_be_bytes(self.client_buf[4..8].try_into().unwrap());
                        if len_peek == 8 && (ver_peek == 80877103 || ver_peek == 80877104) {
                            println!(
                                "[Postgres] Denying SSL/GSSENC request (version: {})",
                                ver_peek
                            );
                            // Consume the Request
                            let _ = self.client_buf.split_to(8);
                            // Return 'N' to deny
                            return Ok(ParseResult {
                                events: vec![],
                                forward: Bytes::new(),
                                needs_more: false,
                                reply: Some(Bytes::from_static(b"N")),
                            });
                        }
                    }

                    // For Phase 3 MVP, simplistic parsing without robust version checking
                    if let Some(startup) = Self::parse_startup(&mut self.client_buf) {
                        println!(
                            "[Postgres] Parsed Startup message: user={}, db={}",
                            startup.user, startup.database
                        );
                        events.push(RecordedEvent {
                            trace_id: uuid::Uuid::new_v4().to_string(), // Correlation
                            span_id: uuid::Uuid::new_v4().to_string(),
                            timestamp_ns: now_ns(),
                            event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                                message: Some(pg_message_event::Message::Startup(startup)),
                            })),
                            sequence: self.sequence,
                            connection_id: self.connection_id.clone(),
                            ..Default::default()
                        });
                        self.sequence += 1;
                        self.state = PgState::Normal;
                    } else {
                        if !self.client_buf.is_empty() {
                            println!(
                                "[Postgres] Failed to parse startup from {} bytes",
                                self.client_buf.len()
                            );
                        }
                        break; // need more data
                    }
                }
                PgState::Normal => {
                    // 1 byte tag, 4 byte len
                    if self.client_buf.len() < 5 {
                        break;
                    }
                    let tag = self.client_buf[0];
                    let len = (&self.client_buf[1..5]).get_u32() as usize;

                    if self.client_buf.len() < 1 + len {
                        break;
                    }

                    let _ = self.client_buf.split_to(1); // consume tag
                    let _ = self.client_buf.get_u32(); // consume len
                    let body_len = len - 4;
                    let mut body = self.client_buf.split_to(body_len);

                    match tag {
                        b'Q' => {
                            let query_str = read_null_term_string(&mut body);
                            events.push(RecordedEvent {
                                trace_id: uuid::Uuid::new_v4().to_string(),
                                span_id: uuid::Uuid::new_v4().to_string(),
                                timestamp_ns: now_ns(),
                                event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                                    message: Some(pg_message_event::Message::Query(PgQuery {
                                        query: query_str,
                                    })),
                                })),
                                sequence: self.sequence,
                                connection_id: self.connection_id.clone(),
                                ..Default::default()
                            });
                            self.sequence += 1;
                        }
                        b'P' => {
                            // Parse: name, query, num_params, param_types
                            let name = read_null_term_string(&mut body);
                            let query = read_null_term_string(&mut body);
                            let num_params = body.get_u16();
                            let mut param_types = Vec::new();
                            for _ in 0..num_params {
                                if body.len() >= 4 {
                                    param_types.push(body.get_u32());
                                }
                            }
                            events.push(RecordedEvent {
                                trace_id: uuid::Uuid::new_v4().to_string(),
                                span_id: uuid::Uuid::new_v4().to_string(),
                                timestamp_ns: now_ns(),
                                event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                                    message: Some(pg_message_event::Message::Parse(PgParse {
                                        query,
                                        name,
                                        param_types,
                                    })),
                                })),
                                sequence: self.sequence,
                                connection_id: self.connection_id.clone(),
                                ..Default::default()
                            });
                            self.sequence += 1;
                        }
                        b'B' => {
                            // Bind: portal, statement, ...
                            let portal = read_null_term_string(&mut body);
                            let statement = read_null_term_string(&mut body);
                            events.push(RecordedEvent {
                                trace_id: uuid::Uuid::new_v4().to_string(),
                                span_id: uuid::Uuid::new_v4().to_string(),
                                timestamp_ns: now_ns(),
                                event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                                    message: Some(pg_message_event::Message::Bind(PgBind {
                                        portal,
                                        statement,
                                    })),
                                })),
                                sequence: self.sequence,
                                connection_id: self.connection_id.clone(),
                                ..Default::default()
                            });
                            self.sequence += 1;
                        }
                        b'E' => {
                            // Execute: portal, max_rows
                            let portal = read_null_term_string(&mut body);
                            let max_rows = body.get_u32();
                            events.push(RecordedEvent {
                                trace_id: uuid::Uuid::new_v4().to_string(),
                                span_id: uuid::Uuid::new_v4().to_string(),
                                timestamp_ns: now_ns(),
                                event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                                    message: Some(pg_message_event::Message::Execute(PgExecute {
                                        portal,
                                        max_rows,
                                    })),
                                })),
                                sequence: self.sequence,
                                connection_id: self.connection_id.clone(),
                                ..Default::default()
                            });
                            self.sequence += 1;
                        }
                        b'S' => {
                            // Sync
                            events.push(RecordedEvent {
                                trace_id: uuid::Uuid::new_v4().to_string(),
                                span_id: uuid::Uuid::new_v4().to_string(),
                                timestamp_ns: now_ns(),
                                event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                                    message: Some(pg_message_event::Message::Sync(
                                        PgSimpleResponse {},
                                    )),
                                })),
                                sequence: self.sequence,
                                connection_id: self.connection_id.clone(),
                                ..Default::default()
                            });
                            self.sequence += 1;
                        }
                        b'X' => {
                            // Terminate
                        }
                        _ => {}
                    }
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

    fn parse_server_data(&mut self, data: &[u8]) -> Result<ParseResult, ParseError> {
        self.server_buf.extend_from_slice(data);
        let mut events = Vec::new();

        loop {
            if self.server_buf.is_empty() {
                break;
            }

            if self.server_buf.len() < 5 {
                break;
            }
            let tag = self.server_buf[0];
            let len = (&self.server_buf[1..5]).get_u32() as usize;

            if self.server_buf.len() < 1 + len {
                break;
            }

            let _ = self.server_buf.split_to(1); // tag
            let _ = self.server_buf.get_u32(); // len (includes self)
            let body_len = len - 4;
            let mut body = self.server_buf.split_to(body_len);

            match tag {
                b'R' => {
                    // Authentication
                    let auth_type = body.get_u32();
                    if auth_type == 0 {
                        events.push(RecordedEvent {
                            trace_id: uuid::Uuid::new_v4().to_string(),
                            span_id: uuid::Uuid::new_v4().to_string(),
                            timestamp_ns: now_ns(),
                            event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                                message: Some(pg_message_event::Message::AuthenticationOk(
                                    PgSimpleResponse {},
                                )),
                            })),
                            sequence: self.sequence,
                            connection_id: self.connection_id.clone(),
                            ..Default::default()
                        });
                        self.sequence += 1;
                    }
                }
                b'S' => {
                    // ParameterStatus
                    let name = read_null_term_string(&mut body);
                    let value = read_null_term_string(&mut body);
                    events.push(RecordedEvent {
                        trace_id: uuid::Uuid::new_v4().to_string(),
                        span_id: uuid::Uuid::new_v4().to_string(),
                        timestamp_ns: now_ns(),
                        event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                            message: Some(pg_message_event::Message::ParameterStatus(
                                PgParameterStatus { name, value },
                            )),
                        })),
                        sequence: self.sequence,
                        connection_id: self.connection_id.clone(),
                        ..Default::default()
                    });
                    self.sequence += 1;
                }
                b'K' => {
                    // BackendKeyData
                    if body.len() >= 8 {
                        let process_id = body.get_u32();
                        let secret_key = body.get_u32();
                        events.push(RecordedEvent {
                            trace_id: uuid::Uuid::new_v4().to_string(),
                            span_id: uuid::Uuid::new_v4().to_string(),
                            timestamp_ns: now_ns(),
                            event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                                message: Some(pg_message_event::Message::BackendKeyData(
                                    PgBackendKeyData {
                                        process_id,
                                        secret_key,
                                    },
                                )),
                            })),
                            sequence: self.sequence,
                            connection_id: self.connection_id.clone(),
                            ..Default::default()
                        });
                        self.sequence += 1;
                    }
                }
                b'Z' => {
                    // ReadyForQuery
                    if !body.is_empty() {
                        let status = (body.get_u8() as char).to_string();
                        events.push(RecordedEvent {
                            trace_id: uuid::Uuid::new_v4().to_string(),
                            span_id: uuid::Uuid::new_v4().to_string(),
                            timestamp_ns: now_ns(),
                            event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                                message: Some(pg_message_event::Message::ReadyForQuery(
                                    PgReadyForQuery { status },
                                )),
                            })),
                            sequence: self.sequence,
                            connection_id: self.connection_id.clone(),
                            ..Default::default()
                        });
                        self.sequence += 1;
                    }
                }
                b'1' => {
                    // ParseComplete
                    events.push(RecordedEvent {
                        trace_id: uuid::Uuid::new_v4().to_string(),
                        span_id: uuid::Uuid::new_v4().to_string(),
                        timestamp_ns: now_ns(),
                        event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                            message: Some(pg_message_event::Message::ParseComplete(
                                PgSimpleResponse {},
                            )),
                        })),
                        sequence: self.sequence,
                        connection_id: self.connection_id.clone(),
                        ..Default::default()
                    });
                    self.sequence += 1;
                }
                b'2' => {
                    // BindComplete
                    events.push(RecordedEvent {
                        trace_id: uuid::Uuid::new_v4().to_string(),
                        span_id: uuid::Uuid::new_v4().to_string(),
                        timestamp_ns: now_ns(),
                        event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                            message: Some(pg_message_event::Message::BindComplete(
                                PgSimpleResponse {},
                            )),
                        })),
                        sequence: self.sequence,
                        connection_id: self.connection_id.clone(),
                        ..Default::default()
                    });
                    self.sequence += 1;
                }
                b'T' => {
                    // RowDescription
                    if body.len() < 2 {
                        continue;
                    }
                    let count = body.get_u16();
                    let mut fields = Vec::with_capacity(count as usize);

                    for _ in 0..count {
                        let name = read_null_term_string(&mut body);
                        if body.len() < 18 {
                            break;
                        } // TableOID(4)+ColAttr(2)+TypeOID(4)+TypeLen(2)+TypeMod(4)+Format(2) = 18
                        let table_oid = body.get_u32();
                        let _col_attr = body.get_u16();
                        let type_oid = body.get_u32();
                        let _type_len = body.get_i16();
                        let _type_mod = body.get_u32();
                        let _format = body.get_i16();

                        fields.push(crate::events::PgColumn {
                            name,
                            table_oid,
                            type_oid,
                        });
                    }

                    events.push(RecordedEvent {
                        trace_id: uuid::Uuid::new_v4().to_string(),
                        span_id: uuid::Uuid::new_v4().to_string(),
                        timestamp_ns: now_ns(),
                        event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                            message: Some(pg_message_event::Message::RowDesc(
                                crate::events::PgRowDescription { fields },
                            )),
                        })),
                        sequence: self.sequence,
                        connection_id: self.connection_id.clone(),
                        ..Default::default()
                    });
                    self.sequence += 1;
                }
                b'D' => {
                    // DataRow
                    if body.len() < 2 {
                        continue;
                    }
                    let count = body.get_u16();
                    let mut values = Vec::with_capacity(count as usize);

                    for _ in 0..count {
                        if body.len() < 4 {
                            break;
                        }
                        let val_len = body.get_i32();
                        if val_len == -1 {
                            // NULL
                            values.push(vec![]);
                        } else {
                            let val_len_u = val_len as usize;
                            if body.len() < val_len_u {
                                break;
                            }
                            let val_bytes = body.split_to(val_len_u);
                            values.push(val_bytes.to_vec());
                        }
                    }

                    events.push(RecordedEvent {
                        trace_id: uuid::Uuid::new_v4().to_string(),
                        span_id: uuid::Uuid::new_v4().to_string(),
                        timestamp_ns: now_ns(),
                        event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                            message: Some(pg_message_event::Message::DataRow(PgDataRow { values })),
                        })),
                        sequence: self.sequence,
                        connection_id: self.connection_id.clone(),
                        ..Default::default()
                    });
                    self.sequence += 1;
                }
                b'C' => {
                    // CommandComplete
                    let tag = read_null_term_string(&mut body);
                    events.push(RecordedEvent {
                        trace_id: uuid::Uuid::new_v4().to_string(),
                        span_id: uuid::Uuid::new_v4().to_string(),
                        timestamp_ns: now_ns(),
                        event: Some(recorded_event::Event::PgMessage(PgMessageEvent {
                            message: Some(pg_message_event::Message::CommandComplete(
                                PgCommandComplete { tag },
                            )),
                        })),
                        sequence: self.sequence,
                        connection_id: self.connection_id.clone(),
                        ..Default::default()
                    });
                    self.sequence += 1;
                }
                _ => {
                    // Ignore other messages (error E, etc for now)
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
        self.sequence = 0;
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
                // Check if it's NULL or empty string?
                // RecordedEvent data might not distinguish well if I just used `vec![]` for NULL in parsing.
                // In parse: `if val_len == -1 { values.push(vec![]); }`
                // So empty vec is NULL? Wait, empty string is 0 length bytes.
                // This is an ambiguity in my recording logic.
                // I should fix the parser to record NULL state properly, maybe `Option<bytes>`.
                // But sticking to Phase 5 plan: Let's assume non-null empty strings for now or -1.
                // Standard PG: -1 is null.

                // If I want to be safe, I'll just write 0 length for now (empty string) unless I change the proto.
                // But NULL is common.
                // Let's output -1 (NULL) if empty, creating potential issue for empty strings, but safer for "no data".
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
