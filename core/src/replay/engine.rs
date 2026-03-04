use super::RecordingIndex;
use crate::events::{pg_message_event, recorded_event, redis_value, RecordedEvent};
use deja_common::{Protocol, ScopeId, ScopeSequenceTracker};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use tokio::fs;
use tracing::{info, warn};

/// Mode for the proxy operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayMode {
    /// Pass through to real backends and record all traffic
    Recording,
    /// Return recorded responses without hitting real backends
    FullMock,
    /// Orchestrated replay - trigger recorded flows against service
    Orchestrated,
}

impl Default for ReplayMode {
    fn default() -> Self {
        Self::Recording
    }
}

impl ReplayMode {
    /// Parse from string (for env var)
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "record" | "recording" => Self::Recording,
            "replay" | "fullmock" | "mock" => Self::FullMock,
            "orchestrated" | "orchestrate" => Self::Orchestrated,
            _ => Self::Recording,
        }
    }
}

pub type ReplayMatch = (RecordedEvent, Vec<RecordedEvent>, Vec<bytes::Bytes>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayMatchError {
    UnresolvedAttribution { trace_id: String, scope_id: String },
}

impl ReplayMatchError {
    pub const UNRESOLVED_ATTRIBUTION_MESSAGE: &'static str = "Replay attribution unresolved";

    pub fn code(&self) -> &'static str {
        match self {
            Self::UnresolvedAttribution { .. } => "unresolved_attribution",
        }
    }

    pub fn stable_message(&self) -> &'static str {
        Self::UNRESOLVED_ATTRIBUTION_MESSAGE
    }
}

impl fmt::Display for ReplayMatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnresolvedAttribution { trace_id, scope_id } => {
                write!(
                    f,
                    "{} (trace_id='{}', scope_id='{}')",
                    self.stable_message(),
                    trace_id,
                    scope_id
                )
            }
        }
    }
}

impl std::error::Error for ReplayMatchError {}

/// Configuration for protocol-specific matching
pub struct MatchConfig {
    pub http: HttpMatchConfig,
    pub postgres: PgMatchConfig,
    pub redis: RedisMatchConfig,
    pub grpc: GrpcMatchConfig,
}

impl Default for MatchConfig {
    fn default() -> Self {
        Self {
            http: HttpMatchConfig::default(),
            postgres: PgMatchConfig::default(),
            redis: RedisMatchConfig::default(),
            grpc: GrpcMatchConfig::default(),
        }
    }
}

pub struct HttpMatchConfig {
    /// Compare body content
    pub compare_body: bool,
    /// Compare headers (all headers compared if true)
    pub compare_headers: bool,
}

impl Default for HttpMatchConfig {
    fn default() -> Self {
        Self {
            compare_body: true,
            compare_headers: true,
        }
    }
}

pub struct PgMatchConfig {
    /// Compare query text
    pub compare_query_text: bool,
    /// Ignore statement names (prepared statement name variations)
    pub ignore_statement_names: bool,
}

impl Default for PgMatchConfig {
    fn default() -> Self {
        Self {
            compare_query_text: true,
            ignore_statement_names: true,
        }
    }
}

pub struct RedisMatchConfig {
    /// Tolerance for TTL values in milliseconds
    pub ttl_tolerance_ms: u64,
}

impl Default for RedisMatchConfig {
    fn default() -> Self {
        Self {
            ttl_tolerance_ms: 1000,
        }
    }
}

pub struct GrpcMatchConfig {
    /// Compare request body
    pub compare_request_body: bool,
}

impl Default for GrpcMatchConfig {
    fn default() -> Self {
        Self {
            compare_request_body: true,
        }
    }
}

pub struct ReplayEngine {
    /// All recordings (kept for orchestration methods and ND value lookup)
    recordings: Vec<RecordedEvent>,

    /// Scope-tree indexed recordings for deterministic matching
    index: RecordingIndex,

    /// Track current scope_sequence per scope
    sequence_tracker: ScopeSequenceTracker,

    /// Protocol-specific match configuration
    match_config: MatchConfig,

    /// Cursor tracking for legacy `handle_runtime_request` (keyed by "trace_id:kind")
    runtime_cursor: HashMap<String, usize>,
}

impl ReplayEngine {
    pub async fn new(
        recording_path: impl Into<PathBuf>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path = recording_path.into();
        let mut recordings = load_recordings(path).await?;

        // Sort recordings by timestamp, then scope_sequence for correct global order
        recordings.sort_by_key(|e| (e.timestamp_ns, e.scope_sequence));
        let len = recordings.len();
        info!("Loaded {} recordings.", len);

        // Build scope-tree index
        let index = RecordingIndex::from_recordings(&recordings);
        info!("Built index with {} traces.", index.trace_count());

        Ok(Self {
            recordings,
            index,
            sequence_tracker: ScopeSequenceTracker::new(),
            match_config: MatchConfig::default(),
            runtime_cursor: HashMap::new(),
        })
    }

    pub fn find_match_with_responses_typed(
        &mut self,
        incoming: &RecordedEvent,
    ) -> Result<Option<ReplayMatch>, ReplayMatchError> {
        let scope_id = ScopeId::from_raw(&incoming.scope_id);
        let unresolved = incoming.trace_id.is_empty()
            || incoming.scope_id.is_empty()
            || incoming.trace_id == "orphan"
            || scope_id.is_orphan();

        if unresolved {
            return Err(ReplayMatchError::UnresolvedAttribution {
                trace_id: incoming.trace_id.clone(),
                scope_id: incoming.scope_id.clone(),
            });
        }

        Ok(self.find_match_with_responses(incoming))
    }

    /// Primary replay path — scope-based deterministic matching.
    ///
    /// The incoming event must have `trace_id` and `scope_id` populated.
    /// Uses (trace_id, scope_id, scope_sequence) as the sole matching strategy.
    pub fn find_match_with_responses(&mut self, incoming: &RecordedEvent) -> Option<ReplayMatch> {
        let trace_id = &incoming.trace_id;
        let scope_id = ScopeId::from_raw(&incoming.scope_id);

        if trace_id.is_empty() || incoming.scope_id.is_empty() {
            warn!(
                "[ReplayEngine] Missing trace_id or scope_id — cannot match. trace_id={:?}, scope_id={:?}",
                trace_id, incoming.scope_id
            );
            return None;
        }

        // Get expected sequence for this scope
        let expected_seq = self.sequence_tracker.peek_scope_sequence(&scope_id);

        tracing::info!(
            "[ReplayEngine] Looking for match — trace_id: {}, scope_id: {}, expected_seq: {}",
            trace_id,
            scope_id,
            expected_seq
        );

        // Try exact sequence match
        if let Some(exchange) = self.index.get_exchange(trace_id, &scope_id, expected_seq) {
            if self.match_request(incoming, &exchange.client_message) {
                // Commit sequence
                self.sequence_tracker.next_scope_sequence(&scope_id);
                tracing::info!(
                    "[ReplayEngine] Exact sequence match at seq={}",
                    expected_seq
                );
                return Some((
                    exchange.client_message.clone(),
                    exchange.response_events.clone(),
                    exchange.server_responses.clone(),
                ));
            }
            warn!(
                "[ReplayEngine] Sequence {} content mismatch in scope {}. Trying lookahead.",
                expected_seq, scope_id
            );
        }

        // Lookahead within same scope (handles minor reordering)
        for offset in 1..=3 {
            let ahead_seq = expected_seq + offset;
            if let Some(exchange) = self.index.get_exchange(trace_id, &scope_id, ahead_seq) {
                if self.match_request(incoming, &exchange.client_message) {
                    tracing::info!(
                        "[ReplayEngine] Lookahead match at seq={} (expected {})",
                        ahead_seq,
                        expected_seq
                    );
                    for _ in 0..=offset {
                        self.sequence_tracker.next_scope_sequence(&scope_id);
                    }
                    return Some((
                        exchange.client_message.clone(),
                        exchange.response_events.clone(),
                        exchange.server_responses.clone(),
                    ));
                }
            }
        }

        // Try legacy protocol-based lookup as last resort
        let protocol = RecordingIndex::detect_event_protocol(incoming);
        if protocol != Protocol::Unknown {
            let legacy_seq = self
                .sequence_tracker
                .peek_scope_sequence(&ScopeId::from_raw(&format!(
                    "legacy:{}:{}",
                    trace_id, protocol
                )));
            if let Some(exchange) = self.index.get(trace_id, protocol, legacy_seq) {
                if self.match_request(incoming, &exchange.client_message) {
                    self.sequence_tracker
                        .next_scope_sequence(&ScopeId::from_raw(&format!(
                            "legacy:{}:{}",
                            trace_id, protocol
                        )));
                    tracing::info!(
                        "[ReplayEngine] Legacy protocol match: {:?} seq={}",
                        protocol,
                        legacy_seq
                    );
                    return Some((
                        exchange.client_message.clone(),
                        exchange.response_events.clone(),
                        exchange.server_responses.clone(),
                    ));
                }
            }
        }

        warn!(
            "[ReplayEngine] No match found for trace={}, scope={}, expected_seq={}",
            trace_id, scope_id, expected_seq
        );
        None
    }

    /// Legacy find_match wrapper
    pub fn find_match(&mut self, incoming: &RecordedEvent) -> Option<RecordedEvent> {
        let (req, _, _) = self.find_match_with_responses(incoming)?;
        Some(req)
    }

    /// Protocol-aware request matching using MatchConfig
    fn match_request(&self, incoming: &RecordedEvent, recorded: &RecordedEvent) -> bool {
        match (&incoming.event, &recorded.event) {
            (
                Some(recorded_event::Event::HttpRequest(inc_req)),
                Some(recorded_event::Event::HttpRequest(rec_req)),
            ) => {
                if inc_req.method != rec_req.method || inc_req.path != rec_req.path {
                    return false;
                }
                if self.match_config.http.compare_headers && inc_req.headers != rec_req.headers {
                    return false;
                }
                if self.match_config.http.compare_body && inc_req.body != rec_req.body {
                    return false;
                }
                true
            }
            (
                Some(recorded_event::Event::RedisCommand(inc_cmd)),
                Some(recorded_event::Event::RedisCommand(rec_cmd)),
            ) => {
                if inc_cmd.command.to_uppercase() != rec_cmd.command.to_uppercase() {
                    return false;
                }
                if inc_cmd.args.len() != rec_cmd.args.len() {
                    return false;
                }
                for (i, inc_arg) in inc_cmd.args.iter().enumerate() {
                    let rec_arg = &rec_cmd.args[i];

                    // Fuzzy matching for SET expiration
                    if i > 0 && inc_cmd.command.to_uppercase() == "SET" {
                        let prev_arg = &inc_cmd.args[i - 1];
                        if let Some(crate::events::redis_value::Kind::BulkString(prev_bytes)) =
                            &prev_arg.kind
                        {
                            if let Ok(prev_str) = std::str::from_utf8(prev_bytes) {
                                if prev_str.to_uppercase() == "EX"
                                    || prev_str.to_uppercase() == "PX"
                                {
                                    if let (Some(inc_int), Some(rec_int)) =
                                        (parse_redis_int(inc_arg), parse_redis_int(rec_arg))
                                    {
                                        let diff = (inc_int - rec_int).unsigned_abs();
                                        let tolerance = self.match_config.redis.ttl_tolerance_ms;
                                        let tol = if prev_str.to_uppercase() == "EX" {
                                            (tolerance / 1000).max(1)
                                        } else {
                                            tolerance
                                        };
                                        if diff <= tol {
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if inc_arg != rec_arg {
                        return false;
                    }
                }
                true
            }
            (
                Some(recorded_event::Event::PgMessage(inc_msg)),
                Some(recorded_event::Event::PgMessage(rec_msg)),
            ) => match (&inc_msg.message, &rec_msg.message) {
                (
                    Some(pg_message_event::Message::Query(inc_q)),
                    Some(pg_message_event::Message::Query(rec_q)),
                ) => !self.match_config.postgres.compare_query_text || inc_q.query == rec_q.query,
                (
                    Some(pg_message_event::Message::Startup(inc_s)),
                    Some(pg_message_event::Message::Startup(rec_s)),
                ) => inc_s.database == rec_s.database && inc_s.user == rec_s.user,
                (
                    Some(pg_message_event::Message::Parse(inc_p)),
                    Some(pg_message_event::Message::Parse(rec_p)),
                ) => !self.match_config.postgres.compare_query_text || inc_p.query == rec_p.query,
                (
                    Some(pg_message_event::Message::Bind(_)),
                    Some(pg_message_event::Message::Bind(_)),
                ) => true,
                (
                    Some(pg_message_event::Message::Execute(_)),
                    Some(pg_message_event::Message::Execute(_)),
                ) => true,
                (
                    Some(pg_message_event::Message::Sync(_)),
                    Some(pg_message_event::Message::Sync(_)),
                ) => true,
                _ => false,
            },
            (
                Some(recorded_event::Event::GrpcRequest(inc_req)),
                Some(recorded_event::Event::GrpcRequest(rec_req)),
            ) => {
                if inc_req.service != rec_req.service {
                    return false;
                }
                if inc_req.method != rec_req.method {
                    return false;
                }
                if self.match_config.grpc.compare_request_body
                    && inc_req.request_body != rec_req.request_body
                {
                    return false;
                }
                true
            }
            (
                Some(recorded_event::Event::TcpData(inc_data)),
                Some(recorded_event::Event::TcpData(rec_data)),
            ) => inc_data.data == rec_data.data,
            _ => false,
        }
    }

    /// Handle a runtime request with scope-based lookup.
    ///
    /// Looks up by (trace_id, scope_id, kind, seq) for deterministic multi-call replay.
    pub fn handle_runtime_request_with_seq(
        &mut self,
        trace_id: &str,
        task_id: Option<&str>,
        kind: &str,
        seq: u64,
    ) -> Option<String> {
        // Look for matching ND event by (trace_id, scope_id from task_id, kind, seq)
        for recorded in &self.recordings {
            // Match trace_id
            if !trace_id.is_empty() && recorded.trace_id != trace_id {
                continue;
            }

            // Check scope_id matches task scope if task_id provided
            if let Some(tid) = task_id {
                let expected_scope = ScopeId::task(trace_id, tid);
                if recorded.scope_id != expected_scope.as_str() {
                    continue;
                }
            }

            // Check metadata for kind and seq
            let event_kind = recorded.metadata.get("nd_kind");
            let event_seq = recorded.metadata.get("nd_seq");

            if let (Some(ek), Some(es)) = (event_kind, event_seq) {
                if ek == kind {
                    if let Ok(event_seq_num) = es.parse::<u64>() {
                        if event_seq_num == seq {
                            return self.extract_nd_value(recorded);
                        }
                    }
                }
            }

            // Fallback: check scope_sequence directly if metadata is missing
            if recorded.scope_sequence == seq {
                if let Some(recorded_event::Event::NonDeterministic(nd_event)) = &recorded.event {
                    if self.matches_nd_kind(nd_event, kind) {
                        return self.extract_nd_value(recorded);
                    }
                }
            }
        }

        warn!(
            "[Replay] No recorded value found for trace={}, task={:?}, kind={}, seq={}",
            trace_id, task_id, kind, seq
        );
        None
    }

    /// Legacy runtime request handler (no sequence).
    ///
    /// Tracks a per-(trace_id, kind) cursor so that sequential calls
    /// return successive recorded values instead of always the first.
    pub fn handle_runtime_request(&mut self, trace_id: &str, kind: &str) -> Option<String> {
        let cursor_key = format!("{}:{}", trace_id, kind);
        let start_idx = self.runtime_cursor.get(&cursor_key).copied().unwrap_or(0);

        for (idx, recorded) in self.recordings.iter().enumerate().skip(start_idx) {
            if !trace_id.is_empty() && recorded.trace_id != trace_id {
                continue;
            }

            if let Some(recorded_event::Event::NonDeterministic(nd_event)) = &recorded.event {
                if let Some(k) = &nd_event.kind {
                    let matched = match (kind, k) {
                        (
                            "uuid" | "uuid_v7",
                            crate::events::non_deterministic_event::Kind::UuidCapture(val),
                        ) => Some(val.clone()),
                        (
                            "time",
                            crate::events::non_deterministic_event::Kind::TimeCaptureNs(val),
                        ) => Some(val.to_string()),
                        (
                            "random",
                            crate::events::non_deterministic_event::Kind::RandomSeedCapture(val),
                        ) => Some(val.to_string()),
                        (
                            "task_spawn",
                            crate::events::non_deterministic_event::Kind::TaskSpawnCapture(val),
                        ) => Some(val.clone()),
                        (
                            "random_bytes",
                            crate::events::non_deterministic_event::Kind::RandomBytesCapture(val),
                        ) => Some(hex::encode(val)),
                        (
                            "nanoid",
                            crate::events::non_deterministic_event::Kind::NanoidCapture(val),
                        ) => Some(val.clone()),
                        _ => None,
                    };
                    if let Some(value) = matched {
                        self.runtime_cursor.insert(cursor_key, idx + 1);
                        return Some(value);
                    }
                }
            }
        }
        None
    }

    fn matches_nd_kind(&self, nd_event: &crate::events::NonDeterministicEvent, kind: &str) -> bool {
        if let Some(k) = &nd_event.kind {
            match (kind, k) {
                (
                    "uuid" | "uuid_v7",
                    crate::events::non_deterministic_event::Kind::UuidCapture(_),
                ) => true,
                ("time", crate::events::non_deterministic_event::Kind::TimeCaptureNs(_)) => true,
                ("random", crate::events::non_deterministic_event::Kind::RandomSeedCapture(_)) => {
                    true
                }
                (
                    "task_spawn",
                    crate::events::non_deterministic_event::Kind::TaskSpawnCapture(_),
                ) => true,
                (
                    "random_bytes",
                    crate::events::non_deterministic_event::Kind::RandomBytesCapture(_),
                ) => true,
                ("nanoid", crate::events::non_deterministic_event::Kind::NanoidCapture(_)) => true,
                _ => false,
            }
        } else {
            false
        }
    }

    fn extract_nd_value(&self, recorded: &RecordedEvent) -> Option<String> {
        if let Some(recorded_event::Event::NonDeterministic(nd_event)) = &recorded.event {
            if let Some(k) = &nd_event.kind {
                return match k {
                    crate::events::non_deterministic_event::Kind::UuidCapture(val) => {
                        Some(val.clone())
                    }
                    crate::events::non_deterministic_event::Kind::TimeCaptureNs(val) => {
                        Some(val.to_string())
                    }
                    crate::events::non_deterministic_event::Kind::RandomSeedCapture(val) => {
                        Some(val.to_string())
                    }
                    crate::events::non_deterministic_event::Kind::TaskSpawnCapture(val) => {
                        Some(val.clone())
                    }
                    crate::events::non_deterministic_event::Kind::RandomBytesCapture(val) => {
                        Some(hex::encode(val))
                    }
                    crate::events::non_deterministic_event::Kind::NanoidCapture(val) => {
                        Some(val.clone())
                    }
                };
            }
        }
        None
    }

    /// Legacy handlers
    pub fn handle_random_request(&mut self) -> Option<u64> {
        self.handle_runtime_request("", "random")
            .and_then(|s| s.parse().ok())
    }

    pub fn handle_uuid_request(&mut self) -> Option<String> {
        self.handle_runtime_request("", "uuid")
    }

    pub fn handle_time_request(&mut self) -> Option<u64> {
        self.handle_runtime_request("", "time")
            .and_then(|s| s.parse().ok())
    }

    /// Get count of total recordings
    pub fn total_count(&self) -> usize {
        self.recordings.len()
    }

    /// Get count of remaining (approximate — counts non-ND events without matched scope sequences)
    pub fn remaining_count(&self) -> usize {
        // Approximate: total minus events that have been sequence-matched
        self.recordings.len()
    }

    /// Reset sequence tracker for fresh replay
    pub fn reset(&mut self) {
        self.sequence_tracker.clear();
        self.runtime_cursor.clear();
    }

    /// Reset sequences for a specific trace
    pub fn reset_trace(&mut self, trace_id: &str) {
        self.sequence_tracker.reset_trace(trace_id);
    }

    // ============ Orchestration Methods ============

    /// Get the trigger event (first HTTP or gRPC request) for a specific trace_id
    pub fn get_trigger_event(&self, trace_id: &str) -> Option<&RecordedEvent> {
        self.recordings.iter().find(|event| {
            event.trace_id == trace_id
                && (matches!(&event.event, Some(recorded_event::Event::HttpRequest(_)))
                    || matches!(&event.event, Some(recorded_event::Event::GrpcRequest(_))))
        })
    }

    pub fn is_http_trigger(&self, event: &RecordedEvent) -> bool {
        matches!(&event.event, Some(recorded_event::Event::HttpRequest(_)))
    }

    pub fn is_grpc_trigger(&self, event: &RecordedEvent) -> bool {
        matches!(&event.event, Some(recorded_event::Event::GrpcRequest(_)))
    }

    /// Get all trace IDs in the recordings
    pub fn get_all_trace_ids(&self) -> Vec<String> {
        self.index.trace_ids().cloned().collect()
    }

    /// Get the expected response for a trace_id
    pub fn get_expected_response(&self, trace_id: &str) -> Option<&RecordedEvent> {
        self.recordings.iter().find(|event| {
            event.trace_id == trace_id
                && (matches!(&event.event, Some(recorded_event::Event::HttpResponse(_)))
                    || matches!(&event.event, Some(recorded_event::Event::GrpcResponse(_))))
        })
    }

    /// Serialize an HTTP request event to bytes for sending
    pub fn serialize_http_request(&self, event: &RecordedEvent) -> Option<bytes::Bytes> {
        if let Some(recorded_event::Event::HttpRequest(req)) = &event.event {
            let mut request = format!(
                "{} {} HTTP/1.1\r\n",
                req.method,
                if req.path.is_empty() { "/" } else { &req.path }
            );

            for (key, value) in &req.headers {
                request.push_str(&format!("{}: {}\r\n", key, value));
            }

            request.push_str("\r\n");

            let mut bytes = bytes::BytesMut::from(request.as_bytes());
            bytes.extend_from_slice(&req.body);
            Some(bytes.freeze())
        } else {
            None
        }
    }

    /// Extract gRPC request info for orchestrated replay
    pub fn get_grpc_request_info(
        &self,
        event: &RecordedEvent,
    ) -> Option<(String, String, Vec<u8>)> {
        if let Some(recorded_event::Event::GrpcRequest(req)) = &event.event {
            Some((
                req.service.clone(),
                req.method.clone(),
                req.request_body.clone(),
            ))
        } else {
            None
        }
    }

    /// Compare two gRPC responses
    pub fn compare_grpc_responses(
        &self,
        expected: &RecordedEvent,
        actual_body: &[u8],
        actual_status: u32,
    ) -> OrchestrationResult {
        let expected_resp = match &expected.event {
            Some(recorded_event::Event::GrpcResponse(resp)) => resp,
            _ => {
                return OrchestrationResult {
                    pass: false,
                    message: "Expected event is not a gRPC response".to_string(),
                    diff: None,
                }
            }
        };

        let expected_status = expected_resp.status_code as u32;
        if actual_status != expected_status {
            return OrchestrationResult {
                pass: false,
                message: format!(
                    "gRPC status code mismatch: expected {}, got {}",
                    expected_status, actual_status
                ),
                diff: Some(ResponseDiff {
                    status_diff: Some((expected_status, actual_status)),
                    header_diffs: vec![],
                    body_diff: None,
                }),
            };
        }

        if actual_body != expected_resp.response_body.as_slice() {
            return OrchestrationResult {
                pass: false,
                message: "gRPC response body mismatch".to_string(),
                diff: Some(ResponseDiff {
                    status_diff: None,
                    header_diffs: vec![],
                    body_diff: Some("Response bodies differ".to_string()),
                }),
            };
        }

        OrchestrationResult {
            pass: true,
            message: "gRPC response matches".to_string(),
            diff: None,
        }
    }

    /// Compare two HTTP responses with body comparison
    pub fn compare_responses(
        &self,
        expected: &RecordedEvent,
        actual_bytes: &[u8],
    ) -> OrchestrationResult {
        let expected_resp = match &expected.event {
            Some(recorded_event::Event::HttpResponse(resp)) => resp,
            _ => {
                return OrchestrationResult {
                    pass: false,
                    message: "Expected event is not an HTTP response".to_string(),
                    diff: None,
                }
            }
        };

        // Parse actual response
        let actual_str = String::from_utf8_lossy(actual_bytes);
        let mut lines = actual_str.lines();

        // Parse status line
        let actual_status = lines
            .next()
            .and_then(|line| {
                let parts: Vec<_> = line.splitn(3, ' ').collect();
                if parts.len() >= 2 {
                    parts[1].parse::<u32>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);

        if actual_status != expected_resp.status {
            return OrchestrationResult {
                pass: false,
                message: format!(
                    "Status code mismatch: expected {}, got {}",
                    expected_resp.status, actual_status
                ),
                diff: Some(ResponseDiff {
                    status_diff: Some((expected_resp.status, actual_status)),
                    header_diffs: vec![],
                    body_diff: None,
                }),
            };
        }

        // Extract body (after empty line)
        let mut body_started = false;
        let mut actual_body = String::new();
        for line in lines {
            if body_started {
                if !actual_body.is_empty() {
                    actual_body.push('\n');
                }
                actual_body.push_str(line);
            } else if line.is_empty() {
                body_started = true;
            }
        }

        // Compare body if expected has one
        if !expected_resp.body.is_empty() {
            let expected_body = String::from_utf8_lossy(&expected_resp.body);

            // Try JSON structural equality for JSON content
            if let (Ok(expected_json), Ok(actual_json)) = (
                serde_json::from_str::<serde_json::Value>(&expected_body),
                serde_json::from_str::<serde_json::Value>(&actual_body),
            ) {
                if expected_json != actual_json {
                    return OrchestrationResult {
                        pass: false,
                        message: "HTTP response body mismatch (JSON)".to_string(),
                        diff: Some(ResponseDiff {
                            status_diff: None,
                            header_diffs: vec![],
                            body_diff: Some(format!(
                                "Expected: {}\nActual: {}",
                                expected_body, actual_body
                            )),
                        }),
                    };
                }
            } else if expected_body != actual_body {
                // Byte-level comparison for non-JSON
                return OrchestrationResult {
                    pass: false,
                    message: "HTTP response body mismatch".to_string(),
                    diff: Some(ResponseDiff {
                        status_diff: None,
                        header_diffs: vec![],
                        body_diff: Some(format!(
                            "Expected: {}\nActual: {}",
                            expected_body, actual_body
                        )),
                    }),
                };
            }
        }

        OrchestrationResult {
            pass: true,
            message: "Response matches".to_string(),
            diff: None,
        }
    }
}

/// Result of orchestrated replay comparison
#[derive(Debug, Clone)]
pub struct OrchestrationResult {
    pub pass: bool,
    pub message: String,
    pub diff: Option<ResponseDiff>,
}

/// Detailed diff between expected and actual responses
#[derive(Debug, Clone)]
pub struct ResponseDiff {
    pub status_diff: Option<(u32, u32)>,
    pub header_diffs: Vec<(String, Option<String>, Option<String>)>,
    pub body_diff: Option<String>,
}

// Helper to extract integer from RedisValue
fn parse_redis_int(val: &crate::events::RedisValue) -> Option<i64> {
    match &val.kind {
        Some(redis_value::Kind::Integer(i)) => Some(*i),
        Some(redis_value::Kind::BulkString(bytes)) => {
            std::str::from_utf8(bytes).ok().and_then(|s| s.parse().ok())
        }
        Some(redis_value::Kind::SimpleString(s)) => s.parse().ok(),
        _ => None,
    }
}

pub async fn load_recordings(
    path: PathBuf,
) -> Result<Vec<RecordedEvent>, Box<dyn std::error::Error>> {
    let mut recordings = Vec::new();

    info!("Reading recordings from path: {:?}", path);

    if path.is_file() {
        if let Some(ext) = path.extension() {
            if ext == "bin" {
                load_binary_file(&path, &mut recordings).await?;
            } else {
                load_jsonl_file(&path, &mut recordings).await?;
            }
        } else {
            load_jsonl_file(&path, &mut recordings).await?;
        }
    } else {
        let events_bin = path.join("events.bin");
        let events_json = path.join("events.jsonl");

        if events_bin.exists() {
            load_binary_file(&events_bin, &mut recordings).await?;
        } else if events_json.exists() {
            load_jsonl_file(&events_json, &mut recordings).await?;
        } else {
            let sessions_dir = path.join("sessions");
            let target_dir = if sessions_dir.exists() {
                sessions_dir
            } else {
                path.clone()
            };

            if target_dir.exists() {
                let mut entries = fs::read_dir(&target_dir).await?;
                while let Some(entry) = entries.next_entry().await? {
                    let entry_path = entry.path();
                    if entry.metadata().await?.is_dir() {
                        let bin = entry_path.join("events.bin");
                        let json = entry_path.join("events.jsonl");
                        if bin.exists() {
                            load_binary_file(&bin, &mut recordings).await?;
                        } else if json.exists() {
                            load_jsonl_file(&json, &mut recordings).await?;
                        }
                    } else if entry_path.extension().is_some_and(|ext| ext == "jsonl") {
                        info!("Loading legacy flat file: {:?}", entry_path);
                        load_jsonl_file(&entry_path, &mut recordings).await?;
                    }
                }
            }
        }
    }

    Ok(recordings)
}

async fn load_binary_file(
    path: &PathBuf,
    recordings: &mut Vec<RecordedEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Loading (binary): {:?}", path);
    let content = fs::read(path).await?;
    let mut cursor = std::io::Cursor::new(content);
    use bytes::Buf;
    while cursor.has_remaining() {
        if cursor.remaining() < 4 {
            break;
        }
        let len = cursor.get_u32() as usize;
        if cursor.remaining() < len {
            break;
        }
        let pos = cursor.position() as usize;
        let slice = &cursor.get_ref()[pos..pos + len];
        let res = bincode::deserialize::<RecordedEvent>(slice);
        cursor.advance(len);

        if let Ok(event) = res {
            recordings.push(event);
        }
    }
    Ok(())
}

async fn load_jsonl_file(
    path: &PathBuf,
    recordings: &mut Vec<RecordedEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Loading (jsonl): {:?}", path);
    let content = fs::read_to_string(path).await?;
    for line in content.lines() {
        if !line.trim().is_empty() {
            if let Ok(event) = serde_json::from_str::<RecordedEvent>(line) {
                recordings.push(event);
            }
        }
    }
    Ok(())
}
