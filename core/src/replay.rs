use crate::events::{pg_message_event, recorded_event, redis_value, RecordedEvent};
use std::path::PathBuf;
use tokio::fs;

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

pub struct ReplayEngine {
    recordings: Vec<RecordedEvent>,
    visited: Vec<bool>,
    lookup: std::collections::HashMap<u64, Vec<usize>>, // Hash -> Indices
    search_start: usize,
    connection_map: std::collections::HashMap<String, String>, // incoming_id -> recorded_id
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum Protocol {
    Postgres,
    Redis,
    Http,
}

impl ReplayEngine {
    pub async fn new(
        recording_path: impl Into<PathBuf>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path = recording_path.into();
        let mut recordings = load_recordings(path).await?;

        // Sort recordings by timestamp, then sequence to ensure correct global order
        recordings.sort_by_key(|e| (e.timestamp_ns, e.sequence));
        let len = recordings.len();
        println!("Loaded {} recordings.", len);

        // Build Hash Index
        let mut lookup = std::collections::HashMap::new();
        use crate::hash::EventHasher;
        for (i, event) in recordings.iter().enumerate() {
            let hash = EventHasher::calculate_hash(event);
            if hash != 0 {
                lookup.entry(hash).or_insert_with(Vec::new).push(i);
            }
        }
        println!("Built index with {} unique hashes.", lookup.len());

        Ok(Self {
            recordings,
            visited: vec![false; len],
            lookup,
            search_start: 0,
            connection_map: std::collections::HashMap::new(),
        })
    }

    pub fn find_match(&mut self, incoming: &RecordedEvent) -> Option<RecordedEvent> {
        let idx = self.find_match_index(incoming)?;
        self.visited[idx] = true;
        Some(self.recordings[idx].clone())
    }

    pub fn find_match_with_responses(
        &mut self,
        incoming: &RecordedEvent,
    ) -> Option<(RecordedEvent, Vec<bytes::Bytes>)> {
        let idx = self.find_match_index(incoming)?;
        self.visited[idx] = true;
        let matched_req = self.recordings[idx].clone();
        let _target_protocol = self.get_event_protocol(&matched_req)?;

        let mut responses = Vec::new();
        let mut curr = idx + 1;

        while curr < self.recordings.len() {
            if self.visited[curr] {
                curr += 1;
                continue;
            }

            let candidate = &self.recordings[curr];

            // If protocols don't match, or it's from a different connection, skip it
            if candidate.connection_id != matched_req.connection_id {
                curr += 1;
                continue;
            }

            // Same connection. Is it a response or a new request?
            if self.is_server_response(candidate) {
                if let Some(bytes) = self.serialize_event(candidate) {
                    responses.push(bytes);
                }
                self.visited[curr] = true;
                curr += 1;
            } else {
                // Found a new client request on the SAME connection -> end of response chain
                break;
            }
        }

        Some((matched_req, responses))
    }

    fn is_server_response(&self, event: &RecordedEvent) -> bool {
        match &event.event {
            Some(recorded_event::Event::PgMessage(msg)) => match &msg.message {
                Some(pg_message_event::Message::RowDesc(_)) => true,
                Some(pg_message_event::Message::DataRow(_)) => true,
                Some(pg_message_event::Message::CommandComplete(_)) => true,
                Some(pg_message_event::Message::ErrorResponse(_)) => true,
                Some(pg_message_event::Message::AuthenticationOk(_)) => true,
                Some(pg_message_event::Message::ParameterStatus(_)) => true,
                Some(pg_message_event::Message::BackendKeyData(_)) => true,
                Some(pg_message_event::Message::ReadyForQuery(_)) => true,
                Some(pg_message_event::Message::ParseComplete(_)) => true,
                Some(pg_message_event::Message::BindComplete(_)) => true,
                Some(pg_message_event::Message::NoData(_)) => true,
                _ => false,
            },
            Some(recorded_event::Event::RedisResponse(_)) => true,
            Some(recorded_event::Event::HttpResponse(_)) => true,
            _ => false,
        }
    }

    fn get_event_protocol(&self, event: &RecordedEvent) -> Option<Protocol> {
        match &event.event {
            Some(recorded_event::Event::PgMessage(_)) => Some(Protocol::Postgres),
            Some(recorded_event::Event::RedisCommand(_))
            | Some(recorded_event::Event::RedisResponse(_)) => Some(Protocol::Redis),
            Some(recorded_event::Event::HttpRequest(_))
            | Some(recorded_event::Event::HttpResponse(_)) => Some(Protocol::Http),
            _ => None,
        }
    }

    fn serialize_event(&self, event: &RecordedEvent) -> Option<bytes::Bytes> {
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
            _ => None,
        }
    }

    fn find_match_index(&mut self, incoming: &RecordedEvent) -> Option<usize> {
        // Optimize: Advance search_start past already visited items
        while self.search_start < self.recordings.len() && self.visited[self.search_start] {
            self.search_start += 1;
        }

        use crate::hash::EventHasher;
        let hash = EventHasher::calculate_hash(incoming);
        if hash != 0 {
            if let Some(recorded_event::Event::PgMessage(_)) = &incoming.event {
                println!(
                    "[ReplayEngine] Searching for Postgres match. Hash: {}",
                    hash
                );
            }
        }

        if let Some(candidates) = self.lookup.get(&hash) {
            // Candidates are indices in recordings.
            // We need to find the *first* valid candidate (respecting order if multiple same reqs).
            // However, just picking first unvisited might violate global order if strictly sequential?
            // But we allow concurrency.
            // Let's iterate candidates.
            for &index in candidates {
                if index < self.search_start {
                    continue;
                } // Too old
                if self.visited[index] {
                    continue;
                } // Already used

                let recorded = &self.recordings[index];

                // Connection ID Mapping Logic
                let inc_id = &incoming.connection_id;
                let rec_id = &recorded.connection_id;

                if !inc_id.is_empty() && !rec_id.is_empty() {
                    if let Some(mapped_id) = self.connection_map.get(inc_id) {
                        if mapped_id != rec_id {
                            // Don't log for every candidate, only if we were specifically looking for this one?
                            continue;
                        }
                    }
                }

                // If index is FAR ahead, warn? But window logic was for linear scan efficiency.
                // With hash, we can jump.
                // BUT, if we have {Req A, Req B} recorded, and we receive Req B, we shouldn't match B if A is expected strictly before?
                // But in concurrent world, A and B might come in any order.
                // So we trust the Hash.

                // Do strict check
                // Re-use logic for deep comparison (or trust hash?)
                // Hash ignores headers/some args. We must verify strictly.
                if self.is_strict_match(incoming, recorded) {
                    // Success! Pin the connection IDs if not already pinned
                    let inc_id = &incoming.connection_id;
                    let rec_id = &recorded.connection_id;
                    if !inc_id.is_empty() && !rec_id.is_empty() {
                        self.connection_map.insert(inc_id.clone(), rec_id.clone());
                    }
                    return Some(index);
                }
            }
        }

        // Fallback or No Match
        None
    }

    // Extracted helper for detailed comparison
    fn is_strict_match(&self, incoming: &RecordedEvent, recorded: &RecordedEvent) -> bool {
        match (&incoming.event, &recorded.event) {
            (
                Some(recorded_event::Event::HttpRequest(inc_req)),
                Some(recorded_event::Event::HttpRequest(rec_req)),
            ) => {
                // Match Method and Path
                if inc_req.method != rec_req.method || inc_req.path != rec_req.path {
                    false
                } else if inc_req.headers != rec_req.headers {
                    // Strict header matching for now
                    false
                } else if inc_req.body != rec_req.body {
                    // Strict body matching for now
                    false
                } else {
                    true
                }
            }
            (
                Some(recorded_event::Event::RedisCommand(inc_cmd)),
                Some(recorded_event::Event::RedisCommand(rec_cmd)),
            ) => {
                // Match Command
                if inc_cmd.command != rec_cmd.command {
                    false
                } else if inc_cmd.args.len() != rec_cmd.args.len() {
                    false
                } else {
                    // Compare args deeply with fuzzy matching logic
                    let mut args_match = true;
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
                                        // This arg is likely a TTL
                                        if let (Some(inc_int), Some(rec_int)) =
                                            (parse_redis_int(inc_arg), parse_redis_int(rec_arg))
                                        {
                                            let diff = (inc_int - rec_int).abs();
                                            let tolerance = if prev_str.to_uppercase() == "EX" {
                                                1
                                            } else {
                                                100
                                            };
                                            if diff <= tolerance {
                                                continue; // Match!
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Default strict equality check
                        if inc_arg != rec_arg {
                            args_match = false;
                            break;
                        }
                    }
                    args_match
                }
            }
            (
                Some(recorded_event::Event::PgMessage(inc_msg)),
                Some(recorded_event::Event::PgMessage(rec_msg)),
            ) => match (&inc_msg.message, &rec_msg.message) {
                (
                    Some(pg_message_event::Message::Query(inc_q)),
                    Some(pg_message_event::Message::Query(rec_q)),
                ) => inc_q.query == rec_q.query,
                (
                    Some(pg_message_event::Message::Startup(inc_s)),
                    Some(pg_message_event::Message::Startup(rec_s)),
                ) => inc_s.database == rec_s.database && inc_s.user == rec_s.user,
                (
                    Some(pg_message_event::Message::Parse(inc_p)),
                    Some(pg_message_event::Message::Parse(rec_p)),
                ) => inc_p.query == rec_p.query,
                (
                    Some(pg_message_event::Message::Bind(_)),
                    Some(pg_message_event::Message::Bind(_)),
                ) => true, // Relaxed matching for Bind
                (
                    Some(pg_message_event::Message::Execute(_)),
                    Some(pg_message_event::Message::Execute(_)),
                ) => true, // Relaxed matching for Execute
                (
                    Some(pg_message_event::Message::Sync(_)),
                    Some(pg_message_event::Message::Sync(_)),
                ) => true,
                _ => false,
            },

            _ => false,
        }
    }

    // Look for the next available TimeCapture event
    pub fn handle_time_request(&mut self) -> Option<u64> {
        for (index, recorded) in self.recordings.iter().enumerate() {
            if self.visited[index] {
                continue;
            }

            if let Some(recorded_event::Event::NonDeterministic(nd_event)) = &recorded.event {
                if let Some(crate::events::non_deterministic_event::Kind::TimeCaptureNs(time_ns)) =
                    &nd_event.kind
                {
                    self.visited[index] = true;
                    return Some(*time_ns);
                }
            }
        }
        None
    }

    /// Look for the next available UUID capture event
    pub fn handle_uuid_request(&mut self) -> Option<String> {
        for (index, recorded) in self.recordings.iter().enumerate() {
            if self.visited[index] {
                continue;
            }

            if let Some(recorded_event::Event::NonDeterministic(nd_event)) = &recorded.event {
                if let Some(crate::events::non_deterministic_event::Kind::UuidCapture(uuid_str)) =
                    &nd_event.kind
                {
                    self.visited[index] = true;
                    return Some(uuid_str.clone());
                }
            }
        }
        None
    }

    /// Look for the next available random seed capture event
    pub fn handle_random_request(&mut self) -> Option<u64> {
        for (index, recorded) in self.recordings.iter().enumerate() {
            if self.visited[index] {
                continue;
            }

            if let Some(recorded_event::Event::NonDeterministic(nd_event)) = &recorded.event {
                if let Some(crate::events::non_deterministic_event::Kind::RandomSeedCapture(seed)) =
                    &nd_event.kind
                {
                    self.visited[index] = true;
                    return Some(*seed);
                }
            }
        }
        None
    }

    /// Get count of remaining unvisited recordings
    pub fn remaining_count(&self) -> usize {
        self.visited.iter().filter(|&&v| !v).count()
    }

    /// Get total recordings count
    pub fn total_count(&self) -> usize {
        self.recordings.len()
    }

    // ============ Orchestration Methods ============

    /// Get the trigger event (first HTTP request) for a specific trace_id
    /// Used in orchestrated replay to initiate the recorded flow
    pub fn get_trigger_event(&self, trace_id: &str) -> Option<&RecordedEvent> {
        self.recordings.iter().find(|event| {
            event.trace_id == trace_id
                && matches!(&event.event, Some(recorded_event::Event::HttpRequest(_)))
        })
    }

    /// Get all trace IDs in the recordings
    pub fn get_all_trace_ids(&self) -> Vec<String> {
        let mut trace_ids: Vec<_> = self
            .recordings
            .iter()
            .filter(|e| !e.trace_id.is_empty())
            .map(|e| e.trace_id.clone())
            .collect();
        trace_ids.sort();
        trace_ids.dedup();
        trace_ids
    }

    /// Get the expected HTTP response for a trace_id
    /// Used to compare with actual response in orchestrated replay
    pub fn get_expected_response(&self, trace_id: &str) -> Option<&RecordedEvent> {
        self.recordings.iter().find(|event| {
            event.trace_id == trace_id
                && matches!(&event.event, Some(recorded_event::Event::HttpResponse(_)))
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

    /// Compare two HTTP responses and return a diff result
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

        // Parse actual response (simple HTTP/1.1 parsing)
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

        // Check status code
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

        // For now, consider status match a pass
        // TODO: Add header and body comparison
        OrchestrationResult {
            pass: true,
            message: "Response matches".to_string(),
            diff: None,
        }
    }

    /// Reset all visited flags for a fresh replay
    pub fn reset(&mut self) {
        self.visited.fill(false);
        self.search_start = 0;
        self.connection_map.clear();
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
    pub status_diff: Option<(u32, u32)>, // (expected, actual)
    pub header_diffs: Vec<(String, Option<String>, Option<String>)>, // (key, expected, actual)
    pub body_diff: Option<String>,
}

// Helper to extract integer from RedisValue regardless of type (Integer or String representation)
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

    println!("Reading recordings from path: {:?}", path);

    if path.is_file() {
        // Direct file load
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
        // Directory logic
        // Check if it's a session dir (contains event files directly) OR a root containing sessions
        let events_bin = path.join("events.bin");
        let events_json = path.join("events.jsonl");

        if events_bin.exists() {
            load_binary_file(&events_bin, &mut recordings).await?;
        } else if events_json.exists() {
            load_jsonl_file(&events_json, &mut recordings).await?;
        } else {
            // Assume it's a root directory with sessions
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
                    } else {
                        // Legacy flat files check
                        if entry_path.extension().map_or(false, |ext| ext == "jsonl") {
                            println!("Loading legacy flat file: {:?}", entry_path);
                            load_jsonl_file(&entry_path, &mut recordings).await?;
                        }
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
    println!("Loading (binary): {:?}", path);
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
    println!("Loading (jsonl): {:?}", path);
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
