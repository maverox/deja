# Deja Architecture: Complete Record-Replay Flow

> A deep-dive into every component of the Deja deterministic record-replay framework, covering data flow, protocol handling, scope-based determinism, and the full lifecycle from recording to replay.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Component Map](#component-map)
3. [Recording Flow](#recording-flow)
4. [Replay Flow](#replay-flow)
5. [The Proxy](#the-proxy)
6. [Protocol Parsers](#protocol-parsers)
7. [Trace Correlation System](#trace-correlation-system)
8. [Scope-Based Determinism](#scope-based-determinism)
9. [The Replay Engine](#the-replay-engine)
10. [SDK Integration](#sdk-integration)
11. [Control API](#control-api)
12. [Non-Deterministic Value Capture](#non-deterministic-value-capture)
13. [TLS MITM Interception](#tls-mitm-interception)
14. [Orchestrated Replay](#orchestrated-replay)
15. [Event Format & Storage](#event-format--storage)
16. [Key Invariants](#key-invariants)
17. [Configuration Reference](#configuration-reference)

---

## System Overview

Deja records real production traffic (HTTP, gRPC, PostgreSQL, Redis, raw TCP) through an intercepting proxy, then replays it deterministically against new code versions. The goal: catch behavioral regressions using real traffic patterns without touching real infrastructure.

```
                    RECORD MODE
    ┌──────────┐    ┌───────────┐    ┌──────────────┐
    │  Client   │───▶│   Proxy   │───▶│  Real Backend│
    │ (Service) │◀───│ (Record)  │◀───│  (DB/Redis)  │
    └──────────┘    └─────┬─────┘    └──────────────┘
                          │
                    ┌─────▼─────┐
                    │ Recordings│
                    │  (disk)   │
                    └───────────┘

                    REPLAY MODE
    ┌──────────┐    ┌───────────┐
    │  Client   │───▶│   Proxy   │──── No real backend needed
    │ (Service) │◀───│ (Replay)  │◀─── Serves from recordings
    └──────────┘    └─────┬─────┘
                          │
                    ┌─────▼─────┐
                    │ Recordings│
                    │  (disk)   │
                    └───────────┘
```

---

## Component Map

```
project_deja_unified/
├── common/                    # Shared types across all crates
│   └── src/
│       ├── control.rs         # ControlMessage enum (StartTrace, Associate, etc.)
│       ├── protocol.rs        # Protocol enum (Postgres, Redis, Http, Grpc, Unknown)
│       ├── runtime.rs         # DejaRuntime trait + deja_run() helper
│       └── sequence.rs        # ScopeSequenceTracker, ScopeId
│
├── core/                      # Core engine logic
│   └── src/
│       ├── protocols/         # Protocol parsers
│       │   ├── filter.rs      # ProtocolDetector: confidence-scored detection
│       │   ├── http.rs        # HTTP/1.1 parser
│       │   ├── grpc.rs        # gRPC (HTTP/2) parser
│       │   ├── postgres.rs    # PostgreSQL wire protocol parser
│       │   ├── redis.rs       # Redis RESP protocol parser
│       │   └── tcp.rs         # Generic TCP fallback parser
│       ├── replay/
│       │   ├── engine.rs      # ReplayEngine: scope-based deterministic matching
│       │   └── recording_index.rs  # RecordingIndex: (trace, scope, seq) → response
│       ├── recording.rs       # Recorder: event persistence (binary/JSON)
│       ├── runtime.rs         # Runtime: SDK runtime implementation
│       └── runtime/
│           ├── trace_context.rs  # Task-local trace ID propagation
│           ├── spawn.rs          # Traced task spawning
│           └── pool_interceptor.rs  # Connection pool interception
│
├── proxy/                     # The intercepting proxy
│   └── src/
│       ├── main.rs            # Entry point, CLI args, mode dispatch
│       ├── connection.rs      # Connection handling, event enrichment, replay matching
│       ├── correlation.rs     # TraceCorrelator: connection → trace mapping
│       ├── control_api.rs     # HTTP control API endpoints
│       └── forward_proxy.rs   # HTTP CONNECT forward proxy mode
│
├── sdk/                       # Client-side instrumentation SDK
│   └── src/
│       ├── lib.rs             # Re-exports from core/common
│       ├── axum.rs            # Axum/Tower middleware for auto-instrumentation
│       ├── reqwest.rs         # HTTP client wrapper for trace injection
│       ├── pool.rs            # DejaPool (sqlx), DejaRedisPool wrappers
│       ├── association.rs     # Connection association helpers
│       └── sync_runtime.rs    # Synchronous DejaRuntime wrapper
│
└── test-server/               # Integration test service
    ├── src/                   # Test service with full SDK instrumentation
    └── tests/                 # Integration test suites
        ├── full_e2e.rs        # Full record→replay E2E test
        ├── concurrent_validation.rs  # 25+ concurrent trace tests
        └── ...                # 9 test suites, 74+ tests
```

---

## Recording Flow

### Step-by-Step Data Path

```
1. Client opens TCP connection to proxy port
   │
2. Proxy accepts connection, extracts peer_port
   │
3. Proxy calls correlator.on_new_connection(peer_port, protocol)
   │  ├── If pre-registered (SDK sent AssociateBySourcePort): returns (trace_id, scope_id)
   │  └── If unknown: returns pending Receiver, waits for SDK association
   │
4. Protocol detection runs on first bytes (confidence scoring)
   │
5. Proxy opens connection to real backend (target)
   │
6. Bidirectional relay begins:
   │
   ├── Client → Proxy → Backend (client_to_target)
   │   ├── Parser extracts events from raw bytes
   │   ├── Events enriched with (trace_id, scope_id, scope_sequence, direction, timestamp)
   │   ├── Recorder.save_event(enriched_event) → appends to events.bin
   │   └── Raw bytes forwarded to backend
   │
   └── Backend → Proxy → Client (target_to_client)
       ├── Parser extracts response events
       ├── Events enriched with ServerToClient direction
       ├── Recorder.save_event(enriched_event) → appends to events.bin
       └── Raw bytes forwarded to client
```

### Event Enrichment (`connection.rs:34`)

Every event passes through `enrich_event()` which stamps it with:

| Field | Source | Purpose |
|-------|--------|---------|
| `trace_id` | From RuntimeAssociation (SDK-provided) | Groups events by logical request |
| `scope_id` | `trace:{id}:conn:{N}` | Identifies the connection within a trace |
| `scope_sequence` | Per-scope atomic counter | Orders events within a scope |
| `global_sequence` | Global atomic counter | Total ordering across all events |
| `direction` | ClientToServer or ServerToClient | Pairs requests with responses |
| `timestamp_ns` | System clock | Debugging/ordering tie-breaking |

---

## Replay Flow

### Step-by-Step Data Path

```
1. ReplayEngine loads recordings from disk → builds RecordingIndex
   │
2. Client opens TCP connection to proxy port
   │
3. Proxy accepts, resolves association (same as recording)
   │
4. NO backend connection is opened
   │
5. Client sends data:
   │
   ├── Parser extracts events from raw bytes
   ├── Events enriched with (trace_id, scope_id, scope_sequence)
   ├── ReplayEngine.find_match_with_responses_typed(event) called
   │   │
   │   ├── Lookup by (trace_id, scope_id, scope_sequence) in RecordingIndex
   │   │   ├── Exact match → return recorded responses
   │   │   ├── Lookahead ±3 → try nearby sequences
   │   │   └── Legacy fallback → protocol-based sequential matching
   │   │
   │   ├── On match: send recorded response bytes to client
   │   ├── On no match: send protocol-appropriate error
   │   └── On error: send typed error response
   │
   └── Client receives response as if real backend answered
```

### Key Difference from Recording

In replay mode:
- **No target connection** — the proxy never connects to a real backend
- **Parser synthetic replies are suppressed** — parsers generate Auth/Ready responses for protocol negotiation, but in replay mode these are skipped to avoid double-sends with the replay engine's matched responses
- **Protocol errors prevent hangs** — if no recorded response matches, the proxy always sends an error instead of leaving the client waiting

---

## The Proxy

### Entry Point (`proxy/src/main.rs`)

The proxy is a multi-port TCP listener that:

1. **Parses CLI args**: `--mode`, `--map`, `--record-dir`, `--control-port`, `--replay-strict-mode`
2. **Initializes mode-specific components**:
   - Record → creates `Recorder`
   - Replay/FullMock → creates `ReplayEngine` (wrapped in `Arc<Mutex>`)
3. **Creates shared state**: `TraceCorrelator`, protocol parsers, TLS config
4. **Spawns listeners**: One TCP listener per `--map` port mapping
5. **Spawns control API**: HTTP server on `--control-port` (default 9999)
6. **Spawns cleanup task**: Periodically removes stale traces (every 300s, max age 600s)

### Connection Handler (`proxy/src/connection.rs`)

Two handlers exist:
- `handle_connection()` — Plain TCP connections
- `handle_tls_connection()` — TLS MITM connections (peek first bytes, generate certs)

Both follow the same pattern:

```
1. Accept connection → get peer_port
2. Resolve association (wait up to ASSOCIATION_TIMEOUT_MS)
3. Detect protocol via confidence scoring on first bytes
4. If TLS detected → generate MITM cert, establish TLS with client
5. Open target connection (record mode) or skip (replay mode)
6. Spawn two tasks:
   ├── client_to_target: process_client_events()
   └── target_to_client: process_server_events()
7. Wait for both tasks or connection close
```

### `process_client_events()` (`connection.rs:205`)

The core event processing loop:

```rust
for event in events {
    // 1. Get current association (trace_id, scope_id)
    let assoc = association.lock().await.clone();

    // 2. If orphan in record mode, buffer for retro-binding
    if is_record_mode && assoc.is_orphan() && within_window {
        correlator.buffer_event_for_retro_bind(...).await;
    }

    // 3. Enrich event with scope, sequence, direction
    enrich_event(&mut event, scope_id, trace_id, direction, correlator).await;

    // 4. Record if recorder present
    if let Some(rec) = recorder { rec.save_event(event.clone()).await; }

    // 5. Replay match if engine present
    if let Some(engine) = replay_engine {
        match engine.lock().await.find_match_with_responses_typed(&event) {
            Ok(Some((_, _, response_bytes))) => { /* send responses */ }
            Ok(None) => { /* send protocol error */ }
            Err(err) => { /* send typed error */ }
        }
    }
}
```

---

## Protocol Parsers

### Detection (`core/src/protocols/filter.rs`)

`ProtocolDetector` runs all parsers' `detect()` against the first bytes and picks the highest confidence:

| Protocol | Confidence | Detection Signal |
|----------|-----------|-----------------|
| gRPC | 0.95 | HTTP/2 preface `PRI * HTTP/2.0` |
| PostgreSQL | 1.0 | Startup message `[0,3,0,0]` at bytes 4-8 |
| HTTP | 0.9 | Starts with `GET`, `POST`, `PUT`, etc. |
| Redis | 0.9 | Starts with `*`, `+`, `-`, `$`, `:` (RESP framing) |
| Generic TCP | 0.1 | Fallback — always matches with lowest score |

### Parser Trait

Each parser implements `ProtocolParser`:

```rust
trait ProtocolParser: Send + Sync {
    fn protocol_id(&self) -> &'static str;
    fn detect(data: &[u8]) -> f64;           // Confidence 0.0–1.0
    fn parse_client_data(&mut self, data: &[u8]) -> ParseResult;
    fn parse_server_data(&mut self, data: &[u8]) -> ParseResult;
    fn set_mode(&mut self, is_replay: bool);  // Affects synthetic responses
    fn on_replay_init(&self) -> Option<Vec<u8>>;  // Initial replay bytes
}
```

### ParseResult

```rust
struct ParseResult {
    events: Vec<RecordedEvent>,  // Extracted protocol events
    forward: Option<Vec<u8>>,    // Bytes to forward to target
    reply: Option<Vec<u8>>,      // Synthetic reply (e.g., PG Auth, Redis PONG)
    consumed: usize,             // Bytes consumed from input
}
```

### PostgreSQL Parser (`core/src/protocols/postgres.rs`)

Handles the PG wire protocol:

1. **Startup phase**: Detects `[0,3,0,0]` version bytes → synthesizes `AuthenticationOk` + `ReadyForQuery` in replay mode
2. **Normal phase**: Reads `tag(1) + length(4 BE) + payload` messages
3. **Query handling**: Recognizes `'Q'` tag, extracts SQL query string
4. **Replay mode**: Returns synthetic auth without forwarding to real PG; special `SELECT 1` handling

### Redis Parser (`core/src/protocols/redis.rs`)

Handles RESP (Redis Serialization Protocol):

1. Uses `redis_protocol` crate's `decode_bytes_mut`
2. Requires proper RESP framing: `*1\r\n$4\r\nPING\r\n` (not raw `PING`)
3. **Replay mode**: Synthesizes `+PONG\r\n` for PING commands
4. Extracts command name from RESP arrays for event metadata

### HTTP Parser (`core/src/protocols/http.rs`)

Handles HTTP/1.1:

1. Uses `httparse` crate for request/response line parsing
2. Extracts method, path, headers, body
3. Content-Length and Transfer-Encoding aware
4. Events carry full request/response data for replay matching

### gRPC Parser (`core/src/protocols/grpc.rs`)

Handles HTTP/2 framed gRPC:

1. Uses `h2` and `hpack` crates
2. Extracts stream IDs for multi-stream multiplexing
3. Each stream gets its own scope: `trace:{id}:conn:{N}:stream:{M}`
4. Handles gRPC status codes and trailers

---

## Trace Correlation System

### TraceCorrelator (`proxy/src/correlation.rs`)

The correlator is the bridge between the SDK (which knows trace IDs) and the proxy (which sees raw TCP connections). It maintains several concurrent data structures:

```
┌─────────────────────────────────────────────────────┐
│                  TraceCorrelator                      │
│                                                       │
│  connection_key_to_trace: HashMap<(port), (trace, proto)>  │
│  ├── Pre-registered by SDK's AssociateBySourcePort    │
│                                                       │
│  pending_connections: HashMap<port, PendingConnection> │
│  ├── Connections waiting for SDK association           │
│  ├── Contains oneshot::Sender to unblock proxy        │
│                                                       │
│  pending_event_buffers: HashMap<port, PendingEventBuffer>  │
│  ├── Events buffered during retro-bind window         │
│                                                       │
│  active_traces: HashMap<trace_id, TraceMetadata>      │
│  ├── Started traces with timestamps                   │
│                                                       │
│  trace_connection_counter: HashMap<trace_id, u64>     │
│  ├── Deterministic conn ID allocation per trace       │
│                                                       │
│  sequence_tracker: ScopeSequenceTracker               │
│  ├── Per-scope sequence counters                      │
│                                                       │
│  quarantined_events: QuarantinedEvents                │
│  └── Orphaned events that expired without binding     │
└─────────────────────────────────────────────────────┘
```

### Association Flow

```
Timeline:
────────────────────────────────────────────────────────
SDK:  StartTrace("t1")
                                                    ← proxy registers active trace
Client: TcpStream::connect(proxy_port)
                                                    ← proxy: on_new_connection(port) → pending
SDK:  AssociateBySourcePort("t1", port, Postgres)
                                                    ← proxy: resolves pending, assigns scope
Client: stream.write(query)
                                                    ← proxy: enriches with trace_id, scope_id
────────────────────────────────────────────────────────
```

**Critical ordering**: `AssociateBySourcePort` must arrive BEFORE the client sends data. Otherwise events are recorded as "orphan" and won't match during replay.

### Retro-Binding

If data arrives before association (within `retro_bind_window_ms`):

1. Events are cloned into `pending_event_buffers`
2. Events also fall through and are recorded as orphan (safety net)
3. When association arrives, buffered events are re-enriched with the correct trace/scope
4. If the window expires without association, events are quarantined

### Connection Lifecycle States

```
PendingAssociation → Attributed(Associated) → Attributed(Leased) → Attributed(Released)
```

- **PendingAssociation**: Connection opened, no SDK registration yet
- **Associated**: SDK has bound this connection to a trace
- **Leased**: Connection pool checkout (for pooled connections like sqlx)
- **Released**: Connection pool return

---

## Scope-Based Determinism

### ScopeId Format

The scope system provides hierarchical, deterministic identifiers:

| Pattern | Example | Meaning |
|---------|---------|---------|
| `trace:{id}:conn:{N}` | `trace:req-1:conn:0` | Nth connection in trace |
| `trace:{id}:conn:{N}:stream:{M}` | `trace:req-1:conn:0:stream:3` | HTTP/2 stream within connection |
| `trace:{id}:task:{path}` | `trace:req-1:task:0.1.2` | Spawned task hierarchy |
| `orphan:{N}` | `orphan:42` | Unattributed connection (no SDK) |

### Why Scopes Matter

During recording, events are indexed as:
```
(trace_id="req-1", scope_id="trace:req-1:conn:0", scope_sequence=0) → PG Startup
(trace_id="req-1", scope_id="trace:req-1:conn:0", scope_sequence=1) → PG Query
(trace_id="req-1", scope_id="trace:req-1:conn:1", scope_sequence=0) → Redis PING
```

During replay, the same scope structure reproduces:
```
New PG connection → allocate_connection_id("req-1") → conn:0
First PG event    → scope_sequence=0 → matches recorded startup
Second PG event   → scope_sequence=1 → matches recorded query
New Redis connection → allocate_connection_id("req-1") → conn:1
First Redis event → scope_sequence=0 → matches recorded PING
```

The determinism comes from:
1. Connection IDs are per-trace atomic counters (not random)
2. Scope sequences are per-scope atomic counters
3. Same trace replayed same way → same scopes → same sequences

---

## The Replay Engine

### RecordingIndex (`core/src/replay/recording_index.rs`)

Built from flat event list at startup:

```
RecordingIndex
└── traces: HashMap<String, RecordedTrace>
    └── "req-1": RecordedTrace
        └── scopes: HashMap<ScopeId, ScopeRecording>
            ├── "trace:req-1:conn:0": ScopeRecording
            │   ├── protocol: Postgres
            │   └── exchanges:
            │       ├── [0] MessageExchange { scope_sequence: 0, client: Startup, responses: [Auth, Ready] }
            │       └── [1] MessageExchange { scope_sequence: 1, client: Query, responses: [RowDesc, DataRow, Complete, Ready] }
            └── "trace:req-1:conn:1": ScopeRecording
                ├── protocol: Redis
                └── exchanges:
                    └── [0] MessageExchange { scope_sequence: 0, client: PING, responses: [+PONG] }
```

### Matching Algorithm (`engine.rs:221`)

```
find_match_with_responses(incoming_event):
  1. TYPED MATCH (scope-based):
     │ lookup index.get_exchange(trace_id, scope_id, scope_sequence)
     │ if found AND match_request(incoming, recorded): return responses
     │
  2. LOOKAHEAD (±3 window):
     │ for offset in [-3..+3]:
     │   lookup index.get_exchange(trace_id, scope_id, scope_sequence + offset)
     │   if match_request(incoming, recorded): return responses
     │
  3. LEGACY FALLBACK (protocol-based):
     │ lookup index.get(trace_id, protocol, sequence)
     │ if match_request(incoming, recorded): return responses
     │
  4. NO MATCH: return None
```

### Protocol-Aware Request Matching (`engine.rs:330`)

Each protocol has custom comparison logic:

- **HTTP**: Method + path + key headers (ignoring ephemeral headers like Date)
- **Redis**: Command + arguments (with TTL tolerance for SET/EXPIRE)
- **PostgreSQL**: Query text comparison
- **gRPC**: Service/method path + request body
- **TCP**: Byte-level comparison (exact match)

---

## SDK Integration

### Instrumentation Pattern

Services using Deja must instrument three things:

1. **Trace propagation** — Extract/inject trace IDs in request headers
2. **Connection association** — Tell proxy which connections belong to which trace
3. **Non-determinism capture** — Record UUIDs, timestamps, random values

### Axum Middleware (`sdk/src/axum.rs`)

```rust
// Automatically:
// 1. Extracts x-trace-id from incoming request headers
// 2. Sets task-local trace context
// 3. Sends StartTrace to proxy control API
// 4. On response: sends EndTrace
app.layer(DejaLayer::new(control_url));
```

### Connection Pool Wrappers (`sdk/src/pool.rs`)

```rust
// PostgreSQL: wraps sqlx::PgPool
let pool = DejaPool::new(pg_pool);
// On acquire(): sends ReassociateConnection + SET application_name = 'deja:<trace_id>'

// Redis: wraps redis::Client
let pool = DejaRedisPool::new(redis_client);
// On get_connection(): sends CLIENT SETNAME deja:<trace_id>
```

### Reqwest Wrapper (`sdk/src/reqwest.rs`)

```rust
// Wraps reqwest::Client to inject x-trace-id header on all outgoing requests
let client = DejaClient::new(reqwest_client);
```

### DejaRuntime + deja_run() (`common/src/runtime.rs`)

```rust
// Record mode: calls generator, captures result
// Replay mode: returns previously captured value
let uuid: Uuid = deja_run(&runtime, "uuid", || Uuid::new_v4()).await;
let now: SystemTime = deja_run(&runtime, "timestamp", || SystemTime::now()).await;
```

---

## Control API

### Endpoints (`proxy/src/control_api.rs`)

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/control/trace` | Handle ControlMessage (StartTrace, EndTrace, Associate, etc.) |
| `POST` | `/capture` | Record single non-deterministic value |
| `POST` | `/captures/batch` | Batch record values (SDK uses this) |
| `GET` | `/replay` | Query recorded non-deterministic value by (trace_id, kind, seq) |
| `POST` | `/orchestrate` | Trigger replay and compare against live service |
| `GET` | `/traces` | List all recorded trace IDs |
| `GET` | `/health` | Health check |

### Control Messages

```json
{"type": "start_trace", "trace_id": "req-1", "timestamp_ns": 1234}
{"type": "end_trace", "trace_id": "req-1", "timestamp_ns": 5678}
{"type": "associate_by_source_port", "trace_id": "req-1", "source_port": 55432, "protocol": "postgres"}
{"type": "reassociate_connection", "trace_id": "req-1", "source_port": 55432, "protocol": "postgres"}
{"type": "pool_checkout", "trace_id": "req-1", "source_port": 55432, "protocol": "postgres"}
{"type": "pool_return", "trace_id": "req-1", "source_port": 55432, "protocol": "postgres"}
```

### Idempotency

Control messages include per-sender-key sequence tracking and fingerprint deduplication. Duplicate messages return success without side effects.

---

## Non-Deterministic Value Capture

### The Problem

Functions like `Uuid::new_v4()`, `SystemTime::now()`, `rand::random()` produce different values every run. For replay to be deterministic, these must return the same values.

### The Solution

```
RECORD MODE:
  deja_run(runtime, "uuid", || Uuid::new_v4())
  │
  ├── Calls Uuid::new_v4() → "550e8400-..."
  ├── runtime.capture_value("uuid", "550e8400-...") → batched
  ├── SDK flush() → POST /captures/batch to proxy
  └── Returns "550e8400-..."

REPLAY MODE:
  deja_run(runtime, "uuid", || Uuid::new_v4())
  │
  ├── runtime.replay_value("uuid") → GET /replay?trace_id=...&kind=uuid&seq=0
  ├── Proxy looks up in recordings → "550e8400-..."
  └── Returns "550e8400-..." (same as recorded)
```

### Batching & Flushing (`core/src/runtime.rs`)

The SDK batches captures for efficiency:
- **Flush threshold**: 100 captures
- **Flush interval**: 5 seconds
- **Manual flush**: `runtime.flush().await` — must be called before proxy shutdown

If captures aren't flushed before the proxy shuts down, replay will fail with "No recorded value found."

---

## TLS MITM Interception

### How It Works

1. Proxy peeks at first bytes of incoming connection
2. If TLS ClientHello detected → extract SNI (Server Name Indication)
3. Generate ephemeral certificate for that hostname using CA cert/key
4. Establish TLS with client using generated cert
5. Establish TLS with real backend using original SNI
6. Bidirectional relay through both TLS sessions (data visible to proxy as plaintext)

### Configuration

```bash
cargo run --bin deja-proxy -- \
  --forward-proxy-port 8080 \
  --ca-cert ./certs/ca.crt \
  --ca-key ./certs/ca.key
```

Client must trust the CA certificate for MITM to work without errors.

---

## Orchestrated Replay

### Concept

Instead of the proxy serving all recorded responses, orchestrated replay:
1. Finds the **trigger event** (first HTTP/gRPC request in a trace)
2. Sends it to the **real service** (new code version)
3. The service processes the request, hitting the proxy for DB/Redis (which replays recorded responses)
4. Compares the service's actual response against the recorded expected response
5. Reports pass/fail with behavioral diff

### Flow

```
POST /orchestrate { trace_id: "req-1" }
│
├── Find trigger: GET /api/payment (recorded HTTP request)
├── Find expected: 200 OK { "status": "success" } (recorded response)
├── Send trigger to service_url
│   └── Service processes request:
│       ├── Queries Postgres → proxy replays recorded PG response
│       ├── Queries Redis → proxy replays recorded Redis response
│       └── Returns actual response
├── Compare actual vs expected:
│   ├── Status code match? ✓
│   ├── Body match? ✓ (with configured tolerances)
│   └── Headers match? ✓
└── Return { pass: true, trace_id: "req-1" }
```

---

## Event Format & Storage

### RecordedEvent

```rust
struct RecordedEvent {
    trace_id: String,
    scope_id: ScopeId,
    scope_sequence: u64,
    global_sequence: u64,
    direction: EventDirection,      // ClientToServer | ServerToClient
    timestamp_ns: u64,
    connection_id: String,
    event_type: String,             // "pg_query", "redis_command", "http_request", etc.
    data: Vec<u8>,                  // Raw protocol data
    metadata: HashMap<String, String>,  // Headers, status codes, etc.
}
```

### Storage Formats

**Binary (default)**: `events.bin`
```
[u32 length][bincode serialized event][u32 length][bincode serialized event]...
```

**JSON** (`DEJA_STORAGE_FORMAT=json`): `events.jsonl`
```
{"trace_id":"req-1","scope_id":"trace:req-1:conn:0",...}\n
{"trace_id":"req-1","scope_id":"trace:req-1:conn:0",...}\n
```

### Directory Structure

```
recordings/
└── sessions/
    └── 20260305_143022/
        └── events.bin (or events.jsonl)
```

---

## Key Invariants

1. **Associate before write**: SDK must send `AssociateBySourcePort` before the client writes data on that connection. Otherwise events become orphans.

2. **StartTrace before associate**: The proxy must know about a trace before connections can be bound to it.

3. **Flush before shutdown**: SDK must call `flush()` to persist batched non-deterministic captures before the proxy process exits.

4. **No synthetic replies in replay**: Parser-generated replies (PG Auth, Redis PONG) are suppressed when a replay engine is present, to prevent double-sends.

5. **Always respond in replay**: Both `Ok(None)` (no match) and `Err` (attribution error) must send a protocol-appropriate error response. Silent drops cause client hangs.

6. **Deterministic connection IDs**: Connection IDs are per-trace atomic counters, not random. Same trace replayed produces same conn IDs.

7. **Scope sequences are per-scope**: Each `(trace_id, scope_id)` pair has its own sequence counter starting at 0.

8. **Retro-bind is a safety net, not the happy path**: Events buffered during the retro-bind window are also recorded as orphans. Retro-binding re-enriches them if association arrives in time.

---

## Configuration Reference

### CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--mode` | `record` | Operation mode: record, replay, fullmock, orchestrated |
| `--map PORT:HOST:PORT` | required | Port mapping (multiple allowed) |
| `--record-dir` | `./recordings` | Directory for recordings |
| `--control-port` | `9999` | Control API HTTP port |
| `--replay-strict-mode` | `lenient` | `strict` (fail-closed) or `lenient` (fail-open) |
| `--forward-proxy-port` | none | Enable forward proxy mode on this port |
| `--ca-cert` / `--ca-key` | none | TLS MITM CA certificate and key |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DEJA_MODE` | `record` | Fallback for --mode |
| `DEJA_RECORDING_PATH` | `./recordings` | Fallback for --record-dir |
| `DEJA_PORT_MAPS` | none | Comma-separated mappings |
| `DEJA_CONTROL_PORT` | `9999` | Fallback for --control-port |
| `DEJA_ASSOCIATION_TIMEOUT_MS` | `500` | How long to wait for SDK association |
| `DEJA_RETRO_BIND_WINDOW_MS` | `500` | Retro-bind buffer window |
| `DEJA_REPLAY_STRICT` | `lenient` | Replay strictness |
| `DEJA_STORAGE_FORMAT` | `binary` | `binary` or `json` |
| `RUST_LOG` | none | Logging level (e.g., `deja_proxy=debug`) |
