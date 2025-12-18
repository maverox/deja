# DÉJÀ

## Production Traffic Replay & Regression Testing Framework

> *"Haven't we seen this request before?"*

---

# Table of Contents

1. [Executive Summary](#executive-summary)
2. [Product Vision](#product-vision)
3. [Brand Identity](#brand-identity)
4. [System Architecture](#system-architecture)
5. [Core Components](#core-components)
   - [Recording System](#recording-system)
   - [Protocol Parsers](#protocol-parsers)
   - [Replay Engine](#replay-engine)
   - [Diff Engine](#diff-engine)
6. [Cross-Stack Support & Pluggability](#cross-stack-support--pluggability)
7. [SDK Design](#sdk-design)
8. [Orchestration & Deployment](#orchestration--deployment)
9. [Interface Design](#interface-design)
   - [CLI Interface](#cli-interface)
   - [Web API](#web-api)
   - [Configuration Files](#configuration-files)
10. [CI/CD Integration](#cicd-integration)
11. [Migration Handling](#migration-handling)
12. [Technical Specifications](#technical-specifications)
13. [Appendix](#appendix)

---

# Executive Summary

**Déjà** is a comprehensive framework for recording live API calls and critical system events (Redis, database, I/O operations), aggregating them by correlation ID, and replaying them in isolated environments against different service versions to detect regressions through behavioral diffs.

## Core Value Proposition

- **Record** production traffic with full context (requests, responses, DB queries, cache operations)
- **Replay** recorded transactions against new code versions in isolated environments
- **Diff** behavioral changes: response differences, database mutations, side effects
- **Catch** regressions before they reach production

## Key Differentiators

| vs. Competitor | Déjà Advantage |
|----------------|----------------|
| Load Testing (k6, Locust) | Uses REAL traffic patterns, not synthetic |
| Chaos Engineering (Gremlin) | Tests CODE changes, not infrastructure |
| Shadow Traffic | Provides DIFFS, not just mirroring |
| Contract Tests (Pact) | Tests BEHAVIOR, not just interfaces |
| Integration Tests | Uses PRODUCTION scenarios, not contrived examples |

---

# Product Vision

## The Problem

Every deployment is a leap of faith. Teams write unit tests, integration tests, and end-to-end tests, but production traffic is infinitely more creative than any test suite. Edge cases, unexpected input combinations, and real-world usage patterns constantly surprise even the most thoroughly tested systems.

## The Solution

Déjà captures the complexity of production and uses it as the ultimate test suite. By recording real transactions and replaying them against new code, teams can see exactly how their changes affect real-world behavior before users do.

## Target Users

- **Platform/Infrastructure Teams** - Building regression safety nets for microservices
- **Backend Engineers** - Validating changes against production behavior
- **DevOps/SRE** - Ensuring deployment confidence
- **QA Engineers** - Augmenting test suites with production reality

---

# Brand Identity

## Name: DÉJÀ

**Etymology:** French for "already" — from the phrase "déjà vu" meaning "already seen"

**Why It Works:**
- Instant recognition — everyone knows "déjà vu"
- Perfect metaphor — "haven't we seen this request before?"
- Short, punchy, memorable
- Incomplete phrase creates intrigue
- Works as a verb: "Déjà your deployment"
- Premium, sophisticated feel

## Taglines

| Primary | Secondary Options |
|---------|-------------------|
| *"Haven't we seen this request before?"* | *"Same request. New code. What changed?"* |
| | *"Already tested. Test again."* |
| | *"Production traffic replay that feels familiar"* |
| | *"The feeling your code has seen this before"* |

## Alternative Name Variations

| Name | Use Case |
|------|----------|
| **Déjà Test** | Emphasizes testing purpose |
| **Déjà Run** | Action-oriented, developer-friendly |
| **Déjà Diff** | Emphasizes comparison aspect |
| **Déjà Flow** | Emphasizes traffic flow |

## Logo Concepts

### Option 1: Circular Arrows (Recommended)
```
       ↺
     déjà
```
Circular arrow forms a halo or integrates with the accent, symbolizing replay/repeat.

### Option 2: Mirror/Reflection
```
     déjà
     ɒſəp
```
Mirrored/faded reflection below represents "seeing again."

### Option 3: Ghost/Echo Effect
```
     déjà
      déjà
       déjà
```
Stacked, fading copies show the "echo" / "repeat" concept.

### Option 4: Rewind Symbol
```
    ◀◀ déjà
```
Classic rewind arrows integrated with the wordmark.

## Color Palette

### Primary Colors

| Color | Hex | Usage |
|-------|-----|-------|
| **Indigo** | `#6366F1` | Primary brand color |
| **Violet** | `#8B5CF6` | Accent, highlights |
| **Slate** | `#0F172A` | Dark backgrounds, text |
| **White** | `#F8FAFC` | Light backgrounds |

### Secondary Colors

| Color | Hex | Usage |
|-------|-----|-------|
| **Emerald** | `#10B981` | Success states |
| **Amber** | `#F59E0B` | Warning states, diffs |
| **Rose** | `#F43F5E` | Error states |
| **Cyan** | `#06B6D4` | Info, links |

### Gradient

```css
background: linear-gradient(135deg, #6366F1 0%, #8B5CF6 100%);
```

## Typography

| Element | Font | Weight |
|---------|------|--------|
| **Headlines** | Inter | Bold (700) |
| **Body** | Inter | Regular (400) |
| **Code/CLI** | JetBrains Mono | Regular (400) |

## Voice & Tone

- **Friendly but professional** — approachable yet trustworthy
- **Confident** — we know this works
- **Developer-first** — speak their language
- **Subtle wordplay** — déjà vu references okay, but don't overdo it

### Example Copy

**Homepage Hero:**
> Stop deploying blind.
> Replay real production traffic against your changes.
> See exactly what breaks before your users do.

**Feature Highlights:**
- "Record once, replay forever"
- "Same traffic, new code, instant diff"
- "Catch regressions before they catch you"

**CLI Personality:**
```bash
$ deja run --version v2.0
✨ Haven't we seen this before? Starting replay...
📼 Loaded 1,247 recordings from the past 24h
🔄 Replaying against user-service:v2.0
✅ 1,241 passed | ⚠️ 6 diffs | ❌ 0 errors
✨ Same requests, but 6 things changed! View report: https://...
```

---

# System Architecture

## High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PRODUCTION ENVIRONMENT                              │
│  ┌─────────┐    ┌─────────────────────────────────────────────────┐        │
│  │ Request │───▶│              Instrumented Service                │        │
│  └─────────┘    │  ┌─────────────────────────────────────────┐    │        │
│                 │  │         Recording Interceptor            │    │        │
│                 │  │  • HTTP in/out  • Redis calls            │    │        │
│                 │  │  • DB queries   • External APIs          │    │        │
│                 │  │  • Time/Random  • File I/O               │    │        │
│                 │  └──────────────────┬──────────────────────┘    │        │
│                 └─────────────────────┼───────────────────────────┘        │
│                                       │                                     │
│                                       ▼                                     │
│                        ┌──────────────────────────┐                        │
│                        │   Recording Store        │                        │
│                        │   (by correlation_id)    │                        │
│                        └──────────────────────────┘                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      REPLAY / REGRESSION ENVIRONMENT                        │
│                                                                             │
│  ┌─────────────────┐     ┌─────────────────────────────────────────┐       │
│  │ Recording Store │────▶│         Delta Version Service           │       │
│  └─────────────────┘     │  ┌─────────────────────────────────┐    │       │
│                          │  │      Mock Interceptor           │    │       │
│                          │  │  • Replay recorded responses    │    │       │
│                          │  │  • Capture new behavior         │    │       │
│                          │  └─────────────────────────────────┘    │       │
│                          └─────────────────────────────────────────┘       │
│                                          │                                  │
│                                          ▼                                  │
│                          ┌───────────────────────────────┐                 │
│                          │        Diff Engine            │                 │
│                          │  • Response diff              │                 │
│                          │  • DB mutation diff           │                 │
│                          │  • Side-effect diff           │                 │
│                          └───────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Component Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         DÉJÀ FRAMEWORK                                          │
│                                                                                 │
│  ┌───────────────────────────────────────────────────────────────────────────┐ │
│  │                     CORE ENGINE (Rust)                                    │ │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐ │ │
│  │  │  Event      │ │  Recording  │ │   Replay    │ │      Diff           │ │ │
│  │  │  Schema     │ │   Store     │ │   Engine    │ │     Engine          │ │ │
│  │  │  (protobuf) │ │             │ │             │ │                     │ │ │
│  │  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────────────┘ │ │
│  │                                                                           │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │                    Protocol Parsers                                 │ │ │
│  │  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────────────────┐│ │ │
│  │  │  │ HTTP   │ │ Redis  │ │Postgres│ │ MySQL  │ │ gRPC/Protobuf      ││ │ │
│  │  │  │ Parser │ │ Parser │ │ Parser │ │ Parser │ │    Parser          ││ │ │
│  │  │  └────────┘ └────────┘ └────────┘ └────────┘ └────────────────────┘│ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────────────────────┘ │
│                                    ▲                                           │
│            ┌───────────────────────┼───────────────────────┐                   │
│            │                       │                       │                   │
│  ┌─────────┴─────────┐ ┌──────────┴──────────┐ ┌─────────┴─────────┐          │
│  │   Language SDKs   │ │   Sidecar Proxy     │ │   eBPF Agent      │          │
│  │                   │ │                     │ │                   │          │
│  │ • Python SDK      │ │ • TCP interception  │ │ • Zero-config     │          │
│  │ • Node SDK        │ │ • TLS termination   │ │ • Kernel-level    │          │
│  │ • Rust SDK        │ │ • Protocol detect   │ │ • Auto-discovery  │          │
│  │ • Go SDK          │ │                     │ │                   │          │
│  │ • Java SDK        │ │                     │ │                   │          │
│  └───────────────────┘ └─────────────────────┘ └───────────────────┘          │
│                                                                                 │
│         CHOOSE YOUR INTEGRATION LEVEL                                           │
│         SDK = Most context, some code changes                                   │
│         Sidecar = No code changes, good context                                 │
│         eBPF = Zero changes, limited context                                    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Replay Orchestration System

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         REPLAY ORCHESTRATION SYSTEM                             │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                         INTERFACES                                       │   │
│  │   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │   │
│  │   │   CLI    │  │  Web UI  │  │   API    │  │ CI/CD    │  │  SDK     │ │   │
│  │   │          │  │          │  │          │  │ Plugins  │  │  Embed   │ │   │
│  │   └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘ │   │
│  └────────┼─────────────┼───────────┼─────────────┼─────────────┼────────┘   │
│           └─────────────┴───────────┼─────────────┴─────────────┘            │
│                                     ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │                      REPLAY CONTROL PLANE                               │  │
│  │                                                                         │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌───────────────┐  │  │
│  │  │  Replay     │  │  Version    │  │ Environment │  │   Diff &      │  │  │
│  │  │  Scheduler  │  │  Manager    │  │  Provisioner│  │   Report Gen  │  │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └───────────────┘  │  │
│  │                                                                         │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌───────────────┐  │  │
│  │  │  Recording  │  │  Migration  │  │   Secret    │  │   Webhook &   │  │  │
│  │  │  Selector   │  │  Runner     │  │   Manager   │  │   Notification│  │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └───────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
│                                     │                                         │
│                                     ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │                    ENVIRONMENT ORCHESTRATOR                             │  │
│  │                                                                         │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │  │
│  │  │                    Ephemeral Environment                        │   │  │
│  │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │   │  │
│  │  │  │ Service │  │   DB    │  │  Redis  │  │  Mock   │            │   │  │
│  │  │  │ (Delta) │  │ (Seeded)│  │ (Seeded)│  │ External│            │   │  │
│  │  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │   │  │
│  │  └─────────────────────────────────────────────────────────────────┘   │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

# Core Components

## Recording System

### Correlation & Causality Tracking

Every IO operation needs to be associated with a root request AND ordered:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CorrelationContext {
    pub trace_id: Uuid,           // Root request identifier
    pub span_id: Uuid,            // Current operation
    pub parent_span_id: Option<Uuid>,
    pub sequence: AtomicU64,      // Ordering within trace
    pub timestamp: SystemTime,
    pub causality_clock: VectorClock,  // For distributed ordering
}
```

### Event Types

```rust
#[derive(Serialize, Deserialize)]
pub enum RecordedEvent {
    // Inbound
    HttpRequest {
        method: String,
        path: String,
        headers: HashMap<String, String>,
        body: Bytes,
    },
    
    // Outbound / Side Effects
    HttpResponse {
        status: u16,
        headers: HashMap<String, String>,
        body: Bytes,
        latency_ms: u64,
    },
    
    DbQuery {
        query: String,
        params: Vec<SqlValue>,
        result: DbResult,
        affected_rows: Option<u64>,
    },
    
    RedisCommand {
        command: String,
        args: Vec<RedisValue>,
        result: RedisValue,
    },
    
    ExternalApiCall {
        url: String,
        request: HttpRequest,
        response: HttpResponse,
    },
    
    // Non-deterministic sources (CRITICAL for replay)
    TimeCapture { timestamp: SystemTime },
    RandomCapture { seed: u64, values: Vec<u8> },
    UuidGeneration { generated: Uuid },
    
    // State snapshots
    DbSnapshot {
        tables: HashMap<String, Vec<Row>>,
        point_in_time: SystemTime,
    },
}
```

### Recording Structure

```rust
#[derive(Serialize, Deserialize)]
pub struct TransactionRecording {
    pub id: Uuid,
    pub correlation_id: String,
    pub recorded_at: SystemTime,
    pub service_version: String,
    pub environment: String,
    
    // The actual recorded data
    pub initial_request: HttpRequest,
    pub final_response: HttpResponse,
    pub events: Vec<TimestampedEvent>,
    
    // State context
    pub db_state_before: Option<DbSnapshot>,
    pub db_state_after: Option<DbSnapshot>,
    pub db_mutations: Vec<DbMutation>,
    
    // Metadata for replay
    pub non_deterministic_inputs: NonDeterministicInputs,
}

#[derive(Serialize, Deserialize)]
pub struct NonDeterministicInputs {
    pub timestamps: Vec<(u64, SystemTime)>,  // sequence -> time
    pub random_values: Vec<(u64, Vec<u8>)>,  // sequence -> random bytes
    pub uuids: Vec<(u64, Uuid)>,             // sequence -> uuid
}
```

---

## Protocol Parsers

Protocol parsers are the **linchpin** of pluggability. They transform raw bytes into semantic events that can be recorded, replayed, and diffed.

### Why Protocol Parsers Are Critical

```
WITHOUT GOOD PROTOCOL PARSERS:
• You can only record raw bytes (useless for replay)
• Can't correlate requests with responses
• Can't do semantic diffing ("same query, different order")
• Can't mock responses (don't know what to mock)
• Can't filter/sample intelligently
• No support for new databases/protocols without code changes

WITH PLUGGABLE PROTOCOL PARSERS:
✓ Support ANY protocol (built-in or custom)
✓ Semantic understanding (queries, keys, operations)
✓ Intelligent mocking during replay
✓ Meaningful diffs ("this query returned 5 rows vs 4")
✓ Zero-code integration via sidecar proxy
✓ Plugin system for proprietary protocols
✓ Same recording format regardless of capture method
```

### Layered Parser Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LAYERED PARSER ARCHITECTURE                              │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                      Application Layer                                │ │
│  │   ┌─────────────────────────────────────────────────────────────┐    │ │
│  │   │              Semantic Events (what we record)               │    │ │
│  │   │   • DbQuery { sql, params, result }                         │    │ │
│  │   │   • CacheGet { key, value, hit }                            │    │ │
│  │   │   • HttpRequest { method, path, headers, body }             │    │ │
│  │   └─────────────────────────────────────────────────────────────┘    │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    ▲                                        │
│                                    │ emit                                   │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                      Protocol Layer                                   │ │
│  │   ┌─────────────────────────────────────────────────────────────┐    │ │
│  │   │           Protocol-Specific Parsers                         │    │ │
│  │   │   • RedisParser (RESP2/3)                                   │    │ │
│  │   │   • PostgresParser (wire protocol)                          │    │ │
│  │   │   • MySQLParser (client/server protocol)                    │    │ │
│  │   │   • HttpParser (h1/h2)                                      │    │ │
│  │   └─────────────────────────────────────────────────────────────┘    │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    ▲                                        │
│                                    │ frames                                 │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                       Framing Layer                                   │ │
│  │   ┌─────────────────────────────────────────────────────────────┐    │ │
│  │   │              Message Boundary Detection                     │    │ │
│  │   │   • Length-prefixed (Postgres, MySQL)                       │    │ │
│  │   │   • Delimiter-based (Redis RESP, HTTP/1.1)                  │    │ │
│  │   │   • Frame-based (HTTP/2)                                    │    │ │
│  │   └─────────────────────────────────────────────────────────────┘    │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    ▲                                        │
│                                    │ bytes                                  │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                       Transport Layer                                 │ │
│  │   ┌─────────────────────────────────────────────────────────────┐    │ │
│  │   │              TLS Termination / Raw TCP                      │    │ │
│  │   └─────────────────────────────────────────────────────────────┘    │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Core Parser Trait

```rust
/// The core trait all protocol parsers implement
#[async_trait]
pub trait ProtocolParser: Send + Sync {
    /// Unique identifier for this protocol
    fn protocol_id(&self) -> &'static str;
    
    /// Try to detect if this parser handles the given bytes
    /// Returns confidence score 0.0 - 1.0
    fn detect(&self, peek: &[u8]) -> f32;
    
    /// Create a new connection state machine
    fn new_connection(&self) -> Box<dyn ConnectionParser>;
}

/// State machine for a single connection
#[async_trait]
pub trait ConnectionParser: Send {
    /// Feed bytes from client -> server direction
    /// Returns parsed events and any bytes to forward
    fn parse_client_data(&mut self, data: &[u8]) -> ParseResult;
    
    /// Feed bytes from server -> client direction  
    fn parse_server_data(&mut self, data: &[u8]) -> ParseResult;
    
    /// Get current connection state (for debugging/metrics)
    fn state(&self) -> ConnectionState;
    
    /// Reset parser state (e.g., after error)
    fn reset(&mut self);
}

pub struct ParseResult {
    /// Semantic events extracted from the data
    pub events: Vec<ProtocolEvent>,
    /// Bytes to forward to the other side (possibly modified)
    pub forward: Bytes,
    /// Any parse errors encountered
    pub errors: Vec<ParseError>,
    /// Does the parser need more data?
    pub needs_more: bool,
}
```

### Protocol Events

```rust
/// Protocol-level events (before conversion to semantic events)
#[derive(Debug, Clone)]
pub enum ProtocolEvent {
    // Redis
    RedisCommand { command: String, args: Vec<RedisValue> },
    RedisResponse { value: RedisValue },
    
    // Postgres  
    PgQuery { query: String },
    PgParse { name: String, query: String, param_types: Vec<Oid> },
    PgBind { portal: String, statement: String, params: Vec<Option<Bytes>> },
    PgExecute { portal: String, max_rows: i32 },
    PgDataRow { values: Vec<Option<Bytes>> },
    PgCommandComplete { tag: String },
    PgError { severity: String, code: String, message: String },
    
    // MySQL
    MySqlQuery { query: String },
    MySqlPrepare { query: String },
    MySqlExecute { statement_id: u32, params: Vec<MySqlValue> },
    MySqlResultSet { columns: Vec<Column>, rows: Vec<Vec<MySqlValue>> },
    MySqlError { code: u16, state: String, message: String },
    
    // HTTP
    HttpRequest { method: String, uri: String, version: Version, headers: HeaderMap, body: Bytes },
    HttpResponse { status: u16, version: Version, headers: HeaderMap, body: Bytes },
    
    // Generic
    Raw { direction: Direction, data: Bytes },
}
```

### Redis Parser Implementation

```rust
pub struct RedisParser;

impl ProtocolParser for RedisParser {
    fn protocol_id(&self) -> &'static str { "redis" }
    
    fn detect(&self, peek: &[u8]) -> f32 {
        if peek.is_empty() {
            return 0.0;
        }
        
        // RESP protocol markers
        match peek[0] {
            b'*' | b'+' | b'-' | b':' | b'$' => 0.9,
            // Inline commands (PING, QUIT, etc.)
            b'P' | b'Q' | b'I' if peek.len() >= 4 => 0.5,
            _ => 0.0,
        }
    }
    
    fn new_connection(&self) -> Box<dyn ConnectionParser> {
        Box::new(RedisConnectionParser::new())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RedisValue {
    Null,
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<RedisValue>),
    // RESP3 additions
    Double(f64),
    Boolean(bool),
    BigNumber(String),
    Map(Vec<(RedisValue, RedisValue)>),
    Set(Vec<RedisValue>),
    Verbatim { format: String, data: Bytes },
    Push { kind: String, data: Vec<RedisValue> },
}
```

### PostgreSQL Parser Implementation

```rust
pub struct PostgresParser;

impl ProtocolParser for PostgresParser {
    fn protocol_id(&self) -> &'static str { "postgres" }
    
    fn detect(&self, peek: &[u8]) -> f32 {
        if peek.len() < 8 {
            return 0.0;
        }
        
        // Startup message: length (4 bytes) + protocol version (4 bytes)
        let len = u32::from_be_bytes([peek[0], peek[1], peek[2], peek[3]]);
        let version = u32::from_be_bytes([peek[4], peek[5], peek[6], peek[7]]);
        
        // Protocol 3.0 = 196608, SSL request = 80877103
        if version == 196608 || version == 80877103 {
            return 0.95;
        }
        
        // Check for message type byte (after startup)
        match peek[0] {
            b'Q' | b'P' | b'B' | b'E' | b'C' | b'D' | b'H' | b'S' | b'F' | b'd' | b'c' | b'f' | b'X' => 0.7,
            _ => 0.0,
        }
    }
    
    fn new_connection(&self) -> Box<dyn ConnectionParser> {
        Box::new(PostgresConnectionParser::new())
    }
}
```

### Protocol Registry

```rust
pub struct ProtocolRegistry {
    parsers: RwLock<Vec<Arc<dyn ProtocolParser>>>,
}

impl ProtocolRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            parsers: RwLock::new(Vec::new()),
        };
        
        // Register built-in parsers
        registry.register(Arc::new(RedisParser));
        registry.register(Arc::new(PostgresParser));
        registry.register(Arc::new(MySqlParser));
        registry.register(Arc::new(HttpParser));
        
        registry
    }
    
    pub fn register(&self, parser: Arc<dyn ProtocolParser>) {
        self.parsers.write().push(parser);
    }
    
    /// Auto-detect protocol from initial bytes
    pub fn detect(&self, peek: &[u8]) -> Option<Arc<dyn ProtocolParser>> {
        let parsers = self.parsers.read();
        
        parsers
            .iter()
            .map(|p| (p.clone(), p.detect(peek)))
            .filter(|(_, score)| *score > 0.5)
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(p, _)| p)
    }
}
```

---

## Replay Engine

```rust
pub struct ReplayEngine {
    recording: TransactionRecording,
    event_index: usize,
    mode: ReplayMode,
}

pub enum ReplayMode {
    /// Use recorded responses for all external calls
    FullMock,
    /// Hit real dependencies, compare results
    LiveComparison,
    /// Hybrid: mock some, live others
    Selective(HashSet<EventType>),
}

impl ReplayEngine {
    /// Provides the next recorded response for an external call
    pub fn mock_response(&mut self, event_type: &EventType) -> Option<RecordedEvent> {
        self.recording.events
            .iter()
            .skip(self.event_index)
            .find(|e| e.matches_type(event_type))
            .cloned()
    }
    
    /// Inject non-deterministic values
    pub fn get_time(&mut self, sequence: u64) -> SystemTime {
        self.recording.non_deterministic_inputs
            .timestamps
            .iter()
            .find(|(seq, _)| *seq == sequence)
            .map(|(_, t)| *t)
            .unwrap_or_else(SystemTime::now)
    }
}
```

---

## Diff Engine

```rust
#[derive(Debug, Serialize)]
pub struct RegressionResult {
    pub recording_id: Uuid,
    pub baseline_version: String,
    pub delta_version: String,
    
    pub response_diff: Option<ResponseDiff>,
    pub db_diff: Option<DbDiff>,
    pub side_effect_diffs: Vec<SideEffectDiff>,
    
    pub verdict: RegressionVerdict,
}

#[derive(Debug, Serialize)]
pub struct ResponseDiff {
    pub status_changed: Option<(u16, u16)>,
    pub body_diff: JsonDiff,
    pub header_diffs: Vec<HeaderDiff>,
}

#[derive(Debug, Serialize)]
pub struct DbDiff {
    pub missing_mutations: Vec<DbMutation>,
    pub extra_mutations: Vec<DbMutation>,
    pub different_mutations: Vec<(DbMutation, DbMutation)>,
}

impl DiffEngine {
    pub fn compare(
        baseline: &TransactionRecording,
        replay_result: &ReplayResult,
    ) -> RegressionResult {
        RegressionResult {
            response_diff: Self::diff_responses(
                &baseline.final_response,
                &replay_result.response,
            ),
            db_diff: Self::diff_db_mutations(
                &baseline.db_mutations,
                &replay_result.db_mutations,
            ),
            // ...
        }
    }
}
```

---

# Cross-Stack Support & Pluggability

## Integration Options

### Option 1: Sidecar Proxy (Most Universal)

```
┌─────────────────────────────────────────────────────────────────┐
│                         Pod / Container                         │
│                                                                 │
│   ┌─────────────┐         ┌─────────────────────────────────┐  │
│   │  Your App   │◀───────▶│      Déjà Proxy (Rust)          │  │
│   │  (any lang) │         │                                 │  │
│   │             │         │  • Intercepts all TCP traffic   │  │
│   │  Redis ─────┼────────▶│  • Protocol-aware parsing       │  │
│   │  Postgres ──┼────────▶│  • Records/replays transparently│  │
│   │  HTTP ──────┼────────▶│  • Zero code changes            │  │
│   └─────────────┘         └─────────────────────────────────┘  │
│                                      │                          │
│                                      ▼                          │
│                           ┌─────────────────┐                   │
│                           │ Actual Services │                   │
│                           └─────────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
```

**Pros:** Zero code changes, works with ANY language  
**Cons:** Can't capture application-level context easily

### Option 2: SDK + Collector Agent (Balanced)

```
┌─────────────────────────────────────────────────────────────────┐
│   ┌─────────────────────────────────────────────────────────┐  │
│   │                    Your Application                      │  │
│   │   ┌─────────────────────────────────────────────────┐   │  │
│   │   │              Thin SDK (per language)            │   │  │
│   │   │  • Wraps DB/Redis/HTTP clients                  │   │  │
│   │   │  • Emits events via UDP/Unix socket             │   │  │
│   │   │  • Propagates correlation context               │   │  │
│   │   └─────────────────────────────────────────────────┘   │  │
│   └─────────────────────────────────────────────────────────┘  │
│                              │                                  │
│                              │ UDP / Unix Socket (async)        │
│                              ▼                                  │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │                Local Agent (Rust binary)                │  │
│   │  • Aggregates events by correlation_id                  │  │
│   │  • Handles storage, batching, compression               │  │
│   │  • Manages replay mock server                           │  │
│   └─────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Option 3: eBPF-based (Zero Instrumentation)

```
┌─────────────────────────────────────────────────────────────────┐
│                          Linux Kernel                           │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │                    eBPF Programs                         │  │
│   │  • Intercept syscalls (read/write/sendto/recvfrom)      │  │
│   │  • Protocol detection & parsing                          │  │
│   │  • Correlation via socket/process tracking               │  │
│   └─────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│              Userspace Agent (Rust + libbpf)                    │
└─────────────────────────────────────────────────────────────────┘
```

**Pros:** Truly zero instrumentation  
**Cons:** Linux only, complex, kernel version dependencies

## Universal Event Schema (Protobuf)

```protobuf
syntax = "proto3";
package deja.v1;

message RecordedEvent {
  string trace_id = 1;
  string span_id = 2;
  optional string parent_span_id = 3;
  uint64 sequence = 4;
  google.protobuf.Timestamp timestamp = 5;
  
  oneof event {
    HttpRequestEvent http_request = 10;
    HttpResponseEvent http_response = 11;
    DbQueryEvent db_query = 12;
    DbResultEvent db_result = 13;
    CacheGetEvent cache_get = 14;
    CacheSetEvent cache_set = 15;
    ExternalCallEvent external_call = 16;
    NonDeterministicEvent non_deterministic = 17;
  }
  
  map<string, string> metadata = 100;
}

message DbQueryEvent {
  DbType db_type = 1;
  string query = 2;
  repeated Value params = 3;
  string connection_id = 4;
  
  enum DbType {
    DB_TYPE_UNSPECIFIED = 0;
    DB_TYPE_POSTGRES = 1;
    DB_TYPE_MYSQL = 2;
    DB_TYPE_SQLITE = 3;
    DB_TYPE_MONGODB = 4;
  }
}

message CacheGetEvent {
  CacheType cache_type = 1;
  string key = 2;
  optional bytes value = 3;
  bool hit = 4;
  
  enum CacheType {
    CACHE_TYPE_UNSPECIFIED = 0;
    CACHE_TYPE_REDIS = 1;
    CACHE_TYPE_MEMCACHED = 2;
    CACHE_TYPE_LOCAL = 3;
  }
}
```

---

# SDK Design

## Core SDK Interface (Language-Agnostic)

```
┌─────────────────────────────────────────────────────────────────┐
│                       SDK Interface                             │
│                                                                 │
│  INITIALIZATION                                                 │
│  ──────────────                                                 │
│  deja.init(config)                                              │
│                                                                 │
│  CONTEXT PROPAGATION                                            │
│  ───────────────────                                            │
│  deja.start_trace(request) -> Context                           │
│  deja.current_context() -> Context                              │
│  deja.with_context(ctx, fn)                                     │
│                                                                 │
│  WRAPPERS (return instrumented versions)                        │
│  ────────                                                       │
│  deja.wrap_db(connection) -> InstrumentedConnection             │
│  deja.wrap_redis(client) -> InstrumentedRedis                   │
│  deja.wrap_http(client) -> InstrumentedHttp                     │
│                                                                 │
│  NON-DETERMINISM                                                │
│  ───────────────                                                │
│  deja.now() -> Timestamp                                        │
│  deja.random() -> RandomSource                                  │
│  deja.uuid() -> UUID                                            │
│                                                                 │
│  REPLAY MODE                                                    │
│  ───────────                                                    │
│  deja.is_replay() -> bool                                       │
│  deja.load_recording(id) -> Recording                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Python SDK

```python
from deja import DejaSDK, deja_middleware
from deja.wrappers import wrap_asyncpg, wrap_redis

app = FastAPI()
sdk = DejaSDK.init(DejaConfig(mode='record'))

# Wrap your clients
db_pool = wrap_asyncpg(await asyncpg.create_pool(...))
redis = wrap_redis(redis.Redis(...))

app.add_middleware(deja_middleware(sdk))

@app.post("/orders")
async def create_order(order: OrderRequest):
    order_id = sdk.uuid()  # Recorded/replayed
    created_at = sdk.now()  # Recorded/replayed
    
    await db_pool.execute(
        "INSERT INTO orders (id, created_at, ...) VALUES ($1, $2, ...)",
        order_id, created_at, ...
    )
    
    await redis.set(f"order:{order_id}", order.json())
    
    return {"id": order_id}
```

## Node.js SDK

```typescript
import express from 'express';
import { DejaSDK, dejaMiddleware } from '@deja/sdk';
import { wrapPg, wrapRedis, wrapFetch } from '@deja/sdk/wrappers';

const sdk = DejaSDK.init({ mode: 'record' });

const db = wrapPg(sdk, new Pool({ ... }));
const redis = wrapRedis(sdk, createClient({ ... }));
const fetch = wrapFetch(sdk, globalThis.fetch);

const app = express();
app.use(dejaMiddleware(sdk));

app.post('/orders', async (req, res) => {
  const orderId = sdk.uuid();
  const createdAt = sdk.now();
  
  await db.query(
    'INSERT INTO orders (id, created_at, ...) VALUES ($1, $2, ...)',
    [orderId, createdAt, ...]
  );
  
  await redis.set(`order:${orderId}`, JSON.stringify(req.body));
  
  res.json({ id: orderId });
});
```

## Rust SDK

```rust
use deja::{DejaSDK, DejaConfig, ReplayMode};

let sdk = DejaSDK::init(DejaConfig {
    mode: ReplayMode::Record,
    ..Default::default()
});

// Wrap your clients
let db = sdk.wrap_db(pool);
let redis = sdk.wrap_redis(client);

// Use wrapped clients - recording happens automatically
let order_id = sdk.uuid();
let created_at = sdk.now();

db.execute(
    "INSERT INTO orders (id, created_at) VALUES ($1, $2)",
    &[&order_id, &created_at],
).await?;
```

---

# Orchestration & Deployment

## Environment Provisioning Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     COMPLETE WORKFLOW                                           │
│                                                                                 │
│  1. DEFINE SERVICE (deja.yaml)                                                 │
│     ┌─────────────────────────────────────────────────────────────────────┐    │
│     │  • Build configuration                                              │    │
│     │  • Dependencies (DB, Redis, other services)                         │    │
│     │  • Migration tool & config                                          │    │
│     │  • Recording settings                                               │    │
│     │  • Diff rules & ignore patterns                                     │    │
│     └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                          │
│                                      ▼                                          │
│  2. RECORD IN PRODUCTION                                                       │
│     ┌─────────────────────────────────────────────────────────────────────┐    │
│     │  • SDK instrumentation OR sidecar proxy                             │    │
│     │  • Automatic sampling                                               │    │
│     │  • PII redaction                                                    │    │
│     │  • Storage in recording store                                       │    │
│     └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                          │
│                                      ▼                                          │
│  3. TRIGGER REPLAY                                                             │
│     ┌─────────────────────────────────────────────────────────────────────┐    │
│     │  CLI:     deja run quick --version v2.0                             │    │
│     │  CI:      deja run ci --pr 123                                      │    │
│     │  API:     POST /api/v1/replay/quick                                 │    │
│     │  Web UI:  Click "Run Replay" button                                 │    │
│     │  Webhook: Automatic on PR open                                      │    │
│     └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                          │
│                                      ▼                                          │
│  4. ORCHESTRATION                                                              │
│     ┌─────────────────────────────────────────────────────────────────────┐    │
│     │  a) Provision isolated environment (K8s namespace / Docker network) │    │
│     │  b) Deploy dependencies (Postgres, Redis)                           │    │
│     │  c) Run migrations (baseline → candidate schema)                    │    │
│     │  d) Seed data from recordings                                       │    │
│     │  e) Deploy service under test                                       │    │
│     │  f) Deploy mock server for external calls                           │    │
│     └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                          │
│                                      ▼                                          │
│  5. EXECUTE REPLAY                                                             │
│     ┌─────────────────────────────────────────────────────────────────────┐    │
│     │  For each recording:                                                │    │
│     │    • Inject recorded external responses (mock server)               │    │
│     │    • Send original request to candidate service                     │    │
│     │    • Capture response, DB mutations, cache operations               │    │
│     │    • Compare with recorded behavior                                 │    │
│     │    • Generate diff report                                           │    │
│     └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                          │
│                                      ▼                                          │
│  6. REPORT & ACTION                                                            │
│     ┌─────────────────────────────────────────────────────────────────────┐    │
│     │  • Generate diff report                                             │    │
│     │  • Post PR comment / status check                                   │    │
│     │  • Send notifications (Slack, email)                                │    │
│     │  • Fail/pass CI pipeline                                            │    │
│     │  • Archive results                                                  │    │
│     └─────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Database State Recreation Options

### Option A: Snapshot + Mutations
```
Store full DB snapshot before request + Apply any setup mutations + Run replay
Pro: Accurate state
Con: Storage heavy, slow setup
```

### Option B: Minimal Relevant State
```
Record only rows READ during request, seed replay DB with just those rows
Pro: Efficient
Con: May miss edge cases
```

### Option C: Synthetic State Generation
```
Record schema + constraints, generate minimal valid state that satisfies recorded queries
Pro: Privacy-safe, efficient
Con: Complex to implement
```

---

# Interface Design

## CLI Interface

### Command Structure

```bash
deja <command> <subcommand> [options]

Commands:
  init        Initialize deja configuration for a service
  recordings  Manage recordings
  run         Run replay jobs
  services    Manage service definitions
  results     View and compare results
  env         Environment management
  agent       Start the local agent/proxy
```

### Quick Replay

```bash
$ deja run quick --version v2.1.0 --baseline v2.0.0 --count 500

✨ Haven't we seen this before? Starting replay...
📼 Selecting 500 recordings (diverse endpoints)...
🏗️  Provisioning environment...
   • Starting PostgreSQL 15.4...
   • Running migrations (v2.0.0 → v2.1.0)...
   • Starting Redis 7.2...
   • Building user-service:v2.1.0...
   • Starting service...
✅ Environment ready: env-a1b2c3d4

🔄 Running replay...
⠹ [████████████████████████████░░░░░░░░░░░░] 350/500 (2m remaining)

📊 Results Summary:
   ✅ Matching: 485 (97%)
   ⚠️  Different: 12 (2.4%)
   ❌ Errors: 3 (0.6%)

⚠️  Found 12 differences:
   • rec_123 - Response body field 'updated_at' differs
   • rec_456 - New field 'metadata.version' in response
   • rec_789 - Status code 200 → 201 for POST /users
   ... and 9 more

📊 Full report: deja results show rr_x9y8z7

🧹 Cleaning up environment...
✅ Done!
```

### CI Mode

```bash
$ deja run ci --pr 1234 --fail-threshold 5 --annotations
```

### Recording Management

```bash
# List recordings
$ deja recordings list --service user-api --since 24h --limit 50

# Show recording details
$ deja recordings show rec_abc123 --full

# Export recordings
$ deja recordings export --output recordings.json --filter "path:/api/users/*"

# Import recordings
$ deja recordings import --input recordings.json
```

## Web API

### Endpoints

```
POST   /api/v1/replay/quick     - Quick replay
GET    /api/v1/jobs             - List replay jobs
GET    /api/v1/jobs/:id         - Get job details
DELETE /api/v1/jobs/:id         - Cancel job
GET    /api/v1/jobs/:id/logs    - Stream job logs
GET    /api/v1/jobs/:id/results - Get job results

GET    /api/v1/recordings       - List recordings
GET    /api/v1/recordings/:id   - Get recording details
POST   /api/v1/recordings/export - Export recordings
POST   /api/v1/recordings/import - Import recordings

GET    /api/v1/services         - List services
POST   /api/v1/services         - Create service
GET    /api/v1/services/:id     - Get service
PUT    /api/v1/services/:id     - Update service

POST   /api/v1/webhooks/github  - GitHub webhook
POST   /api/v1/webhooks/gitlab  - GitLab webhook
```

### Quick Replay Request

```json
{
  "service_id": "user-service",
  "baseline_version": "v2.0.0",
  "candidate_version": "v2.1.0",
  "recording_selector": {
    "time_range": { "since": "24h" },
    "limit": 500,
    "priority": "diverse_endpoints"
  },
  "options": {
    "async_mode": false,
    "timeout": "5m"
  }
}
```

### Quick Replay Response

```json
{
  "job_id": "job_abc123",
  "status": "completed",
  "results": {
    "id": "rr_xyz789",
    "summary": {
      "total_recordings": 500,
      "successful": 485,
      "with_diffs": 12,
      "errors": 3,
      "duration": "3m24s"
    },
    "diffs": [
      {
        "recording_id": "rec_123",
        "type": "response_body",
        "path": "$.updated_at",
        "baseline": "2024-01-15T10:00:00Z",
        "candidate": "2024-01-15T10:00:01Z"
      }
    ]
  },
  "links": {
    "self": "/api/v1/jobs/job_abc123",
    "results": "/api/v1/jobs/job_abc123/results",
    "report": "/api/v1/results/rr_xyz789/report"
  }
}
```

---

# Configuration Files

## Service Definition (deja.yaml)

```yaml
# deja.yaml - Service definition for Déjà

service:
  id: user-service
  name: User Service
  repository: github.com/company/user-service

build:
  type: docker
  image: company/user-service
  tag_template: "{{version}}"

dependencies:
  # PostgreSQL database
  - type: postgres
    name: users-db
    version: "15"
    migration:
      type: tool
      tool: sqlx
      config_path: ./migrations
    
  # Redis cache
  - type: redis
    name: cache
    version: "7.2"
    
  # External service to mock
  - type: external_mock
    name: payment-gateway
    url_patterns:
      - "https://api.stripe.com/*"
      - "https://api.paypal.com/*"

environment:
  template:
    DATABASE_URL: "postgres://{{db.users-db.host}}:{{db.users-db.port}}/users"
    REDIS_URL: "redis://{{redis.cache.host}}:{{redis.cache.port}}"
    PAYMENT_GATEWAY_URL: "{{mock.payment-gateway.url}}"
    LOG_LEVEL: "debug"
    
  secrets:
    - name: JWT_SECRET
      source: vault
      path: secret/user-service/jwt
    - name: API_KEY
      source: env
      key: DEJA_API_KEY

health_check:
  endpoint: /health
  interval: 5s
  timeout: 10s
  retries: 3

ports:
  - name: http
    container: 8080
    protocol: http

# Recording configuration
recording:
  enabled: true
  sample_rate: 0.1  # Record 10% of traffic
  
  targets:
    - type: http
      paths:
        - "/api/*"
      exclude:
        - "/api/health"
        - "/api/metrics"
    
    - type: postgres
      operations: [query, execute]
      
    - type: redis
      operations: [get, set, del]
  
  # Sensitive data handling
  redaction:
    - path: "$.password"
      action: hash
    - path: "$.credit_card"
      action: mask
    - path: "$.ssn"
      action: remove

# Replay configuration  
replay:
  diff:
    ignore:
      - path: "$.timestamp"
        reason: "Timestamps will always differ"
      - path: "$.request_id"
        reason: "Request IDs are regenerated"
    
    comparators:
      - path: "$.items[*]"
        type: unordered_list
        key: "id"
        
    thresholds:
      max_response_time_diff_ms: 500
      max_body_size_diff_percent: 10

  migrations:
    version_detection:
      type: table
      table: _sqlx_migrations
      column: version
    
    runner:
      type: sqlx
      command: "sqlx migrate run"

---
# Job templates

jobs:
  nightly:
    schedule: "0 2 * * *"
    
    recording_selector:
      time_range:
        since: 24h
      sample_rate: 0.2
      limit: 1000
      priority: diverse_endpoints
    
    version:
      baseline:
        type: docker_tag
        tag: latest
      candidate:
        type: git
        reference: main
    
    notifications:
      on_failure:
        - type: slack
          channel: "#engineering-alerts"
          
  pr_check:
    version:
      baseline:
        type: git
        reference: "{{base_branch}}"
      candidate:
        type: git
        reference: "{{head_branch}}"
    
    recording_selector:
      limit: 200
      priority: diverse_endpoints
    
    diff:
      fail_on_diff: true
```

---

# CI/CD Integration

## GitHub Actions

```yaml
# .github/workflows/deja-regression.yml
name: Déjà Regression Test

on:
  pull_request:
    branches: [main]

jobs:
  replay:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Install Déjà CLI
        run: |
          curl -sSL https://deja.dev/install.sh | sh
          
      - name: Run Déjà Regression
        env:
          DEJA_API_KEY: ${{ secrets.DEJA_API_KEY }}
          DEJA_API_URL: ${{ secrets.DEJA_API_URL }}
        run: |
          deja run ci \
            --pr ${{ github.event.pull_request.number }} \
            --commit ${{ github.event.pull_request.head.sha }} \
            --baseline origin/${{ github.base_ref }} \
            --fail-threshold 0 \
            --annotations
            
      - name: Upload Results
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: deja-results
          path: deja-results/
```

## GitLab CI

```yaml
# .gitlab-ci.yml
deja-regression:
  stage: test
  image: deja/cli:latest
  script:
    - deja run ci
        --commit $CI_COMMIT_SHA
        --baseline origin/$CI_MERGE_REQUEST_TARGET_BRANCH_NAME
        --fail-threshold 0
  artifacts:
    reports:
      junit: deja-results/junit.xml
    paths:
      - deja-results/
  rules:
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
```

## Jenkins

```groovy
// Jenkinsfile
pipeline {
    agent any
    
    stages {
        stage('Déjà Regression') {
            steps {
                script {
                    def result = sh(
                        script: '''
                            deja run ci \
                                --commit ${GIT_COMMIT} \
                                --baseline origin/${CHANGE_TARGET} \
                                --output-format json
                        ''',
                        returnStdout: true
                    )
                    
                    def json = readJSON text: result
                    
                    if (json.diff_count > 0) {
                        currentBuild.result = 'UNSTABLE'
                        
                        if (env.CHANGE_ID) {
                            pullRequest.comment(
                              "⚠️ Déjà found ${json.diff_count} differences. " +
                              "[View Report](${json.report_url})"
                            )
                        }
                    }
                }
            }
        }
    }
    
    post {
        always {
            archiveArtifacts artifacts: 'deja-results/**'
            junit 'deja-results/junit.xml'
        }
    }
}
```

---

# Migration Handling

## Supported Migration Tools

| Tool | Language | Status |
|------|----------|--------|
| **SQLx** | Rust | ✅ Full Support |
| **Diesel** | Rust | ✅ Full Support |
| **SeaORM** | Rust | ✅ Full Support |
| **Flyway** | Java/Multi | ✅ Full Support |
| **Liquibase** | Java/Multi | ✅ Full Support |
| **Alembic** | Python | ✅ Full Support |
| **Prisma** | Node.js | ✅ Full Support |
| **Knex** | Node.js | ✅ Full Support |
| **TypeORM** | Node.js | ✅ Full Support |
| **Goose** | Go | ✅ Full Support |
| **Custom** | Any | ✅ Via Command |

## Migration Runner Interface

```rust
#[async_trait]
pub trait MigrationRunner: Send + Sync {
    /// Get current schema version
    async fn current_version(&self, conn: &DbConnection) -> Result<Option<String>>;
    
    /// List available migrations
    async fn list_migrations(&self, source: &MigrationSource) -> Result<Vec<Migration>>;
    
    /// Run migrations from current to target version
    async fn migrate(
        &self,
        conn: &DbConnection,
        source: &MigrationSource,
        target_version: Option<&str>,
    ) -> Result<MigrationResult>;
    
    /// Rollback to a specific version
    async fn rollback(
        &self,
        conn: &DbConnection,
        source: &MigrationSource,
        target_version: &str,
    ) -> Result<MigrationResult>;
}
```

## Migration Strategies

```rust
pub enum MigrationStrategy {
    /// Run migrations from baseline schema to candidate
    RunMigrations { from_version: Option<String> },
    
    /// Use candidate's full schema (fresh DB)
    FreshSchema,
    
    /// No schema changes expected
    None,
    
    /// Custom migration script
    Custom { script: String },
}
```

---

# Technical Specifications

## Package Structure

```
deja/
├── proto/                          # Shared protobuf definitions
│   └── deja/v1/
│       ├── events.proto
│       ├── recording.proto
│       └── service.proto
│
├── core/                           # Rust core engine
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── recording/
│       ├── replay/
│       ├── diff/
│       ├── storage/
│       └── protocols/
│           ├── redis.rs
│           ├── postgres.rs
│           ├── mysql.rs
│           └── http.rs
│
├── agent/                          # Local collection agent
│   ├── Cargo.toml
│   └── src/main.rs
│
├── proxy/                          # Sidecar proxy
│   ├── Cargo.toml
│   └── src/main.rs
│
├── sdks/
│   ├── python/
│   │   ├── pyproject.toml
│   │   └── deja/
│   │       ├── __init__.py
│   │       ├── sdk.py
│   │       ├── wrappers/
│   │       │   ├── sqlalchemy.py
│   │       │   ├── psycopg.py
│   │       │   ├── redis.py
│   │       │   └── httpx.py
│   │       └── frameworks/
│   │           ├── fastapi.py
│   │           ├── django.py
│   │           └── flask.py
│   │
│   ├── node/
│   │   ├── package.json
│   │   └── src/
│   │       ├── index.ts
│   │       ├── wrappers/
│   │       │   ├── pg.ts
│   │       │   ├── redis.ts
│   │       │   └── fetch.ts
│   │       └── frameworks/
│   │           ├── express.ts
│   │           ├── fastify.ts
│   │           └── nextjs.ts
│   │
│   ├── rust/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── wrappers/
│   │       │   ├── sqlx.rs
│   │       │   ├── redis.rs
│   │       │   └── reqwest.rs
│   │       └── frameworks/
│   │           ├── axum.rs
│   │           └── actix.rs
│   │
│   └── go/
│       ├── go.mod
│       └── deja/
│
├── cli/                            # CLI tool
│   ├── Cargo.toml
│   └── src/main.rs
│
├── server/                         # Control plane API
│   ├── Cargo.toml
│   └── src/
│
└── docker/
    ├── Dockerfile.agent
    ├── Dockerfile.proxy
    └── docker-compose.yaml
```

## Protocol Configuration

```yaml
# deja-protocols.yaml

protocols:
  redis:
    enabled: true
    ports: [6379, 6380]
    
  postgres:
    enabled: true
    ports: [5432]
    
  mysql:
    enabled: true
    ports: [3306]
    
  http:
    enabled: true
    ports: [80, 443, 8080, 8443]
    
  # Custom protocol via plugin
  custom_rpc:
    enabled: true
    plugin: "/opt/deja/plugins/custom_rpc.so"
    ports: [9000]
    config:
      message_format: "length_prefixed"
      header_size: 4

detection:
  enabled: true
  fallback: raw
  unknown_sample_rate: 0.01

proxy:
  listen: "0.0.0.0:15000"
  
  upstreams:
    - name: redis
      target: "redis:6379"
      protocol: redis
      
    - name: postgres  
      target: "postgres:5432"
      protocol: postgres
      tls:
        enabled: true
        cert: /etc/deja/certs/pg.crt
        key: /etc/deja/certs/pg.key
```

---

# Appendix

## Domain Model Types

```rust
/// A service that can be replayed
pub struct ServiceDefinition {
    pub id: String,
    pub name: String,
    pub repository: String,
    pub build: BuildSpec,
    pub dependencies: Vec<DependencySpec>,
    pub env_template: HashMap<String, EnvValue>,
    pub health_check: HealthCheck,
    pub ports: Vec<PortMapping>,
}

/// Specifies which version of a service to use
pub enum VersionSpec {
    Exact { version: String },
    Git { reference: String },
    DockerTag { tag: String },
    LatestFrom { branch: String },
    PullRequest { number: u64 },
    Compare {
        baseline: Box<VersionSpec>,
        candidate: Box<VersionSpec>,
    },
}

/// Recording selection criteria
pub struct RecordingSelector {
    pub time_range: Option<TimeRange>,
    pub path_patterns: Vec<String>,
    pub correlation_ids: Option<Vec<String>>,
    pub tags: HashMap<String, String>,
    pub sample_rate: Option<f64>,
    pub limit: Option<usize>,
    pub priority: RecordingPriority,
}

pub enum RecordingPriority {
    Random,
    MostRecent,
    DiverseEndpoints,
    HighErrorRate,
    HighLatency,
    Custom { scorer: String },
}
```

## Comparison with Alternatives

| Feature | Déjà | Shadow Traffic | Load Testing | Contract Tests |
|---------|------|----------------|--------------|----------------|
| Real traffic patterns | ✅ | ✅ | ❌ | ❌ |
| Behavioral diffs | ✅ | ❌ | ❌ | Partial |
| DB mutation tracking | ✅ | ❌ | ❌ | ❌ |
| Deterministic replay | ✅ | ❌ | ❌ | ✅ |
| Zero-code option | ✅ | ✅ | ✅ | ❌ |
| CI/CD integration | ✅ | Partial | ✅ | ✅ |
| Schema migration support | ✅ | ❌ | ❌ | ❌ |

---

## Glossary

| Term | Definition |
|------|------------|
| **Recording** | A captured transaction including request, response, and all side effects |
| **Replay** | Re-executing a recording against a different service version |
| **Diff** | The differences detected between baseline and candidate behavior |
| **Correlation ID** | Unique identifier linking all events in a single transaction |
| **Baseline** | The reference version (usually current production) |
| **Candidate** | The version being tested (usually new code) |
| **Sidecar** | A proxy container that intercepts traffic without code changes |
| **Protocol Parser** | Component that understands wire protocols (Redis, Postgres, etc.) |

---

## Contact & Resources

- **Website**: https://deja.dev
- **Documentation**: https://docs.deja.dev
- **GitHub**: https://github.com/deja-dev/deja
- **Discord**: https://discord.gg/deja
- **Twitter**: @dejadev

---

*Document Version: 1.0.0*  
*Last Updated: 2024*

---

> ✨ *"Haven't we seen this request before?"*
