# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Déjà** is a deterministic record-and-replay framework for testing distributed systems. It captures production traffic (HTTP, gRPC, PostgreSQL, Redis) and replays it against different versions of code to detect regressions through behavioral diffs.

**Key Concept**: Record real requests/responses and database interactions in one environment, then replay them deterministically against new code versions to compare outputs.

**Core Value**: Tests actual code changes (not just infrastructure) using REAL traffic patterns, catching regressions before they reach production.

## Workspace Structure

This is a Rust workspace with multiple crates:

```
├── common/              # Shared types (Protocol, SequenceTracker)
├── core/                # Core replay engine, recording, protocol parsers
├── proxy/               # TCP/HTTP proxy that intercepts traffic
├── sdk/                 # Client-side SDK for service instrumentation
├── cli/                 # CLI tool (not heavily documented)
├── test-service/   # External service with for e2e testing
├── test-server/         # Test service for integration testing
```

## Build & Test Commands

### Building

```bash
# Build all packages
cargo build

# Build specific package (e.g., proxy)
cargo build --package deja-proxy

# Build release mode
cargo build --release

# Build a specific binary
cargo build --bin deja-proxy
```

### Running Tests

```bash
# Run all tests
cargo test

# Run tests for specific package
cargo test --package deja-core

# Run specific test
cargo test test_name

# Run with output (println! will show)
cargo test -- --nocapture

# Run a single test file
cargo test --test replay_test
```

### Key Test Locations

- `core/tests/replay_test.rs` - Core replay engine tests
- `core/tests/replay_protocol_scope.rs` - Protocol-specific replay tests
- `test-server/tests/integration/harness.rs` - Integration test harness
- `test-server/tests/proxy_integration.rs` - Proxy integration tests
- `tests/*.sh` - Shell integration tests (use Docker Compose)

### Running Integration Tests

```bash
# Full integration test with Docker Compose
bash tests/integration_test.sh

# Test concurrent operations
bash tests/concurrent_test.sh

# Test error handling
bash tests/error_test.sh

# Test connector service
bash tests/connector_integration_test.sh
```

## Core Architecture

### Recording Flow

1. **Proxy** (`proxy/src/main.rs`) listens on mapped ports (e.g., 5433→Postgres)
2. Incoming connections are parsed by **Protocol Parsers** (HTTP, gRPC, Redis, Postgres)
3. **Recorder** (`core/src/recording.rs`) captures events and serializes them
4. Events are stored as binary files in `--record-dir`

### Replay Flow

1. **ReplayEngine** (`core/src/replay/`) loads recorded events from disk
2. On incoming request, engine looks up matching recorded response
3. Responds with recorded data deterministically (no real external calls)
4. Supports multiple replay modes: `FullMock`, `Orchestrated`, `Recording`

### Key Modules in `core/src/`

| Module         | Purpose                                                                             |
| -------------- | ----------------------------------------------------------------------------------- |
| `runtime/`     | Trace context management, task spawning with trace inheritance, socket interception |
| `protocols/`   | Protocol parsers: HTTP, gRPC, Redis, Postgres (each has encoder/decoder)            |
| `replay/`      | ReplayEngine, ReplayMode, session index management                                  |
| `recording.rs` | Records events to disk                                                              |
| `control_api/` | gRPC server for SDK communication and trace control                                 |
| `tls_mitm.rs`  | TLS certificate generation and MITM interception                                    |
| `diff.rs`      | Behavioral diff comparison                                                          |

### Protocol Parsers

Located in `core/src/protocols/`:

- **HTTP**: Request/response line parsing
- **gRPC**: HTTP/2 frame parsing (uses h2 crate)
- **Redis**: RESP protocol parsing
- **Postgres**: Wire protocol (using postgres-protocol crate)

Each parser implements `ProtocolParser` trait with `parse()` and `encode()` methods.

### Trace Context System

All requests must be associated with a trace ID for correlation:

- `with_trace_id()` - Wraps an async block with a trace ID
- `generate_trace_id()` - Creates new trace ID
- `spawn()` - Like tokio::spawn but inherits parent's trace ID
- `spawn_blocking()` - For blocking operations

When services are instrumented (via SDK), they call control_api to notify proxy about trace lifecycle.

### DejaRuntime Abstraction

Located in `common/src/runtime.rs`, this trait provides a pluggable interface for non-deterministic operations:

- **Capture/Replay**: Record non-deterministic values (UUIDs, timestamps, random) during recording, replay during testing
- **Trace Management**: Set/get trace IDs for request correlation
- **Control Communication**: Access to ControlClient for SDK-proxy communication

This allows services to be fully deterministic during replay by substituting real randomness with recorded values.

## Proxy Operation Modes

```bash
# Recording mode: capture real traffic
cargo run --bin deja-proxy -- \
  --mode record \
  --map 5433:localhost:5432 \
  --map 6380:localhost:6379 \
  --record-dir ./recordings

# Replay mode: serve recorded responses
cargo run --bin deja-proxy -- \
  --mode replay \
  --map 5433:localhost:5432 \
  --control-port 9999 \
  --record-dir ./recordings

# Forward proxy mode: intercept via HTTP_PROXY env var
cargo run --bin deja-proxy -- \
  --mode record \
  --forward-proxy-port 8080 \
  --ca-cert ./certs/ca.crt \
  --ca-key ./certs/ca.key \
  --record-dir ./recordings
```

Environment variables as fallbacks:

- `DEJA_MODE` - Replay mode (record/replay/full_mock/orchestrated)
- `DEJA_RECORDING_PATH` - Path to recordings directory
- `DEJA_PORT_MAPS` - Comma-separated port mappings (5433:localhost:5432,...)
- `DEJA_STORAGE_FORMAT` - Storage format (json/binary, default binary)

## Architecture Highlights

Key architectural choices and recent advances:

1. **DejaRuntime abstraction** - Generic trait for capture/replay with pluggable backends (`common/src/runtime.rs`)
   - Deterministic value generation during recording
   - Replay-mode substitution for randomness (UUIDs, timestamps, random bytes)
2. **Async boundary capture** - Deterministic task ordering across async boundaries via `deja_core::spawn`
3. **Orchestrated replay** - Trigger-based replay (vs. full mock) for fine-grained control
4. **TLS MITM interception** - Certificate generation and interception for HTTPS traffic
5. **Control API** - gRPC communication between proxy and instrumented services
6. **SDK Auto-instrumentation** - Framework-specific helpers (Axum, Reqwest) for zero-boilerplate integration
7. **Module refactoring** - Moving from `mod.rs` pattern to flat file structure (e.g., `control_api.rs`)

## File Organization Patterns

### New files often go in:

- `core/src/protocols/` - For new protocol support
- `core/src/runtime/` - For runtime/trace context features
- `core/src/replay/` - For replay engine improvements (now has `engine.rs`, `recording_index.rs`)
- `proxy/src/` - For proxy-specific logic (`connection.rs`, `correlation.rs`, `control_api.rs`)
- `sdk/src/` - For SDK helpers (`axum.rs`, `reqwest.rs`, `sync_runtime.rs`)
- `common/src/` - For shared types (`protocol.rs`, `runtime.rs` trait, `sequence.rs`)
- Tests go in `core/tests/` or `test-server/tests/`

### Module structure:

- `mod.rs` files are being phased out (recent refactors: `control_api/mod.rs` → `control_api.rs`, `runtime/mod.rs` → `runtime.rs`)
- Prefer flat file structure: `feature.rs` at module level
- For submodules with multiple files, use directory with files (e.g., `replay/engine.rs`, `replay/recording_index.rs`)

## Dependencies to Know

**Core dependencies:**

- `tokio` - Async runtime with full features
- `serde_json` / `bincode` - Serialization
- `tracing` - Logging/tracing
- `tonic` - gRPC (proto codegen)
- `rustls` / `rcgen` - TLS support
- `uuid` / `chrono` - IDs and timestamps

**Protocol-specific:**

- `httparse` - HTTP parsing
- `h2` / `hpack` - HTTP/2 frames (gRPC)
- `redis-protocol` - Redis RESP protocol
- `postgres-protocol` - Postgres wire protocol

## Docker & CI/CD

### Docker Compose

- `docker-compose.yml` defines: Redis, Postgres, HTTP echo service, and proxy containers
- Test services listen on standard ports; proxies forward to them
- `Dockerfile.proxy` builds the proxy binary

### Running Tests Locally

```bash
docker compose down -v  # Clean up
docker compose up -d --build  # Start services
# Run your test commands
docker compose logs  # View logs
```

## Recording Format

Events are stored in structured format (JSON or binary). A recorded session contains:

- **Trace ID** - Correlates all events in a transaction
- **Connection ID** - Which connection/stream
- **Sequence number** - Event ordering
- **Event type** - HTTP request, DB query, gRPC call, etc.
- **Payload** - Request/response data
- **Metadata** - Headers, status codes, timestamps

Location: `recordings/[session_id]/`

## SDK Integration Pattern

Services using Déjà must:

1. Add `deja-core` dependency
2. Initialize `DejaRuntime` or use built-in implementations
3. Wrap request handlers with `with_trace_id()`
4. Replace `tokio::spawn` with `deja_core::spawn`
5. Use SDK helpers for framework integration:
   - **Axum**: Add `DejaLayer` middleware for auto-instrumentation
   - **Reqwest**: Wrap client with `DejaClient` for outgoing HTTP trace correlation
   - **Raw Runtime**: Use `DejaRuntime::capture()` / `replay()` for determinism

### Common SDK Files

- `sdk/src/axum.rs` - Tower/Axum middleware for automatic trace extraction and control API notifications
- `sdk/src/reqwest.rs` - Reqwest wrapper for automatic trace injection in outgoing HTTP
- `sdk/src/runtime.rs` - DejaRuntime implementation for the SDK
- `common/src/protocol.rs` - Protocol enum (Postgres, Redis, Http, Grpc)

See `INTEGRATION_GUIDE.md` for detailed instrumentation examples.

## Development Tips

### When Adding New Features

1. **Protocol Support**: Add parser in `core/src/protocols/`, implement `ProtocolParser` trait
2. **Replay Logic**: Modify `core/src/replay/` engine; ensure determinism
3. **Runtime Features**: Add to `core/src/runtime/`; maintain trace context invariants
4. **Proxy Behavior**: Modify `proxy/src/` connection/config handlers

### Debugging

- Enable debug logging: `RUST_LOG=deja_core=debug,deja_proxy=debug`
- Check recordings in `recordings/` directory
- Use integration tests with `--nocapture` to see output
- Replay mode uses index files to optimize lookup

### Common Pitfalls

1. **Trace context loss**: Ensure all spawned tasks use `deja_core::spawn`, not `tokio::spawn`
2. **Non-determinism**: Replay must produce identical outputs; avoid timestamps, random numbers, external calls
3. **Protocol parsing**: Partial reads are buffered; ensure parsers handle fragmented data
4. **Session lifecycle**: Properly mark trace start/end for cleanup

## Useful Shell Scripts

- `scripts/run_deja_proxy.sh` - Start proxy in record mode
- `scripts/run_deja_replay.sh` - Start proxy in replay mode
- `scripts/run_hyperswitch_with_deja.sh` - Integration with Hyperswitch service

These provide examples of environment variable and argument configurations.

## Key Abstractions & Traits

### Protocol Trait

Located in `core/src/protocols.rs`:

```rust
pub trait ProtocolParser: Send + Sync {
    fn parse(&self, buffer: &[u8]) -> ParseResult;
    fn encode(&self, message: &Message) -> Vec<u8>;
}
```

Each protocol (HTTP, gRPC, Redis, Postgres) implements this for bidirectional serialization.

### DejaRuntime Trait

Located in `common/src/runtime.rs`:

```rust
pub trait DejaRuntime: Send + Sync {
    async fn uuid(&self) -> Uuid;
    async fn now(&self) -> SystemTime;
    async fn capture(&self, tag: &str, value: &str);
    async fn replay(&self, tag: &str) -> Option<String>;
    // ... and more
}
```

Services use this for deterministic randomness during replay.

### RecordingIndex

Located in `core/src/replay/recording_index.rs`:
Manages session data structure:

- `RecordedTrace` - All events for a single trace/correlation ID
- `MessageExchange` - Individual request/response pair with ordering

## Performance Considerations

- Recording: Proxy adds minimal latency (just intercepts and serializes)
- Replay: Zero external calls; deterministic response serves immediately
- Storage: Binary format is compact; JSON for debugging
- Index: RecordingIndex builds in-memory index for O(1) lookup by trace/sequence

## Current Branch Status & Working Directory

**Branch**: `feat/unified-merge`

### Completed Work (committed)

- Generic DejaRuntime trait and pluggable implementations
- Async boundary capture for deterministic task ordering
- Orchestrated replay trigger logic
- TLS MITM interception
- Control API for SDK communication

### Active Working Directory Changes (42 files)

The branch has significant in-flight refactoring:

**Module Reorganization** - Moving from `mod.rs` pattern to flat files:

- `core/src/control_api/mod.rs` → `core/src/control_api.rs`
- `core/src/runtime/mod.rs` → `core/src/runtime.rs`

**New Core Modules** - Unified trait system:

- `common/src/runtime.rs` - **DejaRuntime trait** (abstraction for capture/replay and determinism)
- `common/src/control.rs` - Control message types
- `common/src/protocol.rs` - Protocol enum (Postgres, Redis, Http, Grpc)
- `common/src/sequence.rs` - Sequence tracking

**New Replay Engine Structure**:

- `core/src/replay/engine.rs` - Replay orchestration
- `core/src/replay/recording_index.rs` - Session indexing and lookup

**SDK Auto-instrumentation**:

- `sdk/src/axum.rs` - Tower/Axum middleware for HTTP services
- `sdk/src/reqwest.rs` - HTTP client wrapper for outgoing trace injection
- Enhanced `sdk/src/runtime.rs` - SDK's DejaRuntime implementation

**Proxy Improvements**:

- `proxy/src/config.rs` - Configuration management
- `proxy/src/connection.rs` - Connection handling
- Enhanced connection management and correlation

**New Tests**:

- `core/tests/replay_protocol_scope.rs` - Protocol-specific replay tests
- `test-server/tests/proxy_integration.rs` - Proxy integration tests
- `test-service/tests/` - Connector service tests

**Dependency Updates**:

- All Cargo.toml files modified for workspace consolidation
- Cargo.lock updated with new dependencies

### Status

The codebase is in active refactoring toward a more modular, pluggable architecture. Future instances should expect these changes to be committed soon. The focus is on cleaner SDK integration patterns and a cohesive DejaRuntime trait system.
