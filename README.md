# Déjà

> *"Haven't we seen this request before?"*

**Deterministic record-and-replay framework for testing distributed systems.**

Déjà captures production traffic (HTTP, gRPC, PostgreSQL, Redis) and replays it against different versions of your code to detect regressions through behavioral diffs — no mocks, no fixtures, just real traffic.

## Why Déjà?

Traditional testing requires hand-crafted test cases. Déjà flips the script:

1. **Record** real requests, database queries, and external API calls in one environment
2. **Replay** them deterministically against new code — no live services needed
3. **Diff** the outputs to catch behavioral regressions before they reach production

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Your App   │────▶│  Déjà Proxy  │────▶│  Postgres /  │
│  (with SDK)  │     │  (intercept) │     │  Redis / API │
└──────────────┘     └──────┬───────┘     └──────────────┘
                            │
                     ┌──────▼───────┐
                     │  Recordings  │
                     │  (on disk)   │
                     └──────┬───────┘
                            │
                     ┌──────▼───────┐
                     │ Replay Engine│  ← No external calls
                     │ (full mock)  │
                     └──────────────┘
```

## Supported Protocols

| Protocol   | Record | Replay | Parser Location |
|-----------|--------|--------|-----------------|
| HTTP      | ✅     | ✅     | `core/src/protocols/http.rs` |
| gRPC      | ✅     | ✅     | `core/src/protocols/grpc/` |
| PostgreSQL| ✅     | ✅     | `core/src/protocols/postgres.rs` |
| Redis     | ✅     | ✅     | `core/src/protocols/redis.rs` |

## Quick Start

### Prerequisites

- Rust 1.75+ (with `cargo`)
- Docker & Docker Compose (for integration tests)

### Build

```bash
cargo build
```

### Record Traffic

```bash
cargo run --bin deja-proxy -- \
  --mode record \
  --map 5433:localhost:5432 \
  --map 6380:localhost:6379 \
  --record-dir ./recordings
```

Point your service at `localhost:5433` (Postgres) and `localhost:6380` (Redis) instead of the real services.

### Replay Traffic

```bash
cargo run --bin deja-proxy -- \
  --mode replay \
  --map 5433:localhost:5432 \
  --control-port 9999 \
  --record-dir ./recordings
```

The proxy serves recorded responses deterministically — zero external calls.

### Run Tests

```bash
# All tests
cargo test

# Specific package
cargo test --package deja-core

# Integration tests (requires Docker)
bash tests/integration_test.sh
```

## Workspace Structure

```
├── common/        # Shared types: Protocol, DejaRuntime trait, SequenceTracker
├── core/          # Replay engine, recording, protocol parsers, control API
├── proxy/         # TCP/HTTP proxy for traffic interception
├── sdk/           # Client SDK with Axum & Reqwest auto-instrumentation
├── cli/           # CLI tool
├── test-server/   # Integration test service
├── proto/         # Protobuf definitions
└── tests/         # Shell-based integration tests
```

## SDK Integration

Instrument your service for deterministic replay:

```rust
use deja_sdk::{DejaLayer, DejaClient};

// Axum: Add middleware for automatic trace extraction
let app = Router::new()
    .route("/api", get(handler))
    .layer(DejaLayer::new());

// Reqwest: Wrap client for outgoing trace injection
let client = DejaClient::new(reqwest::Client::new());
```

See [INTEGRATION_GUIDE.md](INTEGRATION_GUIDE.md) for full examples.

## Key Concepts

- **Trace ID**: Correlates all events in a transaction across services
- **DejaRuntime**: Trait for deterministic capture/replay of non-deterministic values (UUIDs, timestamps, random)
- **Replay Modes**: `FullMock` (all recorded), `Orchestrated` (trigger-based), `Recording` (capture new)
- **Behavioral Diff**: Compare recorded vs replayed outputs to detect regressions

## Configuration

Environment variables (fallbacks for CLI args):

| Variable | Description |
|----------|-------------|
| `DEJA_MODE` | `record` / `replay` / `full_mock` / `orchestrated` |
| `DEJA_RECORDING_PATH` | Path to recordings directory |
| `DEJA_PORT_MAPS` | Comma-separated port mappings (`5433:localhost:5432,...`) |
| `DEJA_STORAGE_FORMAT` | `json` or `binary` (default: binary) |
| `RUST_LOG` | `deja_core=debug,deja_proxy=debug` for debugging |

## License

[MIT](LICENSE)
