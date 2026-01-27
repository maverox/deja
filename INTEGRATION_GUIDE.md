# Deja Integration Guide

This guide provides a comprehensive overview of how to integrate Deja into your Rust services. It covers generic integration, trace correlation, gRPC/HTTP instrumentation, and running in record/replay modes.

## Table of Contents

1. [Overview](#1-overview)
2. [Setup & Dependencies](#2-setup--dependencies)
3. [Instrumentation](#3-instrumentation)
   - [Initialization](#31-initialization)
   - [Trace Context & Spawning](#32-trace-context--spawning)
   - [HTTP Middleware](#33-http-middleware)
   - [gRPC Interceptors](#34-grpc-interceptors)
   - [Database Connections](#35-database-connections)
4. [Running Deja](#4-running-deja)
   - [Recording Mode](#41-recording-mode)
   - [Replay Mode](#42-replay-mode)
   - [Forward Proxy Mode](#43-forward-proxy-mode)
5. [Testing & Verification](#5-testing--verification)

---

## 1. Overview

Deja is a deterministic record-and-replay system for distributed systems. It allows you to:

- **Record** all external interactions (DB, HTTP, gRPC) of a service.
- **Replay** them deterministically to verify service behavior.
- **Correlate** concurrent requests using Trace IDs.

The integration consists of adding the `deja-core` library to your service and acting as a thin shim to propagate trace contexts.

## 2. Setup & Dependencies

Add `deja-core` to your `Cargo.toml`.

```toml
[dependencies]
# Use the local path if in the workspace, or git dependency
deja-core = { path = "../deja/core" }

# Required for async context
tokio = { version = "1.40", features = ["full"] }
tracing = "0.1"
```

## 3. Instrumentation

### 3.1. Initialization

Initialize the `ControlClient` at the start of your application. This client communicates with the Deja Proxy.

```rust
use deja_core::control_api::ControlClient;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Connect to Deja Control Plane (default: localhost:9999)
    let control_client = Arc::new(ControlClient::new("localhost", 9999));

    run_service(control_client).await;
}
```

### 3.2. Trace Context & Spawning

**Critical Role**: You must ensure every request has a Trace ID and that background tasks inherit it.

**Context Wrapper**:
Use `with_trace_id` to wrap the start of a request.

```rust
let trace_id = deja_core::generate_trace_id();
deja_core::with_trace_id(trace_id.clone(), async {
    // Notify proxy start (optional but recommended for visual debug)
    control_client.send(ControlMessage::start_trace(&trace_id)).await.ok();

    // Process request...

    control_client.send(ControlMessage::end_trace(&trace_id)).await.ok();
}).await;
```

**Task Spawning**:
Replace `tokio::spawn` with `deja_core::spawn`. This ensures the new task inherits the parent's Trace ID.

```rust
// ❌ Avoid: Loses trace context
tokio::spawn(async { ... });

// ✅ Use: Inherits trace context
deja_core::spawn(async { ... });
```

### 3.3. HTTP Middleware

For web frameworks like Axum or Actix, add a middleware to generate trace IDs.

```rust
async fn trace_middleware(request: Request, next: Next, client: Arc<ControlClient>) -> Response {
    let trace_id = deja_core::generate_trace_id();

    deja_core::with_trace_id(trace_id, async {
        next.run(request).await
    }).await
}
```

### 3.4. gRPC Interceptors

For `tonic` gRPC services, intercept requests to establish context.

```rust
async fn my_grpc_method(&self, request: Request<MyMsg>) -> Result<Response<MyRes>, Status> {
    let trace_id = deja_core::generate_trace_id();
    deja_core::with_trace_id(trace_id, async {
        // ... logic
    }).await
}
```

### 3.5. Database Connections

Wrap your DB connections to automatically map them to the current Trace ID.

```rust
use deja_core::InterceptedConnection;

let conn = sqlx::postgres::PgPool::acquire().await?;
let intercepted = InterceptedConnection::new(
    conn,
    Some(control_client.clone()),
    "db-connection-1".to_string(),
);

// Use `intercepted` treating it like the original connection
```

## 4. Running Deja

### 4.1. Recording Mode

Start the Deja Proxy with mappings for your dependencies.

```bash
# Example: Mapping Postgres (5432) to 5433, and HTTP Service (8080) to 8081
deja-proxy \
  --map 5433:localhost:5432 \
  --map 8081:localhost:8080 \
  --control-port 9999 \
  --record-dir ./recordings \
  --mode record
```

Configure your service to point to the proxy ports (5433, 8081) instead of the real ones.

### 4.2. Replay Mode

To replay perfectly, restart the proxy in replay mode pointing to the recorded session.

```bash
deja-proxy \
  --map 5433:localhost:5432 \
  --control-port 9999 \
  --record-dir ./recordings \
  --mode replay \
  --session-id <SESSION_ID>
```

Your service will now receive deterministic responses.

### 4.3. Forward Proxy Mode

For connectors that support HTTP_PROXY/HTTPS_PROXY, use Forward Proxy mode to capture all outbound traffic without manual port mapping.

```bash
deja-proxy \
  --forward-proxy-port 8080 \
  --ca-cert ./certs/ca.crt \
  --ca-key ./certs/ca.key \
  --record-dir ./recordings \
  --mode record
```

**Client Config**:

```bash
export HTTP_PROXY=http://localhost:8080
export HTTPS_PROXY=http://localhost:8080
```

## 5. Testing & Verification

1.  **Check Health**: `curl http://localhost:9999/health`
2.  **Verify Spawning**: Use `deja_core::current_trace_id()` inside a spawned task to confirm it returns `Some(id)`.
3.  **Check Logs**: Set `RUST_LOG=deja_core=debug` to see trace association events.
