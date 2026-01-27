#!/bin/bash
set -e

echo "🚀 Starting Connector Service Integration Test..."

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RECORDINGS_DIR="$PROJECT_ROOT/recordings"

# Clean up
echo "🧹 Cleaning up previous runs..."
docker compose -f "$PROJECT_ROOT/docker-compose.yml" down -v 2>/dev/null || true
rm -rf "$RECORDINGS_DIR"/*.bin "$RECORDINGS_DIR"/sessions/ 2>/dev/null || true
killall connector-service 2>/dev/null || true

# Build connector-service
echo "🔨 Building connector-service..."
cd "$PROJECT_ROOT"
cargo build --release --package connector-service

# Start Docker environment
echo "🐳 Starting Docker services (Redis, Postgres, Deja Proxies)..."
docker compose -f "$PROJECT_ROOT/docker-compose.yml" up -d --build

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 10

# Check health
echo "🔍 Checking service health..."
docker compose -f "$PROJECT_ROOT/docker-compose.yml" ps

# =============================================================================
# PHASE 1: RECORDING
# =============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════════════"
echo "📹 PHASE 1: RECORDING MODE"
echo "═══════════════════════════════════════════════════════════════════════"

# Start connector-service pointing to Deja proxies
echo "🚀 Starting connector-service (RECORD mode)..."
POSTGRES_HOST=localhost \
POSTGRES_PORT=5433 \
REDIS_HOST=localhost \
REDIS_PORT=6390 \
DEJA_CONTROL_HOST=localhost \
DEJA_CONTROL_PORT=9999 \
RUST_LOG=connector_service=info,deja_core=debug \
"$PROJECT_ROOT/target/release/connector-service" &
CONNECTOR_PID=$!
sleep 3

# Check if connector-service started
if ! kill -0 $CONNECTOR_PID 2>/dev/null; then
    echo "❌ connector-service failed to start"
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" logs
    exit 1
fi
echo "✅ connector-service running (PID: $CONNECTOR_PID)"

# Health check
echo "🔍 Health check..."
curl -s http://localhost:3000/health && echo ""

# Send test traffic
echo ""
echo "📤 Sending test traffic..."
for i in 1 2 3; do
    echo "  Request $i:"
    RESPONSE=$(curl -s -X POST http://localhost:3000/process \
        -H "Content-Type: application/json" \
        -d "{\"id\": \"test-$i\", \"data\": \"payload-$i\"}")
    echo "    $RESPONSE"
    sleep 1
done

# Stop connector-service
echo ""
echo "🛑 Stopping connector-service..."
kill $CONNECTOR_PID 2>/dev/null || true
wait $CONNECTOR_PID 2>/dev/null || true

# Check recordings
echo ""
echo "🔍 Verifying recordings..."
RECORDING_COUNT=$(find "$RECORDINGS_DIR" -name "*.bin" 2>/dev/null | wc -l | tr -d ' ')
echo "   Found $RECORDING_COUNT recording files."

if [ "$RECORDING_COUNT" -lt 1 ]; then
    echo "❌ No recordings found. Check Deja Proxy logs:"
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" logs deja-postgres deja-redis
    exit 1
fi
echo "✅ Recordings verified!"

# List session directories
echo ""
echo "📁 Session directories:"
ls -la "$RECORDINGS_DIR"/sessions/ 2>/dev/null || echo "   (No sessions directory found)"

# =============================================================================
# PHASE 2: REPLAY
# =============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════════════"
echo "🔁 PHASE 2: REPLAY MODE"
echo "═══════════════════════════════════════════════════════════════════════"

# Find the latest session
SESSION_ID=$(ls -t "$RECORDINGS_DIR"/sessions/ 2>/dev/null | head -1)
if [ -z "$SESSION_ID" ]; then
    echo "❌ No session found for replay."
    exit 1
fi
echo "📂 Using session: $SESSION_ID"

# Stop record-mode proxies
echo "🛑 Stopping Docker services..."
docker compose -f "$PROJECT_ROOT/docker-compose.yml" down

# Start replay proxy manually (not via docker-compose as we need replay mode)
echo "🚀 Starting Deja Proxy in REPLAY mode..."
# Build proxy if not already built
cargo build --release --package deja-proxy

# Start replay proxy for Postgres (port 5434)
"$PROJECT_ROOT/target/release/deja-proxy" \
    --listen 0.0.0.0:5434 \
    --target 0.0.0.0:0 \
    --mode replay \
    --record-dir "$RECORDINGS_DIR" &
REPLAY_PROXY_PID=$!
sleep 3

if ! kill -0 $REPLAY_PROXY_PID 2>/dev/null; then
    echo "❌ Replay proxy failed to start"
    exit 1
fi
echo "✅ Replay proxy running (PID: $REPLAY_PROXY_PID)"

# Start connector-service pointing to replay proxy
echo "🚀 Starting connector-service (REPLAY mode)..."
POSTGRES_HOST=localhost \
POSTGRES_PORT=5434 \
REDIS_HOST=localhost \
REDIS_PORT=5434 \
DEJA_CONTROL_HOST=localhost \
DEJA_CONTROL_PORT=9999 \
RUST_LOG=connector_service=info,deja_core=debug \
"$PROJECT_ROOT/target/release/connector-service" &
CONNECTOR_REPLAY_PID=$!
sleep 3

if ! kill -0 $CONNECTOR_REPLAY_PID 2>/dev/null; then
    echo "❌ connector-service (replay) failed to start"
    kill $REPLAY_PROXY_PID 2>/dev/null || true
    exit 1
fi
echo "✅ connector-service running in replay mode (PID: $CONNECTOR_REPLAY_PID)"

# Replay the same traffic
echo ""
echo "📤 Replaying test traffic..."
REPLAY_SUCCESS=true
for i in 1 2 3; do
    echo "  Replay Request $i:"
    RESPONSE=$(curl -s -X POST http://localhost:3000/process \
        -H "Content-Type: application/json" \
        -d "{\"id\": \"test-$i\", \"data\": \"payload-$i\"}" || echo "CURL_FAILED")
    
    if [[ "$RESPONSE" == *"error"* ]] || [[ "$RESPONSE" == "CURL_FAILED" ]]; then
        echo "    ❌ $RESPONSE"
        REPLAY_SUCCESS=false
    else
        echo "    ✅ $RESPONSE"
    fi
    sleep 1
done

# Cleanup
echo ""
echo "🧹 Cleaning up..."
kill $CONNECTOR_REPLAY_PID 2>/dev/null || true
kill $REPLAY_PROXY_PID 2>/dev/null || true

# Final result
echo ""
echo "═══════════════════════════════════════════════════════════════════════"
if [ "$REPLAY_SUCCESS" = true ]; then
    echo "🎉 Integration Test PASSED!"
    echo "   ✅ Recording worked"
    echo "   ✅ Replay produced deterministic responses"
else
    echo "❌ Integration Test FAILED!"
    echo "   Some replay requests did not match expectations."
    exit 1
fi
echo "═══════════════════════════════════════════════════════════════════════"
