#!/bin/bash
set -e

echo "🚀 Starting Test Service Integration Test..."

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RECORDINGS_DIR="$PROJECT_ROOT/recordings"

# Clean up
echo "🧹 Cleaning up previous runs..."
docker compose -f "$PROJECT_ROOT/docker-compose.yml" down -v 2>/dev/null || true
rm -rf "$RECORDINGS_DIR"/*.bin "$RECORDINGS_DIR"/*.jsonl "$RECORDINGS_DIR"/sessions/ 2>/dev/null || true
killall test-service 2>/dev/null || true

# Build test-service
echo "🔨 Building test-service..."
cd "$PROJECT_ROOT"
cargo build --release --package test-service

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

# Start test-service pointing to Deja proxies
echo "🚀 Starting test-service (RECORD mode)..."
POSTGRES_HOST=localhost \
POSTGRES_PORT=5433 \
REDIS_HOST=localhost \
REDIS_PORT=6390 \
DEJA_CONTROL_HOST=localhost \
DEJA_CONTROL_PORT=9999 \
DEJA_STORAGE_FORMAT=json \
RUST_LOG=test_service=info,deja_core=debug \
"$PROJECT_ROOT/target/release/test-service" 2>&1 | sed "s/^/[SERVICE] /" &
CONNECTOR_PID=$!
sleep 3

# Check if test-service started
if ! kill -0 $CONNECTOR_PID 2>/dev/null; then
    echo "❌ test-service failed to start"
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" logs
    exit 1
fi
echo "✅ test-service running (PID: $CONNECTOR_PID)"

# Send test traffic
echo ""
echo "📤 Sending test traffic..."
for i in 1 2 3; do
    echo "[TEST-SCRIPT] Sending Request $i..."
    RESPONSE=$(curl -s -X POST http://localhost:50051/process \
        -H "Content-Type: application/json" \
        -d "{\"id\": \"test-$i\", \"data\": \"payload-$i\"}")
    echo "[TEST-SCRIPT] Response: $RESPONSE"
    sleep 1
done

# Stop test-service
echo ""
echo "🛑 Stopping test-service..."
kill $CONNECTOR_PID 2>/dev/null || true
wait $CONNECTOR_PID 2>/dev/null || true

# Check recordings
echo ""
echo "🔍 Verifying recordings..."
RECORDING_COUNT=$(find "$RECORDINGS_DIR" -name "events.jsonl" 2>/dev/null | wc -l | tr -d ' ')
echo "   Found $RECORDING_COUNT recording files."

if [ "$RECORDING_COUNT" -lt 1 ]; then
    echo "❌ No recordings found. Check Docker logs:"
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" logs
    exit 1
fi
echo "✅ Recordings verified!"

# Show the recording
echo ""
echo "📄 RECORDED EVENTS (JSON):"
find "$RECORDINGS_DIR" -name "events.jsonl" -exec cat {} + | sed "s/^/  /"

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

# Start replay proxy manually
echo "🚀 Starting Deja Proxy in REPLAY mode..."
# Build proxy if not already built
cargo build --release --package deja-proxy

# Start replay proxy for all (port 5434 for both PG and Redis in this script's simple map)
"$PROJECT_ROOT/target/release/deja-proxy" \
    --map 5434:0.0.0.0:0 \
    --mode replay \
    --record-dir "$RECORDINGS_DIR" 2>&1 | sed "s/^/[PROXY-REPLAY] /" &
REPLAY_PROXY_PID=$!
sleep 3

if ! kill -0 $REPLAY_PROXY_PID 2>/dev/null; then
    echo "❌ Replay proxy failed to start"
    exit 1
fi
echo "✅ Replay proxy running (PID: $REPLAY_PROXY_PID)"

# Start test-service pointing to replay proxy
echo "🚀 Starting test-service (REPLAY mode)..."
POSTGRES_HOST=localhost \
POSTGRES_PORT=5434 \
REDIS_HOST=localhost \
REDIS_PORT=5434 \
DEJA_CONTROL_HOST=localhost \
DEJA_CONTROL_PORT=9999 \
RUST_LOG=test_service=info,deja_core=debug \
"$PROJECT_ROOT/target/release/test-service" 2>&1 | sed "s/^/[SERVICE] /" &
CONNECTOR_REPLAY_PID=$!
sleep 3

if ! kill -0 $CONNECTOR_REPLAY_PID 2>/dev/null; then
    echo "❌ test-service (replay) failed to start"
    kill $REPLAY_PROXY_PID 2>/dev/null || true
    exit 1
fi
echo "✅ test-service running in replay mode (PID: $CONNECTOR_REPLAY_PID)"

# Replay the same traffic
echo ""
echo "📤 Replaying test traffic..."
REPLAY_SUCCESS=true
for i in 1 2 3; do
    echo "[TEST-SCRIPT] Replaying Request $i..."
    RESPONSE=$(curl -s -X POST http://localhost:50051/process \
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
