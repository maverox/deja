#!/bin/bash
set -e

echo "🚀 Starting Unified Test Server"
echo "================================"

# Build if needed
if [ ! -f "./target/debug/test-server" ]; then
    echo "Building test-server..."
    cargo build --package test-server
fi

if [ ! -f "./target/debug/deja-proxy" ]; then
    echo "Building deja-proxy..."
    cargo build --package deja-proxy
fi

# Default ports
GRPC_PORT="${GRPC_PORT:-50051}"
CONNECTOR_PORT="${CONNECTOR_PORT:-50052}"
HTTP_PORT="${HTTP_PORT:-8080}"
MOCK_PORT="${MOCK_PORT:-8090}"
CONTROL_PORT="${CONTROL_PORT:-9999}"
RECORDING_PATH="${RECORDING_PATH:-./recordings}"

# Database URLs
PG_URL="${PG_URL:-postgres://deja_user:password@localhost:5433/deja_test}"
REDIS_URL="${REDIS_URL:-redis://localhost:6390}"

echo ""
echo "Configuration:"
echo "  gRPC Port:      $GRPC_PORT"
echo "  Connector Port: $CONNECTOR_PORT"
echo "  HTTP Port:      $HTTP_PORT"
echo "  Mock Port:      $MOCK_PORT"
echo "  Control Port:   $CONTROL_PORT"
echo "  Recordings:     $RECORDING_PATH"
echo ""

./target/debug/test-server \
    --grpc-port "$GRPC_PORT" \
    --connector-port "$CONNECTOR_PORT" \
    --http-port "$HTTP_PORT" \
    --mock-port "$MOCK_PORT" \
    --recording-path "$RECORDING_PATH" \
    --pg-url "$PG_URL" \
    --redis-url "$REDIS_URL" \
    --deja-control-url "http://127.0.0.1:$CONTROL_PORT"
