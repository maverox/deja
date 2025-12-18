#!/bin/bash
set -e

# Build the proxy first
cargo build --bin deja-proxy

# Use a timestamped session based on current date
SESSION_ID="hyperswitch_session_$(date +%s)"

echo "Starting Deja Proxy for Hyperswitch..."
echo "Mappings:"
echo "  - 5433 -> 127.0.0.1:5432 (PostgreSQL)"
echo "  - 6380 -> 127.0.0.1:6379 (Redis)"
echo "Recording to: recordings/$SESSION_ID"

# Launch Proxy
export DEJA_STORAGE_FORMAT=json
cargo run --bin deja-proxy -- \
    --mode record \
    --map 5433:127.0.0.1:5432 \
    --map 6380:127.0.0.1:6379 \
    --record-dir "recordings/$SESSION_ID"
