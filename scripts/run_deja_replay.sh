#!/bin/bash
set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <path_to_recording_dir>"
    echo "Example: $0 recordings/hyperswitch_session_123456"
    exit 1
fi

RECORDING_DIR="$1"

echo "Starting Deja Proxy in REPLAY mode..."
echo "Loading recordings from: $RECORDING_DIR"
echo "Mappings (Mocking):"
echo "  - 5433 (PostgreSQL Replay)"
echo "  - 6380 (Redis Replay)"

# Launch Proxy in Replay Mode
# Note: Target addresses are ignored in Replay mode but required by CLI argument parser if not optional.
# Our refactored main.rs requires --map with 3 parts, but only uses the first one for listening.
# We pass dummy targets.

export RUST_LOG=info
export DEJA_STORAGE_FORMAT=json

cargo run --bin deja-proxy -- \
    --mode replay \
    --map 5433:127.0.0.1:0 \
    --map 6380:127.0.0.1:0 \
    --record-dir "$RECORDING_DIR"
