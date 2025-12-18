#!/bin/bash
set -e

# Assuming this script is run from project_deja/scripts/ or root?
# Better to expect HYPERSWITCH_DIR env var or assume ~/hyperswitch

HYPERSWITCH_DIR="${HYPERSWITCH_DIR:-$HOME/hyperswitch}"

if [ ! -d "$HYPERSWITCH_DIR" ]; then
    echo "Error: Hyperswitch directory not found at $HYPERSWITCH_DIR"
    echo "Please set HYPERSWITCH_DIR environment variable."
    exit 1
fi

echo "Configuring Hyperswitch to use Deja Proxy..."
export ROUTER__MASTER_DATABASE__PORT=5433
export ROUTER__REDIS__PORT=6380
export ROUTER__ANALYTICS__SQLX__PORT=5433

# Optional: Disable other services that might conflict or we don't want to record yet?
# export ROUTER__LOG__TELEMETRY__METRICS_ENABLED=false

echo "Running Hyperswitch Router from $HYPERSWITCH_DIR"
cd "$HYPERSWITCH_DIR"

# Run the router
just run_v2
