#!/bin/bash
set -e

echo "🚀 Starting Error Handling Test..."

# 1. Start Replay Mode (assuming recordings exist from previous test, or start fresh empty?)
# If recordings exist, 'GET random' likely won't match.
# If no recordings, nothing matches.
# Let's use existing recordings from concurrent test (still in /recordings volume but we rebuilt).
# Note: integration_test.sh clears recordings. concurrent_test.sh clears recordings.
# So if we run this AFTER concurrent_test.sh, we have recordings.

# Build the proxy to ensure latest code (error handling) is used
echo "🔨 Building Proxy..."
docker compose build deja-postgres

echo "🔄 Starting Proxy in Replay Mode..."
docker compose down
# Start Proxy
docker rm -f deja-replay || true
docker compose run -d --name deja-replay \
    -v $(pwd)/recordings:/data/recordings \
    -p 5434:5434 \
    --entrypoint /usr/local/bin/deja-proxy \
    deja-postgres \
    --listen 0.0.0.0:5434 \
    --mode replay \
    --record-dir /data/recordings/ \
    --target 0.0.0.0:0

sleep 5

echo "⚡ Sending Unmatched Redis Request..."
# This key 'random_key_xyz' was definitely not recorded.
RES=$(docker run --rm --network host redis redis-cli -h 127.0.0.1 -p 5434 GET random_key_xyz)
echo "Got Response: '$RES'"

if [[ "$RES" == *"No replay match found"* ]]; then
    echo "✅ Redis Error Correctly Received!"
else
    echo "❌ Expected Error, got: '$RES'"
    exit 1
fi

echo "⚡ Sending Unmatched HTTP Request..."
HTTP_RES=$(curl -s -o /dev/null -w "%{http_code}" -H "Host: localhost:8080" http://127.0.0.1:5434/nonexistent)
echo "Got HTTP Code: $HTTP_RES"

if [[ "$HTTP_RES" == "500" ]]; then
     echo "✅ HTTP 500 Received!"
else
     echo "❌ Expected 500, got: $HTTP_RES"
     # Note: If no match found, we send 500.
     exit 1
fi

echo "🎉 Error Handling Verification Passed!"

docker stop deja-replay
docker rm deja-replay
