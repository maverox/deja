#!/bin/bash
set -e

echo "🚀 Starting Integration Test..."

# 0. Clean up
echo "Cleaning up previous run..."
docker compose down -v
rm -rf recordings/*.bin recordings/sessions/

# 1. Start Environment
echo "🐳 Starting services..."
docker compose up -d --build

# Wait for services to be ready
echo "Waiting for services..."
sleep 10

# 2. Run Traffic
echo "Traffic 1: Redis PING -> Port 6390"
# Use redis-cli from the redis container to hit the proxy
docker compose exec redis redis-cli -h deja-redis -p 6390 PING

echo "Traffic 2: Redis SET -> Port 6390"
docker compose exec redis redis-cli -h deja-redis -p 6390 SET mykey "hello world"

echo "Traffic 3: Redis GET -> Port 6390"
docker compose exec redis redis-cli -h deja-redis -p 6390 GET mykey

echo "Traffic 4: Postgres SELECT 1 -> Port 5433"
# Use psql from postgres container to hit the proxy
# PGPASSWORD is set in env
docker compose exec -e PGPASSWORD=password postgres psql -h deja-postgres -p 5433 -U deja_user -d deja_test -c "SELECT 1"

echo "Traffic 5: HTTP GET -> Port 8080"
# Use curl from postgres or redis container (assuming they have curl/wget? standard postgres:15-alpine usually has wget? verifying...)
# whoami has nothing. Redis alpine has busybox/wget.
# Let's try curl from host against localhost port 8080 (since mapped in docker-compose)
curl -s http://localhost:8080/


# 3. Verify Recording
echo "🔍 Verifying recordings..."
count=$(find recordings -name "*.bin" | wc -l)
echo "Found $count recording files."

if [ "$count" -lt 1 ]; then
    echo "❌ Expected at least 1 recording."
    # Dump logs if failed
    docker compose logs
    exit 1
else
    echo "✅ Recordings found!"
fi

# 4. (Optional) Run Replay
# We would need to stop the real services and start `deja run` here.
# For this script, we stop at verifying recordings are created.

# 5. Replay Verification
echo "🔄 Starting Replay Verification..."

# Stop the proxy (it was running in record mode)
# Since we are running via docker compose, we can just restart the deja-proxy service with different command?
# Or we can run deja-proxy binary locally if we built it.
# But keeping it in docker is cleaner for environment.

# We need to restart using the recorded data.
# 1. Stop services
docker compose down

# 2. Start ONLY deja-proxy in Replay Mode and Validating Client (no real request target needed ideally, but keeping network setup)
# We need to mount the recordings we just made.
# The recordings are in ./recordings (host) -> /data/recordings (container)

echo "🚀 Starting Deja Proxy in REPLAY mode..."
docker rm -f deja-replay || true
docker compose run -d --name deja-replay \
    -v $(pwd)/recordings:/data/recordings \
    -p 5434:5434 \
    --entrypoint /usr/local/bin/deja-proxy \
    deja-postgres \
    --listen 0.0.0.0:5434 \
    --mode replay \
    --record-dir /data/recordings/ \
    --target 0.0.0.0:0 # Target ignored in replay

# Wait for proxy
sleep 5

# 3. Run Client against Replay Proxy

echo "Traffic Replay: Postgres SELECT 1 -> Port 5434"
PGPASSWORD=deja_password psql -h 127.0.0.1 -p 5434 -U deja_user -d deja_test -c "SELECT 1"

if [ $? -eq 0 ]; then
    echo "✅ Postgres Replay Successful!"
else
    echo "❌ Postgres Replay Failed!"
    exit 1
fi

echo "Traffic Replay: Redis GET -> Port 5434"
RES=$(docker run --rm --network host redis redis-cli -h 127.0.0.1 -p 5434 GET mykey)

if [[ "$RES" == *"hello world"* ]]; then
     echo "✅ Redis Replay Successful! Got: $RES"
else
     echo "❌ Redis Replay Failed! Expected 'hello world', Got: '$RES'"
     exit 1
fi

echo "Traffic Replay: HTTP GET -> Port 5434"
# match Host header from recording (localhost:8080)
HTTP_RES=$(curl -s -H "Host: localhost:8080" http://127.0.0.1:5434/)
if [[ "$HTTP_RES" == *"Hostname"* ]]; then
     echo "✅ HTTP Replay Successful! Output contains Hostname."
else
     echo "❌ HTTP Replay Failed! Got: '$HTTP_RES'"
     exit 1
fi

# Cleanup Replay
docker stop deja-replay
docker rm deja-replay

echo "🎉 Integration Test (Record & Replay) Passed!"
docker compose down
