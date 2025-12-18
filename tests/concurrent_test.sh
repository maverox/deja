#!/bin/bash
set -e

echo "🚀 Starting Concurrent Replay Test..."

# 0. Clean up
docker compose down -v
rm -rf recordings/*.bin recordings/sessions/

# 1. Start Environment (Force rebuild to ensure latest proxy)
docker compose up -d --build

echo "Waiting for services..."
sleep 10

# 2. Run Concurrent Traffic
echo "⚡ Generating Concurrent Traffic..."

# Run Redis SET and Postgres SELECT in parallel
docker compose exec -d redis redis-cli -h deja-redis -p 6390 SET concurrent_key "value_1"
PID_REDIS=$!

# Use a complex query or sleep to ensure partial overlap? Simple SELECT 1 is fast.
# Maybe sending multiple requests in loop?
# Let's simple parallel execution.
docker compose exec -d -e PGPASSWORD=password postgres psql -h deja-postgres -p 5433 -U deja_user -d deja_test -c "SELECT pg_sleep(0.5);"
PID_PG=$!

# Run HTTP too?
curl -s http://localhost:8080/ > /dev/null &
PID_HTTP=$!

wait $PID_REDIS $PID_PG $PID_HTTP
echo "t concurrent traffic done."

# Now sequential GET to verify
docker compose exec redis redis-cli -h deja-redis -p 6390 GET concurrent_key

# 3. Verify Recording
echo "🔍 Verifying recordings..."
count=$(find recordings -name "*.bin" | wc -l)
echo "Found $count files."
if [ "$count" -lt 1 ]; then
    echo "❌ Expected at least 1 recording."
    exit 1
fi

# 4. Replay Setup
echo "🔄 Prepare Replay..."
docker compose down
# Start Proxy in Replay Mode
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

# 5. Concurrent Replay
echo "⚡ Running Concurrent Replay..."

# We need to run client commands that MATCH the recorded ones.
# 1. Redis SET
(
    RES=$(docker run --rm --network host redis redis-cli -h 127.0.0.1 -p 5434 SET concurrent_key "value_1")
    if [[ "$RES" == "OK" ]]; then echo "✅ Redis SET OK"; else echo "❌ Redis SET Failed: $RES"; exit 1; fi
) &
PID_REP_REDIS=$!

# 2. Postgres Sleep
(
    PGPASSWORD=deja_password psql -h 127.0.0.1 -p 5434 -U deja_user -d deja_test -c "SELECT pg_sleep(0.5);" > /dev/null
    if [ $? -eq 0 ]; then echo "✅ Postgres SELECT OK"; else echo "❌ Postgres Failed"; exit 1; fi
) &
PID_REP_PG=$!

# 3. HTTP GET
(
    HTTP_RES=$(curl -s -H "Host: localhost:8080" http://127.0.0.1:5434/)
    if [[ "$HTTP_RES" == *"Hostname"* ]]; then echo "✅ HTTP OK"; else echo "❌ HTTP Failed"; exit 1; fi
) &
PID_REP_HTTP=$!

wait $PID_REP_REDIS
CODE_REDIS=$?
wait $PID_REP_PG
CODE_PG=$?
wait $PID_REP_HTTP
CODE_HTTP=$?

if [ $CODE_REDIS -eq 0 ] && [ $CODE_PG -eq 0 ] && [ $CODE_HTTP -eq 0 ]; then
    echo "🎉 Concurrent Replay Passed!"
else
    echo "❌ Concurrent Replay Failed"
    exit 1
fi

docker stop deja-replay
docker rm deja-replay
docker compose down
