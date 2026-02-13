#!/usr/bin/env bash
set -euo pipefail

BASE="http://localhost:${CHRONICLE_PORT:-8700}/api/v1"
FAIL=0

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1 — $2"; FAIL=1; }

echo "=== Chronicle E2E Smoke Tests ==="

# 1. Health check
echo "--- Health ---"
HTTP=$(curl -s -o /tmp/e2e_body -w '%{http_code}' "$BASE/health")
if [ "$HTTP" = "200" ]; then
  pass "GET /api/v1/health → 200"
else
  fail "GET /api/v1/health" "expected 200, got $HTTP"
fi

# 2. List traces (empty DB is fine, just check 200)
echo "--- Traces ---"
HTTP=$(curl -s -o /tmp/e2e_body -w '%{http_code}' "$BASE/traces")
if [ "$HTTP" = "200" ]; then
  pass "GET /api/v1/traces → 200"
else
  fail "GET /api/v1/traces" "expected 200, got $HTTP"
fi

# 3. List events (requires trace_id param — check 400 without it)
echo "--- Events ---"
HTTP=$(curl -s -o /tmp/e2e_body -w '%{http_code}' "$BASE/events")
if [ "$HTTP" = "400" ]; then
  pass "GET /api/v1/events (no trace_id) → 400"
else
  fail "GET /api/v1/events" "expected 400, got $HTTP"
fi

# 4. Events with a dummy trace_id (should return 200 with empty list)
HTTP=$(curl -s -o /tmp/e2e_body -w '%{http_code}' "$BASE/events?trace_id=00000000-0000-0000-0000-000000000000")
if [ "$HTTP" = "200" ]; then
  pass "GET /api/v1/events?trace_id=... → 200"
else
  fail "GET /api/v1/events?trace_id=..." "expected 200, got $HTTP"
fi

# 5. Metrics summary
echo "--- Metrics ---"
HTTP=$(curl -s -o /tmp/e2e_body -w '%{http_code}' "$BASE/metrics/summary")
if [ "$HTTP" = "200" ]; then
  pass "GET /api/v1/metrics/summary → 200"
else
  fail "GET /api/v1/metrics/summary" "expected 200, got $HTTP"
fi

# 6. DLQ list
echo "--- DLQ ---"
HTTP=$(curl -s -o /tmp/e2e_body -w '%{http_code}' "$BASE/dlq/")
if [ "$HTTP" = "200" ]; then
  pass "GET /api/v1/dlq/ → 200"
else
  fail "GET /api/v1/dlq/" "expected 200, got $HTTP"
fi

# 7. DLQ stats
HTTP=$(curl -s -o /tmp/e2e_body -w '%{http_code}' "$BASE/dlq/stats")
if [ "$HTTP" = "200" ]; then
  pass "GET /api/v1/dlq/stats → 200"
else
  fail "GET /api/v1/dlq/stats" "expected 200, got $HTTP"
fi

###############################################################################
# Write-path tests: publish events via NATS, wait for flush, verify via API  #
###############################################################################

echo ""
echo "=== Chronicle E2E Write-Path Tests ==="

NATS_URL="${NATS_URL:-nats://localhost:4222}"
TRACE_ID="e2e-test-$(date +%s)"
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)

echo "--- Publish task lifecycle via NATS (trace_id=$TRACE_ID) ---"

# Chronicle creates JetStream streams on startup (ensureStream), so subjects
# like swarm.task.> are already captured. Plain `nats pub` is sufficient
# because JetStream streams match on the subject.

nats pub --server="$NATS_URL" swarm.task.created \
  "{\"event_id\":\"$(uuidgen)\",\"trace_id\":\"$TRACE_ID\",\"source\":\"e2e\",\"event_type\":\"task.created\",\"timestamp\":\"$NOW\",\"metadata\":{\"title\":\"E2E Test Task\",\"owner\":\"e2e-runner\"}}"
pass "published swarm.task.created"

sleep 0.2

nats pub --server="$NATS_URL" swarm.task.assigned \
  "{\"event_id\":\"$(uuidgen)\",\"trace_id\":\"$TRACE_ID\",\"source\":\"e2e\",\"event_type\":\"task.assigned\",\"timestamp\":\"$NOW\",\"metadata\":{\"agent\":\"e2e-agent\"}}"
pass "published swarm.task.assigned"

sleep 0.2

nats pub --server="$NATS_URL" swarm.task.started \
  "{\"event_id\":\"$(uuidgen)\",\"trace_id\":\"$TRACE_ID\",\"source\":\"e2e\",\"event_type\":\"task.started\",\"timestamp\":\"$NOW\",\"metadata\":{}}"
pass "published swarm.task.started"

sleep 0.2

nats pub --server="$NATS_URL" swarm.task.completed \
  "{\"event_id\":\"$(uuidgen)\",\"trace_id\":\"$TRACE_ID\",\"source\":\"e2e\",\"event_type\":\"task.completed\",\"timestamp\":\"$NOW\",\"metadata\":{}}"
pass "published swarm.task.completed"

# Wait for the batcher to flush (default flush interval is 5s).
echo "--- Waiting for batch flush (8s) ---"
sleep 8

# 8. Verify trace appeared in the API.
echo "--- Verify trace materialized ---"
HTTP=$(curl -s -o /tmp/e2e_body -w '%{http_code}' "$BASE/traces")
if [ "$HTTP" = "200" ]; then
  if grep -q "$TRACE_ID" /tmp/e2e_body; then
    pass "GET /api/v1/traces contains $TRACE_ID"
  else
    fail "GET /api/v1/traces" "trace $TRACE_ID not found in response"
  fi
else
  fail "GET /api/v1/traces" "expected 200, got $HTTP"
fi

# 9. Verify all 4 events returned for the trace.
echo "--- Verify events for trace ---"
HTTP=$(curl -s -o /tmp/e2e_body -w '%{http_code}' "$BASE/events?trace_id=$TRACE_ID")
if [ "$HTTP" = "200" ]; then
  pass "GET /api/v1/events?trace_id=$TRACE_ID → 200"

  # Count how many of our 4 event types appear in the response.
  FOUND=0
  for EVT_TYPE in task.created task.assigned task.started task.completed; do
    if grep -q "\"event_type\":\"$EVT_TYPE\"" /tmp/e2e_body 2>/dev/null || \
       grep -q "\"event_type\": \"$EVT_TYPE\"" /tmp/e2e_body 2>/dev/null; then
      FOUND=$((FOUND + 1))
    fi
  done

  if [ "$FOUND" -eq 4 ]; then
    pass "all 4 lifecycle events found (created, assigned, started, completed)"
  else
    fail "event count" "expected 4 event types, found $FOUND in response"
    echo "  Response body:" && cat /tmp/e2e_body && echo ""
  fi
else
  fail "GET /api/v1/events?trace_id=$TRACE_ID" "expected 200, got $HTTP"
fi

echo ""
if [ "$FAIL" -eq 0 ]; then
  echo "All Chronicle E2E tests passed."
else
  echo "Some Chronicle E2E tests FAILED."
  exit 1
fi
