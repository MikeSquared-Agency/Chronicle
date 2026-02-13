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

echo ""
if [ "$FAIL" -eq 0 ]; then
  echo "All Chronicle E2E tests passed."
else
  echo "Some Chronicle E2E tests FAILED."
  exit 1
fi
