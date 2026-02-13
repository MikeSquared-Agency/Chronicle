#!/usr/bin/env bash
set -euo pipefail

#
# E2E tests for Chronicle Slack v1.5 infrastructure:
# - SLACK_EVENTS stream subscription
# - DLQ → Slack alert pipeline (without real Slack tokens)
#
# Prerequisites: Chronicle + NATS running.
# Usage: NATS_URL=nats://localhost:4222 ./scripts/e2e_slack.sh
#

BASE="http://localhost:${CHRONICLE_PORT:-8700}/api/v1"
NATS_URL="${NATS_URL:-nats://localhost:4222}"
FAIL=0

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1 — $2"; FAIL=1; }

echo "=== Chronicle Slack v1.5 E2E Tests ==="

# ---------------------------------------------------------------------------
# 1. Verify SLACK_EVENTS stream exists (Chronicle creates it on startup)
# ---------------------------------------------------------------------------
echo "--- SLACK_EVENTS stream ---"
if nats stream info SLACK_EVENTS --server="$NATS_URL" >/dev/null 2>&1; then
  pass "SLACK_EVENTS stream exists"
else
  fail "SLACK_EVENTS stream" "stream not found — Chronicle may not have started with Slack subjects"
fi

# ---------------------------------------------------------------------------
# 2. Publish a slack.message event and verify it flows into Chronicle
# ---------------------------------------------------------------------------
echo "--- Slack event ingestion ---"
TRACE_ID="e2e-slack-$(date +%s)"
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)

nats pub --server="$NATS_URL" swarm.slack.message \
  "{\"event_id\":\"$(uuidgen)\",\"trace_id\":\"$TRACE_ID\",\"source\":\"agent.lily\",\"event_type\":\"slack.message\",\"timestamp\":\"$NOW\",\"metadata\":{\"agent_id\":\"lily\",\"direction\":\"out\",\"channel_id\":\"C123\",\"text\":\"e2e test message\",\"thread_ts\":\"1234567890.123456\",\"user_id\":\"U456\",\"message_type\":\"message\"}}"
pass "published swarm.slack.message"

echo "--- Waiting for batch flush (8s) ---"
sleep 8

HTTP=$(curl -s -o /tmp/e2e_slack_body -w '%{http_code}' "$BASE/events?trace_id=$TRACE_ID")
if [ "$HTTP" = "200" ]; then
  if grep -q "slack.message" /tmp/e2e_slack_body; then
    pass "slack.message event materialized in Chronicle"
  else
    fail "slack event" "event not found for trace $TRACE_ID"
  fi
else
  fail "events API" "expected 200, got $HTTP"
fi

# ---------------------------------------------------------------------------
# 3. Publish a DLQ event and verify the NATS alert is published
# ---------------------------------------------------------------------------
echo "--- DLQ alert pipeline ---"

# Subscribe to the alert subject in the background, capture first message.
timeout 10 nats sub --server="$NATS_URL" --count=1 swarm.alert.dlq > /tmp/e2e_dlq_alert 2>&1 &
SUB_PID=$!
sleep 1

nats pub --server="$NATS_URL" dlq.slack.test \
  "{\"event_id\":\"$(uuidgen)\",\"trace_id\":\"dlq-e2e-$(date +%s)\",\"source\":\"e2e\",\"event_type\":\"dlq.entry\",\"timestamp\":\"$NOW\",\"metadata\":{\"reason\":\"test DLQ entry\",\"source\":\"e2e-runner\"}}"
pass "published dlq.slack.test"

# Wait for the subscriber to catch the alert.
if wait $SUB_PID 2>/dev/null; then
  if grep -q "dlq_entry" /tmp/e2e_dlq_alert; then
    pass "swarm.alert.dlq published after DLQ event"
  else
    fail "DLQ alert" "alert published but missing dlq_entry type"
  fi
else
  fail "DLQ alert" "timed out waiting for swarm.alert.dlq"
fi

# ---------------------------------------------------------------------------
# 4. Verify per-stream retention (SLACK_EVENTS should be 14d, not 7d)
# ---------------------------------------------------------------------------
echo "--- Stream retention ---"
MAX_AGE=$(nats stream info SLACK_EVENTS --server="$NATS_URL" --json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['config']['max_age'])" 2>/dev/null || echo "0")
# 14 days in nanoseconds = 1209600000000000
if [ "$MAX_AGE" = "1209600000000000" ]; then
  pass "SLACK_EVENTS retention is 14 days"
else
  fail "SLACK_EVENTS retention" "expected 14d (1209600000000000ns), got $MAX_AGE"
fi

echo ""
if [ "$FAIL" -eq 0 ]; then
  echo "All Chronicle Slack v1.5 E2E tests passed."
else
  echo "Some E2E tests FAILED."
  exit 1
fi
