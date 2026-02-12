# Chronicle

Observability service for the swarm. Consumes events from NATS JetStream, normalizes them into a canonical schema, batch-writes to Supabase/Postgres, and maintains materialized views for traces and agent metrics.

## Architecture

```
NATS JetStream          Chronicle                    Supabase
┌──────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│ TASK_EVENTS  │───▶│ Ingester         │    │ swarm_events        │
│ SYSTEM_EVENTS│───▶│   ↓              │    │ swarm_traces        │
│ DLQ          │───▶│ Normalizer       │    │ agent_metrics       │
└──────────────┘    │   ↓              │    └─────────────────────┘
                    │ Batcher (5s/100) │───▶  batch COPY
                    │   ↓              │
                    │ Trace Processor  │───▶  upsert traces
                    │ Metrics Processor│───▶  upsert metrics
                    │                  │
                    │ HTTP API (:8700) │◀──  dashboard queries
                    └──────────────────┘
```

## Quick Start

```bash
# Set required env vars
export DATABASE_URL="postgresql://postgres:password@db.your-project.supabase.co:5432/postgres"
export NATS_URL="nats://hermes:4222"

# Apply the migration to your Supabase project
psql $DATABASE_URL -f migrations/001_initial_schema.sql

# Build and run
go build -o chronicle ./cmd/chronicle
./chronicle
```

## Docker

```bash
docker build -t chronicle .
docker run -e DATABASE_URL=... -e NATS_URL=... -p 8700:8700 chronicle
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `CHRONICLE_PORT` | HTTP API port | `8700` |
| `NATS_URL` | NATS server URL | `nats://hermes:4222` |
| `DATABASE_URL` | Postgres connection string | — (required) |
| `ALEXANDRIA_URL` | Alexandria secrets API | `http://alexandria:8500` |
| `BATCH_FLUSH_INTERVAL_MS` | Flush timer interval | `5000` |
| `BATCH_FLUSH_THRESHOLD` | Max events before flush | `100` |
| `BUFFER_MAX_SIZE` | In-memory buffer cap | `10000` |
| `LOG_LEVEL` | `debug`, `info`, `warn`, `error` | `info` |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/health` | Health check with buffer size |
| GET | `/api/v1/events?trace_id=X` | Events for a trace |
| GET | `/api/v1/traces?status=X&limit=N` | List traces with filtering |
| GET | `/api/v1/traces/:id` | Single trace with all spans |
| GET | `/api/v1/metrics/:agent_id/latest` | Latest metrics for an agent |
| GET | `/api/v1/metrics/summary` | All agents, current day |

## Event Schema

Every event on `swarm.>` must conform to:

```json
{
  "event_id": "uuid-v4",
  "trace_id": "task-id or correlation-id",
  "source": "dispatch | warren | scout | kai | ...",
  "event_type": "task.created | task.assigned | ...",
  "timestamp": "2026-02-12T14:30:00.000Z",
  "metadata": {}
}
```

Chronicle fills defaults for missing `event_id` (generates UUID), `timestamp` (uses ingestion time), and `metadata` (empty object). `trace_id` is required.

## NATS Subscriptions

| Stream | Subjects | Events |
|--------|----------|--------|
| `TASK_EVENTS` | `swarm.task.>` | Task lifecycle |
| `SYSTEM_EVENTS` | `swarm.agent.>`, `swarm.system.>`, `swarm.lifecycle.>` | Agent and system events |
| `DLQ` | `dlq.>` | Dead letter entries |

Durable consumer `chronicle-{STREAM}` with explicit ack, max 3 redeliveries.

## Database Schema

Three tables in Supabase:

- **`swarm_events`** — Raw event log (30-day retention)
- **`swarm_traces`** — Materialized trace view with status, duration, pickup time (90-day retention)
- **`agent_metrics`** — Rolling daily agent performance stats (kept indefinitely)

See `migrations/001_initial_schema.sql` for full DDL.

## Batch Write Strategy

- Flush every 5 seconds or 100 events (whichever first)
- Write order: events → traces → metrics
- On failure: re-queue batch, retry next flush
- After 3 consecutive failures: publish `swarm.system.chronicle.write_failure`
- Buffer cap: 10,000 events; oldest dropped on overflow with `swarm.system.chronicle.buffer_overflow` alert

## Derived Signals

Computed on each batch flush, not at query time:

- **Task duration** — `completed_at - started_at` on `task.completed`
- **Time-to-pickup** — `started_at - assigned_at` on `task.started`
- **Failure rate** — `tasks_failed / (tasks_completed + tasks_failed)` per agent per day
- **Utilization** — `total_busy_ms / total_uptime_ms * 100` from heartbeat events

Dispatch queries `agent_metrics` for scoring:
```
score = capability_match * availability * policy_compliance * historical_performance
```

## Testing

```bash
# Unit tests (no external deps)
go test ./...

# Integration tests (requires live services)
DATABASE_URL=... NATS_URL=... go test ./... -v
```

40 tests total: 33 unit, 7 integration (auto-skip without env vars).

## Project Structure

```
cmd/chronicle/main.go           Boot sequence and wiring
internal/
  config/config.go              Environment variable loading
  events/schema.go              Canonical event struct and normalizer
  ingester/ingester.go          NATS JetStream durable consumer
  batcher/batcher.go            Batch flush with backpressure and retry
  store/
    interface.go                DataStore interface
    store.go                    pgx/Postgres implementation
  traces/processor.go           Trace state machine
  metrics/processor.go          Agent metric rollups
  api/server.go                 HTTP API (chi router)
  testutil/mock_store.go        In-memory mock for tests
migrations/
  001_initial_schema.sql        Supabase DDL
```
