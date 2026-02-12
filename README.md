# Chronicle

Observability service for the swarm. Consumes events from NATS JetStream, normalizes them into a canonical schema, batch-writes to Supabase/Postgres, and maintains materialized views for traces and agent metrics.

## Architecture

```mermaid
graph LR
    subgraph NATS JetStream
        TE[TASK_EVENTS]
        SE[SYSTEM_EVENTS]
        DLQ[DLQ]
    end

    subgraph Chronicle
        ING[Ingester]
        NORM[Normalizer]
        BAT[Batcher<br/>5s / 100 events]
        TP[Trace Processor]
        MP[Metrics Processor]
        API[HTTP API :8700]
    end

    subgraph Supabase
        EVT[(swarm_events)]
        TRC[(swarm_traces)]
        MET[(agent_metrics)]
    end

    TE --> ING
    SE --> ING
    DLQ --> ING
    ING --> NORM --> BAT
    BAT -- batch COPY --> EVT
    BAT --> TP -- upsert --> TRC
    BAT --> MP -- upsert --> MET
    API -. query .-> EVT
    API -. query .-> TRC
    API -. query .-> MET
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

### Trace Lifecycle

```mermaid
stateDiagram-v2
    [*] --> pending : task.created
    pending --> assigned : task.assigned
    assigned --> in_progress : task.started
    in_progress --> completed : task.completed
    in_progress --> failed : task.failed
    in_progress --> timed_out : task.timeout
    assigned --> timed_out : task.timeout
    pending --> timed_out : task.timeout
```

## NATS Subscriptions

| Stream | Subjects | Events |
|--------|----------|--------|
| `TASK_EVENTS` | `swarm.task.>` | Task lifecycle |
| `SYSTEM_EVENTS` | `swarm.agent.>`, `swarm.system.>`, `swarm.lifecycle.>` | Agent and system events |
| `DLQ` | `dlq.>` | Dead letter entries |

Durable consumer `chronicle-{STREAM}` with explicit ack, max 3 redeliveries.

## Database Schema

```mermaid
erDiagram
    swarm_events {
        uuid event_id PK
        text trace_id
        text source
        text event_type
        timestamptz timestamp
        jsonb metadata
        timestamptz created_at
    }

    swarm_traces {
        text trace_id PK
        text task_title
        text owner
        text status
        text assigned_agent
        timestamptz started_at
        timestamptz completed_at
        bigint duration_ms
        bigint time_to_pickup_ms
        int span_count
        text error
        jsonb scoring_breakdown
    }

    agent_metrics {
        text agent_id PK
        date metric_date PK
        int tasks_completed
        int tasks_failed
        bigint avg_duration_ms
        bigint p95_duration_ms
        numeric utilization_pct
    }

    swarm_events }o--|| swarm_traces : "trace_id"
    swarm_traces }o--|| agent_metrics : "assigned_agent"
```

- **`swarm_events`** — Raw event log (30-day retention)
- **`swarm_traces`** — Materialized trace view with status, duration, pickup time (90-day retention)
- **`agent_metrics`** — Rolling daily agent performance stats (kept indefinitely)

See `migrations/001_initial_schema.sql` for full DDL.

## Batch Write Strategy

```mermaid
flowchart TD
    E[Event arrives] --> BUF[Add to buffer]
    BUF --> CHECK{Buffer >= 100<br/>or 5s elapsed?}
    CHECK -- No --> WAIT[Wait]
    WAIT --> CHECK
    CHECK -- Yes --> FLUSH[Flush batch]
    FLUSH --> INS[INSERT into swarm_events]
    INS -- success --> TRACE[Update swarm_traces]
    TRACE --> METRIC[Update agent_metrics]
    METRIC --> DONE[Reset buffer]
    INS -- failure --> REQUEUE[Re-queue batch]
    REQUEUE --> FAIL{3 consecutive<br/>failures?}
    FAIL -- No --> WAIT
    FAIL -- Yes --> ALERT[Publish write_failure alert]
    ALERT --> WAIT

    BUF --> OVER{Buffer > 10k?}
    OVER -- Yes --> DROP[Drop oldest + alert]
    DROP --> BUF
```

## Derived Signals

Computed on each batch flush, not at query time:

```mermaid
graph TD
    subgraph "Task Events"
        TC[task.completed] --> DUR["duration_ms = completed_at - started_at"]
        TS[task.started] --> PICKUP["time_to_pickup_ms = started_at - assigned_at"]
        TF[task.failed] --> FAIL["tasks_failed++"]
        TC --> COMP["tasks_completed++"]
    end

    subgraph "Agent Events"
        HB[agent.heartbeat] --> UTIL["utilization = busy_ms / uptime_ms * 100"]
    end

    DUR --> AM[(agent_metrics)]
    PICKUP --> AM
    FAIL --> AM
    COMP --> AM
    UTIL --> AM

    AM -. "Dispatch queries" .-> SCORE["score = capability * availability *<br/>policy_compliance * historical_performance"]
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
