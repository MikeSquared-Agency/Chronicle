-- Chronicle: Initial Schema
-- Apply to swarm Supabase project

-- Raw Event Log
create table if not exists swarm_events (
  event_id     uuid primary key default gen_random_uuid(),
  trace_id     text not null,
  source       text not null,
  event_type   text not null,
  timestamp    timestamptz not null default now(),
  metadata     jsonb default '{}',
  created_at   timestamptz default now()
);

create index if not exists idx_swarm_events_trace     on swarm_events (trace_id);
create index if not exists idx_swarm_events_type      on swarm_events (event_type);
create index if not exists idx_swarm_events_source    on swarm_events (source);
create index if not exists idx_swarm_events_timestamp on swarm_events (timestamp desc);
create index if not exists idx_swarm_events_trace_ts  on swarm_events (trace_id, timestamp);

-- Materialized Trace View
create table if not exists swarm_traces (
  trace_id          text primary key,
  task_title        text,
  owner             text,
  status            text not null default 'pending',
  assigned_agent    text,
  started_at        timestamptz,
  completed_at      timestamptz,
  duration_ms       bigint,
  time_to_pickup_ms bigint,
  span_count        int default 0,
  error             text,
  scoring_breakdown jsonb,
  created_at        timestamptz default now(),
  updated_at        timestamptz default now(),

  constraint valid_trace_status check (
    status in ('pending', 'assigned', 'in_progress', 'completed', 'failed', 'timed_out')
  )
);

create index if not exists idx_traces_status on swarm_traces (status);
create index if not exists idx_traces_agent  on swarm_traces (assigned_agent);
create index if not exists idx_traces_owner  on swarm_traces (owner);

-- Rolling Agent Performance Metrics
create table if not exists agent_metrics (
  agent_id              text not null,
  metric_date           date not null,
  tasks_completed       int default 0,
  tasks_failed          int default 0,
  avg_duration_ms       bigint default 0,
  p95_duration_ms       bigint default 0,
  min_duration_ms       bigint default 0,
  max_duration_ms       bigint default 0,
  time_to_pickup_avg_ms bigint default 0,
  utilization_pct       numeric(5,2) default 0,
  total_uptime_ms       bigint default 0,
  total_busy_ms         bigint default 0,
  updated_at            timestamptz default now(),

  primary key (agent_id, metric_date)
);

-- Retention cleanup (run via pg_cron daily at 03:00 UTC)
-- select cron.schedule('chronicle-retention', '0 3 * * *', $$
--   delete from swarm_events where timestamp < now() - interval '30 days';
--   delete from swarm_traces where completed_at < now() - interval '90 days';
-- $$);
