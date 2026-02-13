package metrics

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/MikeSquared-Agency/chronicle/internal/events"
	"github.com/MikeSquared-Agency/chronicle/internal/testutil"
)

func TestProcess_TaskCompleted_IncrementsAndUpdates(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	startedAt := time.Now().UTC().Add(-3 * time.Second)
	completedAt := time.Now().UTC()
	dur := completedAt.Sub(startedAt).Milliseconds()

	ms.SetTrace("task-1", map[string]any{
		"trace_id":       "task-1",
		"assigned_agent": "kai",
		"started_at":     startedAt,
		"duration_ms":    dur,
	})

	e := events.Event{
		EventID:   "e1",
		TraceID:   "task-1",
		Source:    "kai",
		EventType: events.TypeTaskCompleted,
		Timestamp: completedAt,
		Metadata:  json.RawMessage(`{}`),
	}

	p.Process(context.Background(), e)

	if ms.UpsertMetricCalls != 1 {
		t.Errorf("expected 1 metric upsert call, got %d", ms.UpsertMetricCalls)
	}

	// Check that inc_completed was passed.
	key := "kai|" + completedAt.Format("2006-01-02")
	m := ms.Metrics[key]
	if m == nil {
		t.Fatal("expected metrics entry for kai")
	}
	if m["inc_completed"] != true {
		t.Error("expected inc_completed to be true")
	}
}

func TestProcess_TaskFailed_IncrementsFailure(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	ms.SetTrace("task-1", map[string]any{
		"trace_id":       "task-1",
		"assigned_agent": "lily",
	})

	e := events.Event{
		EventID:   "e1",
		TraceID:   "task-1",
		Source:    "lily",
		EventType: events.TypeTaskFailed,
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{}`),
	}

	p.Process(context.Background(), e)

	if ms.UpsertMetricCalls != 1 {
		t.Errorf("expected 1 metric upsert, got %d", ms.UpsertMetricCalls)
	}
}

func TestProcess_TaskStarted_UpdatesPickup(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	ms.SetTrace("task-1", map[string]any{
		"trace_id":          "task-1",
		"assigned_agent":    "scout",
		"time_to_pickup_ms": int64(1500),
	})

	e := events.Event{
		EventID:   "e1",
		TraceID:   "task-1",
		Source:    "scout",
		EventType: events.TypeTaskStarted,
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{}`),
	}

	p.Process(context.Background(), e)

	if ms.UpsertMetricCalls != 1 {
		t.Errorf("expected 1 metric upsert for pickup, got %d", ms.UpsertMetricCalls)
	}
}

func TestProcess_Heartbeat_ComputesUtilization(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	e := events.Event{
		EventID:   "e1",
		TraceID:   "scout",
		Source:    "scout",
		EventType: events.TypeAgentHeartbeat,
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{"uptime_ms":10000,"busy_ms":7500}`),
	}

	p.Process(context.Background(), e)

	if ms.UpsertMetricCalls != 1 {
		t.Errorf("expected 1 metric upsert, got %d", ms.UpsertMetricCalls)
	}

	// Check utilization was computed: 7500/10000 * 100 = 75.
	for _, m := range ms.Metrics {
		if aid, ok := m["agent_id"].(string); ok && aid == "scout" {
			if util, ok := m["utilization_pct"].(float64); ok {
				if util != 75.0 {
					t.Errorf("expected utilization 75.0, got %f", util)
				}
			} else {
				t.Error("expected utilization_pct to be set")
			}
			return
		}
	}
	t.Error("scout metrics not found")
}

func TestProcess_Heartbeat_NoSource(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	e := events.Event{
		EventID:   "e1",
		TraceID:   "xxx",
		Source:    "", // no source
		EventType: events.TypeAgentHeartbeat,
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{"agent":"fallback-agent","uptime_ms":5000}`),
	}

	p.Process(context.Background(), e)

	// Should use metadata "agent" field as fallback.
	if ms.UpsertMetricCalls != 1 {
		t.Errorf("expected 1 metric upsert using fallback agent, got %d", ms.UpsertMetricCalls)
	}
}

func TestProcess_NoAgent_Skips(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	// Trace with no assigned_agent.
	ms.SetTrace("task-1", map[string]any{
		"trace_id": "task-1",
		"status":   "pending",
	})

	e := events.Event{
		EventID:   "e1",
		TraceID:   "task-1",
		Source:    "dispatch",
		EventType: events.TypeTaskCompleted,
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{}`),
	}

	p.Process(context.Background(), e)

	// Should skip since no agent is assigned.
	if ms.UpsertMetricCalls != 0 {
		t.Errorf("expected 0 metric upserts when no agent assigned, got %d", ms.UpsertMetricCalls)
	}
}

func TestProcess_IgnoredEventType(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	e := events.Event{
		EventID:   "e1",
		TraceID:   "task-1",
		Source:    "dispatch",
		EventType: events.TypeTaskCreated, // not handled by metrics processor
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{}`),
	}

	p.Process(context.Background(), e)

	if ms.UpsertMetricCalls != 0 {
		t.Errorf("expected 0 metric upserts for task.created, got %d", ms.UpsertMetricCalls)
	}
}
