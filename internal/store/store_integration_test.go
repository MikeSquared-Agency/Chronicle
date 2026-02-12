package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"chronicle/internal/events"
)

func skipWithoutDB(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set, skipping integration test")
	}
	return url
}

func setupTestStore(t *testing.T) *Store {
	t.Helper()
	url := skipWithoutDB(t)
	ctx := context.Background()
	s, err := New(ctx, url)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestIntegration_InsertAndQueryEvents(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	traceID := "integration-test-" + time.Now().Format("20060102150405")

	evts := []events.Event{
		{
			EventID:   "int-evt-1",
			TraceID:   traceID,
			Source:    "test",
			EventType: "task.created",
			Timestamp: time.Now().UTC(),
			Metadata:  json.RawMessage(`{"title":"integration test"}`),
		},
		{
			EventID:   "int-evt-2",
			TraceID:   traceID,
			Source:    "test",
			EventType: "task.assigned",
			Timestamp: time.Now().UTC(),
			Metadata:  json.RawMessage(`{"agent":"kai"}`),
		},
	}

	if err := s.InsertEvents(ctx, evts); err != nil {
		t.Fatalf("insert events: %v", err)
	}

	results, err := s.QueryEvents(ctx, traceID)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	if len(results) < 2 {
		t.Errorf("expected at least 2 events, got %d", len(results))
	}

	// Cleanup.
	s.pool.Exec(ctx, "DELETE FROM swarm_events WHERE trace_id = $1", traceID)
}

func TestIntegration_UpsertAndGetTrace(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	traceID := "int-trace-" + time.Now().Format("20060102150405")

	// Create trace.
	err := s.UpsertTrace(ctx, traceID, map[string]any{
		"status":     "pending",
		"task_title": "Integration Test Task",
		"owner":      "mike",
	})
	if err != nil {
		t.Fatalf("upsert trace: %v", err)
	}

	// Read it back.
	trace, err := s.GetTrace(ctx, traceID)
	if err != nil {
		t.Fatalf("get trace: %v", err)
	}
	if trace["status"] != "pending" {
		t.Errorf("expected status pending, got %v", trace["status"])
	}

	// Update to assigned.
	err = s.UpsertTrace(ctx, traceID, map[string]any{
		"status":         "assigned",
		"assigned_agent": "kai",
	})
	if err != nil {
		t.Fatalf("upsert trace (assigned): %v", err)
	}

	trace, _ = s.GetTrace(ctx, traceID)
	if trace["status"] != "assigned" {
		t.Errorf("expected status assigned, got %v", trace["status"])
	}

	// Cleanup.
	s.pool.Exec(ctx, "DELETE FROM swarm_traces WHERE trace_id = $1", traceID)
}

func TestIntegration_UpsertAndGetAgentMetrics(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	agentID := "int-agent-" + time.Now().Format("150405")
	now := time.Now().UTC()

	err := s.UpsertAgentMetric(ctx, agentID, now, map[string]any{
		"inc_completed":  true,
		"avg_duration_ms": int64(5000),
	})
	if err != nil {
		t.Fatalf("upsert metric: %v", err)
	}

	m, err := s.GetAgentMetrics(ctx, agentID)
	if err != nil {
		t.Fatalf("get agent metrics: %v", err)
	}
	if m["agent_id"] != agentID {
		t.Errorf("expected agent_id %s, got %v", agentID, m["agent_id"])
	}

	// Cleanup.
	s.pool.Exec(ctx, "DELETE FROM agent_metrics WHERE agent_id = $1", agentID)
}

func TestIntegration_QueryTraces(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	prefix := "int-qt-" + time.Now().Format("150405")

	for i := 0; i < 3; i++ {
		traceID := prefix + "-" + string(rune('a'+i))
		status := "pending"
		if i == 2 {
			status = "completed"
		}
		s.UpsertTrace(ctx, traceID, map[string]any{"status": status})
	}

	// Query all.
	all, err := s.QueryTraces(ctx, "", 100)
	if err != nil {
		t.Fatalf("query all traces: %v", err)
	}
	if len(all) < 3 {
		t.Errorf("expected at least 3 traces, got %d", len(all))
	}

	// Query by status.
	pending, err := s.QueryTraces(ctx, "pending", 100)
	if err != nil {
		t.Fatalf("query pending traces: %v", err)
	}
	for _, tr := range pending {
		if tr["status"] != "pending" {
			t.Errorf("expected all pending, got %v", tr["status"])
		}
	}

	// Cleanup.
	for i := 0; i < 3; i++ {
		traceID := prefix + "-" + string(rune('a'+i))
		s.pool.Exec(ctx, "DELETE FROM swarm_traces WHERE trace_id = $1", traceID)
	}
}

func TestIntegration_GetAllAgentMetricsSummary(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()

	agent1 := "int-summ-a-" + time.Now().Format("150405")
	agent2 := "int-summ-b-" + time.Now().Format("150405")
	now := time.Now().UTC()

	s.UpsertAgentMetric(ctx, agent1, now, map[string]any{"inc_completed": true})
	s.UpsertAgentMetric(ctx, agent2, now, map[string]any{"inc_failed": true})

	summary, err := s.GetAllAgentMetricsSummary(ctx)
	if err != nil {
		t.Fatalf("get summary: %v", err)
	}
	if len(summary) < 2 {
		t.Errorf("expected at least 2 agent summaries, got %d", len(summary))
	}

	// Cleanup.
	s.pool.Exec(ctx, "DELETE FROM agent_metrics WHERE agent_id IN ($1, $2)", agent1, agent2)
}
