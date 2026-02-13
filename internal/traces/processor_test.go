package traces

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/MikeSquared-Agency/chronicle/internal/events"
	"github.com/MikeSquared-Agency/chronicle/internal/testutil"
)

func TestProcess_TaskCreated(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	e := events.Event{
		EventID:   "e1",
		TraceID:   "task-1",
		Source:    "dispatch",
		EventType: events.TypeTaskCreated,
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{"title":"Build feature","owner":"mike"}`),
	}

	p.Process(context.Background(), e)

	trace, err := ms.GetTrace(context.Background(), "task-1")
	if err != nil {
		t.Fatalf("trace not found: %v", err)
	}

	if trace["status"] != "pending" {
		t.Errorf("expected status pending, got %v", trace["status"])
	}
	if trace["task_title"] != "Build feature" {
		t.Errorf("expected title 'Build feature', got %v", trace["task_title"])
	}
	if trace["owner"] != "mike" {
		t.Errorf("expected owner 'mike', got %v", trace["owner"])
	}
}

func TestProcess_TaskAssigned(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	e := events.Event{
		EventID:   "e1",
		TraceID:   "task-1",
		Source:    "dispatch",
		EventType: events.TypeTaskAssigned,
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{"agent":"kai"}`),
	}

	p.Process(context.Background(), e)

	trace, _ := ms.GetTrace(context.Background(), "task-1")
	if trace["status"] != "assigned" {
		t.Errorf("expected status assigned, got %v", trace["status"])
	}
	if trace["assigned_agent"] != "kai" {
		t.Errorf("expected agent 'kai', got %v", trace["assigned_agent"])
	}
}

func TestProcess_TaskStarted_WithPickup(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	assignedAt := time.Now().UTC().Add(-2 * time.Second)
	ms.SetTrace("task-1", map[string]any{
		"trace_id":    "task-1",
		"status":      "assigned",
		"assigned_at": assignedAt,
	})

	startedAt := time.Now().UTC()
	e := events.Event{
		EventID:   "e2",
		TraceID:   "task-1",
		Source:    "kai",
		EventType: events.TypeTaskStarted,
		Timestamp: startedAt,
		Metadata:  json.RawMessage(`{}`),
	}

	p.Process(context.Background(), e)

	trace, _ := ms.GetTrace(context.Background(), "task-1")
	if trace["status"] != "in_progress" {
		t.Errorf("expected status in_progress, got %v", trace["status"])
	}
	if trace["started_at"] != startedAt {
		t.Errorf("expected started_at to be set")
	}
	if pickup, ok := trace["time_to_pickup_ms"].(int64); ok {
		if pickup < 1500 || pickup > 3000 {
			t.Errorf("expected time_to_pickup ~2000ms, got %d", pickup)
		}
	} else {
		t.Error("expected time_to_pickup_ms to be set")
	}
}

func TestProcess_TaskCompleted_WithDuration(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	startedAt := time.Now().UTC().Add(-5 * time.Second)
	ms.SetTrace("task-1", map[string]any{
		"trace_id":   "task-1",
		"status":     "in_progress",
		"started_at": startedAt,
	})

	completedAt := time.Now().UTC()
	e := events.Event{
		EventID:   "e3",
		TraceID:   "task-1",
		Source:    "kai",
		EventType: events.TypeTaskCompleted,
		Timestamp: completedAt,
		Metadata:  json.RawMessage(`{}`),
	}

	p.Process(context.Background(), e)

	trace, _ := ms.GetTrace(context.Background(), "task-1")
	if trace["status"] != "completed" {
		t.Errorf("expected status completed, got %v", trace["status"])
	}
	if dur, ok := trace["duration_ms"].(int64); ok {
		if dur < 4500 || dur > 6000 {
			t.Errorf("expected duration ~5000ms, got %d", dur)
		}
	} else {
		t.Error("expected duration_ms to be set")
	}
}

func TestProcess_TaskFailed(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	e := events.Event{
		EventID:   "e1",
		TraceID:   "task-1",
		Source:    "kai",
		EventType: events.TypeTaskFailed,
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{"error":"out of memory"}`),
	}

	p.Process(context.Background(), e)

	trace, _ := ms.GetTrace(context.Background(), "task-1")
	if trace["status"] != "failed" {
		t.Errorf("expected status failed, got %v", trace["status"])
	}
	if trace["error"] != "out of memory" {
		t.Errorf("expected error message, got %v", trace["error"])
	}
}

func TestProcess_TaskTimeout(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	e := events.Event{
		EventID:   "e1",
		TraceID:   "task-1",
		Source:    "dispatch",
		EventType: events.TypeTaskTimeout,
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{}`),
	}

	p.Process(context.Background(), e)

	trace, _ := ms.GetTrace(context.Background(), "task-1")
	if trace["status"] != "timed_out" {
		t.Errorf("expected status timed_out, got %v", trace["status"])
	}
}

func TestProcess_UnknownEvent_StillTracked(t *testing.T) {
	ms := testutil.NewMockStore()
	p := NewProcessor(ms)

	e := events.Event{
		EventID:   "e1",
		TraceID:   "task-1",
		Source:    "scout",
		EventType: "agent.heartbeat",
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{}`),
	}

	p.Process(context.Background(), e)

	// Should still create/update the trace (for span counting).
	if ms.UpsertTraceCalls != 1 {
		t.Errorf("expected 1 upsert call for unknown event, got %d", ms.UpsertTraceCalls)
	}
}
