package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"chronicle/internal/batcher"
	"chronicle/internal/events"
	"chronicle/internal/store"
	"chronicle/internal/testutil"
)

// noopProcessor satisfies batcher.EventProcessor.
type noopProcessor struct{}

func (n *noopProcessor) Process(_ context.Context, _ events.Event) {}

func setupServer(ms store.DataStore) *Server {
	bat := batcher.New(ms, &noopProcessor{}, &noopProcessor{}, batcher.Config{
		FlushInterval:  1 * time.Hour,
		FlushThreshold: 1000,
		BufferMax:      10000,
	})
	return NewServer(ms, bat, 8700)
}

func TestHealthEndpoint(t *testing.T) {
	ms := testutil.NewMockStore()
	srv := setupServer(ms)

	req := httptest.NewRequest("GET", "/api/v1/health", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body map[string]any
	json.NewDecoder(w.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Errorf("expected status ok, got %v", body["status"])
	}
	if body["service"] != "chronicle" {
		t.Errorf("expected service chronicle, got %v", body["service"])
	}
}

func TestEventsEndpoint_MissingTraceID(t *testing.T) {
	srv := setupServer(testutil.NewMockStore())

	req := httptest.NewRequest("GET", "/api/v1/events", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestEventsEndpoint_WithTraceID(t *testing.T) {
	ms := testutil.NewMockStore()
	ms.Events = append(ms.Events, events.Event{
		EventID:   "e1",
		TraceID:   "task-1",
		Source:    "dispatch",
		EventType: "task.created",
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{}`),
	})
	srv := setupServer(ms)

	req := httptest.NewRequest("GET", "/api/v1/events?trace_id=task-1", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body []map[string]any
	json.NewDecoder(w.Body).Decode(&body)
	if len(body) != 1 {
		t.Errorf("expected 1 event, got %d", len(body))
	}
}

func TestTracesEndpoint_ListAll(t *testing.T) {
	ms := testutil.NewMockStore()
	ms.SetTrace("task-1", map[string]any{"trace_id": "task-1", "status": "completed"})
	ms.SetTrace("task-2", map[string]any{"trace_id": "task-2", "status": "pending"})
	srv := setupServer(ms)

	req := httptest.NewRequest("GET", "/api/v1/traces", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body []map[string]any
	json.NewDecoder(w.Body).Decode(&body)
	if len(body) != 2 {
		t.Errorf("expected 2 traces, got %d", len(body))
	}
}

func TestTracesEndpoint_FilterByStatus(t *testing.T) {
	ms := testutil.NewMockStore()
	ms.SetTrace("task-1", map[string]any{"trace_id": "task-1", "status": "completed"})
	ms.SetTrace("task-2", map[string]any{"trace_id": "task-2", "status": "pending"})
	srv := setupServer(ms)

	req := httptest.NewRequest("GET", "/api/v1/traces?status=pending", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body []map[string]any
	json.NewDecoder(w.Body).Decode(&body)
	if len(body) != 1 {
		t.Errorf("expected 1 pending trace, got %d", len(body))
	}
}

func TestGetTrace_Found(t *testing.T) {
	ms := testutil.NewMockStore()
	ms.SetTrace("task-1", map[string]any{"trace_id": "task-1", "status": "completed"})
	srv := setupServer(ms)

	req := httptest.NewRequest("GET", "/api/v1/traces/task-1", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body map[string]any
	json.NewDecoder(w.Body).Decode(&body)
	if body["trace_id"] != "task-1" {
		t.Errorf("expected trace_id task-1, got %v", body["trace_id"])
	}
}

func TestGetTrace_NotFound(t *testing.T) {
	srv := setupServer(testutil.NewMockStore())

	req := httptest.NewRequest("GET", "/api/v1/traces/nonexistent", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestMetricsEndpoint_Found(t *testing.T) {
	ms := testutil.NewMockStore()
	ms.Metrics["kai|2026-02-12"] = map[string]any{
		"agent_id":        "kai",
		"tasks_completed": 5,
	}
	srv := setupServer(ms)

	req := httptest.NewRequest("GET", "/api/v1/metrics/kai/latest", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestMetricsEndpoint_NotFound(t *testing.T) {
	srv := setupServer(testutil.NewMockStore())

	req := httptest.NewRequest("GET", "/api/v1/metrics/unknown/latest", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestMetricsSummary(t *testing.T) {
	ms := testutil.NewMockStore()
	ms.Metrics["kai|2026-02-12"] = map[string]any{"agent_id": "kai", "tasks_completed": 5}
	ms.Metrics["lily|2026-02-12"] = map[string]any{"agent_id": "lily", "tasks_completed": 3}
	srv := setupServer(ms)

	req := httptest.NewRequest("GET", "/api/v1/metrics/summary", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body []map[string]any
	json.NewDecoder(w.Body).Decode(&body)
	if len(body) != 2 {
		t.Errorf("expected 2 agents, got %d", len(body))
	}
}

func TestTracesEndpoint_CustomLimit(t *testing.T) {
	ms := testutil.NewMockStore()
	for i := 0; i < 10; i++ {
		ms.SetTrace(fmt.Sprintf("task-%d", i), map[string]any{
			"trace_id": fmt.Sprintf("task-%d", i),
			"status":   "completed",
		})
	}
	srv := setupServer(ms)

	req := httptest.NewRequest("GET", "/api/v1/traces?limit=3", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body []map[string]any
	json.NewDecoder(w.Body).Decode(&body)
	if len(body) > 3 {
		t.Errorf("expected at most 3 traces, got %d", len(body))
	}
}
