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
	"chronicle/internal/testutil"

	dlq "github.com/DarlingtonDeveloper/swarm-dlq"
)

// mockNATSPublisher records published messages.
type mockNATSPublisher struct {
	published []struct {
		subject string
		data    []byte
	}
}

func (m *mockNATSPublisher) Publish(subject string, data []byte) error {
	m.published = append(m.published, struct {
		subject string
		data    []byte
	}{subject, data})
	return nil
}

func setupServerWithDLQ(dlqStore dlq.DataStore) *Server {
	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProcessor{}, &noopProcessor{}, batcher.Config{
		FlushInterval:  1 * time.Hour,
		FlushThreshold: 1000,
		BufferMax:      10000,
	})
	nc := &mockNATSPublisher{}
	handler := dlq.NewHandler(dlqStore, nc)
	return NewServer(ms, bat, 8700, handler.Routes())
}

func TestDLQ_ListEmpty(t *testing.T) {
	store := newMockDLQStore()
	srv := setupServerWithDLQ(store)

	req := httptest.NewRequest("GET", "/api/v1/dlq", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body []any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(body) != 0 {
		t.Errorf("expected empty list, got %d items", len(body))
	}
}

func TestDLQ_ListWithEntries(t *testing.T) {
	store := newMockDLQStore()
	store.entries["dlq-1"] = &dlq.Entry{
		DLQID:           "dlq-1",
		OriginalSubject: "swarm.task.request",
		OriginalPayload: json.RawMessage(`{"task_id":"t1"}`),
		Reason:          "no_capable_agent",
		Source:          "dispatch",
		FailedAt:        time.Now().UTC(),
		Recoverable:     true,
		RetryHistory:    []dlq.RetryAttempt{},
	}
	srv := setupServerWithDLQ(store)

	req := httptest.NewRequest("GET", "/api/v1/dlq", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body []map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(body))
	}
	if body[0]["dlq_id"] != "dlq-1" {
		t.Errorf("expected dlq_id dlq-1, got %v", body[0]["dlq_id"])
	}
}

func TestDLQ_GetNotFound(t *testing.T) {
	store := newMockDLQStore()
	srv := setupServerWithDLQ(store)

	req := httptest.NewRequest("GET", "/api/v1/dlq/nonexistent", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestDLQ_GetFound(t *testing.T) {
	store := newMockDLQStore()
	store.entries["dlq-1"] = &dlq.Entry{
		DLQID:           "dlq-1",
		OriginalSubject: "swarm.task.request",
		OriginalPayload: json.RawMessage(`{"task_id":"t1"}`),
		Reason:          "no_capable_agent",
		Source:          "dispatch",
		FailedAt:        time.Now().UTC(),
		RetryHistory:    []dlq.RetryAttempt{},
	}
	srv := setupServerWithDLQ(store)

	req := httptest.NewRequest("GET", "/api/v1/dlq/dlq-1", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["reason"] != "no_capable_agent" {
		t.Errorf("expected reason no_capable_agent, got %v", body["reason"])
	}
}

func TestDLQ_Stats(t *testing.T) {
	store := newMockDLQStore()
	store.entries["dlq-1"] = &dlq.Entry{
		DLQID:        "dlq-1",
		Reason:       "no_capable_agent",
		Source:       "dispatch",
		Recoverable:  true,
		RetryHistory: []dlq.RetryAttempt{},
	}
	srv := setupServerWithDLQ(store)

	req := httptest.NewRequest("GET", "/api/v1/dlq/stats", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["total"] != float64(1) {
		t.Errorf("expected total 1, got %v", body["total"])
	}
}

func TestDLQ_RetryPublishes(t *testing.T) {
	store := newMockDLQStore()
	store.entries["dlq-1"] = &dlq.Entry{
		DLQID:           "dlq-1",
		OriginalSubject: "swarm.task.request",
		OriginalPayload: json.RawMessage(`{"task_id":"t1"}`),
		Reason:          "no_capable_agent",
		Source:          "dispatch",
		Recoverable:     true,
		RetryHistory:    []dlq.RetryAttempt{},
	}

	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProcessor{}, &noopProcessor{}, batcher.Config{
		FlushInterval:  1 * time.Hour,
		FlushThreshold: 1000,
		BufferMax:      10000,
	})
	nc := &mockNATSPublisher{}
	handler := dlq.NewHandler(store, nc)
	srv := NewServer(ms, bat, 8700, handler.Routes())

	req := httptest.NewRequest("POST", "/api/v1/dlq/dlq-1/retry", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	if len(nc.published) != 1 {
		t.Fatalf("expected 1 NATS publish, got %d", len(nc.published))
	}
	if nc.published[0].subject != "swarm.task.request" {
		t.Errorf("expected publish to swarm.task.request, got %s", nc.published[0].subject)
	}

	// Entry should be marked recovered.
	entry := store.entries["dlq-1"]
	if !entry.Recovered {
		t.Error("expected entry to be marked recovered")
	}
}

func TestDLQ_DiscardMarksRecovered(t *testing.T) {
	store := newMockDLQStore()
	store.entries["dlq-1"] = &dlq.Entry{
		DLQID:           "dlq-1",
		OriginalSubject: "swarm.task.request",
		OriginalPayload: json.RawMessage(`{"task_id":"t1"}`),
		Reason:          "policy_denied",
		Source:          "dispatch",
		Recoverable:     false,
		RetryHistory:    []dlq.RetryAttempt{},
	}
	srv := setupServerWithDLQ(store)

	req := httptest.NewRequest("POST", "/api/v1/dlq/dlq-1/discard", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	entry := store.entries["dlq-1"]
	if !entry.Recovered {
		t.Error("expected entry to be marked recovered after discard")
	}
}

// --- Mock DLQ DataStore ---

type mockDLQStore struct {
	entries map[string]*dlq.Entry
}

func newMockDLQStore() *mockDLQStore {
	return &mockDLQStore{entries: make(map[string]*dlq.Entry)}
}

func (m *mockDLQStore) Insert(_ context.Context, e dlq.Entry) error {
	m.entries[e.DLQID] = &e
	return nil
}

func (m *mockDLQStore) Get(_ context.Context, dlqID string) (*dlq.Entry, error) {
	e, ok := m.entries[dlqID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return e, nil
}

func (m *mockDLQStore) List(_ context.Context, opts dlq.ListOpts) ([]dlq.Entry, error) {
	var results []dlq.Entry
	for _, e := range m.entries {
		if opts.Recovered != nil && e.Recovered != *opts.Recovered {
			continue
		}
		if opts.Reason != "" && e.Reason != opts.Reason {
			continue
		}
		if opts.Source != "" && e.Source != opts.Source {
			continue
		}
		results = append(results, *e)
	}
	return results, nil
}

func (m *mockDLQStore) MarkRecovered(_ context.Context, dlqID, recoveredBy string) error {
	e, ok := m.entries[dlqID]
	if !ok {
		return fmt.Errorf("not found")
	}
	if e.Recovered {
		return fmt.Errorf("already recovered")
	}
	now := time.Now().UTC()
	e.Recovered = true
	e.RecoveredAt = &now
	e.RecoveredBy = recoveredBy
	return nil
}

func (m *mockDLQStore) ListRecoverable(_ context.Context) ([]dlq.Entry, error) {
	var results []dlq.Entry
	for _, e := range m.entries {
		if e.Recoverable && !e.Recovered {
			results = append(results, *e)
		}
	}
	return results, nil
}

func (m *mockDLQStore) Stats(_ context.Context) (*dlq.Stats, error) {
	st := &dlq.Stats{
		ByReason: make(map[string]int),
		BySource: make(map[string]int),
	}
	for _, e := range m.entries {
		st.Total++
		if !e.Recovered {
			st.Unrecovered++
			st.ByReason[e.Reason]++
			st.BySource[e.Source]++
			if e.Recoverable {
				st.Recoverable++
			}
		}
	}
	return st, nil
}
