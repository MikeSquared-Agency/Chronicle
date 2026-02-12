package testutil

import (
	"context"
	"fmt"
	"sync"
	"time"

	"chronicle/internal/events"
)

// MockStore is a thread-safe in-memory implementation of store.DataStore for testing.
type MockStore struct {
	mu sync.Mutex

	Events  []events.Event
	Traces  map[string]map[string]any
	Metrics map[string]map[string]any // key: "agentID|date"

	InsertErr       error
	UpsertTraceErr  error
	UpsertMetricErr error

	InsertCalls      int
	UpsertTraceCalls int
	UpsertMetricCalls int
}

func NewMockStore() *MockStore {
	return &MockStore{
		Events:  make([]events.Event, 0),
		Traces:  make(map[string]map[string]any),
		Metrics: make(map[string]map[string]any),
	}
}

func (m *MockStore) InsertEvents(_ context.Context, evts []events.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.InsertCalls++
	if m.InsertErr != nil {
		return m.InsertErr
	}
	m.Events = append(m.Events, evts...)
	return nil
}

func (m *MockStore) UpsertTrace(_ context.Context, traceID string, updates map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.UpsertTraceCalls++
	if m.UpsertTraceErr != nil {
		return m.UpsertTraceErr
	}
	if m.Traces[traceID] == nil {
		m.Traces[traceID] = map[string]any{"trace_id": traceID, "status": "pending"}
	}
	for k, v := range updates {
		m.Traces[traceID][k] = v
	}
	return nil
}

func (m *MockStore) GetTrace(_ context.Context, traceID string) (map[string]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.Traces[traceID]
	if !ok {
		return nil, fmt.Errorf("trace %s not found", traceID)
	}
	// Return a copy.
	cp := make(map[string]any, len(t))
	for k, v := range t {
		cp[k] = v
	}
	return cp, nil
}

func (m *MockStore) GetTraceAssignedAt(_ context.Context, traceID string) (*time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.Traces[traceID]
	if !ok {
		return nil, fmt.Errorf("trace %s not found", traceID)
	}
	if at, ok := t["assigned_at"].(time.Time); ok {
		return &at, nil
	}
	return nil, fmt.Errorf("no assigned_at for trace %s", traceID)
}

func (m *MockStore) UpsertAgentMetric(_ context.Context, agentID string, date time.Time, updates map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.UpsertMetricCalls++
	if m.UpsertMetricErr != nil {
		return m.UpsertMetricErr
	}
	key := agentID + "|" + date.Format("2006-01-02")
	if m.Metrics[key] == nil {
		m.Metrics[key] = map[string]any{"agent_id": agentID}
	}
	for k, v := range updates {
		m.Metrics[key][k] = v
	}
	return nil
}

func (m *MockStore) QueryEvents(_ context.Context, traceID string) ([]map[string]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var results []map[string]any
	for _, e := range m.Events {
		if e.TraceID == traceID {
			results = append(results, map[string]any{
				"event_id":   e.EventID,
				"trace_id":   e.TraceID,
				"source":     e.Source,
				"event_type": e.EventType,
				"timestamp":  e.Timestamp,
				"metadata":   e.Metadata,
			})
		}
	}
	return results, nil
}

func (m *MockStore) QueryTraces(_ context.Context, status string, limit int) ([]map[string]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var results []map[string]any
	for _, t := range m.Traces {
		if status != "" {
			if s, ok := t["status"].(string); ok && s != status {
				continue
			}
		}
		results = append(results, t)
		if limit > 0 && len(results) >= limit {
			break
		}
	}
	return results, nil
}

func (m *MockStore) GetAgentMetrics(_ context.Context, agentID string) (map[string]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.Metrics {
		if aid, ok := v["agent_id"].(string); ok && aid == agentID {
			_ = k
			return v, nil
		}
	}
	return nil, fmt.Errorf("metrics not found for %s", agentID)
}

func (m *MockStore) GetAllAgentMetricsSummary(_ context.Context) ([]map[string]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var results []map[string]any
	for _, v := range m.Metrics {
		results = append(results, v)
	}
	return results, nil
}

func (m *MockStore) Close() {}

// SetTrace seeds a trace for testing.
func (m *MockStore) SetTrace(traceID string, data map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Traces[traceID] = data
}

// GetInsertCalls returns how many times InsertEvents was called.
func (m *MockStore) GetInsertCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.InsertCalls
}

// GetEventCount returns total events stored.
func (m *MockStore) GetEventCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.Events)
}
