package store

import (
	"context"
	"time"

	"chronicle/internal/events"
)

// DataStore is the interface consumed by batcher, processors, and the API.
// The concrete implementation is *Store (pgx-backed).
type DataStore interface {
	InsertEvents(ctx context.Context, evts []events.Event) error
	UpsertTrace(ctx context.Context, traceID string, updates map[string]any) error
	GetTrace(ctx context.Context, traceID string) (map[string]any, error)
	GetTraceAssignedAt(ctx context.Context, traceID string) (*time.Time, error)
	UpsertAgentMetric(ctx context.Context, agentID string, date time.Time, updates map[string]any) error
	QueryEvents(ctx context.Context, traceID string) ([]map[string]any, error)
	QueryTraces(ctx context.Context, status string, limit int) ([]map[string]any, error)
	GetAgentMetrics(ctx context.Context, agentID string) (map[string]any, error)
	GetAllAgentMetricsSummary(ctx context.Context) ([]map[string]any, error)
	Close()
}
