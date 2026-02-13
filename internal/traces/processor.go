package traces

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/MikeSquared-Agency/chronicle/internal/events"
	"github.com/MikeSquared-Agency/chronicle/internal/store"
)

type Processor struct {
	store store.DataStore
}

func NewProcessor(s store.DataStore) *Processor {
	return &Processor{store: s}
}

// Process updates swarm_traces based on the event type.
func (p *Processor) Process(ctx context.Context, e events.Event) {
	updates := map[string]any{}

	switch e.EventType {
	case events.TypeTaskCreated:
		updates["status"] = "pending"
		if title := e.MetadataField("title"); title != "" {
			updates["task_title"] = title
		}
		if owner := e.MetadataField("owner"); owner != "" {
			updates["owner"] = owner
		}
		if scoring := e.MetadataField("scoring_breakdown"); scoring != "" {
			updates["scoring_breakdown"] = json.RawMessage(scoring)
		}

	case events.TypeTaskAssigned:
		updates["status"] = "assigned"
		if agent := e.MetadataField("agent"); agent != "" {
			updates["assigned_agent"] = agent
		}

	case events.TypeTaskStarted:
		updates["status"] = "in_progress"
		updates["started_at"] = e.Timestamp

		// Compute time-to-pickup if we have the assigned_at time.
		assignedAt, err := p.store.GetTraceAssignedAt(ctx, e.TraceID)
		if err == nil && assignedAt != nil {
			pickup := e.Timestamp.Sub(*assignedAt).Milliseconds()
			updates["time_to_pickup_ms"] = pickup
		}

	case events.TypeTaskCompleted:
		updates["status"] = "completed"
		updates["completed_at"] = e.Timestamp

		// Compute duration from the trace's started_at.
		trace, err := p.store.GetTrace(ctx, e.TraceID)
		if err == nil {
			if startedAt, ok := trace["started_at"].(time.Time); ok {
				dur := e.Timestamp.Sub(startedAt).Milliseconds()
				updates["duration_ms"] = dur
			}
		}

	case events.TypeTaskFailed:
		updates["status"] = "failed"
		updates["completed_at"] = e.Timestamp
		if errMsg := e.MetadataField("error"); errMsg != "" {
			updates["error"] = errMsg
		}

		trace, err := p.store.GetTrace(ctx, e.TraceID)
		if err == nil {
			if startedAt, ok := trace["started_at"].(time.Time); ok {
				dur := e.Timestamp.Sub(startedAt).Milliseconds()
				updates["duration_ms"] = dur
			}
		}

	case events.TypeTaskTimeout:
		updates["status"] = "timed_out"
		updates["completed_at"] = e.Timestamp

	default:
		// For non-task events, still track in the trace for span counting.
	}

	if err := p.store.UpsertTrace(ctx, e.TraceID, updates); err != nil {
		slog.Error("failed to upsert trace", "trace_id", e.TraceID, "error", err)
	}
}
