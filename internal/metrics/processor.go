package metrics

import (
	"context"
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

// Process updates agent_metrics based on event type.
func (p *Processor) Process(ctx context.Context, e events.Event) {
	switch e.EventType {
	case events.TypeTaskCompleted:
		p.handleTaskCompleted(ctx, e)
	case events.TypeTaskFailed:
		p.handleTaskFailed(ctx, e)
	case events.TypeTaskStarted:
		p.handleTaskStarted(ctx, e)
	case events.TypeAgentHeartbeat:
		p.handleHeartbeat(ctx, e)
	}
}

func (p *Processor) handleTaskCompleted(ctx context.Context, e events.Event) {
	agent := p.agentForTrace(ctx, e.TraceID)
	if agent == "" {
		return
	}

	updates := map[string]any{"inc_completed": true}

	// Get duration from the trace to update metrics.
	trace, err := p.store.GetTrace(ctx, e.TraceID)
	if err == nil {
		if dur, ok := trace["duration_ms"].(int64); ok && dur > 0 {
			// Recompute rolling average using SQL.
			// For now, store the latest duration and let SQL handle the average.
			updates["avg_duration_ms"] = dur
			updates["min_duration_ms"] = dur
			updates["max_duration_ms"] = dur
		}
	}

	if err := p.store.UpsertAgentMetric(ctx, agent, e.Timestamp, updates); err != nil {
		slog.Error("failed to update agent metrics on completion", "agent", agent, "error", err)
	}
}

func (p *Processor) handleTaskFailed(ctx context.Context, e events.Event) {
	agent := p.agentForTrace(ctx, e.TraceID)
	if agent == "" {
		return
	}

	updates := map[string]any{"inc_failed": true}
	if err := p.store.UpsertAgentMetric(ctx, agent, e.Timestamp, updates); err != nil {
		slog.Error("failed to update agent metrics on failure", "agent", agent, "error", err)
	}
}

func (p *Processor) handleTaskStarted(ctx context.Context, e events.Event) {
	agent := p.agentForTrace(ctx, e.TraceID)
	if agent == "" {
		return
	}

	// Update time-to-pickup rolling average.
	trace, err := p.store.GetTrace(ctx, e.TraceID)
	if err == nil {
		if pickup, ok := trace["time_to_pickup_ms"].(int64); ok && pickup > 0 {
			updates := map[string]any{"time_to_pickup_avg_ms": pickup}
			if err := p.store.UpsertAgentMetric(ctx, agent, e.Timestamp, updates); err != nil {
				slog.Error("failed to update pickup metric", "agent", agent, "error", err)
			}
		}
	}
}

func (p *Processor) handleHeartbeat(ctx context.Context, e events.Event) {
	agent := e.Source
	if agent == "" {
		agent = e.MetadataField("agent")
	}
	if agent == "" {
		return
	}

	m := e.MetadataMap()
	updates := map[string]any{}

	if uptime, ok := m["uptime_ms"].(float64); ok {
		updates["total_uptime_ms"] = int64(uptime)
	}
	if busy, ok := m["busy_ms"].(float64); ok {
		updates["total_busy_ms"] = int64(busy)
	}

	// Compute utilization if we have both values.
	if len(updates) == 2 {
		uptime := updates["total_uptime_ms"].(int64)
		busy := updates["total_busy_ms"].(int64)
		if uptime > 0 {
			util := float64(busy) / float64(uptime) * 100
			updates["utilization_pct"] = util
		}
	}

	if len(updates) > 0 {
		if err := p.store.UpsertAgentMetric(ctx, agent, time.Now().UTC(), updates); err != nil {
			slog.Error("failed to update heartbeat metrics", "agent", agent, "error", err)
		}
	}
}

// agentForTrace looks up which agent is assigned to a trace.
func (p *Processor) agentForTrace(ctx context.Context, traceID string) string {
	trace, err := p.store.GetTrace(ctx, traceID)
	if err != nil {
		return ""
	}
	if agent, ok := trace["assigned_agent"].(string); ok {
		return agent
	}
	return ""
}
