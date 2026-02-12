package events

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	EventID   string          `json:"event_id"`
	TraceID   string          `json:"trace_id"`
	Source    string          `json:"source"`
	EventType string          `json:"event_type"`
	Timestamp time.Time       `json:"timestamp"`
	Metadata  json.RawMessage `json:"metadata"`
}

// Normalize fills in missing fields with sensible defaults.
// It never drops an event — always returns a usable Event.
func Normalize(raw []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return Event{}, err
	}

	if e.EventID == "" {
		e.EventID = uuid.New().String()
	}

	if e.Timestamp.IsZero() {
		slog.Warn("event missing timestamp, using ingestion time", "event_id", e.EventID)
		e.Timestamp = time.Now().UTC()
	}

	if e.Metadata == nil {
		e.Metadata = json.RawMessage(`{}`)
	}

	return e, nil
}

// Known event type prefixes for routing to processors.
const (
	TypeTaskCreated   = "task.created"
	TypeTaskAssigned  = "task.assigned"
	TypeTaskStarted   = "task.started"
	TypeTaskCompleted = "task.completed"
	TypeTaskFailed    = "task.failed"
	TypeTaskTimeout   = "task.timeout"

	TypeAgentWake       = "agent.wake"
	TypeAgentSleep      = "agent.sleep"
	TypeAgentHeartbeat  = "agent.heartbeat"
	TypeAgentRegistered = "agent.registered"

	TypeSystemError = "system.error"
	TypeDLQEntry    = "dlq.entry"
)

// MetadataField extracts a string field from the metadata JSON.
func (e *Event) MetadataField(key string) string {
	var m map[string]any
	if err := json.Unmarshal(e.Metadata, &m); err != nil {
		return ""
	}
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// MetadataMap returns metadata as a generic map.
func (e *Event) MetadataMap() map[string]any {
	var m map[string]any
	if err := json.Unmarshal(e.Metadata, &m); err != nil {
		return nil
	}
	return m
}
