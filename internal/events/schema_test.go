package events

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNormalize_ValidEvent(t *testing.T) {
	ts := time.Date(2026, 2, 12, 14, 30, 0, 0, time.UTC)
	raw, _ := json.Marshal(map[string]any{
		"event_id":   "abc-123",
		"trace_id":   "task-1",
		"source":     "dispatch",
		"event_type": "task.created",
		"timestamp":  ts.Format(time.RFC3339),
		"metadata":   map[string]any{"title": "Build feature"},
	})

	e, err := json.Marshal(json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}

	event, err := Normalize(e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if event.EventID != "abc-123" {
		t.Errorf("expected event_id abc-123, got %s", event.EventID)
	}
	if event.TraceID != "task-1" {
		t.Errorf("expected trace_id task-1, got %s", event.TraceID)
	}
	if event.Source != "dispatch" {
		t.Errorf("expected source dispatch, got %s", event.Source)
	}
	if event.EventType != "task.created" {
		t.Errorf("expected event_type task.created, got %s", event.EventType)
	}
}

func TestNormalize_MissingEventID(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{
		"trace_id":   "task-1",
		"source":     "dispatch",
		"event_type": "task.created",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	})

	event, err := Normalize(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if event.EventID == "" {
		t.Error("expected generated event_id, got empty string")
	}
	// Should be a valid UUID (36 chars with dashes).
	if len(event.EventID) != 36 {
		t.Errorf("expected UUID format, got %s", event.EventID)
	}
}

func TestNormalize_MissingTimestamp(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{
		"event_id":   "abc-123",
		"trace_id":   "task-1",
		"source":     "dispatch",
		"event_type": "task.created",
	})

	event, err := Normalize(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if event.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp when missing")
	}
	// Should be approximately now.
	diff := time.Since(event.Timestamp)
	if diff > 5*time.Second {
		t.Errorf("generated timestamp too far from now: %v", diff)
	}
}

func TestNormalize_MissingMetadata(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{
		"event_id":   "abc-123",
		"trace_id":   "task-1",
		"source":     "dispatch",
		"event_type": "task.created",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	})

	event, err := Normalize(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if event.Metadata == nil {
		t.Error("expected default metadata, got nil")
	}
	if string(event.Metadata) != "{}" {
		t.Errorf("expected empty JSON object, got %s", string(event.Metadata))
	}
}

func TestNormalize_InvalidJSON(t *testing.T) {
	_, err := Normalize([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestMetadataField(t *testing.T) {
	e := Event{
		Metadata: json.RawMessage(`{"title":"Build feature","priority":1}`),
	}

	if got := e.MetadataField("title"); got != "Build feature" {
		t.Errorf("expected 'Build feature', got %q", got)
	}

	if got := e.MetadataField("missing"); got != "" {
		t.Errorf("expected empty string for missing key, got %q", got)
	}

	// Non-string field should return empty.
	if got := e.MetadataField("priority"); got != "" {
		t.Errorf("expected empty string for non-string field, got %q", got)
	}
}

func TestMetadataMap(t *testing.T) {
	e := Event{
		Metadata: json.RawMessage(`{"title":"Build feature","count":5}`),
	}

	m := e.MetadataMap()
	if m == nil {
		t.Fatal("expected non-nil map")
	}
	if m["title"] != "Build feature" {
		t.Errorf("expected 'Build feature', got %v", m["title"])
	}
}

func TestMetadataMap_InvalidJSON(t *testing.T) {
	e := Event{
		Metadata: json.RawMessage(`not json`),
	}

	m := e.MetadataMap()
	if m != nil {
		t.Error("expected nil for invalid JSON metadata")
	}
}

func TestMetadataField_InvalidJSON(t *testing.T) {
	e := Event{
		Metadata: json.RawMessage(`not json`),
	}

	if got := e.MetadataField("anything"); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}
