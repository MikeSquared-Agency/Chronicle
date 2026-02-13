package ingester

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/MikeSquared-Agency/chronicle/internal/batcher"
	"github.com/MikeSquared-Agency/chronicle/internal/events"
	"github.com/MikeSquared-Agency/chronicle/internal/testutil"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// noopProc satisfies batcher.EventProcessor.
type noopProc struct{}

func (n *noopProc) Process(_ context.Context, _ events.Event) {}

func TestSetDLQHandler_CalledForDLQSubject(t *testing.T) {
	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProc{}, &noopProc{}, batcher.Config{
		FlushInterval:  1 * time.Hour,
		FlushThreshold: 1000,
		BufferMax:      10000,
	})

	ing := &Ingester{batcher: bat}

	var mu sync.Mutex
	var captured []string

	ing.SetDLQHandler(func(ctx context.Context, subject string, data []byte) {
		mu.Lock()
		defer mu.Unlock()
		captured = append(captured, subject)
	})

	// Simulate a DLQ message via handleMessage.
	dlqPayload, _ := json.Marshal(map[string]any{
		"event_id":   "dlq-1",
		"trace_id":   "task-dead-1",
		"source":     "dispatch",
		"event_type": "dlq.entry",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"metadata":   map[string]any{"reason": "no_capable_agent"},
	})

	msg := &fakeMsg{subject: "dlq.task.unassignable", data: dlqPayload}
	ing.handleMessage(msg)

	mu.Lock()
	defer mu.Unlock()
	if len(captured) != 1 {
		t.Fatalf("expected 1 DLQ handler call, got %d", len(captured))
	}
	if captured[0] != "dlq.task.unassignable" {
		t.Errorf("expected subject dlq.task.unassignable, got %s", captured[0])
	}
}

func TestSetDLQHandler_NotCalledForNonDLQSubject(t *testing.T) {
	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProc{}, &noopProc{}, batcher.Config{
		FlushInterval:  1 * time.Hour,
		FlushThreshold: 1000,
		BufferMax:      10000,
	})

	ing := &Ingester{batcher: bat}

	called := false
	ing.SetDLQHandler(func(ctx context.Context, subject string, data []byte) {
		called = true
	})

	payload, _ := json.Marshal(map[string]any{
		"event_id":   "e-1",
		"trace_id":   "task-1",
		"source":     "dispatch",
		"event_type": "task.created",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"metadata":   map[string]any{},
	})

	msg := &fakeMsg{subject: "swarm.task.created", data: payload}
	ing.handleMessage(msg)

	if called {
		t.Error("DLQ handler should not be called for non-dlq subjects")
	}
}

func TestHandleMessage_NilDLQHandler_NoPanic(t *testing.T) {
	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProc{}, &noopProc{}, batcher.Config{
		FlushInterval:  1 * time.Hour,
		FlushThreshold: 1000,
		BufferMax:      10000,
	})

	ing := &Ingester{batcher: bat}
	// No DLQ handler set — should not panic.

	payload, _ := json.Marshal(map[string]any{
		"event_id":   "dlq-2",
		"trace_id":   "task-dead-2",
		"source":     "dispatch",
		"event_type": "dlq.entry",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"metadata":   map[string]any{},
	})

	msg := &fakeMsg{subject: "dlq.task.unassignable", data: payload}
	ing.handleMessage(msg) // Should not panic.
}

func TestDLQHandler_ReceivesRawData(t *testing.T) {
	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProc{}, &noopProc{}, batcher.Config{
		FlushInterval:  1 * time.Hour,
		FlushThreshold: 1000,
		BufferMax:      10000,
	})

	ing := &Ingester{batcher: bat}

	var receivedData []byte
	ing.SetDLQHandler(func(ctx context.Context, subject string, data []byte) {
		receivedData = data
	})

	payload, _ := json.Marshal(map[string]any{
		"event_id":   "dlq-3",
		"trace_id":   "task-dead-3",
		"source":     "warren",
		"event_type": "dlq.entry",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"metadata":   map[string]any{"agent": "scout"},
	})

	msg := &fakeMsg{subject: "dlq.agent.boot_failure", data: payload}
	ing.handleMessage(msg)

	if receivedData == nil {
		t.Fatal("DLQ handler should have received data")
	}

	var parsed map[string]any
	if err := json.Unmarshal(receivedData, &parsed); err != nil {
		t.Fatalf("DLQ handler received invalid JSON: %v", err)
	}
	if parsed["event_id"] != "dlq-3" {
		t.Errorf("expected event_id dlq-3, got %v", parsed["event_id"])
	}
}

// fakeMsg implements jetstream.Msg for unit testing without a real NATS connection.
type fakeMsg struct {
	subject string
	data    []byte
	acked   bool
}

func (m *fakeMsg) Data() []byte                          { return m.data }
func (m *fakeMsg) Subject() string                       { return m.subject }
func (m *fakeMsg) Ack() error                            { m.acked = true; return nil }
func (m *fakeMsg) Nak() error                            { return nil }
func (m *fakeMsg) NakWithDelay(d time.Duration) error    { return nil }
func (m *fakeMsg) InProgress() error                     { return nil }
func (m *fakeMsg) Term() error                           { return nil }
func (m *fakeMsg) TermWithReason(reason string) error    { return nil }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return nil, nil
}
func (m *fakeMsg) Headers() nats.Header                  { return nil }
func (m *fakeMsg) Reply() string                         { return "" }
func (m *fakeMsg) DoubleAck(ctx context.Context) error   { return nil }
