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

func TestStreamSubjects_ContainsGatewayEvents(t *testing.T) {
	subjects, ok := streamSubjects["GATEWAY_EVENTS"]
	if !ok {
		t.Fatal("GATEWAY_EVENTS missing from streamSubjects map")
	}
	if len(subjects) != 1 || subjects[0] != "swarm.gateway.>" {
		t.Errorf("expected [swarm.gateway.>], got %v", subjects)
	}
}

func TestStreamSubjects_AllExpectedStreams(t *testing.T) {
	expected := []string{"TASK_EVENTS", "SYSTEM_EVENTS", "DLQ", "SLACK_EVENTS", "GATEWAY_EVENTS"}
	for _, name := range expected {
		if _, ok := streamSubjects[name]; !ok {
			t.Errorf("stream %s missing from streamSubjects", name)
		}
	}
}

func TestHandleMessage_GatewaySessionChunk_NoHandler(t *testing.T) {
	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProc{}, &noopProc{}, batcher.Config{
		FlushInterval:  1 * time.Hour,
		FlushThreshold: 1000,
		BufferMax:      10000,
	})

	ing := &Ingester{batcher: bat}
	// No gateway handler set — falls through to Normalize/batcher.

	payload, _ := json.Marshal(map[string]any{
		"event_id":   "gw-chunk-1",
		"trace_id":   "session-whatsapp-kai",
		"source":     "nats-publisher",
		"event_type": "session.chunk",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"metadata": map[string]any{
			"session_key":   "whatsapp:+447444361435:kai",
			"chunk_index":   0,
			"message_count": 3,
			"flush_reason":  "buffer_full",
		},
	})

	msg := &fakeMsg{subject: "swarm.gateway.session.chunk", data: payload}
	ing.handleMessage(msg)

	if !msg.acked {
		t.Error("gateway session chunk message should be acked")
	}
}

func TestSetGatewayHandler_CalledForGatewaySubject(t *testing.T) {
	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProc{}, &noopProc{}, batcher.Config{
		FlushInterval:  1 * time.Hour,
		FlushThreshold: 1000,
		BufferMax:      10000,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ing := &Ingester{batcher: bat, ctx: ctx}

	var mu sync.Mutex
	var captured []string

	ing.SetGatewayHandler(func(ctx context.Context, subject string, data []byte) {
		mu.Lock()
		defer mu.Unlock()
		captured = append(captured, subject)
	})

	payload, _ := json.Marshal(map[string]any{
		"session_key":  "whatsapp:+447444361435:kai",
		"chunk_id":     "chunk-001",
		"chunk_index":  0,
		"is_final":     false,
		"messages":     []map[string]string{{"role": "user", "content": "hello"}},
		"message_count": 1,
	})

	msg := &fakeMsg{subject: "swarm.gateway.session.chunk", data: payload}
	ing.handleMessage(msg)

	mu.Lock()
	defer mu.Unlock()
	if len(captured) != 1 {
		t.Fatalf("expected 1 gateway handler call, got %d", len(captured))
	}
	if captured[0] != "swarm.gateway.session.chunk" {
		t.Errorf("expected subject swarm.gateway.session.chunk, got %s", captured[0])
	}
	if !msg.acked {
		t.Error("message should be acked after gateway handler")
	}
}

func TestSetGatewayHandler_BypassesBatcher(t *testing.T) {
	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProc{}, &noopProc{}, batcher.Config{
		FlushInterval:  50 * time.Millisecond,
		FlushThreshold: 1,
		BufferMax:      10000,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bat.Start(ctx)

	ing := &Ingester{batcher: bat, ctx: ctx}

	handlerCalled := false
	ing.SetGatewayHandler(func(ctx context.Context, subject string, data []byte) {
		handlerCalled = true
	})

	payload, _ := json.Marshal(map[string]any{
		"session_key":  "whatsapp:+447444361435:kai",
		"chunk_id":     "chunk-001",
		"chunk_index":  0,
		"is_final":     false,
		"messages":     []map[string]string{{"role": "user", "content": "hello"}},
		"message_count": 1,
	})

	msg := &fakeMsg{subject: "swarm.gateway.session.chunk", data: payload}
	ing.handleMessage(msg)

	// Wait for any potential batcher flush.
	time.Sleep(100 * time.Millisecond)

	if !handlerCalled {
		t.Error("gateway handler should have been called")
	}
	if ms.GetEventCount() != 0 {
		t.Errorf("gateway message should NOT enter batcher, but %d events found", ms.GetEventCount())
	}
}

func TestSetGatewayHandler_NotCalledForNonGatewaySubject(t *testing.T) {
	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProc{}, &noopProc{}, batcher.Config{
		FlushInterval:  1 * time.Hour,
		FlushThreshold: 1000,
		BufferMax:      10000,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ing := &Ingester{batcher: bat, ctx: ctx}

	called := false
	ing.SetGatewayHandler(func(ctx context.Context, subject string, data []byte) {
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
		t.Error("gateway handler should not be called for non-gateway subjects")
	}
}

func TestHandleMessage_NilGatewayHandler_NoPanic(t *testing.T) {
	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProc{}, &noopProc{}, batcher.Config{
		FlushInterval:  1 * time.Hour,
		FlushThreshold: 1000,
		BufferMax:      10000,
	})

	ing := &Ingester{batcher: bat}
	// No gateway handler set — should not panic, falls through to Normalize.

	payload, _ := json.Marshal(map[string]any{
		"event_id":   "gw-no-handler-1",
		"trace_id":   "session-1",
		"source":     "nats-publisher",
		"event_type": "session.chunk",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"metadata":   map[string]any{},
	})

	msg := &fakeMsg{subject: "swarm.gateway.session.chunk", data: payload}
	ing.handleMessage(msg) // Should not panic.
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
