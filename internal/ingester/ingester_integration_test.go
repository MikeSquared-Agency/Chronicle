package ingester

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/MikeSquared-Agency/chronicle/internal/batcher"
	"github.com/MikeSquared-Agency/chronicle/internal/events"
	"github.com/MikeSquared-Agency/chronicle/internal/store"
	"github.com/MikeSquared-Agency/chronicle/internal/testutil"

	"github.com/nats-io/nats.go"
)

func skipWithoutNATS(t *testing.T) string {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("NATS_URL not set, skipping integration test")
	}
	return url
}

// noopProcessor satisfies batcher.EventProcessor.
type noopProcessor struct{}

func (n *noopProcessor) Process(_ context.Context, _ events.Event) {}

func TestIntegration_IngestFromNATS(t *testing.T) {
	natsURL := skipWithoutNATS(t)

	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProcessor{}, &noopProcessor{}, batcher.Config{
		FlushInterval:  100 * time.Millisecond,
		FlushThreshold: 1,
		BufferMax:      10000,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bat.Start(ctx)

	ing, err := New(natsURL, bat)
	if err != nil {
		t.Fatalf("failed to create ingester: %v", err)
	}
	defer ing.Close()

	if err := ing.Start(); err != nil {
		t.Fatalf("failed to start ingester: %v", err)
	}

	// Publish a test event via plain NATS (JetStream will capture it).
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect to NATS: %v", err)
	}
	defer func() { _ = nc.Drain() }()

	evt, _ := json.Marshal(map[string]any{
		"event_id":   "nats-test-1",
		"trace_id":   "nats-test-trace",
		"source":     "test",
		"event_type": "task.created",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"metadata":   map[string]any{"title": "NATS integration test"},
	})

	if err := nc.Publish("swarm.task.created", evt); err != nil {
		t.Fatalf("publish: %v", err)
	}
	nc.Flush()

	// Wait for the event to be consumed and flushed.
	time.Sleep(500 * time.Millisecond)

	if ms.GetEventCount() < 1 {
		t.Errorf("expected at least 1 event, got %d", ms.GetEventCount())
	}
}

func TestIntegration_PublishAnnouncement(t *testing.T) {
	natsURL := skipWithoutNATS(t)

	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProcessor{}, &noopProcessor{}, batcher.Config{
		FlushInterval:  1 * time.Hour,
		FlushThreshold: 1000,
		BufferMax:      10000,
	})

	ing, err := New(natsURL, bat)
	if err != nil {
		t.Fatalf("failed to create ingester: %v", err)
	}
	defer ing.Close()

	data, _ := json.Marshal(map[string]any{
		"event_type": "agent.registered",
		"source":     "chronicle-test",
	})

	if err := ing.Publish("swarm.agent.chronicle.test", data); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}
}

func TestIntegration_GatewayHandlerFiredFromNATS(t *testing.T) {
	natsURL := skipWithoutNATS(t)

	ms := testutil.NewMockStore()
	bat := batcher.New(ms, &noopProcessor{}, &noopProcessor{}, batcher.Config{
		FlushInterval:  100 * time.Millisecond,
		FlushThreshold: 1,
		BufferMax:      10000,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bat.Start(ctx)

	ing, err := New(natsURL, bat)
	if err != nil {
		t.Fatalf("failed to create ingester: %v", err)
	}
	defer ing.Close()

	// Wire a gateway handler — gateway messages should go here, NOT the batcher.
	var mu sync.Mutex
	var gatewayCalls []struct {
		subject string
		data    []byte
	}

	ing.SetGatewayHandler(func(ctx context.Context, subject string, data []byte) {
		mu.Lock()
		defer mu.Unlock()
		gatewayCalls = append(gatewayCalls, struct {
			subject string
			data    []byte
		}{subject, data})
	})

	if err := ing.Start(); err != nil {
		t.Fatalf("failed to start ingester: %v", err)
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect to NATS: %v", err)
	}
	defer func() { _ = nc.Drain() }()

	evt, _ := json.Marshal(map[string]any{
		"session_key":      "whatsapp:+447444361435:kai",
		"chunk_id":         "gw-integ-1",
		"chunk_index":      0,
		"is_final":         false,
		"messages":         []map[string]string{{"role": "user", "content": "hello"}},
		"message_count":    1,
		"session_metadata": map[string]any{"channel": "whatsapp", "agent_id": "kai"},
		"flush_reason":     "idle_timeout",
	})

	if err := nc.Publish("swarm.gateway.session.chunk", evt); err != nil {
		t.Fatalf("publish: %v", err)
	}
	nc.Flush()

	// Poll for the gateway handler to be called.
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		mu.Lock()
		n := len(gatewayCalls)
		mu.Unlock()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for gateway handler call")
		case <-tick.C:
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if len(gatewayCalls) != 1 {
		t.Fatalf("expected 1 gateway handler call, got %d", len(gatewayCalls))
	}
	if gatewayCalls[0].subject != "swarm.gateway.session.chunk" {
		t.Errorf("expected subject swarm.gateway.session.chunk, got %s", gatewayCalls[0].subject)
	}

	// Verify it did NOT go through the batcher.
	if ms.GetEventCount() != 0 {
		t.Errorf("gateway message should bypass batcher, but %d events found in store", ms.GetEventCount())
	}
}

// Verify that the mock satisfies the interface at compile time.
var _ store.DataStore = (*testutil.MockStore)(nil)
