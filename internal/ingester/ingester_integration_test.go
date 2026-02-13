package ingester

import (
	"context"
	"encoding/json"
	"os"
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
	defer nc.Drain()

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

// Verify that the mock satisfies the interface at compile time.
var _ store.DataStore = (*testutil.MockStore)(nil)
