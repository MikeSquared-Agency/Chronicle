package ingester

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/MikeSquared-Agency/chronicle/internal/batcher"
	"github.com/MikeSquared-Agency/chronicle/internal/testutil"

	"github.com/nats-io/nats.go"
)

func TestIntegration_DLQHandlerFiredFromNATS(t *testing.T) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		t.Skip("NATS_URL not set, skipping integration test")
	}

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

	// Track DLQ handler calls.
	var mu sync.Mutex
	var dlqCalls []struct {
		subject string
		data    []byte
	}

	ing.SetDLQHandler(func(ctx context.Context, subject string, data []byte) {
		mu.Lock()
		defer mu.Unlock()
		dlqCalls = append(dlqCalls, struct {
			subject string
			data    []byte
		}{subject, data})
	})

	if err := ing.Start(); err != nil {
		t.Fatalf("failed to start ingester: %v", err)
	}

	// Publish a DLQ event via plain NATS.
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect to NATS: %v", err)
	}
	defer nc.Drain()

	dlqPayload, _ := json.Marshal(map[string]any{
		"event_id":   "dlq-int-1",
		"trace_id":   "task-dead-int-1",
		"source":     "dispatch",
		"event_type": "dlq.entry",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"metadata": map[string]any{
			"reason":  "no_capable_agent",
			"task_id": "test-task-123",
		},
	})

	if err := nc.Publish("dlq.task.unassignable", dlqPayload); err != nil {
		t.Fatalf("publish DLQ event: %v", err)
	}
	nc.Flush()

	// Also publish a non-DLQ event to verify it doesn't trigger the handler.
	normalPayload, _ := json.Marshal(map[string]any{
		"event_id":   "normal-int-1",
		"trace_id":   "task-normal-1",
		"source":     "dispatch",
		"event_type": "task.created",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"metadata":   map[string]any{"title": "Normal task"},
	})

	if err := nc.Publish("swarm.task.created", normalPayload); err != nil {
		t.Fatalf("publish normal event: %v", err)
	}
	nc.Flush()

	// Poll for the DLQ handler to be called.
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		mu.Lock()
		n := len(dlqCalls)
		mu.Unlock()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for DLQ handler call")
		case <-tick.C:
		}
	}

	mu.Lock()
	defer mu.Unlock()

	// DLQ handler should have been called exactly once (for the dlq.> message).
	if len(dlqCalls) != 1 {
		t.Fatalf("expected 1 DLQ handler call, got %d", len(dlqCalls))
	}
	if dlqCalls[0].subject != "dlq.task.unassignable" {
		t.Errorf("expected subject dlq.task.unassignable, got %s", dlqCalls[0].subject)
	}
}

func TestIntegration_DLQAlertPublished(t *testing.T) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		t.Skip("NATS_URL not set, skipping integration test")
	}

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

	// Wire DLQ handler that publishes an alert (mimics main.go wiring).
	ing.SetDLQHandler(func(ctx context.Context, subject string, data []byte) {
		alertPayload, _ := json.Marshal(map[string]any{
			"channel": "alerts",
			"type":    "dlq_entry",
			"subject": subject,
		})
		ing.Publish("swarm.alert.dlq", alertPayload)
	})

	if err := ing.Start(); err != nil {
		t.Fatalf("failed to start ingester: %v", err)
	}

	// Subscribe to alert subject to verify it gets published.
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect to NATS: %v", err)
	}
	defer nc.Drain()

	alertCh := make(chan *nats.Msg, 1)
	sub, err := nc.Subscribe("swarm.alert.dlq", func(msg *nats.Msg) {
		alertCh <- msg
	})
	if err != nil {
		t.Fatalf("subscribe to alerts: %v", err)
	}
	defer sub.Unsubscribe()

	// Publish a DLQ event.
	dlqPayload, _ := json.Marshal(map[string]any{
		"event_id":   "dlq-alert-1",
		"trace_id":   "task-dead-alert-1",
		"source":     "warren",
		"event_type": "dlq.entry",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"metadata":   map[string]any{"agent": "scout"},
	})

	if err := nc.Publish("dlq.agent.boot_failure", dlqPayload); err != nil {
		t.Fatalf("publish DLQ event: %v", err)
	}
	nc.Flush()

	// Wait for the alert to arrive.
	select {
	case msg := <-alertCh:
		var alert map[string]any
		if err := json.Unmarshal(msg.Data, &alert); err != nil {
			t.Fatalf("invalid alert JSON: %v", err)
		}
		if alert["type"] != "dlq_entry" {
			t.Errorf("expected alert type dlq_entry, got %v", alert["type"])
		}
		if alert["subject"] != "dlq.agent.boot_failure" {
			t.Errorf("expected alert subject dlq.agent.boot_failure, got %v", alert["subject"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DLQ alert")
	}
}

