package ingester

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/MikeSquared-Agency/chronicle/internal/batcher"
	"github.com/MikeSquared-Agency/chronicle/internal/events"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DLQHandlerFunc is called for every message on dlq.> subjects.
type DLQHandlerFunc func(ctx context.Context, subject string, data []byte)

type Ingester struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	batcher    *batcher.Batcher
	subs       []jetstream.ConsumeContext
	dlqHandler DLQHandlerFunc
	ctx        context.Context
	cancel     context.CancelFunc
}

// streamSubjects maps JetStream stream names to the subjects Chronicle subscribes to.
var streamSubjects = map[string][]string{
	"TASK_EVENTS":   {"swarm.task.>"},
	"SYSTEM_EVENTS": {"swarm.agent.>", "swarm.system.>", "swarm.lifecycle.>"},
	"DLQ":           {"dlq.>"},
}

func New(natsURL string, b *batcher.Batcher) (*Ingester, error) {
	nc, err := nats.Connect(natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			slog.Warn("NATS disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			slog.Info("NATS reconnected")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream init: %w", err)
	}

	ictx, ican := context.WithCancel(context.Background())
	ing := &Ingester{
		nc:      nc,
		js:      js,
		batcher: b,
		ctx:     ictx,
		cancel:  ican,
	}

	// Give the batcher a way to publish alerts back to NATS.
	b.SetNATSPublisher(func(subject string, data []byte) error {
		return nc.Publish(subject, data)
	})

	return ing, nil
}

// Start binds to durable consumers on each stream and begins consuming.
func (ing *Ingester) Start() error {
	ctx := context.Background()

	for stream, subjects := range streamSubjects {
		if err := ing.ensureStream(ctx, stream, subjects); err != nil {
			slog.Warn("stream not available, skipping", "stream", stream, "error", err)
			continue
		}

		consumerName := fmt.Sprintf("chronicle-%s", stream)
		if err := ing.subscribe(ctx, stream, consumerName); err != nil {
			return fmt.Errorf("subscribe to %s: %w", stream, err)
		}

		slog.Info("subscribed to stream", "stream", stream, "consumer", consumerName)
	}

	return nil
}

func (ing *Ingester) ensureStream(ctx context.Context, name string, subjects []string) error {
	// Try to get existing stream first.
	_, err := ing.js.Stream(ctx, name)
	if err == nil {
		return nil
	}

	// Create stream if it doesn't exist.
	_, err = ing.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      name,
		Subjects:  subjects,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    7 * 24 * time.Hour,
		Storage:   jetstream.FileStorage,
		Replicas:  1,
	})
	if err != nil {
		return fmt.Errorf("create stream %s: %w", name, err)
	}

	slog.Info("created stream", "name", name, "subjects", subjects)
	return nil
}

func (ing *Ingester) subscribe(ctx context.Context, stream, consumerName string) error {
	consumer, err := ing.js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    3,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("create consumer %s: %w", consumerName, err)
	}

	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		ing.handleMessage(msg)
	})
	if err != nil {
		return fmt.Errorf("consume %s: %w", consumerName, err)
	}

	ing.subs = append(ing.subs, cc)
	return nil
}

func (ing *Ingester) handleMessage(msg jetstream.Msg) {
	e, err := events.Normalize(msg.Data())
	if err != nil {
		slog.Warn("malformed event, skipping",
			"subject", msg.Subject(),
			"error", err,
		)
		// Ack to avoid redelivery of permanently broken messages.
		_ = msg.Ack()
		return
	}

	// Infer source from NATS subject if not set.
	if e.Source == "" {
		e.Source = msg.Subject()
	}

	// Fork DLQ messages to the dedicated DLQ processor.
	if strings.HasPrefix(msg.Subject(), "dlq.") && ing.dlqHandler != nil {
		ing.dlqHandler(ing.ctx, msg.Subject(), msg.Data())
	}

	ing.batcher.Add(e)

	// Ack after adding to buffer. The durable consumer will redeliver if Chronicle
	// crashes before the batch is flushed, but we accept this tradeoff for throughput.
	// The swarm_events table uses event_id as PK so duplicates are harmless (upsert/ignore).
	if err := msg.Ack(); err != nil {
		slog.Warn("failed to ack message", "subject", msg.Subject(), "error", err)
	}
}

// SetDLQHandler registers a callback for DLQ messages.
func (ing *Ingester) SetDLQHandler(fn DLQHandlerFunc) {
	ing.dlqHandler = fn
}

// NATSConn returns the underlying NATS connection (for sharing with DLQ components).
func (ing *Ingester) NATSConn() *nats.Conn {
	return ing.nc
}

// Publish sends a message to NATS (used for announcing Chronicle's own lifecycle).
func (ing *Ingester) Publish(subject string, data []byte) error {
	return ing.nc.Publish(subject, data)
}

// Close drains subscriptions and closes the NATS connection.
func (ing *Ingester) Close() {
	ing.cancel()
	for _, cc := range ing.subs {
		cc.Stop()
	}
	ing.nc.Drain()
}
