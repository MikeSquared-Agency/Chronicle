package batcher

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/MikeSquared-Agency/chronicle/internal/events"
	"github.com/MikeSquared-Agency/chronicle/internal/store"
)

// EventProcessor processes a single event (used for trace and metrics processors).
type EventProcessor interface {
	Process(ctx context.Context, e events.Event)
}

type Batcher struct {
	store           store.DataStore
	traceProc       EventProcessor
	metricsProc     EventProcessor
	flushInterval   time.Duration
	flushThreshold  int
	bufferMax       int

	mu              sync.Mutex
	buffer          []events.Event
	consecutiveFail int
	natsPublish     func(subject string, data []byte) error

	done chan struct{}
}

type Config struct {
	FlushInterval  time.Duration
	FlushThreshold int
	BufferMax      int
}

func New(s store.DataStore, tp EventProcessor, mp EventProcessor, cfg Config) *Batcher {
	return &Batcher{
		store:          s,
		traceProc:      tp,
		metricsProc:    mp,
		flushInterval:  cfg.FlushInterval,
		flushThreshold: cfg.FlushThreshold,
		bufferMax:      cfg.BufferMax,
		buffer:         make([]events.Event, 0, cfg.FlushThreshold),
		done:           make(chan struct{}),
	}
}

// SetNATSPublisher sets the function used to publish system alerts back to NATS.
func (b *Batcher) SetNATSPublisher(fn func(subject string, data []byte) error) {
	b.natsPublish = fn
}

// Add enqueues a normalized event for batched writing.
func (b *Batcher) Add(e events.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Backpressure: drop oldest if buffer full.
	if len(b.buffer) >= b.bufferMax {
		dropped := len(b.buffer) - b.bufferMax + 1
		b.buffer = b.buffer[dropped:]
		slog.Warn("buffer overflow, dropping oldest events", "dropped", dropped, "buffer_size", b.bufferMax)
		b.publishAlert("swarm.system.chronicle.buffer_overflow", []byte(`{"message":"buffer overflow, dropping events"}`))
	}

	b.buffer = append(b.buffer, e)

	// Flush immediately if threshold reached.
	if len(b.buffer) >= b.flushThreshold {
		go b.flush()
	}
}

// Start begins the periodic flush ticker.
func (b *Batcher) Start(ctx context.Context) {
	ticker := time.NewTicker(b.flushInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				b.flush()
			case <-ctx.Done():
				// Final flush on shutdown.
				b.flush()
				close(b.done)
				return
			}
		}
	}()
}

// Wait blocks until the batcher has completed its final flush.
func (b *Batcher) Wait() {
	<-b.done
}

// BufferLen returns the current buffer size (for health checks).
func (b *Batcher) BufferLen() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buffer)
}

func (b *Batcher) flush() {
	b.mu.Lock()
	if len(b.buffer) == 0 {
		b.mu.Unlock()
		return
	}
	batch := b.buffer
	b.buffer = make([]events.Event, 0, b.flushThreshold)
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	slog.Info("flushing batch", "count", len(batch))

	// Step 1: Write raw events.
	if err := b.store.InsertEvents(ctx, batch); err != nil {
		slog.Error("failed to insert events", "error", err, "count", len(batch))
		b.handleWriteFailure(batch)
		return
	}

	// Reset failure counter on success.
	b.consecutiveFail = 0

	// Step 2: Update traces.
	for _, e := range batch {
		b.traceProc.Process(ctx, e)
	}

	// Step 3: Update agent metrics.
	for _, e := range batch {
		b.metricsProc.Process(ctx, e)
	}

	slog.Info("batch flushed successfully", "count", len(batch))
}

func (b *Batcher) handleWriteFailure(batch []events.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.consecutiveFail++

	// Re-queue the failed batch (prepend so order is maintained).
	b.buffer = append(batch, b.buffer...)

	// Trim if re-queueing caused overflow.
	if len(b.buffer) > b.bufferMax {
		b.buffer = b.buffer[len(b.buffer)-b.bufferMax:]
	}

	if b.consecutiveFail >= 3 {
		slog.Error("3 consecutive write failures", "buffer_size", len(b.buffer))
		b.publishAlert("swarm.system.chronicle.write_failure", []byte(`{"message":"3 consecutive Supabase write failures"}`))
	}
}

func (b *Batcher) publishAlert(subject string, data []byte) {
	if b.natsPublish != nil {
		if err := b.natsPublish(subject, data); err != nil {
			slog.Error("failed to publish alert", "subject", subject, "error", err)
		}
	}
}
