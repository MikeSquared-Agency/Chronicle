package batcher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/MikeSquared-Agency/chronicle/internal/events"
	"github.com/MikeSquared-Agency/chronicle/internal/testutil"
)

func makeEvent(id, traceID, eventType string) events.Event {
	return events.Event{
		EventID:   id,
		TraceID:   traceID,
		Source:    "test",
		EventType: eventType,
		Timestamp: time.Now().UTC(),
		Metadata:  json.RawMessage(`{}`),
	}
}

// noopProcessor is an EventProcessor that does nothing.
type noopProcessor struct{}

func (n *noopProcessor) Process(_ context.Context, _ events.Event) {}

func newTestBatcher(ms *testutil.MockStore, threshold, bufMax int) *Batcher {
	return New(ms, &noopProcessor{}, &noopProcessor{}, Config{
		FlushInterval:  1 * time.Hour, // long interval so we control flush manually
		FlushThreshold: threshold,
		BufferMax:      bufMax,
	})
}

func TestAdd_BuffersEvents(t *testing.T) {
	ms := testutil.NewMockStore()
	b := newTestBatcher(ms, 1000, 10000) // high threshold so no auto-flush

	b.Add(makeEvent("1", "t1", "task.created"))
	b.Add(makeEvent("2", "t1", "task.assigned"))

	if b.BufferLen() != 2 {
		t.Errorf("expected buffer length 2, got %d", b.BufferLen())
	}

	if ms.GetInsertCalls() != 0 {
		t.Errorf("expected 0 insert calls before flush, got %d", ms.GetInsertCalls())
	}
}

func TestFlush_WritesAndClearsBuffer(t *testing.T) {
	ms := testutil.NewMockStore()
	b := newTestBatcher(ms, 1000, 10000)

	b.Add(makeEvent("1", "t1", "task.created"))
	b.Add(makeEvent("2", "t1", "task.assigned"))
	b.flush()

	if b.BufferLen() != 0 {
		t.Errorf("expected empty buffer after flush, got %d", b.BufferLen())
	}
	if ms.GetInsertCalls() != 1 {
		t.Errorf("expected 1 insert call, got %d", ms.GetInsertCalls())
	}
	if ms.GetEventCount() != 2 {
		t.Errorf("expected 2 events stored, got %d", ms.GetEventCount())
	}
}

func TestFlush_EmptyBufferIsNoop(t *testing.T) {
	ms := testutil.NewMockStore()
	b := newTestBatcher(ms, 1000, 10000)

	b.flush()
	if ms.GetInsertCalls() != 0 {
		t.Errorf("expected 0 insert calls on empty buffer, got %d", ms.GetInsertCalls())
	}
}

func TestThreshold_TriggersFlush(t *testing.T) {
	ms := testutil.NewMockStore()
	threshold := 5
	b := newTestBatcher(ms, threshold, 10000)

	for i := 0; i < threshold; i++ {
		b.Add(makeEvent(fmt.Sprintf("%d", i), "t1", "task.created"))
	}

	// The threshold-triggered flush runs in a goroutine. Wait briefly.
	time.Sleep(100 * time.Millisecond)

	if ms.GetInsertCalls() < 1 {
		t.Errorf("expected at least 1 insert call after reaching threshold, got %d", ms.GetInsertCalls())
	}
}

func TestBackpressure_DropsOldestEvents(t *testing.T) {
	ms := testutil.NewMockStore()
	ms.InsertErr = fmt.Errorf("db down") // prevent auto-flush from clearing buffer
	bufMax := 10
	b := newTestBatcher(ms, 1000, bufMax)

	// Fill buffer beyond capacity.
	for i := 0; i < bufMax+5; i++ {
		b.Add(makeEvent(fmt.Sprintf("evt-%d", i), "t1", "task.created"))
	}

	// Buffer should be capped at bufMax.
	if b.BufferLen() > bufMax {
		t.Errorf("expected buffer <= %d, got %d", bufMax, b.BufferLen())
	}
}

func TestWriteFailure_RequeueBatch(t *testing.T) {
	ms := testutil.NewMockStore()
	ms.InsertErr = fmt.Errorf("connection refused")
	b := newTestBatcher(ms, 1000, 10000)

	b.Add(makeEvent("1", "t1", "task.created"))
	b.Add(makeEvent("2", "t1", "task.assigned"))
	b.flush()

	// Events should be re-queued.
	if b.BufferLen() != 2 {
		t.Errorf("expected 2 events re-queued, got %d", b.BufferLen())
	}
}

func TestConsecutiveFailures_AlertsAfterThree(t *testing.T) {
	ms := testutil.NewMockStore()
	ms.InsertErr = fmt.Errorf("connection refused")
	b := newTestBatcher(ms, 1000, 10000)

	var alerts []string
	var mu sync.Mutex
	b.SetNATSPublisher(func(subject string, _ []byte) error {
		mu.Lock()
		alerts = append(alerts, subject)
		mu.Unlock()
		return nil
	})

	// Fail 3 times.
	for i := 0; i < 3; i++ {
		b.Add(makeEvent(fmt.Sprintf("%d", i), "t1", "task.created"))
		b.flush()
	}

	mu.Lock()
	defer mu.Unlock()

	found := false
	for _, a := range alerts {
		if a == "swarm.system.chronicle.write_failure" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected write_failure alert after 3 consecutive failures, got alerts: %v", alerts)
	}
}

func TestConsecutiveFailures_ResetsOnSuccess(t *testing.T) {
	ms := testutil.NewMockStore()
	ms.InsertErr = fmt.Errorf("connection refused")
	b := newTestBatcher(ms, 1000, 10000)

	// Fail twice.
	b.Add(makeEvent("1", "t1", "task.created"))
	b.flush()
	// Clear re-queued events.
	b.mu.Lock()
	b.buffer = b.buffer[:0]
	b.mu.Unlock()

	b.Add(makeEvent("2", "t1", "task.created"))
	b.flush()
	b.mu.Lock()
	b.buffer = b.buffer[:0]
	b.mu.Unlock()

	// Now succeed.
	ms.InsertErr = nil
	b.Add(makeEvent("3", "t1", "task.created"))
	b.flush()

	b.mu.Lock()
	cf := b.consecutiveFail
	b.mu.Unlock()

	if cf != 0 {
		t.Errorf("expected consecutiveFail reset to 0, got %d", cf)
	}
}

func TestStartAndShutdown(t *testing.T) {
	ms := testutil.NewMockStore()
	b := newTestBatcher(ms, 1000, 10000)
	b.flushInterval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)

	b.Add(makeEvent("1", "t1", "task.created"))

	// Let the ticker fire at least once.
	time.Sleep(150 * time.Millisecond)

	cancel()
	b.Wait()

	// After shutdown, buffer should be empty (final flush).
	if b.BufferLen() != 0 {
		t.Errorf("expected empty buffer after shutdown, got %d", b.BufferLen())
	}
}

func TestConcurrentAdds(t *testing.T) {
	ms := testutil.NewMockStore()
	b := newTestBatcher(ms, 1000, 100000)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			b.Add(makeEvent(fmt.Sprintf("evt-%d", n), "t1", "task.created"))
		}(i)
	}
	wg.Wait()

	if b.BufferLen() != 100 {
		t.Errorf("expected 100 events, got %d", b.BufferLen())
	}
}
