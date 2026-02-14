package transcript

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// mockStore is an in-memory implementation of TranscriptStore for testing.
type mockStore struct {
	mu          sync.Mutex
	chunks      map[string][]ChunkRowData // keyed by session_key
	transcripts map[string]bool           // keyed by session_key
	insertedTranscripts []insertedTranscript
}

type insertedTranscript struct {
	SessionID  string
	SessionKey string
	SessionRef string
	OwnerUUID  *string
	Title      string
	Surface    string
	Transcript string
	MsgCount   int
	ChunkCount int
	Duration   string
}

func newMockStore() *mockStore {
	return &mockStore{
		chunks:      make(map[string][]ChunkRowData),
		transcripts: make(map[string]bool),
	}
}

func (m *mockStore) InsertChunk(_ context.Context, chunkID, sessionKey string, chunkIndex int, isFinal bool, messages json.RawMessage, messageCount int, sessionMetadata json.RawMessage, flushReason string, flushedAt *time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chunks[sessionKey] = append(m.chunks[sessionKey], ChunkRowData{
		ChunkID:         chunkID,
		SessionKey:      sessionKey,
		ChunkIndex:      chunkIndex,
		IsFinal:         isFinal,
		Messages:        messages,
		MessageCount:    messageCount,
		SessionMetadata: sessionMetadata,
		FlushReason:     flushReason,
		FlushedAt:       flushedAt,
		CreatedAt:       time.Now(),
	})
	return nil
}

func (m *mockStore) GetChunksForSession(_ context.Context, sessionKey string) ([]ChunkRowData, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.chunks[sessionKey], nil
}

func (m *mockStore) InsertTranscript(_ context.Context, sessionID, sessionKey, sessionRef string, ownerUUID *string, title, surface, transcript string, messageCount, chunkCount int, duration string, _, _ *time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.transcripts[sessionKey] {
		return nil // ON CONFLICT DO NOTHING
	}
	m.transcripts[sessionKey] = true
	m.insertedTranscripts = append(m.insertedTranscripts, insertedTranscript{
		SessionID:  sessionID,
		SessionKey: sessionKey,
		SessionRef: sessionRef,
		OwnerUUID:  ownerUUID,
		Title:      title,
		Surface:    surface,
		Transcript: transcript,
		MsgCount:   messageCount,
		ChunkCount: chunkCount,
		Duration:   duration,
	})
	return nil
}

func (m *mockStore) TranscriptExists(_ context.Context, sessionKey string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.transcripts[sessionKey], nil
}

func TestHandleChunk_StoresChunk(t *testing.T) {
	store := newMockStore()
	var published [][]byte
	assembler := NewAssembler(store, func(subject string, data []byte) error {
		published = append(published, data)
		return nil
	}, "default-owner-uuid")

	chunk := GatewayChunk{
		SessionKey:   "whatsapp:+447444361435:kai",
		ChunkID:      "chunk-001",
		ChunkIndex:   0,
		IsFinal:      false,
		Messages:     []ChatMessage{{Role: "user", Content: "hello"}},
		MessageCount: 1,
		SessionMetadata: SessionMeta{
			Channel: "whatsapp",
			AgentID: "kai",
		},
		FlushReason: "buffer_full",
	}
	data, _ := json.Marshal(chunk)

	assembler.HandleChunk(context.Background(), "swarm.gateway.session.chunk", data)

	// Chunk should be stored.
	chunks := store.chunks["whatsapp:+447444361435:kai"]
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk stored, got %d", len(chunks))
	}
	if chunks[0].ChunkIndex != 0 {
		t.Errorf("expected chunk_index 0, got %d", chunks[0].ChunkIndex)
	}
	if chunks[0].IsFinal {
		t.Error("expected is_final=false")
	}

	// No transcript should be assembled (not final).
	if len(store.insertedTranscripts) != 0 {
		t.Error("expected no transcript for non-final chunk")
	}
	if len(published) != 0 {
		t.Error("expected no NATS publish for non-final chunk")
	}
}

func TestHandleChunk_FinalTriggersAssembly(t *testing.T) {
	store := newMockStore()
	var publishedSubjects []string
	var publishedPayloads [][]byte
	assembler := NewAssembler(store, func(subject string, data []byte) error {
		publishedSubjects = append(publishedSubjects, subject)
		publishedPayloads = append(publishedPayloads, data)
		return nil
	}, "default-owner-uuid")

	t0 := time.Date(2026, 2, 14, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 2, 14, 10, 5, 0, 0, time.UTC)

	// Chunk 0: non-final.
	chunk0 := GatewayChunk{
		SessionKey:   "whatsapp:+447444361435:kai",
		ChunkID:      "chunk-001",
		ChunkIndex:   0,
		IsFinal:      false,
		Messages:     []ChatMessage{{Role: "user", Content: "hello"}},
		MessageCount: 1,
		SessionMetadata: SessionMeta{
			Channel: "whatsapp",
			AgentID: "kai",
		},
		FlushedAt:   &t0,
		FlushReason: "buffer_full",
	}
	data0, _ := json.Marshal(chunk0)
	assembler.HandleChunk(context.Background(), "swarm.gateway.session.chunk", data0)

	// Chunk 1: final.
	chunk1 := GatewayChunk{
		SessionKey:   "whatsapp:+447444361435:kai",
		ChunkID:      "chunk-002",
		ChunkIndex:   1,
		IsFinal:      true,
		Messages:     []ChatMessage{{Role: "assistant", Content: "hi there"}},
		MessageCount: 1,
		SessionMetadata: SessionMeta{
			Channel: "whatsapp",
			AgentID: "kai",
		},
		FlushedAt:   &t1,
		FlushReason: "session_stop",
	}
	data1, _ := json.Marshal(chunk1)
	assembler.HandleChunk(context.Background(), "swarm.gateway.session.chunk", data1)

	// Transcript should be assembled.
	if len(store.insertedTranscripts) != 1 {
		t.Fatalf("expected 1 transcript, got %d", len(store.insertedTranscripts))
	}

	tr := store.insertedTranscripts[0]
	if tr.SessionKey != "whatsapp:+447444361435:kai" {
		t.Errorf("unexpected session_key: %s", tr.SessionKey)
	}
	if tr.MsgCount != 2 {
		t.Errorf("expected 2 messages, got %d", tr.MsgCount)
	}
	if tr.ChunkCount != 2 {
		t.Errorf("expected 2 chunks, got %d", tr.ChunkCount)
	}
	if tr.Surface != "whatsapp" {
		t.Errorf("expected surface=whatsapp, got %s", tr.Surface)
	}
	if tr.Title != "whatsapp session with kai" {
		t.Errorf("unexpected title: %s", tr.Title)
	}
	expectedTranscript := "[user]: hello\n[assistant]: hi there\n"
	if tr.Transcript != expectedTranscript {
		t.Errorf("unexpected transcript:\ngot:  %q\nwant: %q", tr.Transcript, expectedTranscript)
	}
	if tr.Duration != "5m0s" {
		t.Errorf("expected duration=5m0s, got %s", tr.Duration)
	}
	owner := "default-owner-uuid"
	if tr.OwnerUUID == nil || *tr.OwnerUUID != owner {
		t.Errorf("expected owner_uuid=%s, got %v", owner, tr.OwnerUUID)
	}

	// NATS event should be published.
	if len(publishedSubjects) != 1 {
		t.Fatalf("expected 1 NATS publish, got %d", len(publishedSubjects))
	}
	if publishedSubjects[0] != "swarm.chronicle.transcript.stored" {
		t.Errorf("unexpected subject: %s", publishedSubjects[0])
	}

	var evt TranscriptEvent
	if err := json.Unmarshal(publishedPayloads[0], &evt); err != nil {
		t.Fatalf("failed to unmarshal published event: %v", err)
	}
	if evt.SessionRef != "whatsapp:+447444361435:kai" {
		t.Errorf("unexpected session_ref: %s", evt.SessionRef)
	}
	if evt.Surface != "whatsapp" {
		t.Errorf("unexpected surface: %s", evt.Surface)
	}
	if evt.Transcript != expectedTranscript {
		t.Errorf("unexpected transcript in event")
	}
	if evt.OwnerUUID != "default-owner-uuid" {
		t.Errorf("unexpected owner_uuid in event: %s", evt.OwnerUUID)
	}
}

func TestParseSessionKey(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		wantSurface string
		wantPart    string
		wantAgent   string
	}{
		{
			name:        "full key",
			key:         "whatsapp:+447444361435:kai",
			wantSurface: "whatsapp",
			wantPart:    "+447444361435",
			wantAgent:   "kai",
		},
		{
			name:        "web session",
			key:         "web:user123:lily",
			wantSurface: "web",
			wantPart:    "user123",
			wantAgent:   "lily",
		},
		{
			name:        "slack session",
			key:         "slack:U12345:scout",
			wantSurface: "slack",
			wantPart:    "U12345",
			wantAgent:   "scout",
		},
		{
			name:        "two parts only",
			key:         "test:participant",
			wantSurface: "test",
			wantPart:    "participant",
			wantAgent:   "",
		},
		{
			name:        "single part",
			key:         "minimal",
			wantSurface: "minimal",
			wantPart:    "",
			wantAgent:   "",
		},
		{
			name:        "agent with colons",
			key:         "web:user:agent:extra",
			wantSurface: "web",
			wantPart:    "user",
			wantAgent:   "agent:extra",
		},
		{
			name:        "empty string",
			key:         "",
			wantSurface: "",
			wantPart:    "",
			wantAgent:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parseSessionKey(tt.key)
			if p.Surface != tt.wantSurface {
				t.Errorf("surface: got %q, want %q", p.Surface, tt.wantSurface)
			}
			if p.Participant != tt.wantPart {
				t.Errorf("participant: got %q, want %q", p.Participant, tt.wantPart)
			}
			if p.AgentID != tt.wantAgent {
				t.Errorf("agent_id: got %q, want %q", p.AgentID, tt.wantAgent)
			}
		})
	}
}

func TestAssemble_Idempotent(t *testing.T) {
	store := newMockStore()
	publishCount := 0
	assembler := NewAssembler(store, func(subject string, data []byte) error {
		publishCount++
		return nil
	}, "default-owner-uuid")

	t0 := time.Date(2026, 2, 14, 10, 0, 0, 0, time.UTC)

	// Single final chunk (chunk 0 is also final).
	chunk := GatewayChunk{
		SessionKey:   "test:+44:kai",
		ChunkID:      "chunk-001",
		ChunkIndex:   0,
		IsFinal:      true,
		Messages:     []ChatMessage{{Role: "user", Content: "hello"}},
		MessageCount: 1,
		SessionMetadata: SessionMeta{
			Channel: "test",
			AgentID: "kai",
		},
		FlushedAt:   &t0,
		FlushReason: "session_stop",
	}
	data, _ := json.Marshal(chunk)

	// First call: should assemble.
	assembler.HandleChunk(context.Background(), "swarm.gateway.session.chunk", data)
	if len(store.insertedTranscripts) != 1 {
		t.Fatalf("expected 1 transcript after first call, got %d", len(store.insertedTranscripts))
	}
	if publishCount != 1 {
		t.Fatalf("expected 1 publish after first call, got %d", publishCount)
	}

	// Second call (duplicate final): should NOT produce another transcript.
	assembler.HandleChunk(context.Background(), "swarm.gateway.session.chunk", data)
	if len(store.insertedTranscripts) != 1 {
		t.Errorf("expected still 1 transcript after duplicate, got %d", len(store.insertedTranscripts))
	}
	if publishCount != 1 {
		t.Errorf("expected still 1 publish after duplicate, got %d", publishCount)
	}
}

func TestHandleChunk_OwnerFromMetadata(t *testing.T) {
	store := newMockStore()
	assembler := NewAssembler(store, func(subject string, data []byte) error {
		return nil
	}, "fallback-uuid")

	t0 := time.Date(2026, 2, 14, 10, 0, 0, 0, time.UTC)

	chunk := GatewayChunk{
		SessionKey:   "web:user1:lily",
		ChunkID:      "chunk-001",
		ChunkIndex:   0,
		IsFinal:      true,
		Messages:     []ChatMessage{{Role: "user", Content: "hi"}},
		MessageCount: 1,
		SessionMetadata: SessionMeta{
			Channel:   "web",
			AgentID:   "lily",
			OwnerUUID: "metadata-owner-uuid",
		},
		FlushedAt:   &t0,
		FlushReason: "session_stop",
	}
	data, _ := json.Marshal(chunk)

	assembler.HandleChunk(context.Background(), "swarm.gateway.session.chunk", data)

	if len(store.insertedTranscripts) != 1 {
		t.Fatalf("expected 1 transcript, got %d", len(store.insertedTranscripts))
	}
	tr := store.insertedTranscripts[0]
	if tr.OwnerUUID == nil || *tr.OwnerUUID != "metadata-owner-uuid" {
		t.Errorf("expected owner from metadata, got %v", tr.OwnerUUID)
	}
}
