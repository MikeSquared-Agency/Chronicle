package transcript

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
)

// TranscriptStore abstracts the DB operations the assembler needs.
// Uses primitive types to avoid import cycles with the store package.
type TranscriptStore interface {
	InsertChunk(ctx context.Context, chunkID, sessionKey string, chunkIndex int, isFinal bool, messages json.RawMessage, messageCount int, sessionMetadata json.RawMessage, flushReason string, flushedAt *time.Time) error
	GetChunksForSession(ctx context.Context, sessionKey string) ([]ChunkRowData, error)
	InsertTranscript(ctx context.Context, sessionID, sessionKey, sessionRef string, ownerUUID *string, title, surface, transcript string, messageCount, chunkCount int, duration string, firstMessageAt, lastMessageAt *time.Time) error
	TranscriptExists(ctx context.Context, sessionKey string) (bool, error)
}

// ChunkRowData mirrors store.ChunkRow without importing the store package.
type ChunkRowData struct {
	ChunkID         string
	SessionKey      string
	ChunkIndex      int
	IsFinal         bool
	Messages        json.RawMessage
	MessageCount    int
	SessionMetadata json.RawMessage
	FlushReason     string
	FlushedAt       *time.Time
	CreatedAt       time.Time
}

// NATSPublishFunc is the callback signature for publishing to NATS.
type NATSPublishFunc func(subject string, data []byte) error

// Assembler receives gateway session chunks, persists them, and assembles
// full transcripts when the final chunk arrives.
type Assembler struct {
	store            TranscriptStore
	publish          NATSPublishFunc
	defaultOwnerUUID string
}

// NewAssembler creates an Assembler wired to the given store and NATS publisher.
func NewAssembler(store TranscriptStore, publish NATSPublishFunc, defaultOwnerUUID string) *Assembler {
	return &Assembler{
		store:            store,
		publish:          publish,
		defaultOwnerUUID: defaultOwnerUUID,
	}
}

// HandleChunk processes a single gateway session chunk message.
// This is the callback wired into the ingester's gateway handler.
func (a *Assembler) HandleChunk(ctx context.Context, subject string, data []byte) {
	var chunk GatewayChunk
	if err := json.Unmarshal(data, &chunk); err != nil {
		slog.Warn("transcript: failed to unmarshal gateway chunk",
			"subject", subject,
			"error", err,
		)
		return
	}

	if chunk.SessionKey == "" {
		slog.Warn("transcript: chunk missing session_key", "subject", subject)
		return
	}

	// Generate chunk_id if not provided.
	if chunk.ChunkID == "" {
		chunk.ChunkID = uuid.New().String()
	}

	// Marshal messages and session_metadata back to JSON for DB storage.
	messagesJSON, _ := json.Marshal(chunk.Messages)
	metaJSON, _ := json.Marshal(chunk.SessionMetadata)

	if err := a.store.InsertChunk(ctx, chunk.ChunkID, chunk.SessionKey, chunk.ChunkIndex, chunk.IsFinal, messagesJSON, chunk.MessageCount, metaJSON, chunk.FlushReason, chunk.FlushedAt); err != nil {
		slog.Error("transcript: failed to insert chunk",
			"session_key", chunk.SessionKey,
			"chunk_index", chunk.ChunkIndex,
			"error", err,
		)
		return
	}

	slog.Info("transcript: chunk stored",
		"session_key", chunk.SessionKey,
		"chunk_index", chunk.ChunkIndex,
		"is_final", chunk.IsFinal,
		"message_count", chunk.MessageCount,
	)

	if chunk.IsFinal {
		a.assemble(ctx, chunk.SessionKey, chunk.SessionMetadata)
	}
}

// assemble queries all chunks for a session, concatenates messages, stores the
// transcript, and publishes the NATS event for Dredd.
func (a *Assembler) assemble(ctx context.Context, sessionKey string, meta SessionMeta) {
	// Idempotency check: skip if transcript already exists.
	exists, err := a.store.TranscriptExists(ctx, sessionKey)
	if err != nil {
		slog.Error("transcript: failed to check transcript existence", "session_key", sessionKey, "error", err)
		return
	}
	if exists {
		slog.Info("transcript: already assembled, skipping", "session_key", sessionKey)
		return
	}

	chunks, err := a.store.GetChunksForSession(ctx, sessionKey)
	if err != nil {
		slog.Error("transcript: failed to get chunks", "session_key", sessionKey, "error", err)
		return
	}
	if len(chunks) == 0 {
		slog.Warn("transcript: no chunks found for session", "session_key", sessionKey)
		return
	}

	// Concatenate all messages in chunk order.
	var allMessages []ChatMessage
	var totalMessages int
	var firstFlushed, lastFlushed *time.Time

	for _, c := range chunks {
		var msgs []ChatMessage
		if err := json.Unmarshal(c.Messages, &msgs); err != nil {
			slog.Warn("transcript: failed to unmarshal chunk messages",
				"session_key", sessionKey,
				"chunk_index", c.ChunkIndex,
				"error", err,
			)
			continue
		}
		allMessages = append(allMessages, msgs...)
		totalMessages += c.MessageCount

		if c.FlushedAt != nil {
			if firstFlushed == nil || c.FlushedAt.Before(*firstFlushed) {
				t := *c.FlushedAt
				firstFlushed = &t
			}
			if lastFlushed == nil || c.FlushedAt.After(*lastFlushed) {
				t := *c.FlushedAt
				lastFlushed = &t
			}
		}
	}

	// Build transcript text: "[role]: content\n" format.
	var sb strings.Builder
	for _, m := range allMessages {
		fmt.Fprintf(&sb, "[%s]: %s\n", m.Role, m.Content)
	}
	transcriptText := sb.String()

	// Derive fields from session key and metadata.
	parsed := parseSessionKey(sessionKey)
	surface := meta.Channel
	if surface == "" {
		surface = parsed.Surface
	}

	agentID := meta.AgentID
	if agentID == "" {
		agentID = parsed.AgentID
	}

	title := fmt.Sprintf("%s session with %s", surface, agentID)

	// Duration from first to last chunk flushed_at.
	var duration string
	if firstFlushed != nil && lastFlushed != nil {
		d := lastFlushed.Sub(*firstFlushed)
		duration = d.String()
	}

	// Owner UUID: prefer metadata, fall back to config default.
	ownerUUID := meta.OwnerUUID
	if ownerUUID == "" {
		ownerUUID = a.defaultOwnerUUID
	}
	var ownerPtr *string
	if ownerUUID != "" {
		ownerPtr = &ownerUUID
	}

	sessionID := uuid.New().String()

	if err := a.store.InsertTranscript(ctx, sessionID, sessionKey, sessionKey, ownerPtr, title, surface, transcriptText, totalMessages, len(chunks), duration, firstFlushed, lastFlushed); err != nil {
		slog.Error("transcript: failed to insert transcript", "session_key", sessionKey, "error", err)
		return
	}

	slog.Info("transcript: assembled",
		"session_key", sessionKey,
		"session_id", sessionID,
		"chunks", len(chunks),
		"messages", totalMessages,
	)

	// Publish event for Dredd.
	evt := TranscriptEvent{
		SessionID:  sessionID,
		OwnerUUID:  ownerUUID,
		SessionRef: sessionKey,
		Title:      title,
		Duration:   duration,
		Surface:    surface,
		Transcript: transcriptText,
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		slog.Error("transcript: failed to marshal transcript event", "error", err)
		return
	}

	if err := a.publish("swarm.chronicle.transcript.stored", payload); err != nil {
		slog.Error("transcript: failed to publish transcript event",
			"session_key", sessionKey,
			"error", err,
		)
	} else {
		slog.Info("transcript: published to NATS",
			"subject", "swarm.chronicle.transcript.stored",
			"session_id", sessionID,
		)
	}
}

// parsedSessionKey holds the components extracted from a session_key.
type parsedSessionKey struct {
	Surface     string
	Participant string
	AgentID     string
}

// parseSessionKey splits "{surface}:{participant}:{agent_id}" into components.
func parseSessionKey(key string) parsedSessionKey {
	parts := strings.SplitN(key, ":", 3)
	var p parsedSessionKey
	if len(parts) >= 1 {
		p.Surface = parts[0]
	}
	if len(parts) >= 2 {
		p.Participant = parts[1]
	}
	if len(parts) >= 3 {
		p.AgentID = parts[2]
	}
	return p
}
