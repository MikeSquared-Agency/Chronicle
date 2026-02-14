package transcript

import (
	"context"
	"encoding/json"
	"time"

	"github.com/MikeSquared-Agency/chronicle/internal/store"
)

// StoreAdapter wraps *store.Store to satisfy the TranscriptStore interface,
// converting store.ChunkRow to transcript.ChunkRowData.
type StoreAdapter struct {
	s *store.Store
}

// NewStoreAdapter creates a StoreAdapter from a *store.Store.
func NewStoreAdapter(s *store.Store) *StoreAdapter {
	return &StoreAdapter{s: s}
}

func (a *StoreAdapter) InsertChunk(ctx context.Context, chunkID, sessionKey string, chunkIndex int, isFinal bool, messages json.RawMessage, messageCount int, sessionMetadata json.RawMessage, flushReason string, flushedAt *time.Time) error {
	return a.s.InsertChunk(ctx, chunkID, sessionKey, chunkIndex, isFinal, messages, messageCount, sessionMetadata, flushReason, flushedAt)
}

func (a *StoreAdapter) GetChunksForSession(ctx context.Context, sessionKey string) ([]ChunkRowData, error) {
	rows, err := a.s.GetChunksForSession(ctx, sessionKey)
	if err != nil {
		return nil, err
	}
	result := make([]ChunkRowData, len(rows))
	for i, r := range rows {
		result[i] = ChunkRowData{
			ChunkID:         r.ChunkID,
			SessionKey:      r.SessionKey,
			ChunkIndex:      r.ChunkIndex,
			IsFinal:         r.IsFinal,
			Messages:        r.Messages,
			MessageCount:    r.MessageCount,
			SessionMetadata: r.SessionMetadata,
			FlushReason:     r.FlushReason,
			FlushedAt:       r.FlushedAt,
			CreatedAt:       r.CreatedAt,
		}
	}
	return result, nil
}

func (a *StoreAdapter) InsertTranscript(ctx context.Context, sessionID, sessionKey, sessionRef string, ownerUUID *string, title, surface, transcript string, messageCount, chunkCount int, duration string, firstMessageAt, lastMessageAt *time.Time) error {
	return a.s.InsertTranscript(ctx, sessionID, sessionKey, sessionRef, ownerUUID, title, surface, transcript, messageCount, chunkCount, duration, firstMessageAt, lastMessageAt)
}

func (a *StoreAdapter) TranscriptExists(ctx context.Context, sessionKey string) (bool, error) {
	return a.s.TranscriptExists(ctx, sessionKey)
}
