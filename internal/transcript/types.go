package transcript

import "time"

// GatewayChunk matches the JSON schema published by gateway-nats-hook.
type GatewayChunk struct {
	SessionKey      string       `json:"session_key"`
	ChunkID         string       `json:"chunk_id"`
	ChunkIndex      int          `json:"chunk_index"`
	IsFinal         bool         `json:"is_final"`
	Messages        []ChatMessage `json:"messages"`
	MessageCount    int          `json:"message_count"`
	SessionMetadata SessionMeta  `json:"session_metadata"`
	FlushedAt       *time.Time   `json:"flushed_at"`
	FlushReason     string       `json:"flush_reason"`
}

// SessionMeta carries per-session context from the gateway.
type SessionMeta struct {
	Channel   string `json:"channel"`
	Participant string `json:"participant"`
	AgentID   string `json:"agent_id"`
	OwnerUUID string `json:"owner_uuid"`
}

// ChatMessage is a single message in a conversation.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// TranscriptEvent is the NATS payload published to swarm.chronicle.transcript.stored,
// consumed by Dredd for decision extraction.
type TranscriptEvent struct {
	SessionID  string `json:"session_id"`
	OwnerUUID  string `json:"owner_uuid"`
	SessionRef string `json:"session_ref"`
	Title      string `json:"title"`
	Duration   string `json:"duration"`
	Surface    string `json:"surface"`
	Transcript string `json:"transcript"`
}
