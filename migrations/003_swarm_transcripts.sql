-- 003_swarm_transcripts.sql
-- Transcript chunk staging and assembled transcripts for the gateway → Chronicle → Dredd pipeline.

-- Raw chunk staging: each gateway session flush produces one row.
CREATE TABLE IF NOT EXISTS swarm_transcript_chunks (
    chunk_id         UUID PRIMARY KEY,
    session_key      TEXT NOT NULL,
    chunk_index      INT NOT NULL,
    is_final         BOOLEAN NOT NULL DEFAULT FALSE,
    messages         JSONB NOT NULL DEFAULT '[]'::jsonb,
    message_count    INT NOT NULL DEFAULT 0,
    session_metadata JSONB,
    flush_reason     TEXT,
    flushed_at       TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transcript_chunks_session_index
    ON swarm_transcript_chunks (session_key, chunk_index);

CREATE INDEX IF NOT EXISTS idx_transcript_chunks_session_key
    ON swarm_transcript_chunks (session_key);

-- Assembled transcripts: one row per completed session.
CREATE TABLE IF NOT EXISTS swarm_transcripts (
    session_id       UUID PRIMARY KEY,
    session_key      TEXT NOT NULL UNIQUE,
    session_ref      TEXT,
    owner_uuid       UUID,
    title            TEXT,
    surface          TEXT,
    transcript       TEXT,
    message_count    INT NOT NULL DEFAULT 0,
    chunk_count      INT NOT NULL DEFAULT 0,
    duration         TEXT,
    first_message_at TIMESTAMPTZ,
    last_message_at  TIMESTAMPTZ,
    assembled_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
