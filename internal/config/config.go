package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port               int
	NatsURL            string
	DatabaseURL        string
	AlexandriaURL      string
	BatchFlushInterval time.Duration
	BatchFlushThreshold int
	BufferMaxSize      int
	LogLevel           string
}

func Load() Config {
	return Config{
		Port:                envInt("CHRONICLE_PORT", 8700),
		NatsURL:             envStr("NATS_URL", "nats://hermes:4222"),
		DatabaseURL:         envStr("DATABASE_URL", ""),
		AlexandriaURL:       envStr("ALEXANDRIA_URL", "http://alexandria:8500"),
		BatchFlushInterval:  time.Duration(envInt("BATCH_FLUSH_INTERVAL_MS", 5000)) * time.Millisecond,
		BatchFlushThreshold: envInt("BATCH_FLUSH_THRESHOLD", 100),
		BufferMaxSize:       envInt("BUFFER_MAX_SIZE", 10000),
		LogLevel:            envStr("LOG_LEVEL", "info"),
	}
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
