package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear any env vars that might interfere.
	for _, k := range []string{"CHRONICLE_PORT", "NATS_URL", "DATABASE_URL", "ALEXANDRIA_URL",
		"BATCH_FLUSH_INTERVAL_MS", "BATCH_FLUSH_THRESHOLD", "BUFFER_MAX_SIZE", "LOG_LEVEL",
		"DEFAULT_OWNER_UUID"} {
		os.Unsetenv(k)
	}

	cfg := Load()

	if cfg.Port != 8700 {
		t.Errorf("expected port 8700, got %d", cfg.Port)
	}
	if cfg.NatsURL != "nats://hermes:4222" {
		t.Errorf("expected default nats url, got %s", cfg.NatsURL)
	}
	if cfg.DatabaseURL != "" {
		t.Errorf("expected empty database url, got %s", cfg.DatabaseURL)
	}
	if cfg.AlexandriaURL != "http://alexandria:8500" {
		t.Errorf("expected default alexandria url, got %s", cfg.AlexandriaURL)
	}
	if cfg.BatchFlushInterval != 5000*time.Millisecond {
		t.Errorf("expected 5s flush interval, got %v", cfg.BatchFlushInterval)
	}
	if cfg.BatchFlushThreshold != 100 {
		t.Errorf("expected threshold 100, got %d", cfg.BatchFlushThreshold)
	}
	if cfg.BufferMaxSize != 10000 {
		t.Errorf("expected buffer max 10000, got %d", cfg.BufferMaxSize)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected log level info, got %s", cfg.LogLevel)
	}
	if cfg.DefaultOwnerUUID != "" {
		t.Errorf("expected empty default owner uuid, got %s", cfg.DefaultOwnerUUID)
	}
}

func TestLoad_CustomValues(t *testing.T) {
	os.Setenv("CHRONICLE_PORT", "9090")
	os.Setenv("NATS_URL", "nats://localhost:4222")
	os.Setenv("DATABASE_URL", "postgres://test:test@localhost/test")
	os.Setenv("BATCH_FLUSH_INTERVAL_MS", "2000")
	os.Setenv("BATCH_FLUSH_THRESHOLD", "50")
	os.Setenv("BUFFER_MAX_SIZE", "5000")
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("DEFAULT_OWNER_UUID", "e621e1f8-c36c-495a-93fc-0c247a3e6e5f")
	defer func() {
		for _, k := range []string{"CHRONICLE_PORT", "NATS_URL", "DATABASE_URL",
			"BATCH_FLUSH_INTERVAL_MS", "BATCH_FLUSH_THRESHOLD", "BUFFER_MAX_SIZE", "LOG_LEVEL",
			"DEFAULT_OWNER_UUID"} {
			os.Unsetenv(k)
		}
	}()

	cfg := Load()

	if cfg.Port != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.Port)
	}
	if cfg.NatsURL != "nats://localhost:4222" {
		t.Errorf("expected custom nats url, got %s", cfg.NatsURL)
	}
	if cfg.DatabaseURL != "postgres://test:test@localhost/test" {
		t.Errorf("expected custom database url, got %s", cfg.DatabaseURL)
	}
	if cfg.BatchFlushInterval != 2000*time.Millisecond {
		t.Errorf("expected 2s flush interval, got %v", cfg.BatchFlushInterval)
	}
	if cfg.BatchFlushThreshold != 50 {
		t.Errorf("expected threshold 50, got %d", cfg.BatchFlushThreshold)
	}
	if cfg.BufferMaxSize != 5000 {
		t.Errorf("expected buffer max 5000, got %d", cfg.BufferMaxSize)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected log level debug, got %s", cfg.LogLevel)
	}
	if cfg.DefaultOwnerUUID != "e621e1f8-c36c-495a-93fc-0c247a3e6e5f" {
		t.Errorf("expected custom owner uuid, got %s", cfg.DefaultOwnerUUID)
	}
}

func TestLoad_InvalidInt(t *testing.T) {
	os.Setenv("CHRONICLE_PORT", "notanumber")
	defer os.Unsetenv("CHRONICLE_PORT")

	cfg := Load()
	if cfg.Port != 8700 {
		t.Errorf("expected default port on invalid value, got %d", cfg.Port)
	}
}
