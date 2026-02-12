package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"chronicle/internal/api"
	"chronicle/internal/batcher"
	"chronicle/internal/config"
	"chronicle/internal/ingester"
	"chronicle/internal/metrics"
	"chronicle/internal/store"
	"chronicle/internal/traces"
)

func main() {
	cfg := config.Load()
	setupLogging(cfg.LogLevel)

	slog.Info("chronicle starting",
		"port", cfg.Port,
		"nats_url", cfg.NatsURL,
		"flush_interval", cfg.BatchFlushInterval,
		"flush_threshold", cfg.BatchFlushThreshold,
		"buffer_max", cfg.BufferMaxSize,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Step 1: Connect to database (Supabase Postgres).
	if cfg.DatabaseURL == "" {
		slog.Error("DATABASE_URL is required")
		os.Exit(1)
	}

	db, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	slog.Info("database connected")

	// Step 2: Initialize processors.
	traceProc := traces.NewProcessor(db)
	metricsProc := metrics.NewProcessor(db)

	// Step 3: Initialize batcher.
	bat := batcher.New(db, traceProc, metricsProc, batcher.Config{
		FlushInterval:  cfg.BatchFlushInterval,
		FlushThreshold: cfg.BatchFlushThreshold,
		BufferMax:      cfg.BufferMaxSize,
	})
	bat.Start(ctx)

	// Step 4: Connect to NATS and start ingesting.
	ing, err := ingester.New(cfg.NatsURL, bat)
	if err != nil {
		slog.Error("failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer ing.Close()

	if err := ing.Start(); err != nil {
		slog.Error("failed to start ingester", "error", err)
		os.Exit(1)
	}
	slog.Info("NATS ingester started")

	// Step 5: Announce availability.
	announcement, _ := json.Marshal(map[string]any{
		"event_type": "agent.registered",
		"source":     "chronicle",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"metadata":   map[string]any{"port": cfg.Port},
	})
	if err := ing.Publish("swarm.agent.chronicle.registered", announcement); err != nil {
		slog.Warn("failed to publish registration event", "error", err)
	}

	// Step 6: Start HTTP API.
	srv := api.NewServer(db, bat, cfg.Port)
	go func() {
		if err := srv.Start(); err != nil {
			slog.Error("HTTP server error", "error", err)
		}
	}()

	slog.Info("chronicle ready", "port", cfg.Port)

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	slog.Info("shutting down", "signal", sig)
	cancel()
	bat.Wait()
	slog.Info("chronicle stopped")
}

func setupLogging(level string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	slog.SetDefault(slog.New(handler))
}
