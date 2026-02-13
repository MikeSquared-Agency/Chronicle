package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MikeSquared-Agency/chronicle/internal/api"
	"github.com/MikeSquared-Agency/chronicle/internal/batcher"
	"github.com/MikeSquared-Agency/chronicle/internal/config"
	"github.com/MikeSquared-Agency/chronicle/internal/ingester"
	"github.com/MikeSquared-Agency/chronicle/internal/metrics"
	slackalert "github.com/MikeSquared-Agency/chronicle/internal/slack"
	"github.com/MikeSquared-Agency/chronicle/internal/store"
	"github.com/MikeSquared-Agency/chronicle/internal/traces"

	dlq "github.com/MikeSquared-Agency/swarm-dlq"
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

	// Step 5: Initialize DLQ components (store, processor, handler, scanner).
	dlqStore := dlq.NewStore(db.Pool())
	dlqProc := dlq.NewProcessor(dlqStore)
	dlqHandler := dlq.NewHandler(dlqStore, ing.NATSConn())
	dlqScanner := dlq.NewScanner(dlqStore, ing.NATSConn(), cfg.DLQScanInterval)

	// Conditionally create Slack alerter for DLQ notifications.
	var slackAlerter *slackalert.Alerter
	if cfg.SlackBotToken != "" && cfg.SlackAlertChannel != "" {
		slackAlerter = slackalert.NewAlerter(cfg.SlackBotToken, cfg.SlackAlertChannel)
		slog.Info("Slack DLQ alerter enabled", "channel", cfg.SlackAlertChannel)
	}

	// Wire DLQ processor into the ingester — on dlq.> messages, persist to swarm_dlq
	// and publish an alert to NATS for Slack notifications.
	ing.SetDLQHandler(func(ctx context.Context, subject string, data []byte) {
		dlqProc.Process(ctx, subject, data)

		// Publish alert for downstream watchers (e.g. Slack #alerts).
		alertPayload, _ := json.Marshal(map[string]any{
			"channel":  "alerts",
			"type":     "dlq_entry",
			"subject":  subject,
			"raw_size": len(data),
		})
		if err := ing.Publish("swarm.alert.dlq", alertPayload); err != nil {
			slog.Warn("failed to publish DLQ alert", "error", err)
		}

		// Post to Slack if alerter is configured.
		if slackAlerter != nil {
			if err := slackAlerter.PostDLQAlert(ctx, subject, data); err != nil {
				slog.Warn("failed to post DLQ alert to Slack", "error", err)
			}
		}
	})

	if err := ing.Start(); err != nil {
		slog.Error("failed to start ingester", "error", err)
		os.Exit(1)
	}
	slog.Info("NATS ingester started")

	// Start DLQ recovery scanner.
	dlqScanner.Start(ctx)
	slog.Info("DLQ scanner started", "interval", cfg.DLQScanInterval)

	// Step 6: Announce availability.
	announcement, _ := json.Marshal(map[string]any{
		"event_type": "agent.registered",
		"source":     "chronicle",
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"metadata":   map[string]any{"port": cfg.Port},
	})
	if err := ing.Publish("swarm.agent.chronicle.registered", announcement); err != nil {
		slog.Warn("failed to publish registration event", "error", err)
	}

	// Step 7: Start HTTP API with DLQ routes mounted at /api/v1/dlq.
	srv := api.NewServer(db, bat, cfg.Port, dlqHandler.Routes())
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
	dlqScanner.Wait()
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
