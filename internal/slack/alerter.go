package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Alerter posts DLQ alerts to a Slack channel via chat.postMessage.
type Alerter struct {
	token   string
	channel string
	client  *http.Client
	apiURL  string

	mu       sync.Mutex
	lastSent time.Time
}

// NewAlerter creates a new Slack alerter.
func NewAlerter(token, channel string) *Alerter {
	return &Alerter{
		token:   token,
		channel: channel,
		client:  &http.Client{Timeout: 10 * time.Second},
		apiURL:  "https://slack.com/api/chat.postMessage",
	}
}

// dlqPayload is the subset of fields we extract from the DLQ NATS message.
type dlqPayload struct {
	Source       string `json:"source"`
	ErrorMessage string `json:"error_message"`
	RetryCount   int    `json:"retry_count"`
	MaxRetries   int    `json:"max_retries"`
	Reason       string `json:"reason"`
}

// PostDLQAlert sends a Block Kit message for a DLQ event. It rate-limits
// to at most one alert per 30 seconds to protect against burst storms.
func (a *Alerter) PostDLQAlert(ctx context.Context, subject string, payload []byte) error {
	a.mu.Lock()
	if time.Since(a.lastSent) < 30*time.Second {
		a.mu.Unlock()
		return nil
	}
	a.lastSent = time.Now()
	a.mu.Unlock()

	var dlq dlqPayload
	_ = json.Unmarshal(payload, &dlq)

	source := dlq.Source
	if source == "" {
		source = subject
	}
	errMsg := dlq.ErrorMessage
	if errMsg == "" {
		errMsg = dlq.Reason
	}
	if errMsg == "" {
		errMsg = "unknown"
	}

	blocks := []map[string]any{
		{
			"type": "header",
			"text": map[string]any{
				"type": "plain_text",
				"text": "Dead Letter Queue Alert",
			},
		},
		{
			"type": "section",
			"fields": []map[string]any{
				{"type": "mrkdwn", "text": fmt.Sprintf("*Source:*\n%s", source)},
				{"type": "mrkdwn", "text": fmt.Sprintf("*Subject:*\n%s", subject)},
				{"type": "mrkdwn", "text": fmt.Sprintf("*Error:*\n%s", errMsg)},
				{"type": "mrkdwn", "text": fmt.Sprintf("*Retries:*\n%d / %d", dlq.RetryCount, dlq.MaxRetries)},
			},
		},
		{
			"type": "context",
			"elements": []map[string]any{
				{"type": "mrkdwn", "text": fmt.Sprintf("Sent at %s", time.Now().UTC().Format(time.RFC3339))},
			},
		},
	}

	body, err := json.Marshal(map[string]any{
		"channel": a.channel,
		"blocks":  blocks,
		"text":    fmt.Sprintf("DLQ alert: %s — %s", source, errMsg),
	})
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.apiURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Authorization", "Bearer "+a.token)

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("slack post: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack returned %d", resp.StatusCode)
	}

	slog.Info("DLQ alert posted to Slack", "channel", a.channel, "subject", subject)
	return nil
}
