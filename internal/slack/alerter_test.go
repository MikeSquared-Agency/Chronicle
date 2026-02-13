package slack

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// newTestAlerter creates an Alerter pointing at the given test server URL.
func newTestAlerter(url, token, channel string) *Alerter {
	a := NewAlerter(token, channel)
	a.apiURL = url
	return a
}

func TestNewAlerter(t *testing.T) {
	a := NewAlerter("xoxb-test-token", "#alerts")

	if a.token != "xoxb-test-token" {
		t.Errorf("expected token xoxb-test-token, got %s", a.token)
	}
	if a.channel != "#alerts" {
		t.Errorf("expected channel #alerts, got %s", a.channel)
	}
	if a.client == nil {
		t.Fatal("expected non-nil http client")
	}
	if a.apiURL != "https://slack.com/api/chat.postMessage" {
		t.Errorf("expected default api url, got %s", a.apiURL)
	}
}

func TestPostDLQAlert_Success(t *testing.T) {
	var (
		gotMethod      string
		gotContentType string
		gotAuth        string
		gotBody        []byte
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")
		gotAuth = r.Header.Get("Authorization")
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := newTestAlerter(srv.URL, "xoxb-secret", "#dlq-alerts")

	payload := `{"source":"dispatch","error_message":"timeout","retry_count":2,"max_retries":5}`
	err := a.PostDLQAlert(context.Background(), "swarm.task.request", []byte(payload))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("expected POST method, got %s", gotMethod)
	}
	if gotContentType != "application/json; charset=utf-8" {
		t.Errorf("expected content-type application/json; charset=utf-8, got %s", gotContentType)
	}
	if gotAuth != "Bearer xoxb-secret" {
		t.Errorf("expected Authorization Bearer xoxb-secret, got %s", gotAuth)
	}

	var body map[string]any
	if err := json.Unmarshal(gotBody, &body); err != nil {
		t.Fatalf("failed to unmarshal request body: %v", err)
	}
	if body["channel"] != "#dlq-alerts" {
		t.Errorf("expected channel #dlq-alerts, got %v", body["channel"])
	}
	blocks, ok := body["blocks"].([]any)
	if !ok {
		t.Fatalf("expected blocks to be an array, got %T", body["blocks"])
	}
	if len(blocks) != 3 {
		t.Errorf("expected 3 blocks, got %d", len(blocks))
	}
}

func TestPostDLQAlert_RateLimit(t *testing.T) {
	var callCount atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := newTestAlerter(srv.URL, "xoxb-token", "#alerts")

	payload := `{"source":"test","error_message":"err"}`

	// First call should go through.
	err := a.PostDLQAlert(context.Background(), "subj", []byte(payload))
	if err != nil {
		t.Fatalf("first call: expected no error, got %v", err)
	}

	// Second call immediately after should be rate-limited (silently skipped).
	err = a.PostDLQAlert(context.Background(), "subj", []byte(payload))
	if err != nil {
		t.Fatalf("second call: expected no error, got %v", err)
	}

	if got := callCount.Load(); got != 1 {
		t.Errorf("expected exactly 1 HTTP request, got %d", got)
	}
}

func TestPostDLQAlert_ParsesPayload(t *testing.T) {
	var gotBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := newTestAlerter(srv.URL, "xoxb-tok", "#ch")

	payload := `{"source":"ingester","error_message":"schema validation failed","retry_count":3,"max_retries":10}`
	err := a.PostDLQAlert(context.Background(), "chronicle.ingest", []byte(payload))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	bodyStr := string(gotBody)

	for _, want := range []string{"ingester", "schema validation failed", "3 / 10"} {
		if !strings.Contains(bodyStr, want) {
			t.Errorf("expected body to contain %q, body was: %s", want, bodyStr)
		}
	}
}

func TestPostDLQAlert_FallbackFields(t *testing.T) {
	var gotBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	a := newTestAlerter(srv.URL, "xoxb-tok", "#ch")

	// Only "reason" is set, no "error_message". The alerter should fall back to reason.
	payload := `{"source":"dispatch","reason":"no_capable_agent"}`
	err := a.PostDLQAlert(context.Background(), "swarm.task.request", []byte(payload))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	bodyStr := string(gotBody)
	if !strings.Contains(bodyStr, "no_capable_agent") {
		t.Errorf("expected body to contain reason fallback no_capable_agent, body was: %s", bodyStr)
	}
}

func TestPostDLQAlert_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	a := newTestAlerter(srv.URL, "xoxb-tok", "#ch")

	payload := `{"source":"test","error_message":"boom"}`
	err := a.PostDLQAlert(context.Background(), "subj", []byte(payload))
	if err == nil {
		t.Fatal("expected an error for 500 response, got nil")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected error to mention status 500, got: %v", err)
	}
}
