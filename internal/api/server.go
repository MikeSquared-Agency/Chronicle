package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"chronicle/internal/batcher"
	"chronicle/internal/store"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type Server struct {
	store   store.DataStore
	batcher *batcher.Batcher
	router  chi.Router
	port    int
}

func NewServer(s store.DataStore, b *batcher.Batcher, port int, dlqRoutes chi.Router) *Server {
	srv := &Server{
		store:   s,
		batcher: b,
		port:    port,
	}

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)

	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/health", srv.handleHealth)
		r.Get("/events", srv.handleGetEvents)
		r.Get("/traces", srv.handleListTraces)
		r.Get("/traces/{traceID}", srv.handleGetTrace)
		r.Get("/metrics/{agentID}/latest", srv.handleGetAgentMetrics)
		r.Get("/metrics/summary", srv.handleMetricsSummary)
		if dlqRoutes != nil {
			r.Mount("/dlq", dlqRoutes)
		}
	})

	srv.router = r
	return srv
}

func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	slog.Info("starting HTTP API", "addr", addr)
	return http.ListenAndServe(addr, s.router)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ok",
		"service":     "chronicle",
		"buffer_size": s.batcher.BufferLen(),
	})
}

func (s *Server) handleGetEvents(w http.ResponseWriter, r *http.Request) {
	traceID := r.URL.Query().Get("trace_id")
	if traceID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "trace_id is required"})
		return
	}

	evts, err := s.store.QueryEvents(r.Context(), traceID)
	if err != nil {
		slog.Error("query events failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	writeJSON(w, http.StatusOK, evts)
}

func (s *Server) handleListTraces(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}

	traces, err := s.store.QueryTraces(r.Context(), status, limit)
	if err != nil {
		slog.Error("query traces failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	writeJSON(w, http.StatusOK, traces)
}

func (s *Server) handleGetTrace(w http.ResponseWriter, r *http.Request) {
	traceID := chi.URLParam(r, "traceID")

	trace, err := s.store.GetTrace(r.Context(), traceID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "trace not found"})
		return
	}

	// Include all events as spans.
	evts, _ := s.store.QueryEvents(r.Context(), traceID)
	trace["spans"] = evts

	writeJSON(w, http.StatusOK, trace)
}

func (s *Server) handleGetAgentMetrics(w http.ResponseWriter, r *http.Request) {
	agentID := chi.URLParam(r, "agentID")

	m, err := s.store.GetAgentMetrics(r.Context(), agentID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "metrics not found"})
		return
	}

	writeJSON(w, http.StatusOK, m)
}

func (s *Server) handleMetricsSummary(w http.ResponseWriter, r *http.Request) {
	summary, err := s.store.GetAllAgentMetricsSummary(r.Context())
	if err != nil {
		slog.Error("query metrics summary failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	writeJSON(w, http.StatusOK, summary)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
