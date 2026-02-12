package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"chronicle/internal/events"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

// InsertEvents batch-inserts normalized events into swarm_events.
func (s *Store) InsertEvents(ctx context.Context, evts []events.Event) error {
	if len(evts) == 0 {
		return nil
	}

	rows := make([][]any, len(evts))
	for i, e := range evts {
		rows[i] = []any{e.EventID, e.TraceID, e.Source, e.EventType, e.Timestamp, e.Metadata}
	}

	_, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"swarm_events"},
		[]string{"event_id", "trace_id", "source", "event_type", "timestamp", "metadata"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy events: %w", err)
	}

	slog.Debug("inserted events", "count", len(evts))
	return nil
}

// UpsertTrace creates or updates a trace record based on incoming event data.
func (s *Store) UpsertTrace(ctx context.Context, traceID string, updates map[string]any) error {
	// Build the upsert dynamically based on what fields are provided.
	query := `
		INSERT INTO swarm_traces (trace_id, status, updated_at)
		VALUES ($1, 'pending', now())
		ON CONFLICT (trace_id) DO UPDATE SET updated_at = now()
	`
	if _, err := s.pool.Exec(ctx, query, traceID); err != nil {
		return fmt.Errorf("upsert trace base: %w", err)
	}

	// Apply individual field updates.
	for field, value := range updates {
		var q string
		switch field {
		case "status":
			q = `UPDATE swarm_traces SET status = $2, updated_at = now() WHERE trace_id = $1`
		case "task_title":
			q = `UPDATE swarm_traces SET task_title = $2, updated_at = now() WHERE trace_id = $1`
		case "owner":
			q = `UPDATE swarm_traces SET owner = $2, updated_at = now() WHERE trace_id = $1`
		case "assigned_agent":
			q = `UPDATE swarm_traces SET assigned_agent = $2, updated_at = now() WHERE trace_id = $1`
		case "started_at":
			q = `UPDATE swarm_traces SET started_at = $2, updated_at = now() WHERE trace_id = $1`
		case "completed_at":
			q = `UPDATE swarm_traces SET completed_at = $2, updated_at = now() WHERE trace_id = $1`
		case "duration_ms":
			q = `UPDATE swarm_traces SET duration_ms = $2, updated_at = now() WHERE trace_id = $1`
		case "time_to_pickup_ms":
			q = `UPDATE swarm_traces SET time_to_pickup_ms = $2, updated_at = now() WHERE trace_id = $1`
		case "error":
			q = `UPDATE swarm_traces SET error = $2, updated_at = now() WHERE trace_id = $1`
		case "scoring_breakdown":
			q = `UPDATE swarm_traces SET scoring_breakdown = $2, updated_at = now() WHERE trace_id = $1`
		default:
			continue
		}
		if _, err := s.pool.Exec(ctx, q, traceID, value); err != nil {
			return fmt.Errorf("update trace field %s: %w", field, err)
		}
	}

	// Increment span count.
	_, err := s.pool.Exec(ctx,
		`UPDATE swarm_traces SET span_count = span_count + 1, updated_at = now() WHERE trace_id = $1`,
		traceID,
	)
	return err
}

// UpsertAgentMetric updates rolling metrics for an agent on a given date.
func (s *Store) UpsertAgentMetric(ctx context.Context, agentID string, date time.Time, updates map[string]any) error {
	d := date.Format("2006-01-02")

	// Ensure row exists.
	_, err := s.pool.Exec(ctx, `
		INSERT INTO agent_metrics (agent_id, metric_date)
		VALUES ($1, $2)
		ON CONFLICT (agent_id, metric_date) DO NOTHING
	`, agentID, d)
	if err != nil {
		return fmt.Errorf("ensure agent_metrics row: %w", err)
	}

	for field, value := range updates {
		var q string
		switch field {
		case "inc_completed":
			q = `UPDATE agent_metrics SET tasks_completed = tasks_completed + 1, updated_at = now() WHERE agent_id = $1 AND metric_date = $2`
			if _, err := s.pool.Exec(ctx, q, agentID, d); err != nil {
				return fmt.Errorf("inc completed: %w", err)
			}
			continue
		case "inc_failed":
			q = `UPDATE agent_metrics SET tasks_failed = tasks_failed + 1, updated_at = now() WHERE agent_id = $1 AND metric_date = $2`
			if _, err := s.pool.Exec(ctx, q, agentID, d); err != nil {
				return fmt.Errorf("inc failed: %w", err)
			}
			continue
		case "avg_duration_ms":
			q = `UPDATE agent_metrics SET avg_duration_ms = $3, updated_at = now() WHERE agent_id = $1 AND metric_date = $2`
		case "p95_duration_ms":
			q = `UPDATE agent_metrics SET p95_duration_ms = $3, updated_at = now() WHERE agent_id = $1 AND metric_date = $2`
		case "min_duration_ms":
			q = `UPDATE agent_metrics SET min_duration_ms = LEAST(min_duration_ms, $3), updated_at = now() WHERE agent_id = $1 AND metric_date = $2`
		case "max_duration_ms":
			q = `UPDATE agent_metrics SET max_duration_ms = GREATEST(max_duration_ms, $3), updated_at = now() WHERE agent_id = $1 AND metric_date = $2`
		case "time_to_pickup_avg_ms":
			q = `UPDATE agent_metrics SET time_to_pickup_avg_ms = $3, updated_at = now() WHERE agent_id = $1 AND metric_date = $2`
		case "total_uptime_ms":
			q = `UPDATE agent_metrics SET total_uptime_ms = $3, updated_at = now() WHERE agent_id = $1 AND metric_date = $2`
		case "total_busy_ms":
			q = `UPDATE agent_metrics SET total_busy_ms = $3, updated_at = now() WHERE agent_id = $1 AND metric_date = $2`
		case "utilization_pct":
			q = `UPDATE agent_metrics SET utilization_pct = $3, updated_at = now() WHERE agent_id = $1 AND metric_date = $2`
		default:
			continue
		}
		if _, err := s.pool.Exec(ctx, q, agentID, d, value); err != nil {
			return fmt.Errorf("update metric %s: %w", field, err)
		}
	}

	return nil
}

// GetTrace returns a single trace by ID.
func (s *Store) GetTrace(ctx context.Context, traceID string) (map[string]any, error) {
	row := s.pool.QueryRow(ctx, `SELECT trace_id, task_title, owner, status, assigned_agent, started_at, completed_at, duration_ms, time_to_pickup_ms, span_count, error, scoring_breakdown, created_at, updated_at FROM swarm_traces WHERE trace_id = $1`, traceID)

	var (
		tid, status                        string
		title, own, agent, errStr          *string
		startedAt, completedAt             *time.Time
		durationMs, pickupMs               *int64
		spanCount                          int
		scoring                            *json.RawMessage
		createdAt, updatedAt               time.Time
	)
	if err := row.Scan(&tid, &title, &own, &status, &agent, &startedAt, &completedAt, &durationMs, &pickupMs, &spanCount, &errStr, &scoring, &createdAt, &updatedAt); err != nil {
		return nil, err
	}

	result := map[string]any{
		"trace_id":   tid,
		"status":     status,
		"span_count": spanCount,
		"created_at": createdAt,
		"updated_at": updatedAt,
	}
	if title != nil {
		result["task_title"] = *title
	}
	if own != nil {
		result["owner"] = *own
	}
	if agent != nil {
		result["assigned_agent"] = *agent
	}
	if startedAt != nil {
		result["started_at"] = *startedAt
	}
	if completedAt != nil {
		result["completed_at"] = *completedAt
	}
	if durationMs != nil {
		result["duration_ms"] = *durationMs
	}
	if pickupMs != nil {
		result["time_to_pickup_ms"] = *pickupMs
	}
	if errStr != nil {
		result["error"] = *errStr
	}
	if scoring != nil {
		result["scoring_breakdown"] = *scoring
	}
	return result, nil
}

// QueryEvents returns events filtered by trace_id.
func (s *Store) QueryEvents(ctx context.Context, traceID string) ([]map[string]any, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT event_id, trace_id, source, event_type, timestamp, metadata FROM swarm_events WHERE trace_id = $1 ORDER BY timestamp`,
		traceID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]any
	for rows.Next() {
		var (
			eid, tid, src, etype string
			ts                   time.Time
			meta                 json.RawMessage
		)
		if err := rows.Scan(&eid, &tid, &src, &etype, &ts, &meta); err != nil {
			return nil, err
		}
		results = append(results, map[string]any{
			"event_id":   eid,
			"trace_id":   tid,
			"source":     src,
			"event_type": etype,
			"timestamp":  ts,
			"metadata":   meta,
		})
	}
	return results, rows.Err()
}

// QueryTraces returns traces filtered by status with a limit.
func (s *Store) QueryTraces(ctx context.Context, status string, limit int) ([]map[string]any, error) {
	q := `SELECT trace_id, task_title, owner, status, assigned_agent, started_at, completed_at, duration_ms, span_count, created_at FROM swarm_traces`
	args := []any{}
	argN := 1

	if status != "" {
		q += fmt.Sprintf(` WHERE status = $%d`, argN)
		args = append(args, status)
		argN++
	}

	q += ` ORDER BY created_at DESC`

	if limit > 0 {
		q += fmt.Sprintf(` LIMIT $%d`, argN)
		args = append(args, limit)
	}

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]any
	for rows.Next() {
		var (
			tid, status                string
			title, own, agent          *string
			startedAt, completedAt     *time.Time
			durationMs                 *int64
			spanCount                  int
			createdAt                  time.Time
		)
		if err := rows.Scan(&tid, &title, &own, &status, &agent, &startedAt, &completedAt, &durationMs, &spanCount, &createdAt); err != nil {
			return nil, err
		}
		r := map[string]any{
			"trace_id":   tid,
			"status":     status,
			"span_count": spanCount,
			"created_at": createdAt,
		}
		if title != nil {
			r["task_title"] = *title
		}
		if own != nil {
			r["owner"] = *own
		}
		if agent != nil {
			r["assigned_agent"] = *agent
		}
		if startedAt != nil {
			r["started_at"] = *startedAt
		}
		if completedAt != nil {
			r["completed_at"] = *completedAt
		}
		if durationMs != nil {
			r["duration_ms"] = *durationMs
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// GetAgentMetrics returns the latest metrics row for an agent.
func (s *Store) GetAgentMetrics(ctx context.Context, agentID string) (map[string]any, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT agent_id, metric_date, tasks_completed, tasks_failed, avg_duration_ms, p95_duration_ms,
		       min_duration_ms, max_duration_ms, time_to_pickup_avg_ms, utilization_pct,
		       total_uptime_ms, total_busy_ms
		FROM agent_metrics
		WHERE agent_id = $1
		ORDER BY metric_date DESC
		LIMIT 1
	`, agentID)

	var (
		aid          string
		mdate        time.Time
		completed, failed int
		avgD, p95D, minD, maxD, pickup, uptime, busy int64
		util         float64
	)
	if err := row.Scan(&aid, &mdate, &completed, &failed, &avgD, &p95D, &minD, &maxD, &pickup, &util, &uptime, &busy); err != nil {
		return nil, err
	}

	return map[string]any{
		"agent_id":              aid,
		"metric_date":           mdate.Format("2006-01-02"),
		"tasks_completed":       completed,
		"tasks_failed":          failed,
		"avg_duration_ms":       avgD,
		"p95_duration_ms":       p95D,
		"min_duration_ms":       minD,
		"max_duration_ms":       maxD,
		"time_to_pickup_avg_ms": pickup,
		"utilization_pct":       util,
		"total_uptime_ms":       uptime,
		"total_busy_ms":         busy,
	}, nil
}

// GetAllAgentMetricsSummary returns today's metrics for all agents.
func (s *Store) GetAllAgentMetricsSummary(ctx context.Context) ([]map[string]any, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (agent_id)
		       agent_id, metric_date, tasks_completed, tasks_failed, avg_duration_ms,
		       p95_duration_ms, utilization_pct
		FROM agent_metrics
		ORDER BY agent_id, metric_date DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]any
	for rows.Next() {
		var (
			aid          string
			mdate        time.Time
			completed, failed int
			avgD, p95D   int64
			util         float64
		)
		if err := rows.Scan(&aid, &mdate, &completed, &failed, &avgD, &p95D, &util); err != nil {
			return nil, err
		}
		results = append(results, map[string]any{
			"agent_id":        aid,
			"metric_date":     mdate.Format("2006-01-02"),
			"tasks_completed": completed,
			"tasks_failed":    failed,
			"avg_duration_ms": avgD,
			"p95_duration_ms": p95D,
			"utilization_pct": util,
		})
	}
	return results, rows.Err()
}

// GetTraceAssignedAt retrieves when a trace was assigned (for time-to-pickup calculation).
func (s *Store) GetTraceAssignedAt(ctx context.Context, traceID string) (*time.Time, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT timestamp FROM swarm_events WHERE trace_id = $1 AND event_type = 'task.assigned' ORDER BY timestamp DESC LIMIT 1`,
		traceID,
	)
	var t time.Time
	if err := row.Scan(&t); err != nil {
		return nil, err
	}
	return &t, nil
}
