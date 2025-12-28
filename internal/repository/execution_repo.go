package repository

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/abdul-hamid-achik/job-queue/internal/job"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobExecution struct {
	ID           string          `json:"id"`
	JobID        string          `json:"job_id"`
	JobType      string          `json:"job_type"`
	Queue        string          `json:"queue"`
	Payload      json.RawMessage `json:"payload"`
	State        string          `json:"state"`
	Attempt      int             `json:"attempt"`
	MaxRetries   int             `json:"max_retries"`
	ErrorMessage *string         `json:"error_message,omitempty"`
	StackTrace   *string         `json:"stack_trace,omitempty"`
	WorkerID     *string         `json:"worker_id,omitempty"`
	StartedAt    *time.Time      `json:"started_at,omitempty"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
	DurationMs   *int64          `json:"duration_ms,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

type ExecutionFilter struct {
	JobID    *string
	JobType  *string
	Queue    *string
	State    *string
	FromDate *time.Time
	ToDate   *time.Time
	Limit    int
	Offset   int
}

type ExecutionRepository struct {
	pool *pgxpool.Pool
}

func NewExecutionRepository(pool *pgxpool.Pool) *ExecutionRepository {
	return &ExecutionRepository{pool: pool}
}

func (r *ExecutionRepository) Create(ctx context.Context, exec *JobExecution) error {
	query := `
		INSERT INTO job_executions (
			job_id, job_type, queue, payload, state, attempt, max_retries,
			error_message, stack_trace, worker_id, started_at, completed_at, duration_ms
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING id, created_at`

	return r.pool.QueryRow(ctx, query,
		exec.JobID, exec.JobType, exec.Queue, exec.Payload, exec.State,
		exec.Attempt, exec.MaxRetries, exec.ErrorMessage, exec.StackTrace,
		exec.WorkerID, exec.StartedAt, exec.CompletedAt, exec.DurationMs,
	).Scan(&exec.ID, &exec.CreatedAt)
}

func (r *ExecutionRepository) RecordExecution(ctx context.Context, j *job.Job, workerID string, execErr error) error {
	exec := &JobExecution{
		JobID:       j.ID,
		JobType:     j.Type,
		Queue:       j.Queue,
		Payload:     j.Payload,
		State:       j.State.String(),
		Attempt:     j.RetryCount + 1,
		MaxRetries:  j.MaxRetries,
		StartedAt:   j.StartedAt,
		CompletedAt: j.CompletedAt,
	}

	if workerID != "" {
		exec.WorkerID = &workerID
	}

	if execErr != nil {
		errMsg := execErr.Error()
		exec.ErrorMessage = &errMsg
	}

	if j.StartedAt != nil && j.CompletedAt != nil {
		durationMs := j.CompletedAt.Sub(*j.StartedAt).Milliseconds()
		exec.DurationMs = &durationMs
	}

	return r.Create(ctx, exec)
}

func (r *ExecutionRepository) GetByJobID(ctx context.Context, jobID string) ([]*JobExecution, error) {
	query := `
		SELECT id, job_id, job_type, queue, payload, state, attempt, max_retries,
			error_message, stack_trace, worker_id, started_at, completed_at, duration_ms, created_at
		FROM job_executions
		WHERE job_id = $1
		ORDER BY created_at DESC`

	rows, err := r.pool.Query(ctx, query, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanExecutions(rows)
}

func (r *ExecutionRepository) List(ctx context.Context, filter ExecutionFilter) ([]*JobExecution, error) {
	query := `
		SELECT id, job_id, job_type, queue, payload, state, attempt, max_retries,
			error_message, stack_trace, worker_id, started_at, completed_at, duration_ms, created_at
		FROM job_executions
		WHERE 1=1`

	args := []interface{}{}
	argPos := 1

	if filter.JobID != nil {
		query += ` AND job_id = $` + itoa(argPos)
		args = append(args, *filter.JobID)
		argPos++
	}
	if filter.JobType != nil {
		query += ` AND job_type = $` + itoa(argPos)
		args = append(args, *filter.JobType)
		argPos++
	}
	if filter.Queue != nil {
		query += ` AND queue = $` + itoa(argPos)
		args = append(args, *filter.Queue)
		argPos++
	}
	if filter.State != nil {
		query += ` AND state = $` + itoa(argPos)
		args = append(args, *filter.State)
		argPos++
	}
	if filter.FromDate != nil {
		query += ` AND created_at >= $` + itoa(argPos)
		args = append(args, *filter.FromDate)
		argPos++
	}
	if filter.ToDate != nil {
		query += ` AND created_at <= $` + itoa(argPos)
		args = append(args, *filter.ToDate)
		argPos++
	}

	query += ` ORDER BY created_at DESC`

	if filter.Limit > 0 {
		query += ` LIMIT $` + itoa(argPos)
		args = append(args, filter.Limit)
		argPos++
	}
	if filter.Offset > 0 {
		query += ` OFFSET $` + itoa(argPos)
		args = append(args, filter.Offset)
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanExecutions(rows)
}

func (r *ExecutionRepository) GetStats(ctx context.Context, fromDate, toDate time.Time) (*ExecutionStats, error) {
	query := `
		SELECT
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE state = 'completed') as completed,
			COUNT(*) FILTER (WHERE state = 'failed') as failed,
			COUNT(*) FILTER (WHERE state = 'dead') as dead,
			AVG(duration_ms) FILTER (WHERE duration_ms IS NOT NULL) as avg_duration_ms
		FROM job_executions
		WHERE created_at BETWEEN $1 AND $2`

	stats := &ExecutionStats{}
	var avgDuration *float64

	err := r.pool.QueryRow(ctx, query, fromDate, toDate).Scan(
		&stats.Total, &stats.Completed, &stats.Failed, &stats.Dead, &avgDuration,
	)
	if err != nil {
		return nil, err
	}

	if avgDuration != nil {
		stats.AvgDurationMs = *avgDuration
	}

	return stats, nil
}

type ExecutionStats struct {
	Total         int64   `json:"total"`
	Completed     int64   `json:"completed"`
	Failed        int64   `json:"failed"`
	Dead          int64   `json:"dead"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

func scanExecutions(rows pgx.Rows) ([]*JobExecution, error) {
	var executions []*JobExecution
	for rows.Next() {
		exec := &JobExecution{}
		err := rows.Scan(
			&exec.ID, &exec.JobID, &exec.JobType, &exec.Queue, &exec.Payload,
			&exec.State, &exec.Attempt, &exec.MaxRetries, &exec.ErrorMessage,
			&exec.StackTrace, &exec.WorkerID, &exec.StartedAt, &exec.CompletedAt,
			&exec.DurationMs, &exec.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		executions = append(executions, exec)
	}
	return executions, rows.Err()
}

func itoa(i int) string {
	return strconv.Itoa(i)
}
