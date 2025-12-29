package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("record not found")

type DeadLetterJob struct {
	ID                string           `json:"id"`
	JobID             string           `json:"job_id"`
	JobType           string           `json:"job_type"`
	Queue             string           `json:"queue"`
	Payload           json.RawMessage  `json:"payload"`
	Priority          string           `json:"priority"`
	ErrorMessage      string           `json:"error_message"`
	StackTrace        *string          `json:"stack_trace,omitempty"`
	RetryCount        int              `json:"retry_count"`
	MaxRetries        int              `json:"max_retries"`
	OriginalCreatedAt time.Time        `json:"original_created_at"`
	FailedAt          time.Time        `json:"failed_at"`
	RequeuedAt        *time.Time       `json:"requeued_at,omitempty"`
	RequeueCount      int              `json:"requeue_count"`
	Metadata          *json.RawMessage `json:"metadata,omitempty"`
}

type DLQFilter struct {
	JobType  *string
	Queue    *string
	FromDate *time.Time
	ToDate   *time.Time
	Limit    int
	Offset   int
}

type DLQRepository struct {
	pool *pgxpool.Pool
}

func NewDLQRepository(pool *pgxpool.Pool) *DLQRepository {
	return &DLQRepository{pool: pool}
}

func (r *DLQRepository) Add(ctx context.Context, j *job.Job, errMsg string) error {
	var metadata *json.RawMessage
	if len(j.Metadata) > 0 {
		m, _ := json.Marshal(j.Metadata)
		raw := json.RawMessage(m)
		metadata = &raw
	}

	query := `
		INSERT INTO dead_letter_queue (
			job_id, job_type, queue, payload, priority, error_message,
			retry_count, max_retries, original_created_at, metadata
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (job_id) DO UPDATE SET
			error_message = EXCLUDED.error_message,
			retry_count = EXCLUDED.retry_count,
			failed_at = NOW()`

	_, err := r.pool.Exec(ctx, query,
		j.ID, j.Type, j.Queue, j.Payload, j.Priority.String(),
		errMsg, j.RetryCount, j.MaxRetries, j.CreatedAt, metadata,
	)
	return err
}

func (r *DLQRepository) AddFromJob(ctx context.Context, j *job.Job) error {
	return r.Add(ctx, j, j.LastError)
}

func (r *DLQRepository) GetByID(ctx context.Context, id string) (*DeadLetterJob, error) {
	query := `
		SELECT id, job_id, job_type, queue, payload, priority, error_message,
			stack_trace, retry_count, max_retries, original_created_at,
			failed_at, requeued_at, requeue_count, metadata
		FROM dead_letter_queue
		WHERE id = $1`

	dlj := &DeadLetterJob{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&dlj.ID, &dlj.JobID, &dlj.JobType, &dlj.Queue, &dlj.Payload,
		&dlj.Priority, &dlj.ErrorMessage, &dlj.StackTrace, &dlj.RetryCount,
		&dlj.MaxRetries, &dlj.OriginalCreatedAt, &dlj.FailedAt,
		&dlj.RequeuedAt, &dlj.RequeueCount, &dlj.Metadata,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return dlj, err
}

func (r *DLQRepository) GetByJobID(ctx context.Context, jobID string) (*DeadLetterJob, error) {
	query := `
		SELECT id, job_id, job_type, queue, payload, priority, error_message,
			stack_trace, retry_count, max_retries, original_created_at,
			failed_at, requeued_at, requeue_count, metadata
		FROM dead_letter_queue
		WHERE job_id = $1`

	dlj := &DeadLetterJob{}
	err := r.pool.QueryRow(ctx, query, jobID).Scan(
		&dlj.ID, &dlj.JobID, &dlj.JobType, &dlj.Queue, &dlj.Payload,
		&dlj.Priority, &dlj.ErrorMessage, &dlj.StackTrace, &dlj.RetryCount,
		&dlj.MaxRetries, &dlj.OriginalCreatedAt, &dlj.FailedAt,
		&dlj.RequeuedAt, &dlj.RequeueCount, &dlj.Metadata,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return dlj, err
}

func (r *DLQRepository) List(ctx context.Context, filter DLQFilter) ([]*DeadLetterJob, error) {
	query := `
		SELECT id, job_id, job_type, queue, payload, priority, error_message,
			stack_trace, retry_count, max_retries, original_created_at,
			failed_at, requeued_at, requeue_count, metadata
		FROM dead_letter_queue
		WHERE requeued_at IS NULL`

	args := []interface{}{}
	argPos := 1

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
	if filter.FromDate != nil {
		query += ` AND failed_at >= $` + itoa(argPos)
		args = append(args, *filter.FromDate)
		argPos++
	}
	if filter.ToDate != nil {
		query += ` AND failed_at <= $` + itoa(argPos)
		args = append(args, *filter.ToDate)
		argPos++
	}

	query += ` ORDER BY failed_at DESC`

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

	var jobs []*DeadLetterJob
	for rows.Next() {
		dlj := &DeadLetterJob{}
		err := rows.Scan(
			&dlj.ID, &dlj.JobID, &dlj.JobType, &dlj.Queue, &dlj.Payload,
			&dlj.Priority, &dlj.ErrorMessage, &dlj.StackTrace, &dlj.RetryCount,
			&dlj.MaxRetries, &dlj.OriginalCreatedAt, &dlj.FailedAt,
			&dlj.RequeuedAt, &dlj.RequeueCount, &dlj.Metadata,
		)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, dlj)
	}

	return jobs, rows.Err()
}

func (r *DLQRepository) MarkRequeued(ctx context.Context, id string) error {
	query := `
		UPDATE dead_letter_queue
		SET requeued_at = NOW(), requeue_count = requeue_count + 1
		WHERE id = $1`

	result, err := r.pool.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *DLQRepository) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM dead_letter_queue WHERE id = $1`
	result, err := r.pool.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *DLQRepository) Count(ctx context.Context) (int64, error) {
	var count int64
	err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dead_letter_queue WHERE requeued_at IS NULL`).Scan(&count)
	return count, err
}

func (dlj *DeadLetterJob) ToJob() *job.Job {
	return &job.Job{
		ID:         dlj.JobID,
		Type:       dlj.JobType,
		Queue:      dlj.Queue,
		Payload:    dlj.Payload,
		Priority:   job.ParsePriority(dlj.Priority),
		MaxRetries: dlj.MaxRetries,
		RetryCount: 0,
		CreatedAt:  dlj.OriginalCreatedAt,
		State:      job.StatePending,
	}
}
