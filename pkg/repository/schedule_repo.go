package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobSchedule struct {
	ID             string           `json:"id"`
	Name           string           `json:"name"`
	JobType        string           `json:"job_type"`
	Payload        *json.RawMessage `json:"payload,omitempty"`
	CronExpression string           `json:"cron_expression"`
	Timezone       string           `json:"timezone"`
	Queue          string           `json:"queue"`
	Priority       string           `json:"priority"`
	MaxRetries     int              `json:"max_retries"`
	TimeoutSeconds int              `json:"timeout_seconds"`
	IsActive       bool             `json:"is_active"`
	LastRunAt      *time.Time       `json:"last_run_at,omitempty"`
	LastRunStatus  *string          `json:"last_run_status,omitempty"`
	NextRunAt      *time.Time       `json:"next_run_at,omitempty"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
}

type CreateScheduleInput struct {
	Name           string
	JobType        string
	Payload        json.RawMessage
	CronExpression string
	Timezone       string
	Queue          string
	Priority       string
	MaxRetries     int
	TimeoutSeconds int
}

type ScheduleRepository struct {
	pool *pgxpool.Pool
}

func NewScheduleRepository(pool *pgxpool.Pool) *ScheduleRepository {
	return &ScheduleRepository{pool: pool}
}

func (r *ScheduleRepository) Create(ctx context.Context, input CreateScheduleInput) (*JobSchedule, error) {
	if input.Timezone == "" {
		input.Timezone = "UTC"
	}
	if input.Queue == "" {
		input.Queue = "default"
	}
	if input.Priority == "" {
		input.Priority = "medium"
	}
	if input.MaxRetries == 0 {
		input.MaxRetries = 3
	}
	if input.TimeoutSeconds == 0 {
		input.TimeoutSeconds = 300
	}

	var payload *json.RawMessage
	if len(input.Payload) > 0 {
		payload = &input.Payload
	}

	query := `
		INSERT INTO job_schedules (
			name, job_type, payload, cron_expression, timezone,
			queue, priority, max_retries, timeout_seconds
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id, is_active, created_at, updated_at`

	schedule := &JobSchedule{
		Name:           input.Name,
		JobType:        input.JobType,
		Payload:        payload,
		CronExpression: input.CronExpression,
		Timezone:       input.Timezone,
		Queue:          input.Queue,
		Priority:       input.Priority,
		MaxRetries:     input.MaxRetries,
		TimeoutSeconds: input.TimeoutSeconds,
	}

	err := r.pool.QueryRow(ctx, query,
		input.Name, input.JobType, payload, input.CronExpression,
		input.Timezone, input.Queue, input.Priority, input.MaxRetries, input.TimeoutSeconds,
	).Scan(&schedule.ID, &schedule.IsActive, &schedule.CreatedAt, &schedule.UpdatedAt)

	return schedule, err
}

func (r *ScheduleRepository) GetByID(ctx context.Context, id string) (*JobSchedule, error) {
	return r.getOne(ctx, `WHERE id = $1`, id)
}

func (r *ScheduleRepository) GetByName(ctx context.Context, name string) (*JobSchedule, error) {
	return r.getOne(ctx, `WHERE name = $1`, name)
}

func (r *ScheduleRepository) getOne(ctx context.Context, where string, arg interface{}) (*JobSchedule, error) {
	query := `
		SELECT id, name, job_type, payload, cron_expression, timezone,
			queue, priority, max_retries, timeout_seconds, is_active,
			last_run_at, last_run_status, next_run_at, created_at, updated_at
		FROM job_schedules ` + where

	s := &JobSchedule{}
	err := r.pool.QueryRow(ctx, query, arg).Scan(
		&s.ID, &s.Name, &s.JobType, &s.Payload, &s.CronExpression,
		&s.Timezone, &s.Queue, &s.Priority, &s.MaxRetries, &s.TimeoutSeconds,
		&s.IsActive, &s.LastRunAt, &s.LastRunStatus, &s.NextRunAt,
		&s.CreatedAt, &s.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return s, err
}

func (r *ScheduleRepository) ListActive(ctx context.Context) ([]*JobSchedule, error) {
	query := `
		SELECT id, name, job_type, payload, cron_expression, timezone,
			queue, priority, max_retries, timeout_seconds, is_active,
			last_run_at, last_run_status, next_run_at, created_at, updated_at
		FROM job_schedules
		WHERE is_active = true
		ORDER BY name`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanSchedules(rows)
}

func (r *ScheduleRepository) ListDue(ctx context.Context, until time.Time) ([]*JobSchedule, error) {
	query := `
		SELECT id, name, job_type, payload, cron_expression, timezone,
			queue, priority, max_retries, timeout_seconds, is_active,
			last_run_at, last_run_status, next_run_at, created_at, updated_at
		FROM job_schedules
		WHERE is_active = true AND next_run_at <= $1
		ORDER BY next_run_at`

	rows, err := r.pool.Query(ctx, query, until)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanSchedules(rows)
}

func (r *ScheduleRepository) UpdateNextRun(ctx context.Context, id string, nextRunAt time.Time) error {
	query := `UPDATE job_schedules SET next_run_at = $1 WHERE id = $2`
	_, err := r.pool.Exec(ctx, query, nextRunAt, id)
	return err
}

func (r *ScheduleRepository) RecordRun(ctx context.Context, id string, status string, nextRunAt time.Time) error {
	query := `
		UPDATE job_schedules
		SET last_run_at = NOW(), last_run_status = $1, next_run_at = $2
		WHERE id = $3`
	_, err := r.pool.Exec(ctx, query, status, nextRunAt, id)
	return err
}

func (r *ScheduleRepository) SetActive(ctx context.Context, id string, active bool) error {
	query := `UPDATE job_schedules SET is_active = $1 WHERE id = $2`
	result, err := r.pool.Exec(ctx, query, active, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *ScheduleRepository) Delete(ctx context.Context, id string) error {
	result, err := r.pool.Exec(ctx, `DELETE FROM job_schedules WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func scanSchedules(rows pgx.Rows) ([]*JobSchedule, error) {
	var schedules []*JobSchedule
	for rows.Next() {
		s := &JobSchedule{}
		err := rows.Scan(
			&s.ID, &s.Name, &s.JobType, &s.Payload, &s.CronExpression,
			&s.Timezone, &s.Queue, &s.Priority, &s.MaxRetries, &s.TimeoutSeconds,
			&s.IsActive, &s.LastRunAt, &s.LastRunStatus, &s.NextRunAt,
			&s.CreatedAt, &s.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, s)
	}
	return schedules, rows.Err()
}
