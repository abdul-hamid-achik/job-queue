package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var ErrInvalidJob = errors.New("invalid job")

type Job struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`
	Payload     json.RawMessage   `json:"payload"`
	Queue       string            `json:"queue"`
	Priority    Priority          `json:"priority"`
	MaxRetries  int               `json:"max_retries"`
	RetryCount  int               `json:"retry_count"`
	Timeout     time.Duration     `json:"timeout"`
	ScheduledAt *time.Time        `json:"scheduled_at,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	StartedAt   *time.Time        `json:"started_at,omitempty"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	LastError   string            `json:"last_error,omitempty"`
	State       State             `json:"state"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type Option func(*Job)

func New(jobType string, payload any) (*Job, error) {
	if jobType == "" {
		return nil, ErrInvalidJob
	}

	var jobData json.RawMessage
	if payload == nil {
		jobData = json.RawMessage("{}")
	} else {
		var err error
		jobData, err = json.Marshal(payload)
		if err != nil {
			return nil, err
		}
	}

	return &Job{
		ID:         uuid.New().String(),
		Type:       jobType,
		Payload:    jobData,
		Queue:      "default",
		Priority:   PriorityMedium,
		MaxRetries: 3,
		Timeout:    5 * time.Minute,
		State:      StatePending,
		CreatedAt:  time.Now(),
		Metadata:   make(map[string]string),
	}, nil
}

func WithQueue(queue string) Option {
	return func(j *Job) {
		j.Queue = queue
	}
}

func WithPriority(p Priority) Option {
	return func(j *Job) {
		j.Priority = p
	}
}

func WithMaxRetries(n int) Option {
	return func(j *Job) {
		j.MaxRetries = n
	}
}

func WithTimeout(d time.Duration) Option {
	return func(j *Job) {
		j.Timeout = d
	}
}

func WithScheduledAt(t time.Time) Option {
	return func(j *Job) {
		j.ScheduledAt = &t
		j.State = StateScheduled
	}
}

func WithDelay(d time.Duration) Option {
	return func(j *Job) {
		scheduledAt := time.Now().Add(d)
		j.ScheduledAt = &scheduledAt
		j.State = StateScheduled
	}
}

func WithMetadata(key, value string) Option {
	return func(j *Job) {
		if j.Metadata == nil {
			j.Metadata = make(map[string]string)
		}
		j.Metadata[key] = value
	}
}

func NewWithOptions(jobType string, payload any, opts ...Option) (*Job, error) {
	j, err := New(jobType, payload)
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		opt(j)
	}

	return j, nil
}

func (j *Job) Validate() error {
	if j.ID == "" {
		return errors.New("job ID cannot be empty")
	}
	if j.Type == "" {
		return errors.New("job type cannot be empty")
	}
	if j.Queue == "" {
		return errors.New("job queue cannot be empty")
	}
	if j.Timeout <= 0 {
		return errors.New("job timeout must be positive")
	}
	if j.MaxRetries < 0 {
		return errors.New("job max retries cannot be negative")
	}
	return nil
}

func (j *Job) CanRetry() bool {
	return j.RetryCount < j.MaxRetries
}

func (j *Job) IncrementRetry(err error) {
	j.RetryCount++
	if err != nil {
		j.LastError = err.Error()
	}
}

func (j *Job) MarkStarted() error {
	if j.State != StatePending && j.State != StateQueued {
		return fmt.Errorf("cannot start job in state %v", j.State)
	}
	j.State = StateProcessing
	now := time.Now()
	j.StartedAt = &now
	return nil
}

func (j *Job) MarkCompleted() error {
	if j.State != StateProcessing {
		return fmt.Errorf("cannot complete job in state %v", j.State)
	}
	j.State = StateCompleted
	now := time.Now()
	j.CompletedAt = &now
	return nil
}

func (j *Job) MarkFailed(err error) error {
	if j.State == StateCompleted || j.State == StateFailed {
		return fmt.Errorf("cannot fail job in state %v", j.State)
	}
	j.State = StateFailed
	now := time.Now()
	j.CompletedAt = &now
	if err != nil {
		j.LastError = err.Error()
	}
	return nil
}

func (j *Job) MarkDead() error {
	if j.RetryCount < j.MaxRetries {
		return fmt.Errorf("cannot kill this job there still %v retries left", j.MaxRetries-j.RetryCount)
	}
	j.State = StateDead
	now := time.Now()
	j.CompletedAt = &now
	return nil
}

func (j *Job) Duration() time.Duration {
	if j.StartedAt == nil || j.CompletedAt == nil {
		return 0
	}
	return j.CompletedAt.Sub(*j.StartedAt)
}

func (j *Job) IsDelayed() bool {
	return j.ScheduledAt != nil && j.ScheduledAt.After(time.Now())
}

func (j *Job) IsReady() bool {
	return !j.IsDelayed()
}

func (j *Job) UnmarshalPayload(v any) error {
	return json.Unmarshal(j.Payload, v)
}

func (j *Job) Clone() *Job {
	clone := &Job{
		ID:         j.ID,
		Type:       j.Type,
		Queue:      j.Queue,
		Priority:   j.Priority,
		MaxRetries: j.MaxRetries,
		RetryCount: j.RetryCount,
		Timeout:    j.Timeout,
		State:      j.State,
		LastError:  j.LastError,
		CreatedAt:  j.CreatedAt,
	}

	if j.Payload != nil {
		clone.Payload = make(json.RawMessage, len(j.Payload))
		copy(clone.Payload, j.Payload)
	}

	if j.ScheduledAt != nil {
		scheduledAt := *j.ScheduledAt
		clone.ScheduledAt = &scheduledAt
	}

	if j.StartedAt != nil {
		startedAt := *j.StartedAt
		clone.StartedAt = &startedAt
	}

	if j.CompletedAt != nil {
		completedAt := *j.CompletedAt
		clone.CompletedAt = &completedAt
	}

	if j.Metadata != nil {
		clone.Metadata = make(map[string]string, len(j.Metadata))
		for k, v := range j.Metadata {
			clone.Metadata[k] = v
		}
	}

	return clone
}
