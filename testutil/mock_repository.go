package testutil

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/abdul-hamid-achik/job-queue/pkg/repository"
	"github.com/google/uuid"
)

// MockDLQRepository is a test double for DLQRepository.
type MockDLQRepository struct {
	mu   sync.RWMutex
	jobs map[string]*repository.DeadLetterJob

	ListFunc         func(ctx context.Context, filter repository.DLQFilter) ([]*repository.DeadLetterJob, error)
	GetByIDFunc      func(ctx context.Context, id string) (*repository.DeadLetterJob, error)
	MarkRequeuedFunc func(ctx context.Context, id string) error
	DeleteFunc       func(ctx context.Context, id string) error
	CountFunc        func(ctx context.Context) (int64, error)
}

func NewMockDLQRepository() *MockDLQRepository {
	return &MockDLQRepository{
		jobs: make(map[string]*repository.DeadLetterJob),
	}
}

func (m *MockDLQRepository) Add(ctx context.Context, j *job.Job, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	dlj := &repository.DeadLetterJob{
		ID:                uuid.New().String(),
		JobID:             j.ID,
		JobType:           j.Type,
		Queue:             j.Queue,
		Payload:           j.Payload,
		Priority:          j.Priority.String(),
		ErrorMessage:      errMsg,
		RetryCount:        j.RetryCount,
		MaxRetries:        j.MaxRetries,
		OriginalCreatedAt: j.CreatedAt,
		FailedAt:          time.Now(),
	}
	m.jobs[dlj.ID] = dlj
	return nil
}

func (m *MockDLQRepository) List(ctx context.Context, filter repository.DLQFilter) ([]*repository.DeadLetterJob, error) {
	if m.ListFunc != nil {
		return m.ListFunc(ctx, filter)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var jobs []*repository.DeadLetterJob
	for _, j := range m.jobs {
		if j.RequeuedAt != nil {
			continue
		}
		if filter.JobType != nil && j.JobType != *filter.JobType {
			continue
		}
		if filter.Queue != nil && j.Queue != *filter.Queue {
			continue
		}
		jobs = append(jobs, j)
		if filter.Limit > 0 && len(jobs) >= filter.Limit {
			break
		}
	}
	return jobs, nil
}

func (m *MockDLQRepository) GetByID(ctx context.Context, id string) (*repository.DeadLetterJob, error) {
	if m.GetByIDFunc != nil {
		return m.GetByIDFunc(ctx, id)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if j, ok := m.jobs[id]; ok {
		return j, nil
	}
	return nil, repository.ErrNotFound
}

func (m *MockDLQRepository) MarkRequeued(ctx context.Context, id string) error {
	if m.MarkRequeuedFunc != nil {
		return m.MarkRequeuedFunc(ctx, id)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if j, ok := m.jobs[id]; ok {
		now := time.Now()
		j.RequeuedAt = &now
		j.RequeueCount++
		return nil
	}
	return repository.ErrNotFound
}

func (m *MockDLQRepository) Delete(ctx context.Context, id string) error {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(ctx, id)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.jobs[id]; ok {
		delete(m.jobs, id)
		return nil
	}
	return repository.ErrNotFound
}

func (m *MockDLQRepository) Count(ctx context.Context) (int64, error) {
	if m.CountFunc != nil {
		return m.CountFunc(ctx)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var count int64
	for _, j := range m.jobs {
		if j.RequeuedAt == nil {
			count++
		}
	}
	return count, nil
}

func (m *MockDLQRepository) AddJob(dlj *repository.DeadLetterJob) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[dlj.ID] = dlj
}

// MockExecutionRepository is a test double for ExecutionRepository.
type MockExecutionRepository struct {
	mu         sync.RWMutex
	executions []*repository.JobExecution

	ListFunc       func(ctx context.Context, filter repository.ExecutionFilter) ([]*repository.JobExecution, error)
	GetByJobIDFunc func(ctx context.Context, jobID string) ([]*repository.JobExecution, error)
	GetStatsFunc   func(ctx context.Context, fromDate, toDate time.Time) (*repository.ExecutionStats, error)
}

func NewMockExecutionRepository() *MockExecutionRepository {
	return &MockExecutionRepository{
		executions: make([]*repository.JobExecution, 0),
	}
}

func (m *MockExecutionRepository) Create(ctx context.Context, exec *repository.JobExecution) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	exec.ID = uuid.New().String()
	exec.CreatedAt = time.Now()
	m.executions = append(m.executions, exec)
	return nil
}

func (m *MockExecutionRepository) RecordExecution(ctx context.Context, j *job.Job, workerID string, execErr error) error {
	exec := &repository.JobExecution{
		JobID:      j.ID,
		JobType:    j.Type,
		Queue:      j.Queue,
		Payload:    j.Payload,
		State:      j.State.String(),
		Attempt:    j.RetryCount + 1,
		MaxRetries: j.MaxRetries,
	}
	if workerID != "" {
		exec.WorkerID = &workerID
	}
	if execErr != nil {
		errMsg := execErr.Error()
		exec.ErrorMessage = &errMsg
	}
	return m.Create(ctx, exec)
}

func (m *MockExecutionRepository) List(ctx context.Context, filter repository.ExecutionFilter) ([]*repository.JobExecution, error) {
	if m.ListFunc != nil {
		return m.ListFunc(ctx, filter)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*repository.JobExecution
	for _, e := range m.executions {
		if filter.JobType != nil && e.JobType != *filter.JobType {
			continue
		}
		if filter.State != nil && e.State != *filter.State {
			continue
		}
		result = append(result, e)
		if filter.Limit > 0 && len(result) >= filter.Limit {
			break
		}
	}
	return result, nil
}

func (m *MockExecutionRepository) GetByJobID(ctx context.Context, jobID string) ([]*repository.JobExecution, error) {
	if m.GetByJobIDFunc != nil {
		return m.GetByJobIDFunc(ctx, jobID)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*repository.JobExecution
	for _, e := range m.executions {
		if e.JobID == jobID {
			result = append(result, e)
		}
	}
	return result, nil
}

func (m *MockExecutionRepository) GetStats(ctx context.Context, fromDate, toDate time.Time) (*repository.ExecutionStats, error) {
	if m.GetStatsFunc != nil {
		return m.GetStatsFunc(ctx, fromDate, toDate)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := &repository.ExecutionStats{}
	for _, e := range m.executions {
		if e.CreatedAt.Before(fromDate) || e.CreatedAt.After(toDate) {
			continue
		}
		stats.Total++
		switch e.State {
		case "completed":
			stats.Completed++
		case "failed":
			stats.Failed++
		case "dead":
			stats.Dead++
		}
	}
	return stats, nil
}

func (m *MockExecutionRepository) AddExecution(exec *repository.JobExecution) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executions = append(m.executions, exec)
}

// MockScheduleRepository is a test double for ScheduleRepository.
type MockScheduleRepository struct {
	mu        sync.RWMutex
	schedules map[string]*repository.JobSchedule

	ListActiveFunc    func(ctx context.Context) ([]*repository.JobSchedule, error)
	ListDueFunc       func(ctx context.Context, until time.Time) ([]*repository.JobSchedule, error)
	UpdateNextRunFunc func(ctx context.Context, id string, nextRunAt time.Time) error
	RecordRunFunc     func(ctx context.Context, id string, status string, nextRunAt time.Time) error
}

func NewMockScheduleRepository() *MockScheduleRepository {
	return &MockScheduleRepository{
		schedules: make(map[string]*repository.JobSchedule),
	}
}

func (m *MockScheduleRepository) Create(ctx context.Context, input repository.CreateScheduleInput) (*repository.JobSchedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var payload *json.RawMessage
	if len(input.Payload) > 0 {
		payload = &input.Payload
	}

	schedule := &repository.JobSchedule{
		ID:             uuid.New().String(),
		Name:           input.Name,
		JobType:        input.JobType,
		Payload:        payload,
		CronExpression: input.CronExpression,
		Timezone:       input.Timezone,
		Queue:          input.Queue,
		Priority:       input.Priority,
		MaxRetries:     input.MaxRetries,
		TimeoutSeconds: input.TimeoutSeconds,
		IsActive:       true,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	if schedule.Timezone == "" {
		schedule.Timezone = "UTC"
	}
	if schedule.Queue == "" {
		schedule.Queue = "default"
	}
	if schedule.Priority == "" {
		schedule.Priority = "medium"
	}
	if schedule.MaxRetries == 0 {
		schedule.MaxRetries = 3
	}
	if schedule.TimeoutSeconds == 0 {
		schedule.TimeoutSeconds = 300
	}

	m.schedules[schedule.ID] = schedule
	return schedule, nil
}

func (m *MockScheduleRepository) GetByID(ctx context.Context, id string) (*repository.JobSchedule, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if s, ok := m.schedules[id]; ok {
		return s, nil
	}
	return nil, repository.ErrNotFound
}

func (m *MockScheduleRepository) ListActive(ctx context.Context) ([]*repository.JobSchedule, error) {
	if m.ListActiveFunc != nil {
		return m.ListActiveFunc(ctx)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*repository.JobSchedule
	for _, s := range m.schedules {
		if s.IsActive {
			result = append(result, s)
		}
	}
	return result, nil
}

func (m *MockScheduleRepository) ListDue(ctx context.Context, until time.Time) ([]*repository.JobSchedule, error) {
	if m.ListDueFunc != nil {
		return m.ListDueFunc(ctx, until)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*repository.JobSchedule
	for _, s := range m.schedules {
		if s.IsActive && s.NextRunAt != nil && !s.NextRunAt.After(until) {
			result = append(result, s)
		}
	}
	return result, nil
}

func (m *MockScheduleRepository) UpdateNextRun(ctx context.Context, id string, nextRunAt time.Time) error {
	if m.UpdateNextRunFunc != nil {
		return m.UpdateNextRunFunc(ctx, id, nextRunAt)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.schedules[id]; ok {
		s.NextRunAt = &nextRunAt
		return nil
	}
	return repository.ErrNotFound
}

func (m *MockScheduleRepository) RecordRun(ctx context.Context, id string, status string, nextRunAt time.Time) error {
	if m.RecordRunFunc != nil {
		return m.RecordRunFunc(ctx, id, status, nextRunAt)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.schedules[id]; ok {
		now := time.Now()
		s.LastRunAt = &now
		s.LastRunStatus = &status
		s.NextRunAt = &nextRunAt
		return nil
	}
	return repository.ErrNotFound
}

func (m *MockScheduleRepository) SetActive(ctx context.Context, id string, active bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.schedules[id]; ok {
		s.IsActive = active
		return nil
	}
	return repository.ErrNotFound
}

func (m *MockScheduleRepository) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.schedules[id]; ok {
		delete(m.schedules, id)
		return nil
	}
	return repository.ErrNotFound
}

func (m *MockScheduleRepository) AddSchedule(s *repository.JobSchedule) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schedules[s.ID] = s
}
