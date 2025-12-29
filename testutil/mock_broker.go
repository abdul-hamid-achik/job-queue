package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/broker"
	"github.com/abdul-hamid-achik/job-queue/pkg/job"
)

// MockBroker is a test double for the Broker interface.
type MockBroker struct {
	mu sync.RWMutex

	// Stored jobs
	jobs      map[string]*job.Job
	queued    []*job.Job
	delayed   []*job.Job
	processed []*job.Job
	failed    []*job.Job

	// Function overrides for custom behavior
	EnqueueFunc            func(ctx context.Context, j *job.Job) error
	DequeueFunc            func(ctx context.Context, queues []string, timeout time.Duration) (*job.Job, error)
	AckFunc                func(ctx context.Context, j *job.Job) error
	NackFunc               func(ctx context.Context, j *job.Job, err error) error
	ScheduleFunc           func(ctx context.Context, j *job.Job, at time.Time) error
	GetJobFunc             func(ctx context.Context, jobID string) (*job.Job, error)
	DeleteJobFunc          func(ctx context.Context, jobID string) error
	GetQueueDepthFunc      func(ctx context.Context, queue string) (int64, error)
	GetDelayedJobsFunc     func(ctx context.Context, until time.Time, limit int64) ([]*job.Job, error)
	MoveDelayedToQueueFunc func(ctx context.Context, j *job.Job) error
	GetPendingJobsFunc     func(ctx context.Context, queue string, idleTime time.Duration) ([]*job.Job, error)
	RequeueStaleJobFunc    func(ctx context.Context, j *job.Job) error
	PingFunc               func(ctx context.Context) error
	CloseFunc              func() error
}

// NewMockBroker creates a new MockBroker with default behavior.
func NewMockBroker() *MockBroker {
	return &MockBroker{
		jobs:      make(map[string]*job.Job),
		queued:    make([]*job.Job, 0),
		delayed:   make([]*job.Job, 0),
		processed: make([]*job.Job, 0),
		failed:    make([]*job.Job, 0),
	}
}

// Enqueue adds a job to the mock queue.
func (m *MockBroker) Enqueue(ctx context.Context, j *job.Job) error {
	if m.EnqueueFunc != nil {
		return m.EnqueueFunc(ctx, j)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobs[j.ID] = j
	m.queued = append(m.queued, j)
	return nil
}

// Dequeue returns the next job from the mock queue.
func (m *MockBroker) Dequeue(ctx context.Context, queues []string, timeout time.Duration) (*job.Job, error) {
	if m.DequeueFunc != nil {
		return m.DequeueFunc(ctx, queues, timeout)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.queued) == 0 {
		return nil, broker.ErrQueueEmpty
	}

	j := m.queued[0]
	m.queued = m.queued[1:]
	return j, nil
}

// Ack marks a job as successfully processed.
func (m *MockBroker) Ack(ctx context.Context, j *job.Job) error {
	if m.AckFunc != nil {
		return m.AckFunc(ctx, j)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.processed = append(m.processed, j)
	return nil
}

// Nack marks a job as failed.
func (m *MockBroker) Nack(ctx context.Context, j *job.Job, err error) error {
	if m.NackFunc != nil {
		return m.NackFunc(ctx, j, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.failed = append(m.failed, j)
	return nil
}

// Schedule adds a job for delayed execution.
func (m *MockBroker) Schedule(ctx context.Context, j *job.Job, at time.Time) error {
	if m.ScheduleFunc != nil {
		return m.ScheduleFunc(ctx, j, at)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	j.ScheduledAt = &at
	m.jobs[j.ID] = j
	m.delayed = append(m.delayed, j)
	return nil
}

// GetJob retrieves a job by ID.
func (m *MockBroker) GetJob(ctx context.Context, jobID string) (*job.Job, error) {
	if m.GetJobFunc != nil {
		return m.GetJobFunc(ctx, jobID)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	j, ok := m.jobs[jobID]
	if !ok {
		return nil, broker.ErrJobNotFound
	}
	return j, nil
}

// DeleteJob removes a job.
func (m *MockBroker) DeleteJob(ctx context.Context, jobID string) error {
	if m.DeleteJobFunc != nil {
		return m.DeleteJobFunc(ctx, jobID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.jobs, jobID)
	return nil
}

// GetQueueDepth returns the number of queued jobs.
func (m *MockBroker) GetQueueDepth(ctx context.Context, queue string) (int64, error) {
	if m.GetQueueDepthFunc != nil {
		return m.GetQueueDepthFunc(ctx, queue)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	return int64(len(m.queued)), nil
}

// GetDelayedJobs returns delayed jobs ready for processing.
func (m *MockBroker) GetDelayedJobs(ctx context.Context, until time.Time, limit int64) ([]*job.Job, error) {
	if m.GetDelayedJobsFunc != nil {
		return m.GetDelayedJobsFunc(ctx, until, limit)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var ready []*job.Job
	for _, j := range m.delayed {
		if j.ScheduledAt != nil && !j.ScheduledAt.After(until) {
			ready = append(ready, j)
			if int64(len(ready)) >= limit {
				break
			}
		}
	}
	return ready, nil
}

// MoveDelayedToQueue moves a delayed job to the queue.
func (m *MockBroker) MoveDelayedToQueue(ctx context.Context, j *job.Job) error {
	if m.MoveDelayedToQueueFunc != nil {
		return m.MoveDelayedToQueueFunc(ctx, j)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove from delayed
	for i, dj := range m.delayed {
		if dj.ID == j.ID {
			m.delayed = append(m.delayed[:i], m.delayed[i+1:]...)
			break
		}
	}

	// Add to queued
	j.ScheduledAt = nil
	m.queued = append(m.queued, j)
	return nil
}

// GetPendingJobs returns jobs that are being processed.
func (m *MockBroker) GetPendingJobs(ctx context.Context, queue string, idleTime time.Duration) ([]*job.Job, error) {
	if m.GetPendingJobsFunc != nil {
		return m.GetPendingJobsFunc(ctx, queue, idleTime)
	}
	return nil, nil
}

// RequeueStaleJob requeues a stale job.
func (m *MockBroker) RequeueStaleJob(ctx context.Context, j *job.Job) error {
	if m.RequeueStaleJobFunc != nil {
		return m.RequeueStaleJobFunc(ctx, j)
	}
	return m.Enqueue(ctx, j)
}

// Ping checks the connection.
func (m *MockBroker) Ping(ctx context.Context) error {
	if m.PingFunc != nil {
		return m.PingFunc(ctx)
	}
	return nil
}

// Close closes the broker.
func (m *MockBroker) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

// Test helper methods

// QueuedJobs returns all queued jobs.
func (m *MockBroker) QueuedJobs() []*job.Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.queued
}

// ProcessedJobs returns all successfully processed jobs.
func (m *MockBroker) ProcessedJobs() []*job.Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.processed
}

// FailedJobs returns all failed jobs.
func (m *MockBroker) FailedJobs() []*job.Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.failed
}

// DelayedJobs returns all delayed jobs.
func (m *MockBroker) DelayedJobs() []*job.Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.delayed
}

// Reset clears all state.
func (m *MockBroker) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobs = make(map[string]*job.Job)
	m.queued = make([]*job.Job, 0)
	m.delayed = make([]*job.Job, 0)
	m.processed = make([]*job.Job, 0)
	m.failed = make([]*job.Job, 0)
}
