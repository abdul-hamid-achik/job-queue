package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/abdul-hamid-achik/job-queue/testutil"
	"github.com/rs/zerolog"
)

func TestNew(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb)

	if s.broker != mb {
		t.Error("expected broker to be set")
	}
	if s.pollInterval != 1*time.Second {
		t.Errorf("expected default poll interval 1s, got %v", s.pollInterval)
	}
	if s.batchSize != 100 {
		t.Errorf("expected default batch size 100, got %d", s.batchSize)
	}
	if s.staleJobTimeout != 5*time.Minute {
		t.Errorf("expected default stale timeout 5m, got %v", s.staleJobTimeout)
	}
}

func TestNewWithOptions(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb,
		WithSchedulerPollInterval(500*time.Millisecond),
		WithBatchSize(50),
		WithStaleJobTimeout(2*time.Minute),
	)

	if s.pollInterval != 500*time.Millisecond {
		t.Errorf("expected poll interval 500ms, got %v", s.pollInterval)
	}
	if s.batchSize != 50 {
		t.Errorf("expected batch size 50, got %d", s.batchSize)
	}
	if s.staleJobTimeout != 2*time.Minute {
		t.Errorf("expected stale timeout 2m, got %v", s.staleJobTimeout)
	}
}

func TestScheduler_IsRunning(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb, WithSchedulerPollInterval(50*time.Millisecond))

	if s.IsRunning() {
		t.Error("expected scheduler not running initially")
	}
}

func TestScheduler_StartStop(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb, WithSchedulerPollInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		s.Start(ctx)
	}()

	// Wait for scheduler to start
	time.Sleep(100 * time.Millisecond)

	if !s.IsRunning() {
		t.Error("expected scheduler to be running")
	}

	// Stop the scheduler
	s.Stop()

	// Wait for it to fully stop
	wg.Wait()

	if s.IsRunning() {
		t.Error("expected scheduler to not be running after stop")
	}
}

func TestScheduler_ProcessDelayedJobs(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb, WithSchedulerPollInterval(50*time.Millisecond))

	// Schedule a job to run in the past
	j := testutil.NewTestJob()
	pastTime := time.Now().Add(-1 * time.Minute)
	mb.Schedule(context.Background(), j, pastTime)

	// Verify job is in delayed queue
	if len(mb.DelayedJobs()) != 1 {
		t.Errorf("expected 1 delayed job, got %d", len(mb.DelayedJobs()))
	}
	if len(mb.QueuedJobs()) != 0 {
		t.Errorf("expected 0 queued jobs, got %d", len(mb.QueuedJobs()))
	}

	// Process delayed jobs
	err := s.processDelayedJobs(context.Background())
	if err != nil {
		t.Fatalf("processDelayedJobs failed: %v", err)
	}

	// Verify job moved to queue
	if len(mb.DelayedJobs()) != 0 {
		t.Errorf("expected 0 delayed jobs after processing, got %d", len(mb.DelayedJobs()))
	}
	if len(mb.QueuedJobs()) != 1 {
		t.Errorf("expected 1 queued job after processing, got %d", len(mb.QueuedJobs()))
	}
}

func TestScheduler_ProcessDelayedJobs_NotDue(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb, WithSchedulerPollInterval(50*time.Millisecond))

	// Schedule a job to run in the future
	j := testutil.NewTestJob()
	futureTime := time.Now().Add(1 * time.Hour)
	mb.Schedule(context.Background(), j, futureTime)

	// Verify job is in delayed queue
	if len(mb.DelayedJobs()) != 1 {
		t.Errorf("expected 1 delayed job, got %d", len(mb.DelayedJobs()))
	}

	// Process delayed jobs
	err := s.processDelayedJobs(context.Background())
	if err != nil {
		t.Fatalf("processDelayedJobs failed: %v", err)
	}

	// Verify job is still delayed (not moved)
	if len(mb.DelayedJobs()) != 1 {
		t.Errorf("expected 1 delayed job (not due), got %d", len(mb.DelayedJobs()))
	}
	if len(mb.QueuedJobs()) != 0 {
		t.Errorf("expected 0 queued jobs, got %d", len(mb.QueuedJobs()))
	}
}

func TestScheduler_ProcessStaleJobs(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb,
		WithSchedulerPollInterval(50*time.Millisecond),
		WithStaleJobTimeout(1*time.Minute),
	)

	// Create a "stale" pending job
	j := testutil.NewTestJob()
	j.State = job.StateProcessing

	// Mock GetPendingJobs to return our stale job
	mb.GetPendingJobsFunc = func(ctx context.Context, queue string, idleTime time.Duration) ([]*job.Job, error) {
		if queue == "default" {
			return []*job.Job{j}, nil
		}
		return nil, nil
	}

	// Track requeued jobs
	var requeuedJobs []*job.Job
	mb.RequeueStaleJobFunc = func(ctx context.Context, job *job.Job) error {
		requeuedJobs = append(requeuedJobs, job)
		return nil
	}

	// Process stale jobs
	err := s.processStaleJobs(context.Background())
	if err != nil {
		t.Fatalf("processStaleJobs failed: %v", err)
	}

	// Verify the stale job was requeued
	if len(requeuedJobs) != 1 {
		t.Errorf("expected 1 requeued job, got %d", len(requeuedJobs))
	}
	if len(requeuedJobs) > 0 && requeuedJobs[0].ID != j.ID {
		t.Errorf("expected requeued job ID %s, got %s", j.ID, requeuedJobs[0].ID)
	}
}

func TestScheduler_DoubleStart(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb, WithSchedulerPollInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Start first instance
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Start(ctx)
	}()

	// Wait for first start
	time.Sleep(100 * time.Millisecond)

	// Try to start again (should be no-op)
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Start(ctx)
	}()

	// Give time for the second start to return
	time.Sleep(100 * time.Millisecond)

	if !s.IsRunning() {
		t.Error("expected scheduler to be running")
	}

	// Stop
	s.Stop()
	wg.Wait()
}

func TestScheduler_MultipleDelayedJobs(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb,
		WithSchedulerPollInterval(50*time.Millisecond),
		WithBatchSize(10),
	)

	// Schedule multiple jobs with different times
	pastTime := time.Now().Add(-1 * time.Minute)
	futureTime := time.Now().Add(1 * time.Hour)

	j1 := testutil.NewTestJob()
	j2 := testutil.NewTestJob()
	j3 := testutil.NewTestJob()

	mb.Schedule(context.Background(), j1, pastTime)
	mb.Schedule(context.Background(), j2, pastTime)
	mb.Schedule(context.Background(), j3, futureTime)

	// Verify all jobs are in delayed queue
	if len(mb.DelayedJobs()) != 3 {
		t.Errorf("expected 3 delayed jobs, got %d", len(mb.DelayedJobs()))
	}

	// Process delayed jobs
	err := s.processDelayedJobs(context.Background())
	if err != nil {
		t.Fatalf("processDelayedJobs failed: %v", err)
	}

	// Verify only due jobs were moved
	if len(mb.DelayedJobs()) != 1 {
		t.Errorf("expected 1 delayed job remaining, got %d", len(mb.DelayedJobs()))
	}
	if len(mb.QueuedJobs()) != 2 {
		t.Errorf("expected 2 queued jobs, got %d", len(mb.QueuedJobs()))
	}
}

func TestScheduler_ProcessDelayedJobs_Error(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb, WithSchedulerPollInterval(50*time.Millisecond))

	// Simulate error getting delayed jobs
	expectedErr := errors.New("redis connection error")
	mb.GetDelayedJobsFunc = func(ctx context.Context, until time.Time, limit int64) ([]*job.Job, error) {
		return nil, expectedErr
	}

	err := s.processDelayedJobs(context.Background())
	if err == nil {
		t.Error("expected error, got nil")
	}
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestScheduler_ProcessDelayedJobs_MoveError(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb, WithSchedulerPollInterval(50*time.Millisecond))

	// Schedule a job in the past
	j := testutil.NewTestJob()
	pastTime := time.Now().Add(-1 * time.Minute)
	mb.Schedule(context.Background(), j, pastTime)

	// Simulate error moving job
	mb.MoveDelayedToQueueFunc = func(ctx context.Context, job *job.Job) error {
		return errors.New("move failed")
	}

	// Should not return error, just log it and continue
	err := s.processDelayedJobs(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Job should still be in delayed queue since move failed
	if len(mb.DelayedJobs()) != 1 {
		t.Errorf("expected job to remain in delayed queue, got %d jobs", len(mb.DelayedJobs()))
	}
}

func TestScheduler_ProcessStaleJobs_GetPendingError(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb,
		WithSchedulerPollInterval(50*time.Millisecond),
		WithStaleJobTimeout(1*time.Minute),
	)

	// Simulate error getting pending jobs
	mb.GetPendingJobsFunc = func(ctx context.Context, queue string, idleTime time.Duration) ([]*job.Job, error) {
		return nil, errors.New("redis error")
	}

	// Should not return error, just log and continue
	err := s.processStaleJobs(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestScheduler_ProcessStaleJobs_RequeueError(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb,
		WithSchedulerPollInterval(50*time.Millisecond),
		WithStaleJobTimeout(1*time.Minute),
	)

	staleJob := testutil.NewTestJob()
	staleJob.State = job.StateProcessing

	mb.GetPendingJobsFunc = func(ctx context.Context, queue string, idleTime time.Duration) ([]*job.Job, error) {
		if queue == "default" {
			return []*job.Job{staleJob}, nil
		}
		return nil, nil
	}

	mb.RequeueStaleJobFunc = func(ctx context.Context, j *job.Job) error {
		return errors.New("requeue failed")
	}

	// Should not return error, just log and continue
	err := s.processStaleJobs(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestScheduler_StopBeforeStart(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb, WithSchedulerPollInterval(50*time.Millisecond))

	// Should not panic when stopping before starting
	s.Stop()

	if s.IsRunning() {
		t.Error("scheduler should not be running")
	}
}

func TestScheduler_WithLogger(t *testing.T) {
	mb := testutil.NewMockBroker()
	logger := zerolog.Nop()

	s := New(mb, WithSchedulerLogger(logger))

	if s.logger.GetLevel() != zerolog.Disabled {
		t.Error("expected nop logger to be set")
	}
}

func TestScheduler_BatchSizeLimit(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb,
		WithSchedulerPollInterval(50*time.Millisecond),
		WithBatchSize(2), // Only process 2 at a time
	)

	// Schedule 5 jobs in the past
	pastTime := time.Now().Add(-1 * time.Minute)
	for i := 0; i < 5; i++ {
		j := testutil.NewTestJob()
		mb.Schedule(context.Background(), j, pastTime)
	}

	if len(mb.DelayedJobs()) != 5 {
		t.Fatalf("expected 5 delayed jobs, got %d", len(mb.DelayedJobs()))
	}

	// First process should only move 2 (batch size limit)
	err := s.processDelayedJobs(context.Background())
	if err != nil {
		t.Fatalf("processDelayedJobs failed: %v", err)
	}

	// Should have moved 2 jobs
	if len(mb.QueuedJobs()) != 2 {
		t.Errorf("expected 2 queued jobs after first batch, got %d", len(mb.QueuedJobs()))
	}
	if len(mb.DelayedJobs()) != 3 {
		t.Errorf("expected 3 delayed jobs remaining, got %d", len(mb.DelayedJobs()))
	}

	// Process again
	err = s.processDelayedJobs(context.Background())
	if err != nil {
		t.Fatalf("processDelayedJobs failed: %v", err)
	}

	// Should have moved 2 more
	if len(mb.QueuedJobs()) != 4 {
		t.Errorf("expected 4 queued jobs after second batch, got %d", len(mb.QueuedJobs()))
	}
}

func TestScheduler_ContextCancellation(t *testing.T) {
	mb := testutil.NewMockBroker()
	s := New(mb, WithSchedulerPollInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(done)
	}()

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)

	if !s.IsRunning() {
		t.Error("expected scheduler to be running")
	}

	// Cancel context
	cancel()

	// Should stop within reasonable time
	select {
	case <-done:
		// Good
	case <-time.After(2 * time.Second):
		t.Error("scheduler did not stop after context cancellation")
	}

	if s.IsRunning() {
		t.Error("scheduler should not be running after context cancellation")
	}
}
