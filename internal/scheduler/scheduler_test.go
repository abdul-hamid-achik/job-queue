package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/abdul-hamid-achik/job-queue/internal/job"
	"github.com/abdul-hamid-achik/job-queue/testutil"
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
