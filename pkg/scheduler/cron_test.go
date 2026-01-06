package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/abdul-hamid-achik/job-queue/pkg/repository"
	"github.com/abdul-hamid-achik/job-queue/testutil"
	"github.com/rs/zerolog"
)

func TestNewCronScheduler(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	cs := NewCronScheduler(mb, repo)

	if cs.broker != mb {
		t.Error("expected broker to be set")
	}
	if cs.pollInterval != 1*time.Second {
		t.Errorf("expected default poll interval 1s, got %v", cs.pollInterval)
	}
}

func TestNewCronScheduler_WithOptions(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()
	logger := zerolog.Nop()

	cs := NewCronScheduler(mb, repo,
		WithCronPollInterval(500*time.Millisecond),
		WithCronLogger(logger),
	)

	if cs.pollInterval != 500*time.Millisecond {
		t.Errorf("expected poll interval 500ms, got %v", cs.pollInterval)
	}
}

func TestCronScheduler_IsRunning(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	cs := NewCronScheduler(mb, repo)

	if cs.IsRunning() {
		t.Error("expected scheduler not running initially")
	}
}

func TestCronScheduler_StartStop(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		cs.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	if !cs.IsRunning() {
		t.Error("expected scheduler to be running")
	}

	cs.Stop()
	wg.Wait()

	if cs.IsRunning() {
		t.Error("expected scheduler to not be running after stop")
	}
}

func TestCronScheduler_DoubleStart(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		cs.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	wg.Add(1)
	go func() {
		defer wg.Done()
		cs.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	if !cs.IsRunning() {
		t.Error("expected scheduler to be running")
	}

	cs.Stop()
	wg.Wait()
}

func TestCronScheduler_StopBeforeStart(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	cs := NewCronScheduler(mb, repo)

	cs.Stop()

	if cs.IsRunning() {
		t.Error("scheduler should not be running")
	}
}

func TestCronScheduler_InitializeSchedules(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	schedule := &repository.JobSchedule{
		ID:             "sched-1",
		Name:           "test-schedule",
		JobType:        "email.send",
		CronExpression: "*/5 * * * *",
		Timezone:       "UTC",
		Queue:          "default",
		Priority:       "medium",
		MaxRetries:     3,
		TimeoutSeconds: 300,
		IsActive:       true,
	}
	repo.AddSchedule(schedule)

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		cs.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	cs.Stop()
	wg.Wait()

	updated, _ := repo.GetByID(context.Background(), "sched-1")
	if updated.NextRunAt == nil {
		t.Error("expected next_run_at to be set")
	}
}

func TestCronScheduler_InitializeSchedules_InvalidCron(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	schedule := &repository.JobSchedule{
		ID:             "sched-1",
		Name:           "invalid-schedule",
		JobType:        "email.send",
		CronExpression: "invalid cron",
		Timezone:       "UTC",
		Queue:          "default",
		IsActive:       true,
	}
	repo.AddSchedule(schedule)

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		cs.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	cs.Stop()
	wg.Wait()
}

func TestCronScheduler_ProcessDueSchedules(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	pastTime := time.Now().Add(-1 * time.Minute)
	payload := json.RawMessage(`{"to":"test@example.com"}`)
	schedule := &repository.JobSchedule{
		ID:             "sched-1",
		Name:           "due-schedule",
		JobType:        "email.send",
		Payload:        &payload,
		CronExpression: "*/5 * * * *",
		Timezone:       "UTC",
		Queue:          "default",
		Priority:       "high",
		MaxRetries:     3,
		TimeoutSeconds: 300,
		IsActive:       true,
		NextRunAt:      &pastTime,
	}
	repo.AddSchedule(schedule)

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	err := cs.processDueSchedules(context.Background())
	if err != nil {
		t.Fatalf("processDueSchedules failed: %v", err)
	}

	if len(mb.QueuedJobs()) != 1 {
		t.Errorf("expected 1 job enqueued, got %d", len(mb.QueuedJobs()))
	}
}

func TestCronScheduler_ProcessDueSchedules_NoPayload(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	pastTime := time.Now().Add(-1 * time.Minute)
	schedule := &repository.JobSchedule{
		ID:             "sched-1",
		Name:           "no-payload-schedule",
		JobType:        "cleanup.task",
		CronExpression: "*/5 * * * *",
		Timezone:       "UTC",
		Queue:          "default",
		Priority:       "low",
		MaxRetries:     3,
		TimeoutSeconds: 300,
		IsActive:       true,
		NextRunAt:      &pastTime,
	}
	repo.AddSchedule(schedule)

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	err := cs.processDueSchedules(context.Background())
	if err != nil {
		t.Fatalf("processDueSchedules failed: %v", err)
	}

	if len(mb.QueuedJobs()) != 1 {
		t.Errorf("expected 1 job enqueued, got %d", len(mb.QueuedJobs()))
	}
}

func TestCronScheduler_ProcessDueSchedules_ListError(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()
	repo.ListDueFunc = func(ctx context.Context, until time.Time) ([]*repository.JobSchedule, error) {
		return nil, errors.New("db error")
	}

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	err := cs.processDueSchedules(context.Background())
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestCronScheduler_ProcessDueSchedules_EnqueueError(t *testing.T) {
	mb := testutil.NewMockBroker()
	mb.EnqueueFunc = func(ctx context.Context, j *job.Job) error {
		return errors.New("redis error")
	}
	repo := testutil.NewMockScheduleRepository()

	pastTime := time.Now().Add(-1 * time.Minute)
	schedule := &repository.JobSchedule{
		ID:             "sched-1",
		Name:           "test-schedule",
		JobType:        "email.send",
		CronExpression: "*/5 * * * *",
		Timezone:       "UTC",
		Queue:          "default",
		IsActive:       true,
		NextRunAt:      &pastTime,
	}
	repo.AddSchedule(schedule)

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	err := cs.processDueSchedules(context.Background())
	if err != nil {
		t.Errorf("processDueSchedules should not return error for individual job failures: %v", err)
	}
}

func TestCronScheduler_CalculateNextRun(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	cs := NewCronScheduler(mb, repo)

	now := time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC)
	next, err := cs.calculateNextRun("*/5 * * * *", "UTC", now)
	if err != nil {
		t.Fatalf("calculateNextRun failed: %v", err)
	}

	expected := time.Date(2025, 1, 6, 10, 5, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Errorf("expected next run at %v, got %v", expected, next)
	}
}

func TestCronScheduler_CalculateNextRun_InvalidCron(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	cs := NewCronScheduler(mb, repo)

	_, err := cs.calculateNextRun("invalid", "UTC", time.Now())
	if err == nil {
		t.Error("expected error for invalid cron expression")
	}
}

func TestCronScheduler_CalculateNextRun_InvalidTimezone(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	cs := NewCronScheduler(mb, repo)

	_, err := cs.calculateNextRun("*/5 * * * *", "Invalid/Timezone", time.Now())
	if err != nil {
		t.Errorf("should fall back to UTC for invalid timezone: %v", err)
	}
}

func TestCronScheduler_CalculateNextRun_WithTimezone(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	cs := NewCronScheduler(mb, repo)

	now := time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC)
	next, err := cs.calculateNextRun("0 9 * * *", "America/New_York", now)
	if err != nil {
		t.Fatalf("calculateNextRun failed: %v", err)
	}

	if next.IsZero() {
		t.Error("expected valid next run time")
	}
}

func TestCronScheduler_ContextCancellation(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		cs.Start(ctx)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)

	if !cs.IsRunning() {
		t.Error("expected scheduler to be running")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("scheduler did not stop after context cancellation")
	}

	if cs.IsRunning() {
		t.Error("scheduler should not be running after context cancellation")
	}
}

func TestCronScheduler_InitializeSchedules_Error(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()
	repo.ListActiveFunc = func(ctx context.Context) ([]*repository.JobSchedule, error) {
		return nil, errors.New("db error")
	}

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		cs.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	cs.Stop()
	wg.Wait()
}

func TestCronScheduler_UpdateNextRunError(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	schedule := &repository.JobSchedule{
		ID:             "sched-1",
		Name:           "test-schedule",
		JobType:        "email.send",
		CronExpression: "*/5 * * * *",
		Timezone:       "UTC",
		Queue:          "default",
		IsActive:       true,
	}
	repo.AddSchedule(schedule)

	repo.UpdateNextRunFunc = func(ctx context.Context, id string, nextRunAt time.Time) error {
		return errors.New("db error")
	}

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		cs.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	cs.Stop()
	wg.Wait()
}

func TestCronScheduler_RecordRunError(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	pastTime := time.Now().Add(-1 * time.Minute)
	schedule := &repository.JobSchedule{
		ID:             "sched-1",
		Name:           "test-schedule",
		JobType:        "email.send",
		CronExpression: "*/5 * * * *",
		Timezone:       "UTC",
		Queue:          "default",
		IsActive:       true,
		NextRunAt:      &pastTime,
	}
	repo.AddSchedule(schedule)

	repo.RecordRunFunc = func(ctx context.Context, id string, status string, nextRunAt time.Time) error {
		return errors.New("db error")
	}

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	err := cs.processDueSchedules(context.Background())
	if err != nil {
		t.Errorf("should not return error for record run failures: %v", err)
	}
}

func TestCronScheduler_ExecuteSchedule_InvalidNextCron(t *testing.T) {
	mb := testutil.NewMockBroker()
	repo := testutil.NewMockScheduleRepository()

	pastTime := time.Now().Add(-1 * time.Minute)
	schedule := &repository.JobSchedule{
		ID:             "sched-1",
		Name:           "test-schedule",
		JobType:        "email.send",
		CronExpression: "invalid",
		Timezone:       "UTC",
		Queue:          "default",
		IsActive:       true,
		NextRunAt:      &pastTime,
	}

	cs := NewCronScheduler(mb, repo, WithCronPollInterval(50*time.Millisecond))

	err := cs.executeSchedule(context.Background(), schedule, time.Now())
	if err != nil {
		t.Errorf("should handle invalid cron gracefully: %v", err)
	}
}
