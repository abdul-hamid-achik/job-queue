package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getDBPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("PostgreSQL not available: %v", err)
	}

	return pool
}

func cleanupDB(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	pool.Exec(ctx, "DELETE FROM job_executions")
	pool.Exec(ctx, "DELETE FROM dead_letter_queue")
	pool.Exec(ctx, "DELETE FROM job_schedules")
}

// DLQ Repository Tests

func TestDLQRepository_Integration_Add(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool := getDBPool(t)
	defer pool.Close()
	cleanupDB(t, pool)
	defer cleanupDB(t, pool)

	repo := NewDLQRepository(pool)
	ctx := context.Background()

	j, _ := job.NewWithOptions("test.dlq", map[string]string{"key": "value"})
	j.RetryCount = 3
	j.MaxRetries = 3

	err := repo.Add(ctx, j, "test error message")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Verify it was added
	dlj, err := repo.GetByJobID(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetByJobID failed: %v", err)
	}

	if dlj.JobID != j.ID {
		t.Errorf("expected job ID %s, got %s", j.ID, dlj.JobID)
	}
	if dlj.JobType != "test.dlq" {
		t.Errorf("expected job type 'test.dlq', got %s", dlj.JobType)
	}
	if dlj.ErrorMessage != "test error message" {
		t.Errorf("expected error 'test error message', got %s", dlj.ErrorMessage)
	}
}

func TestDLQRepository_Integration_List(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool := getDBPool(t)
	defer pool.Close()
	cleanupDB(t, pool)
	defer cleanupDB(t, pool)

	repo := NewDLQRepository(pool)
	ctx := context.Background()

	// Add multiple jobs
	for i := 0; i < 5; i++ {
		j, _ := job.NewWithOptions("test.list", nil)
		repo.Add(ctx, j, "error")
	}

	// List with limit
	jobs, err := repo.List(ctx, DLQFilter{Limit: 3})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(jobs) != 3 {
		t.Errorf("expected 3 jobs, got %d", len(jobs))
	}
}

func TestDLQRepository_Integration_MarkRequeued(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool := getDBPool(t)
	defer pool.Close()
	cleanupDB(t, pool)
	defer cleanupDB(t, pool)

	repo := NewDLQRepository(pool)
	ctx := context.Background()

	j, _ := job.NewWithOptions("test.requeue", nil)
	repo.Add(ctx, j, "error")

	dlj, _ := repo.GetByJobID(ctx, j.ID)

	err := repo.MarkRequeued(ctx, dlj.ID)
	if err != nil {
		t.Fatalf("MarkRequeued failed: %v", err)
	}

	// Verify it was marked
	updated, _ := repo.GetByID(ctx, dlj.ID)
	if updated.RequeuedAt == nil {
		t.Error("expected RequeuedAt to be set")
	}
	if updated.RequeueCount != 1 {
		t.Errorf("expected RequeueCount 1, got %d", updated.RequeueCount)
	}
}

func TestDLQRepository_Integration_Delete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool := getDBPool(t)
	defer pool.Close()
	cleanupDB(t, pool)
	defer cleanupDB(t, pool)

	repo := NewDLQRepository(pool)
	ctx := context.Background()

	j, _ := job.NewWithOptions("test.delete", nil)
	repo.Add(ctx, j, "error")

	dlj, _ := repo.GetByJobID(ctx, j.ID)

	err := repo.Delete(ctx, dlj.ID)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it was deleted
	_, err = repo.GetByID(ctx, dlj.ID)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestDLQRepository_Integration_Count(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool := getDBPool(t)
	defer pool.Close()
	cleanupDB(t, pool)
	defer cleanupDB(t, pool)

	repo := NewDLQRepository(pool)
	ctx := context.Background()

	// Add 3 jobs
	for i := 0; i < 3; i++ {
		j, _ := job.NewWithOptions("test.count", nil)
		repo.Add(ctx, j, "error")
	}

	count, err := repo.Count(ctx)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}

	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
}

func TestDLQRepository_Integration_ToJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool := getDBPool(t)
	defer pool.Close()
	cleanupDB(t, pool)
	defer cleanupDB(t, pool)

	repo := NewDLQRepository(pool)
	ctx := context.Background()

	original, _ := job.NewWithOptions("test.tojob", map[string]string{"key": "value"},
		job.WithQueue("critical"),
		job.WithPriority(job.PriorityHigh),
		job.WithMaxRetries(5),
	)
	repo.Add(ctx, original, "error")

	dlj, _ := repo.GetByJobID(ctx, original.ID)
	converted := dlj.ToJob()

	if converted.ID != original.ID {
		t.Errorf("expected ID %s, got %s", original.ID, converted.ID)
	}
	if converted.Type != original.Type {
		t.Errorf("expected Type %s, got %s", original.Type, converted.Type)
	}
	if converted.Queue != original.Queue {
		t.Errorf("expected Queue %s, got %s", original.Queue, converted.Queue)
	}
	if converted.Priority != original.Priority {
		t.Errorf("expected Priority %s, got %s", original.Priority, converted.Priority)
	}
	if converted.RetryCount != 0 {
		t.Errorf("expected RetryCount 0 (reset), got %d", converted.RetryCount)
	}
	if converted.State != job.StatePending {
		t.Errorf("expected State Pending, got %s", converted.State)
	}
}

// Execution Repository Tests

func TestExecutionRepository_Integration_Create(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool := getDBPool(t)
	defer pool.Close()
	cleanupDB(t, pool)
	defer cleanupDB(t, pool)

	repo := NewExecutionRepository(pool)
	ctx := context.Background()

	now := time.Now()
	exec := &JobExecution{
		JobID:       "test-job-123",
		JobType:     "email.send",
		Queue:       "default",
		Payload:     []byte(`{"to": "test@example.com"}`),
		State:       "completed",
		Attempt:     1,
		MaxRetries:  3,
		StartedAt:   &now,
		CompletedAt: &now,
	}

	err := repo.Create(ctx, exec)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if exec.ID == "" {
		t.Error("expected ID to be set")
	}
	if exec.CreatedAt.IsZero() {
		t.Error("expected CreatedAt to be set")
	}
}

func TestExecutionRepository_Integration_RecordExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool := getDBPool(t)
	defer pool.Close()
	cleanupDB(t, pool)
	defer cleanupDB(t, pool)

	repo := NewExecutionRepository(pool)
	ctx := context.Background()

	j, _ := job.NewWithOptions("test.record", nil)
	j.MarkStarted()
	j.MarkCompleted()

	err := repo.RecordExecution(ctx, j, "worker-1", nil)
	if err != nil {
		t.Fatalf("RecordExecution failed: %v", err)
	}

	// Verify
	executions, err := repo.GetByJobID(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetByJobID failed: %v", err)
	}

	if len(executions) != 1 {
		t.Errorf("expected 1 execution, got %d", len(executions))
	}
}

func TestExecutionRepository_Integration_GetStats(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool := getDBPool(t)
	defer pool.Close()
	cleanupDB(t, pool)
	defer cleanupDB(t, pool)

	repo := NewExecutionRepository(pool)
	ctx := context.Background()

	// Create various executions
	now := time.Now()
	durationMs := int64(100)

	executions := []*JobExecution{
		{JobID: "1", JobType: "test", Queue: "default", Payload: []byte("{}"), State: "completed", Attempt: 1, MaxRetries: 3, DurationMs: &durationMs},
		{JobID: "2", JobType: "test", Queue: "default", Payload: []byte("{}"), State: "completed", Attempt: 1, MaxRetries: 3, DurationMs: &durationMs},
		{JobID: "3", JobType: "test", Queue: "default", Payload: []byte("{}"), State: "failed", Attempt: 1, MaxRetries: 3},
		{JobID: "4", JobType: "test", Queue: "default", Payload: []byte("{}"), State: "dead", Attempt: 1, MaxRetries: 3},
	}

	for _, exec := range executions {
		repo.Create(ctx, exec)
	}

	stats, err := repo.GetStats(ctx, now.Add(-1*time.Hour), now.Add(1*time.Hour))
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}

	if stats.Total != 4 {
		t.Errorf("expected Total 4, got %d", stats.Total)
	}
	if stats.Completed != 2 {
		t.Errorf("expected Completed 2, got %d", stats.Completed)
	}
	if stats.Failed != 1 {
		t.Errorf("expected Failed 1, got %d", stats.Failed)
	}
	if stats.Dead != 1 {
		t.Errorf("expected Dead 1, got %d", stats.Dead)
	}
	if stats.AvgDurationMs != 100 {
		t.Errorf("expected AvgDurationMs 100, got %f", stats.AvgDurationMs)
	}
}

func TestExecutionRepository_Integration_List(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool := getDBPool(t)
	defer pool.Close()
	cleanupDB(t, pool)
	defer cleanupDB(t, pool)

	repo := NewExecutionRepository(pool)
	ctx := context.Background()

	// Create executions
	for i := 0; i < 5; i++ {
		exec := &JobExecution{
			JobID:      "list-" + string(rune('a'+i)),
			JobType:    "test.list",
			Queue:      "default",
			Payload:    []byte("{}"),
			State:      "completed",
			Attempt:    1,
			MaxRetries: 3,
		}
		repo.Create(ctx, exec)
	}

	// List with filter
	jobType := "test.list"
	executions, err := repo.List(ctx, ExecutionFilter{
		JobType: &jobType,
		Limit:   3,
	})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(executions) != 3 {
		t.Errorf("expected 3 executions, got %d", len(executions))
	}
}
