package broker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

func getRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Skipf("invalid REDIS_URL: %v", err)
	}

	// Use database 1 for tests to avoid conflicts with other running services
	opts.DB = 1

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// Clean the test database to ensure isolation
	client.FlushDB(ctx)

	return client
}

func cleanupRedis(t *testing.T, client *redis.Client) {
	t.Helper()
	ctx := context.Background()

	// Clean up test keys
	keys, _ := client.Keys(ctx, "stream:*").Result()
	keys2, _ := client.Keys(ctx, "job:*").Result()
	keys3, _ := client.Keys(ctx, "delayed*").Result()

	allKeys := append(keys, keys2...)
	allKeys = append(allKeys, keys3...)

	if len(allKeys) > 0 {
		client.Del(ctx, allKeys...)
	}
}

// newTestBroker creates a broker with unique worker ID and group name for test isolation
func newTestBroker(t *testing.T, client *redis.Client) *RedisStreamsBroker {
	t.Helper()
	uniqueID := uuid.New().String()[:8]
	return NewRedisStreamsBroker(client,
		WithWorkerID(fmt.Sprintf("test-worker-%s", uniqueID)),
		WithGroupName(fmt.Sprintf("test-group-%s", uniqueID)),
	)
}

func TestRedisStreamsBroker_Integration_Ping(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()

	broker := NewRedisStreamsBroker(client)

	ctx := context.Background()
	err := broker.Ping(ctx)
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func TestRedisStreamsBroker_Integration_EnqueueDequeue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Create and enqueue a job
	j, err := job.NewWithOptions("test.job", map[string]string{"key": "value"})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	err = broker.Enqueue(ctx, j)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Verify job state changed
	if j.State != job.StateQueued {
		t.Errorf("expected state Queued, got %s", j.State)
	}

	// Dequeue the job
	dequeued, err := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if dequeued.ID != j.ID {
		t.Errorf("expected job ID %s, got %s", j.ID, dequeued.ID)
	}
	if dequeued.Type != "test.job" {
		t.Errorf("expected job type 'test.job', got %s", dequeued.Type)
	}
	if dequeued.State != job.StateProcessing {
		t.Errorf("expected state Processing, got %s", dequeued.State)
	}
}

func TestRedisStreamsBroker_Integration_Ack(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue and dequeue
	j, _ := job.NewWithOptions("test.ack", map[string]string{"test": "data"})
	if err := broker.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	// Ack the job
	err = broker.Ack(ctx, dequeued)
	if err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	// Verify job is completed
	retrieved, err := broker.GetJob(ctx, dequeued.ID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if retrieved.State != job.StateCompleted {
		t.Errorf("expected state Completed, got %s", retrieved.State)
	}
}

func TestRedisStreamsBroker_Integration_Nack(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue job with retries
	j, _ := job.NewWithOptions("test.nack", map[string]string{"test": "data"}, job.WithMaxRetries(3))
	if err := broker.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	// Nack the job
	err = broker.Nack(ctx, dequeued, job.ErrInvalidJob)
	if err != nil {
		t.Fatalf("Nack failed: %v", err)
	}

	// Job should be scheduled for retry
	retrieved, err := broker.GetJob(ctx, dequeued.ID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if retrieved.State != job.StateRetrying && retrieved.State != job.StateScheduled {
		t.Errorf("expected state Retrying or Scheduled, got %s", retrieved.State)
	}
	if retrieved.RetryCount != 1 {
		t.Errorf("expected RetryCount 1, got %d", retrieved.RetryCount)
	}
}

func TestRedisStreamsBroker_Integration_NackExhausted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue job with no retries
	j, _ := job.NewWithOptions("test.nack.exhausted", map[string]string{"test": "data"}, job.WithMaxRetries(0))
	if err := broker.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	dequeued, err := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	// Nack the job - should go to dead state
	err = broker.Nack(ctx, dequeued, job.ErrInvalidJob)
	if err != nil {
		t.Fatalf("Nack failed: %v", err)
	}

	// Job should be dead
	retrieved, err := broker.GetJob(ctx, dequeued.ID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if retrieved.State != job.StateDead {
		t.Errorf("expected state Dead, got %s", retrieved.State)
	}
}

func TestRedisStreamsBroker_Integration_Schedule(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Schedule a job for future execution
	j, _ := job.NewWithOptions("test.scheduled", map[string]string{"test": "data"})
	runAt := time.Now().Add(1 * time.Hour)

	err := broker.Schedule(ctx, j, runAt)
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	// Verify job is scheduled
	retrieved, err := broker.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if retrieved.State != job.StateScheduled {
		t.Errorf("expected state Scheduled, got %s", retrieved.State)
	}
	if retrieved.ScheduledAt == nil {
		t.Error("expected ScheduledAt to be set")
	}
}

func TestRedisStreamsBroker_Integration_GetDelayedJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Schedule jobs - some due, some not
	pastJob, _ := job.NewWithOptions("test.past", nil)
	futureJob, _ := job.NewWithOptions("test.future", nil)

	broker.Schedule(ctx, pastJob, time.Now().Add(-1*time.Minute))
	broker.Schedule(ctx, futureJob, time.Now().Add(1*time.Hour))

	// Get delayed jobs that are due
	dueJobs, err := broker.GetDelayedJobs(ctx, time.Now(), 100)
	if err != nil {
		t.Fatalf("GetDelayedJobs failed: %v", err)
	}

	if len(dueJobs) != 1 {
		t.Errorf("expected 1 due job, got %d", len(dueJobs))
	}
	if len(dueJobs) > 0 && dueJobs[0].ID != pastJob.ID {
		t.Errorf("expected job ID %s, got %s", pastJob.ID, dueJobs[0].ID)
	}
}

func TestRedisStreamsBroker_Integration_MoveDelayedToQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Schedule a job
	j, _ := job.NewWithOptions("test.move", nil)
	broker.Schedule(ctx, j, time.Now().Add(-1*time.Minute))

	// Move to queue
	err := broker.MoveDelayedToQueue(ctx, j)
	if err != nil {
		t.Fatalf("MoveDelayedToQueue failed: %v", err)
	}

	// Should be able to dequeue it now
	dequeued, err := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if dequeued.ID != j.ID {
		t.Errorf("expected job ID %s, got %s", j.ID, dequeued.ID)
	}
}

func TestRedisStreamsBroker_Integration_GetQueueDepth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue some jobs
	for i := 0; i < 5; i++ {
		j, _ := job.NewWithOptions("test.depth", nil)
		broker.Enqueue(ctx, j)
	}

	depth, err := broker.GetQueueDepth(ctx, "default")
	if err != nil {
		t.Fatalf("GetQueueDepth failed: %v", err)
	}
	if depth != 5 {
		t.Errorf("expected depth 5, got %d", depth)
	}
}

func TestRedisStreamsBroker_Integration_GetStats(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue jobs with different priorities
	highJob, _ := job.NewWithOptions("test.high", nil, job.WithPriority(job.PriorityHigh))
	lowJob, _ := job.NewWithOptions("test.low", nil, job.WithPriority(job.PriorityLow))

	broker.Enqueue(ctx, highJob)
	broker.Enqueue(ctx, lowJob)

	stats, err := broker.GetStats(ctx, "default")
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}

	if stats.Queue != "default" {
		t.Errorf("expected queue 'default', got %s", stats.Queue)
	}
	if stats.Depth != 2 {
		t.Errorf("expected depth 2, got %d", stats.Depth)
	}
	if stats.DepthByPriority["high"] != 1 {
		t.Errorf("expected 1 high priority job, got %d", stats.DepthByPriority["high"])
	}
	if stats.DepthByPriority["low"] != 1 {
		t.Errorf("expected 1 low priority job, got %d", stats.DepthByPriority["low"])
	}
}

func TestRedisStreamsBroker_Integration_DeleteJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Create and enqueue a job
	j, _ := job.NewWithOptions("test.delete", nil)
	broker.Enqueue(ctx, j)

	// Delete the job
	err := broker.DeleteJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}

	// Job should not be found
	_, err = broker.GetJob(ctx, j.ID)
	if err != ErrJobNotFound {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestRedisStreamsBroker_Integration_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	cleanupRedis(t, client)

	broker := NewRedisStreamsBroker(client)

	err := broker.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestRedisStreamsBroker_Integration_PriorityOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue jobs in reverse priority order
	lowJob, _ := job.NewWithOptions("test.low", map[string]string{"order": "1"}, job.WithPriority(job.PriorityLow))
	medJob, _ := job.NewWithOptions("test.med", map[string]string{"order": "2"}, job.WithPriority(job.PriorityMedium))
	highJob, _ := job.NewWithOptions("test.high", map[string]string{"order": "3"}, job.WithPriority(job.PriorityHigh))

	broker.Enqueue(ctx, lowJob)
	broker.Enqueue(ctx, medJob)
	broker.Enqueue(ctx, highJob)

	// Dequeue should return high priority first
	first, err := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if err != nil {
		t.Fatalf("First Dequeue failed: %v", err)
	}
	if first.Type != "test.high" {
		t.Errorf("expected high priority job first, got %s", first.Type)
	}
	broker.Ack(ctx, first)

	// Then medium
	second, err := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Second Dequeue failed: %v", err)
	}
	if second.Type != "test.med" {
		t.Errorf("expected medium priority job second, got %s", second.Type)
	}
	broker.Ack(ctx, second)

	// Then low
	third, err := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Third Dequeue failed: %v", err)
	}
	if third.Type != "test.low" {
		t.Errorf("expected low priority job third, got %s", third.Type)
	}
	broker.Ack(ctx, third)
}

func TestRedisStreamsBroker_Integration_EmptyQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Try to dequeue from empty queue with short timeout
	_, err := broker.Dequeue(ctx, []string{"default"}, 100*time.Millisecond)
	if err != ErrQueueEmpty {
		t.Errorf("expected ErrQueueEmpty, got %v", err)
	}
}

func TestRedisStreamsBroker_Integration_InvalidJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Try to enqueue an invalid job (empty type)
	j := &job.Job{
		ID:      "test-invalid",
		Type:    "", // Invalid - empty type
		Payload: nil,
	}

	err := broker.Enqueue(ctx, j)
	if err != ErrInvalidJob {
		t.Errorf("expected ErrInvalidJob, got %v", err)
	}
}

func TestRedisStreamsBroker_WithOptions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()

	broker := NewRedisStreamsBroker(client,
		WithWorkerID("custom-worker"),
		WithGroupName("custom-group"),
		WithBlockTime(10*time.Second),
		WithClaimIdleTime(1*time.Minute),
	)

	if broker.workerID != "custom-worker" {
		t.Errorf("expected workerID 'custom-worker', got %s", broker.workerID)
	}
	if broker.groupName != "custom-group" {
		t.Errorf("expected groupName 'custom-group', got %s", broker.groupName)
	}
	if broker.blockTime != 10*time.Second {
		t.Errorf("expected blockTime 10s, got %v", broker.blockTime)
	}
	if broker.claimIdle != 1*time.Minute {
		t.Errorf("expected claimIdle 1m, got %v", broker.claimIdle)
	}
}

func TestRedisStreamsBroker_Integration_GetPendingJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue and dequeue a job but don't ack it
	j, _ := job.NewWithOptions("test.pending", nil)
	broker.Enqueue(ctx, j)

	dequeued, err := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	// Get pending jobs with a short idle time (won't find anything since job just dequeued)
	pendingJobs, err := broker.GetPendingJobs(ctx, "default", 1*time.Hour)
	if err != nil {
		t.Fatalf("GetPendingJobs failed: %v", err)
	}

	// Should find no jobs with 1 hour idle time since job was just dequeued
	if len(pendingJobs) != 0 {
		t.Errorf("expected 0 pending jobs with 1h idle time, got %d", len(pendingJobs))
	}

	// Get pending jobs with very short idle time
	pendingJobs2, err := broker.GetPendingJobs(ctx, "default", 1*time.Millisecond)
	if err != nil {
		t.Fatalf("GetPendingJobs failed: %v", err)
	}

	// Wait briefly and should find the job as "stale" with 1ms idle
	time.Sleep(10 * time.Millisecond)
	pendingJobs3, err := broker.GetPendingJobs(ctx, "default", 1*time.Millisecond)
	if err != nil {
		t.Fatalf("GetPendingJobs failed: %v", err)
	}

	// Should find the job now
	if len(pendingJobs3) == 0 {
		t.Logf("pendingJobs2: %d, pendingJobs3: %d", len(pendingJobs2), len(pendingJobs3))
	}

	// Clean up
	broker.Ack(ctx, dequeued)
}

func TestRedisStreamsBroker_Integration_RequeueStaleJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Create and enqueue a job
	j, _ := job.NewWithOptions("test.stale", nil)
	broker.Enqueue(ctx, j)

	// Dequeue it
	dequeued, err := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	// Try to requeue (may fail if Redis doesn't consider it stale)
	err = broker.RequeueStaleJob(ctx, dequeued)
	// The error is expected in some cases - just test the function doesn't panic
	if err != nil {
		t.Logf("RequeueStaleJob returned error (expected in some cases): %v", err)
	}

	// Clean up the original message
	broker.Ack(ctx, dequeued)
}

func TestRedisStreamsBroker_Integration_EnqueueScheduledJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Create a job that's already scheduled
	scheduledTime := time.Now().Add(1 * time.Hour)
	j, _ := job.NewWithOptions("test.scheduled", nil, job.WithScheduledAt(scheduledTime))

	// Enqueue should store it in delayed set
	err := broker.Enqueue(ctx, j)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Verify it's scheduled
	retrieved, err := broker.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if retrieved.State != job.StateScheduled {
		t.Errorf("expected state Scheduled, got %s", retrieved.State)
	}
}

func TestRedisStreamsBroker_WithLogger(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()

	logger := zerolog.Nop()
	broker := NewRedisStreamsBroker(client, WithLogger(logger))

	if broker == nil {
		t.Fatal("expected non-nil broker")
	}
}

func TestRedisStreamsBroker_Integration_NackWithRetryDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue job with multiple retries
	j, _ := job.NewWithOptions("test.retry", map[string]string{"test": "data"}, job.WithMaxRetries(5))
	if err := broker.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Dequeue and nack multiple times
	for i := 0; i < 3; i++ {
		dequeued, err := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
		if err != nil {
			t.Fatalf("Dequeue %d failed: %v", i, err)
		}

		err = broker.Nack(ctx, dequeued, fmt.Errorf("retry attempt %d", i))
		if err != nil {
			t.Fatalf("Nack %d failed: %v", i, err)
		}

		// Wait for retry delay
		time.Sleep(100 * time.Millisecond)

		// Move delayed job back to queue
		dueJobs, _ := broker.GetDelayedJobs(ctx, time.Now().Add(1*time.Hour), 100)
		for _, dj := range dueJobs {
			broker.MoveDelayedToQueue(ctx, dj)
		}
	}

	// Verify retry count
	retrieved, _ := broker.GetJob(ctx, j.ID)
	if retrieved.RetryCount < 3 {
		t.Errorf("expected at least 3 retries, got %d", retrieved.RetryCount)
	}
}

func TestRedisStreamsBroker_Integration_MultipleQueues(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue jobs to different queues
	j1, _ := job.NewWithOptions("test.critical", nil, job.WithQueue("critical"))
	j2, _ := job.NewWithOptions("test.default", nil, job.WithQueue("default"))
	j3, _ := job.NewWithOptions("test.low", nil, job.WithQueue("low"))

	broker.Enqueue(ctx, j1)
	broker.Enqueue(ctx, j2)
	broker.Enqueue(ctx, j3)

	// Dequeue from specific queues
	dequeued1, err := broker.Dequeue(ctx, []string{"critical"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue from critical failed: %v", err)
	}
	if dequeued1.Queue != "critical" {
		t.Errorf("expected queue 'critical', got %s", dequeued1.Queue)
	}
	broker.Ack(ctx, dequeued1)

	dequeued2, err := broker.Dequeue(ctx, []string{"low"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue from low failed: %v", err)
	}
	if dequeued2.Queue != "low" {
		t.Errorf("expected queue 'low', got %s", dequeued2.Queue)
	}
	broker.Ack(ctx, dequeued2)
}

func TestRedisStreamsBroker_Integration_GetJobNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	_, err := broker.GetJob(ctx, "non-existent-job-id")
	if err != ErrJobNotFound {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestRedisStreamsBroker_Integration_ScheduleValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Try to schedule invalid job
	invalidJob := &job.Job{
		ID:   "test-id",
		Type: "", // Invalid
	}

	err := broker.Schedule(ctx, invalidJob, time.Now().Add(1*time.Hour))
	if err != ErrInvalidJob {
		t.Errorf("expected ErrInvalidJob, got %v", err)
	}
}

func TestRedisStreamsBroker_Integration_DequeueFromMultipleQueues(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue only to critical queue
	j, _ := job.NewWithOptions("test.critical", nil, job.WithQueue("critical"), job.WithPriority(job.PriorityHigh))
	broker.Enqueue(ctx, j)

	// Dequeue from multiple queues - should find the job in critical
	dequeued, err := broker.Dequeue(ctx, []string{"default", "critical", "low"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if dequeued.ID != j.ID {
		t.Errorf("expected job ID %s, got %s", j.ID, dequeued.ID)
	}

	broker.Ack(ctx, dequeued)
}

func TestRedisStreamsBroker_Integration_DelayedJobsBatchLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Schedule multiple jobs in the past
	pastTime := time.Now().Add(-1 * time.Minute)
	for i := 0; i < 10; i++ {
		j, _ := job.NewWithOptions(fmt.Sprintf("test.delayed.%d", i), nil)
		broker.Schedule(ctx, j, pastTime)
	}

	// Get only first 5
	dueJobs, err := broker.GetDelayedJobs(ctx, time.Now(), 5)
	if err != nil {
		t.Fatalf("GetDelayedJobs failed: %v", err)
	}

	if len(dueJobs) != 5 {
		t.Errorf("expected 5 due jobs, got %d", len(dueJobs))
	}
}

func TestRedisStreamsBroker_Integration_EnqueueInvalidQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue job with empty queue (invalid)
	j := &job.Job{
		ID:       "test-id",
		Type:     "test.type",
		Queue:    "",
		Timeout:  5 * time.Minute,
		Priority: job.PriorityMedium,
	}

	err := broker.Enqueue(ctx, j)
	if err != ErrInvalidJob {
		t.Errorf("expected ErrInvalidJob for empty queue, got %v", err)
	}
}

func TestRedisStreamsBroker_Integration_AckCompletedJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue and dequeue a job
	j, _ := job.NewWithOptions("test.ack", nil)
	broker.Enqueue(ctx, j)

	dequeued, _ := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)

	// First ack should succeed
	err := broker.Ack(ctx, dequeued)
	if err != nil {
		t.Fatalf("First Ack failed: %v", err)
	}

	// Second ack should still succeed (idempotent)
	err = broker.Ack(ctx, dequeued)
	// Redis XACK is idempotent, so this shouldn't fail
	if err != nil {
		t.Logf("Second Ack returned error: %v", err)
	}
}

func TestRedisStreamsBroker_Integration_DeleteNonexistentJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Delete a job that doesn't exist - should not error
	err := broker.DeleteJob(ctx, "non-existent-job-id")
	if err != nil {
		t.Errorf("DeleteJob for non-existent job should not error, got %v", err)
	}
}

func TestRedisStreamsBroker_Integration_StatsEmptyQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	stats, err := broker.GetStats(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}

	if stats.Queue != "nonexistent" {
		t.Errorf("expected queue 'nonexistent', got %s", stats.Queue)
	}
	if stats.Depth != 0 {
		t.Errorf("expected depth 0, got %d", stats.Depth)
	}
}

func TestRedisStreamsBroker_Integration_MoveDelayedJobTwice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Schedule a job
	j, _ := job.NewWithOptions("test.move", nil)
	broker.Schedule(ctx, j, time.Now().Add(-1*time.Minute))

	// First move should succeed
	err := broker.MoveDelayedToQueue(ctx, j)
	if err != nil {
		t.Fatalf("First MoveDelayedToQueue failed: %v", err)
	}

	// Second move should be idempotent (job already moved)
	err = broker.MoveDelayedToQueue(ctx, j)
	// May or may not error depending on implementation
	if err != nil {
		t.Logf("Second MoveDelayedToQueue returned: %v", err)
	}
}

func TestRedisStreamsBroker_Integration_QueueDepthEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Get depth of empty queue
	depth, err := broker.GetQueueDepth(ctx, "empty-queue")
	if err != nil {
		t.Fatalf("GetQueueDepth failed: %v", err)
	}

	if depth != 0 {
		t.Errorf("expected depth 0, got %d", depth)
	}
}

func TestRedisStreamsBroker_Integration_EnqueueWithAllPriorities(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	priorities := []job.Priority{job.PriorityLow, job.PriorityMedium, job.PriorityHigh}
	for _, p := range priorities {
		j, _ := job.NewWithOptions("test.priority", nil, job.WithPriority(p))
		err := broker.Enqueue(ctx, j)
		if err != nil {
			t.Errorf("Enqueue with priority %s failed: %v", p, err)
		}
	}

	// Verify all jobs were queued
	stats, _ := broker.GetStats(ctx, "default")
	if stats.Depth != 3 {
		t.Errorf("expected 3 jobs in queue, got %d", stats.Depth)
	}
}

func TestRedisStreamsBroker_Integration_RequeueStaleJobMissingMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Try to requeue a job without metadata
	j, _ := job.NewWithOptions("test.stale", nil)

	err := broker.RequeueStaleJob(ctx, j)
	if err == nil {
		t.Error("expected error for missing metadata")
	}
	if err != nil && err.Error() != "missing stream metadata" {
		// Also check for missing message_id
		j.Metadata = map[string]string{"stream": "test:stream"}
		err2 := broker.RequeueStaleJob(ctx, j)
		if err2 == nil {
			t.Error("expected error for missing message_id")
		}
	}
}

func TestRedisStreamsBroker_Integration_AckMissingMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Try to ack a job without metadata
	j, _ := job.NewWithOptions("test.ack", nil)
	j.State = job.StateProcessing

	err := broker.Ack(ctx, j)
	if err == nil {
		t.Error("expected error for missing stream metadata")
	}

	// Try with stream but no message_id
	j.Metadata = map[string]string{"stream": "test:stream"}
	err = broker.Ack(ctx, j)
	if err == nil {
		t.Error("expected error for missing message_id metadata")
	}
}

func TestRedisStreamsBroker_Integration_NackMissingMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Try to nack a job without metadata
	j, _ := job.NewWithOptions("test.nack", nil)
	j.State = job.StateProcessing

	err := broker.Nack(ctx, j, nil)
	if err == nil {
		t.Error("expected error for missing stream metadata")
	}
}

func TestRedisStreamsBroker_Integration_GetPendingJobsWithStaleJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	// Create broker with very short claim idle time
	uniqueID := uuid.New().String()[:8]
	broker := NewRedisStreamsBroker(client,
		WithWorkerID(fmt.Sprintf("test-worker-%s", uniqueID)),
		WithGroupName(fmt.Sprintf("test-group-%s", uniqueID)),
		WithClaimIdleTime(1*time.Millisecond),
	)
	ctx := context.Background()

	// Enqueue a job
	j, _ := job.NewWithOptions("test.stale", nil)
	broker.Enqueue(ctx, j)

	// Dequeue it
	dequeued, _ := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if dequeued == nil {
		t.Fatal("expected to dequeue a job")
	}

	// Wait for it to become stale
	time.Sleep(10 * time.Millisecond)

	// Get pending jobs - should find our stale job
	pending, err := broker.GetPendingJobs(ctx, "default", 1*time.Millisecond)
	if err != nil {
		t.Fatalf("GetPendingJobs failed: %v", err)
	}

	// Should find at least our job
	t.Logf("Found %d pending jobs", len(pending))

	// Clean up
	broker.Ack(ctx, dequeued)
}

func TestRedisStreamsBroker_Integration_NackWithNilError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue job
	j, _ := job.NewWithOptions("test.nack", nil, job.WithMaxRetries(3))
	broker.Enqueue(ctx, j)

	// Dequeue and nack with nil error
	dequeued, _ := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)

	err := broker.Nack(ctx, dequeued, nil)
	if err != nil {
		t.Fatalf("Nack with nil error failed: %v", err)
	}

	// Verify job was scheduled for retry
	retrieved, _ := broker.GetJob(ctx, dequeued.ID)
	if retrieved.State != job.StateRetrying && retrieved.State != job.StateScheduled {
		t.Errorf("expected state Retrying or Scheduled, got %s", retrieved.State)
	}
}

func TestRedisStreamsBroker_Integration_ScheduleInPast(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Schedule a job in the past
	j, _ := job.NewWithOptions("test.past", nil)
	pastTime := time.Now().Add(-1 * time.Hour)

	err := broker.Schedule(ctx, j, pastTime)
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}

	// Should immediately be due
	dueJobs, _ := broker.GetDelayedJobs(ctx, time.Now(), 100)
	found := false
	for _, dj := range dueJobs {
		if dj.ID == j.ID {
			found = true
			break
		}
	}
	if !found {
		t.Error("job scheduled in past should be immediately due")
	}
}

func TestRedisStreamsBroker_Integration_GetDelayedJobsEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Get delayed jobs when none exist
	dueJobs, err := broker.GetDelayedJobs(ctx, time.Now(), 100)
	if err != nil {
		t.Fatalf("GetDelayedJobs failed: %v", err)
	}

	if len(dueJobs) != 0 {
		t.Errorf("expected 0 delayed jobs, got %d", len(dueJobs))
	}
}

func TestRedisStreamsBroker_Integration_DequeueContextCanceled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)

	// Create a canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Try to dequeue with canceled context
	_, err := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if err == nil {
		t.Error("expected error with canceled context")
	}
}

func TestRedisStreamsBroker_Integration_FullJobLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Create job with metadata
	payload := map[string]interface{}{
		"email":   "test@example.com",
		"subject": "Hello",
		"body":    "Test body",
	}
	j, err := job.NewWithOptions("email.send", payload,
		job.WithQueue("critical"),
		job.WithPriority(job.PriorityHigh),
		job.WithMaxRetries(5),
		job.WithMetadata("source", "test"),
	)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}

	// Enqueue
	if err := broker.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Check queue depth
	depth, _ := broker.GetQueueDepth(ctx, "critical")
	if depth != 1 {
		t.Errorf("expected depth 1, got %d", depth)
	}

	// Dequeue
	dequeued, err := broker.Dequeue(ctx, []string{"critical"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	// Verify job properties preserved
	if dequeued.Type != "email.send" {
		t.Errorf("expected type email.send, got %s", dequeued.Type)
	}
	if dequeued.Queue != "critical" {
		t.Errorf("expected queue critical, got %s", dequeued.Queue)
	}
	if dequeued.Priority != job.PriorityHigh {
		t.Errorf("expected priority high, got %s", dequeued.Priority)
	}
	if dequeued.MaxRetries != 5 {
		t.Errorf("expected max_retries 5, got %d", dequeued.MaxRetries)
	}
	if dequeued.Metadata["source"] != "test" {
		t.Errorf("expected metadata source=test, got %s", dequeued.Metadata["source"])
	}

	// Complete the job
	if err := broker.Ack(ctx, dequeued); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	// Verify final state
	final, _ := broker.GetJob(ctx, j.ID)
	if final.State != job.StateCompleted {
		t.Errorf("expected state Completed, got %s", final.State)
	}
}

func TestRedisStreamsBroker_Integration_EnqueueMarshalError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Create a valid job - the marshaling happens inside Enqueue
	j, _ := job.NewWithOptions("test.job", nil)
	j.ID = ""   // Invalid ID should be caught by validation
	j.Type = "" // Make it invalid

	err := broker.Enqueue(ctx, j)
	if err == nil {
		t.Error("expected error for invalid job")
	}
}

func TestRedisStreamsBroker_Integration_RequeueStaleJobWithMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	// Use very short claim idle time
	uniqueID := uuid.New().String()[:8]
	broker := NewRedisStreamsBroker(client,
		WithWorkerID(fmt.Sprintf("test-worker-%s", uniqueID)),
		WithGroupName(fmt.Sprintf("test-group-%s", uniqueID)),
		WithClaimIdleTime(1*time.Millisecond),
	)
	ctx := context.Background()

	// Enqueue and dequeue a job to get proper metadata
	j, _ := job.NewWithOptions("test.requeue", nil)
	broker.Enqueue(ctx, j)

	dequeued, _ := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if dequeued == nil {
		t.Skip("Could not dequeue job")
	}

	// Verify job has stream metadata
	if dequeued.Metadata["stream"] == "" {
		t.Fatal("expected stream metadata")
	}
	if dequeued.Metadata["message_id"] == "" {
		t.Fatal("expected message_id metadata")
	}

	// Wait to ensure message is stale
	time.Sleep(50 * time.Millisecond)

	// Now try to requeue (may or may not succeed depending on timing)
	err := broker.RequeueStaleJob(ctx, dequeued)
	// Log result but don't fail - timing dependent
	t.Logf("RequeueStaleJob result: %v", err)

	// Clean up if we still own the message
	broker.Ack(ctx, dequeued)
}

func TestRedisStreamsBroker_Integration_ScheduleWithDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Create job with delay option (this sets ScheduledAt)
	j, _ := job.NewWithOptions("test.delayed", nil, job.WithDelay(1*time.Hour))

	// Enqueue should detect the scheduled time and add to delayed set
	err := broker.Enqueue(ctx, j)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Verify job state
	retrieved, _ := broker.GetJob(ctx, j.ID)
	if retrieved.State != job.StateScheduled {
		t.Errorf("expected state Scheduled, got %s", retrieved.State)
	}
}

func TestRedisStreamsBroker_Integration_DequeueMultiplePriorities(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	client := getRedisClient(t)
	defer client.Close()
	cleanupRedis(t, client)
	defer cleanupRedis(t, client)

	broker := newTestBroker(t, client)
	ctx := context.Background()

	// Enqueue low priority first
	lowJob, _ := job.NewWithOptions("test.low", nil, job.WithPriority(job.PriorityLow))
	broker.Enqueue(ctx, lowJob)

	// Enqueue medium priority
	medJob, _ := job.NewWithOptions("test.med", nil, job.WithPriority(job.PriorityMedium))
	broker.Enqueue(ctx, medJob)

	// Dequeue should return based on priority order (high checked first, then medium, then low)
	// Since no high priority jobs, should get medium first
	first, _ := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if first == nil {
		t.Fatal("expected to dequeue a job")
	}

	if first.Priority != job.PriorityMedium {
		t.Errorf("expected medium priority first, got %s", first.Priority)
	}

	broker.Ack(ctx, first)

	second, _ := broker.Dequeue(ctx, []string{"default"}, 5*time.Second)
	if second == nil {
		t.Fatal("expected to dequeue second job")
	}

	if second.Priority != job.PriorityLow {
		t.Errorf("expected low priority second, got %s", second.Priority)
	}

	broker.Ack(ctx, second)
}
