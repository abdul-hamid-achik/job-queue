package broker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/abdul-hamid-achik/job-queue/internal/job"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
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

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

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

	workerID := fmt.Sprintf("test-worker-%s", uuid.New().String()[:8])
	broker := NewRedisStreamsBroker(client, WithWorkerID(workerID))

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

	workerID := fmt.Sprintf("test-worker-%s", uuid.New().String()[:8])
	broker := NewRedisStreamsBroker(client, WithWorkerID(workerID))
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

	workerID := fmt.Sprintf("test-worker-%s", uuid.New().String()[:8])
	broker := NewRedisStreamsBroker(client, WithWorkerID(workerID))
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

	workerID := fmt.Sprintf("test-worker-%s", uuid.New().String()[:8])
	broker := NewRedisStreamsBroker(client, WithWorkerID(workerID))
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

	broker := NewRedisStreamsBroker(client)
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

	broker := NewRedisStreamsBroker(client)
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

	workerID := fmt.Sprintf("test-worker-%s", uuid.New().String()[:8])
	broker := NewRedisStreamsBroker(client, WithWorkerID(workerID))
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

	broker := NewRedisStreamsBroker(client)
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

	broker := NewRedisStreamsBroker(client)
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

	broker := NewRedisStreamsBroker(client)
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

	workerID := fmt.Sprintf("test-worker-%s", uuid.New().String()[:8])
	broker := NewRedisStreamsBroker(client, WithWorkerID(workerID))
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

	workerID := fmt.Sprintf("test-worker-%s", uuid.New().String()[:8])
	broker := NewRedisStreamsBroker(client, WithWorkerID(workerID))
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

	broker := NewRedisStreamsBroker(client)
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
