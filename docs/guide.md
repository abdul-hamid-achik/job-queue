# Usage Guide

Quick reference for integrating this job queue into your project.

## Installation

```bash
go get github.com/abdul-hamid-achik/job-queue
```

## Basic Setup

### 1. Create Broker

```go
import (
    "github.com/abdul-hamid-achik/job-queue/internal/broker"
    "github.com/redis/go-redis/v9"
)

redisClient := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})
b := broker.NewRedisStreamsBroker(redisClient)
```

### 2. Create and Enqueue Jobs

```go
import "github.com/abdul-hamid-achik/job-queue/internal/job"

// Simple job
j, err := job.New("email.send", map[string]string{
    "to":      "user@example.com",
    "subject": "Hello",
})
b.Enqueue(ctx, j)

// With options
j, err := job.NewWithOptions("payment.process", payload,
    job.WithQueue("critical"),
    job.WithPriority(job.PriorityHigh),
    job.WithMaxRetries(5),
    job.WithTimeout(2 * time.Minute),
)
b.Enqueue(ctx, j)

// Delayed job
j, err := job.NewWithOptions("reminder.send", payload,
    job.WithDelay(24 * time.Hour),
)
b.Enqueue(ctx, j)
```

### 3. Register Handlers

```go
import "github.com/abdul-hamid-achik/job-queue/internal/worker"

registry := worker.NewRegistry()

registry.MustRegister("email.send", func(ctx context.Context, j *job.Job) error {
    var payload EmailPayload
    if err := j.UnmarshalPayload(&payload); err != nil {
        return err
    }
    return sendEmail(payload.To, payload.Subject, payload.Body)
})

registry.MustRegister("payment.process", processPayment)
```

### 4. Add Middleware

```go
import "github.com/abdul-hamid-achik/job-queue/internal/middleware"

registry.Use(
    middleware.RecoveryMiddleware(logger),    // Catch panics
    middleware.LoggingMiddleware(logger),     // Log execution
    middleware.TimeoutMiddleware(5 * time.Minute), // Enforce timeout
)
```

### 5. Start Worker Pool

```go
pool := worker.NewPool(b, registry,
    worker.WithConcurrency(10),
    worker.WithPoolQueues([]string{"critical", "default", "low"}),
    worker.WithShutdownTimeout(30 * time.Second),
)

// Start processing (blocks until context cancelled)
if err := pool.Start(ctx); err != nil {
    log.Fatal(err)
}
```

### 6. Graceful Shutdown

```go
ctx, cancel := context.WithCancel(context.Background())

// Handle signals
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

go func() {
    <-sigChan
    cancel() // Triggers graceful shutdown
}()

pool.Start(ctx)
```

## Common Patterns

### Retry Configuration

```go
// Custom retry with backoff
j, _ := job.NewWithOptions("flaky.api", payload,
    job.WithMaxRetries(5),
    job.WithRetryBackoff(time.Second, 5*time.Minute), // base, max
)
```

### Priority Queues

```go
// Critical jobs processed first
job.NewWithOptions("payment", payload, job.WithPriority(job.PriorityCritical))

// Background jobs processed last
job.NewWithOptions("report", payload, job.WithPriority(job.PriorityLow))
```

### Job Payload

```go
// Struct payload (recommended)
type OrderPayload struct {
    OrderID   string `json:"order_id"`
    UserID    string `json:"user_id"`
    Amount    int64  `json:"amount"`
}

j, _ := job.New("order.process", OrderPayload{
    OrderID: "ord_123",
    UserID:  "usr_456",
    Amount:  9999,
})

// In handler
func processOrder(ctx context.Context, j *job.Job) error {
    var p OrderPayload
    j.UnmarshalPayload(&p)
    // use p.OrderID, p.UserID, p.Amount
}
```

### Multiple Queues

```go
// Separate queues for different workloads
job.NewWithOptions("email", payload, job.WithQueue("notifications"))
job.NewWithOptions("resize", payload, job.WithQueue("media"))
job.NewWithOptions("report", payload, job.WithQueue("batch"))

// Workers can listen to specific queues
worker.NewPool(b, registry,
    worker.WithPoolQueues([]string{"notifications", "media"}),
)
```

## HTTP API Usage

```bash
# Enqueue job
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "email.send",
    "queue": "default",
    "priority": "high",
    "payload": {"to": "user@example.com", "subject": "Hello"}
  }'

# Check job status
curl http://localhost:8080/api/v1/jobs/{job_id}

# Queue stats
curl http://localhost:8080/api/v1/queues/default/stats

# Dead letter queue
curl http://localhost:8080/api/v1/dlq

# Retry failed job
curl -X POST http://localhost:8080/api/v1/dlq/{job_id}/retry

# Health check
curl http://localhost:8080/health
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection |
| `DATABASE_URL` | - | PostgreSQL connection |
| `WORKER_CONCURRENCY` | `10` | Parallel workers |
| `WORKER_QUEUES` | `critical,default,low` | Queue priority order |
| `JOB_DEFAULT_TIMEOUT` | `5m` | Default job timeout |

## Testing

Use the mock broker for unit tests:

```go
import "github.com/abdul-hamid-achik/job-queue/testutil"

func TestMyHandler(t *testing.T) {
    broker := testutil.NewMockBroker()
    
    // Enqueue job
    j := testutil.NewTestJob()
    broker.Enqueue(ctx, j)
    
    // Verify
    assert.Len(t, broker.QueuedJobs(), 1)
    
    // Process
    dequeued, _ := broker.Dequeue(ctx, []string{"default"}, time.Second)
    assert.Equal(t, j.ID, dequeued.ID)
}
```
