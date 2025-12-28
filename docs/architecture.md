# Architecture

## Overview

This job queue system uses Redis Streams for message passing and PostgreSQL for persistence of failed jobs and execution history.

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  Producers  │────▶│ Redis Streams│────▶│   Workers   │
│   (HTTP)    │     │   (Queues)   │     │   (Pool)    │
└─────────────┘     └──────────────┘     └──────┬──────┘
                                                │
                    ┌──────────────┐     ┌──────▼──────┐
                    │  PostgreSQL  │◀────│  Handlers   │
                    │ (DLQ/History)│     │  (Registry) │
                    └──────────────┘     └─────────────┘
                                                ▲
                    ┌──────────────┐            │
                    │  Scheduler   │────────────┘
                    │(Delayed/Cron)│
                    └──────────────┘
```

## Components

### Broker (`internal/broker/`)

The broker handles all Redis Streams operations:

- **Enqueue**: Adds jobs to priority-based streams (`stream:{queue}:{priority}`)
- **Dequeue**: Reads from streams using consumer groups (XREADGROUP)
- **Ack/Nack**: Acknowledges or rejects processed jobs (XACK)
- **Schedule**: Stores delayed jobs in a sorted set by execution time

Key implementation: `redis_streams.go`

### Job (`internal/job/`)

Job struct with state machine:

```
pending → queued → running → completed
                         ↘→ failed → (retry) → queued
                                   → (exhausted) → dead
```

Priorities: `critical > high > medium > low`

### Worker Pool (`internal/worker/`)

- **Registry**: Maps job types to handler functions
- **Worker**: Single goroutine that dequeues and processes jobs
- **Pool**: Manages N workers with graceful shutdown

Workers process queues in priority order - always checking higher priority streams first.

### Middleware (`internal/middleware/`)

Composable handlers wrapping job execution:

- **Retry**: Exponential backoff with jitter strategies
- **Logging**: Structured logging for each job
- **Timeout**: Context-based execution timeout

### Scheduler (`internal/scheduler/`)

Two polling loops:

1. **Delayed Jobs**: Moves jobs from sorted set to work queue when their time arrives
2. **Cron Jobs**: Parses cron expressions, calculates next run, enqueues jobs

### Repositories (`internal/repository/`)

PostgreSQL persistence:

- **DLQ Repository**: Dead letter queue for failed jobs
- **Execution Repository**: Job execution history with timing and error details
- **Schedule Repository**: Cron job definitions

### API Handler (`internal/handler/`)

REST endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/jobs` | POST | Enqueue a job |
| `/api/v1/jobs/:id` | GET | Get job details |
| `/api/v1/queues/:name/stats` | GET | Queue statistics |
| `/api/v1/dlq` | GET | List dead jobs |
| `/api/v1/dlq/:id/retry` | POST | Retry a dead job |
| `/health` | GET | Health check |

## Data Flow

### Job Enqueue

1. API receives POST request with job type and payload
2. Job created with UUID, validated
3. Broker adds to Redis Stream: `XADD stream:{queue}:{priority} * job {json}`
4. Job state: `pending → queued`

### Job Processing

1. Worker calls `XREADGROUP` on streams (high → medium → low priority)
2. Job deserialized, state set to `running`
3. Handler looked up in registry by job type
4. Middleware chain executed (logging → timeout → retry → handler)
5. On success: `XACK` removes from stream, state → `completed`
6. On failure: Retry middleware checks attempts, either re-enqueues or moves to DLQ

### Delayed Jobs

1. Job scheduled with `ScheduledAt` timestamp
2. Broker stores in sorted set: `ZADD delayed {timestamp} {job_id}`
3. Scheduler polls sorted set every second
4. When `timestamp <= now`, moves job to work queue

## Redis Data Structures

| Key Pattern | Type | Purpose |
|-------------|------|---------|
| `stream:{queue}:{priority}` | Stream | Work queue per priority |
| `job:{id}` | Hash | Job data storage |
| `delayed` | Sorted Set | Delayed jobs by timestamp |
| `workers` | Consumer Group | Tracks which worker has which job |

## PostgreSQL Tables

| Table | Purpose |
|-------|---------|
| `job_executions` | Execution history (start, end, duration, error) |
| `dead_letter_queue` | Failed jobs after max retries |
| `job_schedules` | Cron job definitions |
