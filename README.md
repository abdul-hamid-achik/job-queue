# Job Queue

A production-grade background job processing system built with Go, Redis Streams, and PostgreSQL.

[![Go Version](https://img.shields.io/badge/Go-1.23+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Features

- **Redis Streams Broker** - Reliable message delivery with consumer groups and priority queues
- **Worker Pools** - Concurrent job processing with configurable concurrency
- **Graceful Shutdown** - Clean shutdown with in-flight job completion
- **Retry with Backoff** - Exponential backoff with jitter (full, equal, decorrelated strategies)
- **Dead Letter Queue** - Failed jobs persisted to PostgreSQL for inspection and retry
- **Delayed Jobs** - Schedule jobs for future execution
- **Cron Scheduling** - Recurring jobs with cron expressions and timezone support
- **Execution History** - Track job outcomes in PostgreSQL
- **HTTP API** - RESTful endpoints for job management and monitoring
- **OpenAPI Spec** - Full API documentation for client generation
- **MCP Server** - Model Context Protocol support for LLM integration
- **Middleware** - Composable middleware for logging, timeouts, and custom behavior

## Architecture

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

## Quick Start

### Prerequisites

- Go 1.23+
- Docker and Docker Compose
- [Task](https://taskfile.dev/) (recommended) or run commands manually

### Setup

```bash
# Clone the repository
git clone https://github.com/abdul-hamid-achik/job-queue.git
cd job-queue

# Quick setup (starts infra + runs migrations)
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable"
task setup

# Or manually:
docker compose up -d redis db
task migrate

# Run tests to verify everything works
task test
```

### Running the Services

```bash
# Terminal 1: Start the API server
task run:api

# Terminal 2: Start workers
task run:worker

# Terminal 3: Start the scheduler (for delayed/cron jobs)
task run:scheduler
```

### Using Docker

```bash
# Build all images
task docker:build

# Run everything
task docker:up
```

## Usage

### Creating Jobs

```go
package main

import (
    "github.com/abdul-hamid-achik/job-queue/internal/job"
    "github.com/abdul-hamid-achik/job-queue/internal/broker"
    "github.com/redis/go-redis/v9"
)

func main() {
    // Connect to Redis
    client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    b := broker.NewRedisStreamsBroker(client)

    // Create a job
    j, _ := job.NewWithOptions("email.send", map[string]string{
        "to":      "user@example.com",
        "subject": "Welcome!",
    },
        job.WithQueue("default"),
        job.WithPriority(job.PriorityHigh),
        job.WithMaxRetries(3),
    )

    // Enqueue it
    b.Enqueue(context.Background(), j)
}
```

### Processing Jobs

```go
package main

import (
    "context"
    "github.com/abdul-hamid-achik/job-queue/internal/worker"
    "github.com/abdul-hamid-achik/job-queue/internal/broker"
    "github.com/abdul-hamid-achik/job-queue/internal/job"
    "github.com/abdul-hamid-achik/job-queue/internal/middleware"
)

func main() {
    // Create broker
    b := broker.NewRedisStreamsBroker(redisClient)

    // Create handler registry
    registry := worker.NewRegistry()

    // Register handlers
    registry.MustRegister("email.send", func(ctx context.Context, j *job.Job) error {
        // Process the email job
        var payload struct {
            To      string `json:"to"`
            Subject string `json:"subject"`
        }
        j.UnmarshalPayload(&payload)
        
        return sendEmail(payload.To, payload.Subject)
    })

    // Add middleware
    registry.Use(
        middleware.RecoveryMiddleware(logger),
        middleware.LoggingMiddleware(logger),
        middleware.TimeoutMiddleware(5 * time.Minute),
    )

    // Create and start worker pool
    pool := worker.NewPool(b, registry,
        worker.WithConcurrency(10),
        worker.WithPoolQueues([]string{"critical", "default", "low"}),
    )

    pool.Start(context.Background())
}
```

### Delayed Jobs

```go
// Schedule a job for 1 hour from now
j, _ := job.NewWithOptions("reminder.send", payload,
    job.WithDelay(time.Hour),
)
b.Enqueue(ctx, j)

// Or schedule for a specific time
scheduledTime := time.Date(2024, 12, 25, 9, 0, 0, 0, time.UTC)
b.Schedule(ctx, j, scheduledTime)
```

### HTTP API

```bash
# Enqueue a job
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "email.send", "payload": {"to": "user@example.com"}}'

# Get job status
curl http://localhost:8080/api/v1/jobs/{job_id}

# Get queue statistics
curl http://localhost:8080/api/v1/queues/default/stats

# List dead letter queue
curl http://localhost:8080/api/v1/dlq

# Retry a dead job
curl -X POST http://localhost:8080/api/v1/dlq/{job_id}/retry

# Health check
curl http://localhost:8080/health

# Get OpenAPI spec
curl http://localhost:8080/api/v1/openapi.yaml
```

## OpenAPI & Client Generation

The API is fully documented with OpenAPI 3.1. You can:

1. **View the spec**: `curl http://localhost:8080/api/v1/openapi.yaml`
2. **Use with Swagger UI**: Point any OpenAPI viewer to the spec URL
3. **Generate clients**: Use openapi-generator for any language

```bash
# Generate TypeScript client
npx @openapitools/openapi-generator-cli generate \
  -i http://localhost:8080/api/v1/openapi.yaml \
  -g typescript-fetch \
  -o ./client

# Generate Python client
openapi-generator generate \
  -i api/openapi.yaml \
  -g python \
  -o ./python-client
```

The spec is also available at `api/openapi.yaml` in the repository.

## MCP Server (LLM Integration)

This project includes an MCP (Model Context Protocol) server, allowing LLMs like Claude to interact with the job queue directly.

### Available Tools

| Tool | Description |
|------|-------------|
| `enqueue_job` | Create and enqueue a new job |
| `get_job` | Get job details by ID |
| `delete_job` | Delete/cancel a job |
| `list_queues` | List all queues with depths |
| `get_queue_depth` | Get depth of specific queue |
| `list_dlq` | List dead letter queue jobs |
| `get_dlq_job` | Get specific DLQ job |
| `retry_dlq_job` | Retry a job from DLQ |
| `delete_dlq_job` | Remove job from DLQ |
| `list_executions` | List execution history |
| `get_job_executions` | Get executions for specific job |
| `get_stats` | Get overall statistics |
| `health_check` | Check system health |

### Claude Desktop Configuration

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "job-queue": {
      "command": "/path/to/job-queue/bin/mcp",
      "env": {
        "REDIS_URL": "redis://localhost:6379",
        "DATABASE_URL": "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable"
      }
    }
  }
}
```

### Build and Run

```bash
# Build MCP server
task build:mcp

# Run directly (for testing)
task run:mcp
```

## Configuration

Configuration is loaded from environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `DATABASE_URL` | `postgres://...` | PostgreSQL connection URL |
| `API_PORT` | `8080` | HTTP API port |
| `WORKER_CONCURRENCY` | `10` | Number of concurrent workers |
| `WORKER_QUEUES` | `critical,default,low` | Queues to process (priority order) |
| `WORKER_POLL_INTERVAL` | `1s` | How often to poll for jobs |
| `WORKER_SHUTDOWN_TIMEOUT` | `30s` | Time to wait for graceful shutdown |
| `JOB_DEFAULT_TIMEOUT` | `5m` | Default job execution timeout |
| `LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |
| `LOG_FORMAT` | `json` | Log format (json, console) |

See [.env.example](.env.example) for a complete list.

## Project Structure

```
job-queue/
├── api/
│   └── openapi.yaml     # OpenAPI 3.1 specification
├── cmd/
│   ├── server/          # HTTP API server
│   ├── worker/          # Background worker process
│   ├── scheduler/       # Delayed job scheduler
│   └── mcp/             # MCP server for LLM integration
├── internal/
│   ├── api/             # Embedded OpenAPI spec
│   ├── broker/          # Redis Streams queue implementation
│   ├── job/             # Job types, state machine, priorities
│   ├── worker/          # Worker pool and handler registry
│   ├── middleware/      # Retry, logging, timeout middleware
│   ├── repository/      # PostgreSQL repositories (DLQ, history)
│   ├── scheduler/       # Delayed jobs and cron scheduling
│   ├── handler/         # HTTP API handlers
│   ├── mcp/             # MCP server implementation
│   └── config/          # Configuration loading
├── migrations/          # PostgreSQL migrations
├── testutil/            # Test helpers and mocks
└── docs/                # Additional documentation
```

## Priority Queues

Jobs are processed in priority order. Each priority level has its own Redis Stream:

| Priority | Stream Name | Use Case |
|----------|-------------|----------|
| `critical` | `stream:default:critical` | Payment processing, auth |
| `high` | `stream:default:high` | User-facing notifications |
| `medium` | `stream:default:medium` | Default priority |
| `low` | `stream:default:low` | Background tasks, reports |

Workers always check higher priority queues first before processing lower priority jobs.

## Retry Strategy

Failed jobs are retried with exponential backoff and jitter:

```
delay = min(base * 2^attempt, maxDelay) + jitter
```

Available jitter strategies:
- **Full Jitter**: `random(0, delay)` - Maximum spread
- **Equal Jitter**: `delay/2 + random(0, delay/2)` - Balanced
- **Decorrelated Jitter**: `random(base, previousDelay * 3)` - AWS-recommended

After exhausting retries, jobs move to the Dead Letter Queue (PostgreSQL) for manual inspection.

## Testing

```bash
# Run all tests
task test

# Run unit tests only
task test:unit

# Run with coverage
task test:coverage

# Run integration tests (requires Docker)
task docker:infra
task test:integration
```

## Development

```bash
# List all available tasks
task

# Install dependencies
task tidy

# Run linter
task lint

# Format code
task fmt

# Build all binaries
task build
```

## Acknowledgments

Inspired by:
- [Sidekiq](https://github.com/mperham/sidekiq) - Ruby background jobs
- [Asynq](https://github.com/hibiken/asynq) - Go distributed task queue
- [Bull](https://github.com/OptimalBits/bull) - Node.js queue

## License

MIT License - see [LICENSE](LICENSE) for details.
