# Deployment Guide

This guide covers deploying the job queue system in production environments.

## Prerequisites

- Go 1.23+
- Redis 7.0+
- PostgreSQL 14+
- Docker (optional, for containerized deployment)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `DATABASE_URL` | - | PostgreSQL connection URL (required) |
| `API_PORT` | `8080` | HTTP API port |
| `WORKER_CONCURRENCY` | `10` | Number of concurrent workers |
| `WORKER_QUEUES` | `critical,default,low` | Queues to process (priority order) |
| `WORKER_POLL_INTERVAL` | `1s` | How often to poll for jobs |
| `WORKER_SHUTDOWN_TIMEOUT` | `30s` | Time to wait for graceful shutdown |
| `JOB_DEFAULT_TIMEOUT` | `5m` | Default job execution timeout |
| `LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |
| `LOG_FORMAT` | `json` | Log format (json, console) |

## Docker Deployment

### Build Images

```bash
# Build all images
docker build -t job-queue-server -f Dockerfile --target server .
docker build -t job-queue-worker -f Dockerfile --target worker .
docker build -t job-queue-scheduler -f Dockerfile --target scheduler .
```

### Docker Compose (Production)

```yaml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    command: redis-server --appendonly yes
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: jobqueue
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 3s
      retries: 5

  api:
    image: job-queue-server
    environment:
      - REDIS_URL=redis://redis:6379
      - DATABASE_URL=postgres://postgres:${DB_PASSWORD}@db:5432/jobqueue?sslmode=disable
      - API_PORT=8080
      - LOG_LEVEL=info
      - LOG_FORMAT=json
    ports:
      - "8080:8080"
    depends_on:
      redis:
        condition: service_healthy
      db:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  worker:
    image: job-queue-worker
    environment:
      - REDIS_URL=redis://redis:6379
      - DATABASE_URL=postgres://postgres:${DB_PASSWORD}@db:5432/jobqueue?sslmode=disable
      - WORKER_CONCURRENCY=10
      - WORKER_QUEUES=critical,default,low
      - LOG_LEVEL=info
    depends_on:
      redis:
        condition: service_healthy
      db:
        condition: service_healthy
    deploy:
      replicas: 3

  scheduler:
    image: job-queue-scheduler
    environment:
      - REDIS_URL=redis://redis:6379
      - DATABASE_URL=postgres://postgres:${DB_PASSWORD}@db:5432/jobqueue?sslmode=disable
      - LOG_LEVEL=info
    depends_on:
      redis:
        condition: service_healthy
      db:
        condition: service_healthy

volumes:
  redis_data:
  postgres_data:
```

## Kubernetes Deployment

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: job-queue-config
data:
  WORKER_CONCURRENCY: "10"
  WORKER_QUEUES: "critical,default,low"
  WORKER_POLL_INTERVAL: "1s"
  WORKER_SHUTDOWN_TIMEOUT: "30s"
  JOB_DEFAULT_TIMEOUT: "5m"
  LOG_LEVEL: "info"
  LOG_FORMAT: "json"
```

### Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: job-queue-secrets
type: Opaque
stringData:
  REDIS_URL: "redis://redis:6379"
  DATABASE_URL: "postgres://postgres:password@postgres:5432/jobqueue?sslmode=disable"
```

### API Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: job-queue-api
spec:
  replicas: 2
  selector:
    matchLabels:
      app: job-queue-api
  template:
    metadata:
      labels:
        app: job-queue-api
    spec:
      containers:
      - name: api
        image: job-queue-server:latest
        ports:
        - containerPort: 8080
        envFrom:
        - configMapRef:
            name: job-queue-config
        - secretRef:
            name: job-queue-secrets
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
        resources:
          requests:
            memory: "64Mi"
            cpu: "100m"
          limits:
            memory: "256Mi"
            cpu: "500m"
---
apiVersion: v1
kind: Service
metadata:
  name: job-queue-api
spec:
  selector:
    app: job-queue-api
  ports:
  - port: 80
    targetPort: 8080
  type: ClusterIP
```

### Worker Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: job-queue-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: job-queue-worker
  template:
    metadata:
      labels:
        app: job-queue-worker
    spec:
      terminationGracePeriodSeconds: 60
      containers:
      - name: worker
        image: job-queue-worker:latest
        envFrom:
        - configMapRef:
            name: job-queue-config
        - secretRef:
            name: job-queue-secrets
        resources:
          requests:
            memory: "128Mi"
            cpu: "200m"
          limits:
            memory: "512Mi"
            cpu: "1000m"
```

### Scheduler Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: job-queue-scheduler
spec:
  replicas: 1  # Only one scheduler instance
  selector:
    matchLabels:
      app: job-queue-scheduler
  template:
    metadata:
      labels:
        app: job-queue-scheduler
    spec:
      containers:
      - name: scheduler
        image: job-queue-scheduler:latest
        envFrom:
        - configMapRef:
            name: job-queue-config
        - secretRef:
            name: job-queue-secrets
        resources:
          requests:
            memory: "64Mi"
            cpu: "50m"
          limits:
            memory: "128Mi"
            cpu: "200m"
```

## Scaling Considerations

### Workers

- Scale workers horizontally based on queue depth
- Use Kubernetes HPA with custom metrics (queue depth)
- Each worker can process `WORKER_CONCURRENCY` jobs simultaneously

### API Servers

- Scale based on request rate
- Use standard CPU/memory-based HPA
- Stateless, can scale freely

### Scheduler

- **Run only ONE instance** to avoid duplicate job scheduling
- Use leader election if high availability is required

### Redis

- Use Redis Cluster for high availability
- Consider Redis Sentinel for automatic failover
- Monitor memory usage - streams can grow large

### PostgreSQL

- Use connection pooling (PgBouncer)
- Regular VACUUM on job_executions table
- Consider partitioning for large execution history

## Monitoring

### Health Endpoints

- `GET /health` - Basic liveness check
- `GET /ready` - Readiness check (verifies Redis/DB connectivity)

### Key Metrics to Monitor

1. **Queue Depth** - Jobs waiting to be processed
2. **Processing Time** - Job execution duration
3. **Failure Rate** - Jobs moving to DLQ
4. **Worker Utilization** - Active workers / total capacity
5. **Redis Memory** - Stream size growth

### Logging

Logs are structured JSON by default:

```json
{
  "level": "info",
  "time": "2024-01-06T12:00:00Z",
  "worker_id": "abc123",
  "job_id": "def456",
  "job_type": "email.send",
  "duration": 1.234,
  "message": "job completed"
}
```

## Database Migrations

Run migrations before deploying new versions:

```bash
# Using golang-migrate
migrate -path migrations -database "$DATABASE_URL" up

# Or using Task
task migrate
```

## Troubleshooting

### Jobs Not Processing

1. Check worker logs for errors
2. Verify Redis connectivity: `redis-cli ping`
3. Check queue depth: `GET /api/v1/queues/default/stats`
4. Verify consumer groups exist: `redis-cli XINFO GROUPS stream:default:medium`

### High Memory Usage

1. Check Redis stream lengths
2. Consider trimming old entries: `XTRIM stream:default:medium MAXLEN 10000`
3. Review job payload sizes

### Jobs Stuck in Processing

1. Check for stale jobs: scheduler reclaims after `staleJobTimeout`
2. Verify worker graceful shutdown is working
3. Check for long-running handlers

### DLQ Growing

1. Review failed job errors in DLQ
2. Check for systematic issues (external API down, etc.)
3. Consider retry policies and backoff settings
