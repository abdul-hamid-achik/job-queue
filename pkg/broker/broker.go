package broker

import (
	"context"
	"errors"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
)

var (
	ErrJobNotFound   = errors.New("job not found")
	ErrQueueEmpty    = errors.New("queue is empty")
	ErrBrokerClosed  = errors.New("broker is closed")
	ErrInvalidJob    = errors.New("invalid job")
	ErrAckFailed     = errors.New("failed to acknowledge job")
	ErrEnqueueFailed = errors.New("failed to enqueue job")
	ErrDequeueFailed = errors.New("failed to dequeue job")
)

type Broker interface {
	Enqueue(ctx context.Context, j *job.Job) error
	Dequeue(ctx context.Context, queues []string, timeout time.Duration) (*job.Job, error)
	Ack(ctx context.Context, j *job.Job) error
	Nack(ctx context.Context, j *job.Job, err error) error
	Schedule(ctx context.Context, j *job.Job, at time.Time) error
	GetJob(ctx context.Context, jobID string) (*job.Job, error)
	DeleteJob(ctx context.Context, jobID string) error
	GetQueueDepth(ctx context.Context, queue string) (int64, error)
	GetDelayedJobs(ctx context.Context, until time.Time, limit int64) ([]*job.Job, error)
	MoveDelayedToQueue(ctx context.Context, j *job.Job) error
	GetPendingJobs(ctx context.Context, queue string, idleTime time.Duration) ([]*job.Job, error)
	RequeueStaleJob(ctx context.Context, j *job.Job) error
	Ping(ctx context.Context) error
	Close() error
}

type Stats struct {
	Queue           string           `json:"queue"`
	Depth           int64            `json:"depth"`
	DepthByPriority map[string]int64 `json:"depth_by_priority"`
	Processing      int64            `json:"processing"`
	Delayed         int64            `json:"delayed"`
	Failed          int64            `json:"failed"`
}

type StatsProvider interface {
	GetStats(ctx context.Context, queue string) (*Stats, error)
	GetAllStats(ctx context.Context) ([]*Stats, error)
}

type MessageID string

type StreamEntry struct {
	ID     MessageID
	Job    *job.Job
	Stream string
}
