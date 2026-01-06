package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

const (
	keyPrefixStream      = "stream:"
	keyPrefixJob         = "job:"
	keyPrefixDelayed     = "delayed"
	keyPrefixConsumerGrp = "workers"

	defaultBlockTimeout = 5 * time.Second
	defaultClaimMinIdle = 30 * time.Second
)

type RedisStreamsBroker struct {
	client    redis.UniversalClient
	workerID  string
	groupName string
	blockTime time.Duration
	claimIdle time.Duration
	logger    zerolog.Logger
}

type RedisStreamsBrokerOption func(*RedisStreamsBroker)

func WithWorkerID(id string) RedisStreamsBrokerOption {
	return func(b *RedisStreamsBroker) {
		b.workerID = id
	}
}

func WithGroupName(name string) RedisStreamsBrokerOption {
	return func(b *RedisStreamsBroker) {
		b.groupName = name
	}
}

func WithBlockTime(d time.Duration) RedisStreamsBrokerOption {
	return func(b *RedisStreamsBroker) {
		b.blockTime = d
	}
}

func WithClaimIdleTime(d time.Duration) RedisStreamsBrokerOption {
	return func(b *RedisStreamsBroker) {
		b.claimIdle = d
	}
}

func WithLogger(logger zerolog.Logger) RedisStreamsBrokerOption {
	return func(b *RedisStreamsBroker) {
		b.logger = logger
	}
}

func NewRedisStreamsBroker(client redis.UniversalClient, opts ...RedisStreamsBrokerOption) *RedisStreamsBroker {
	b := &RedisStreamsBroker{
		client:    client,
		workerID:  uuid.New().String(),
		groupName: keyPrefixConsumerGrp,
		blockTime: defaultBlockTimeout,
		claimIdle: defaultClaimMinIdle,
		logger:    zerolog.Nop(),
	}

	for _, opt := range opts {
		opt(b)
	}

	b.logger = b.logger.With().Str("worker_id", b.workerID).Logger()
	return b
}

func (b *RedisStreamsBroker) streamKey(queue string, priority job.Priority) string {
	return fmt.Sprintf("%s%s:%s", keyPrefixStream, queue, priority.String())
}

func (b *RedisStreamsBroker) jobKey(jobID string) string {
	return keyPrefixJob + jobID
}

func (b *RedisStreamsBroker) ensureConsumerGroup(ctx context.Context, stream string) error {
	err := b.client.XGroupCreateMkStream(ctx, stream, b.groupName, "0").Err()
	if err != nil && !isGroupExistsError(err) {
		b.logger.Error().Err(err).Str("stream", stream).Str("group", b.groupName).Msg("failed to create consumer group")
		return fmt.Errorf("failed to create consumer group: %w", err)
	}
	if err == nil {
		b.logger.Info().Str("stream", stream).Str("group", b.groupName).Msg("consumer group created")
	}
	return nil
}

func isGroupExistsError(err error) bool {
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
}

func isNoGroupError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return len(errStr) >= 7 && errStr[:7] == "NOGROUP"
}

func (b *RedisStreamsBroker) Enqueue(ctx context.Context, j *job.Job) error {
	if err := j.Validate(); err != nil {
		b.logger.Debug().Str("job_id", j.ID).Msg("job validation failed")
		return ErrInvalidJob
	}

	if j.IsDelayed() {
		b.logger.Debug().
			Str("job_id", j.ID).
			Time("scheduled_at", *j.ScheduledAt).
			Msg("job is delayed, scheduling")
		return b.Schedule(ctx, j, *j.ScheduledAt)
	}

	j.State = job.StateQueued

	jobData, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	streamKey := b.streamKey(j.Queue, j.Priority)

	// Ensure consumer group exists BEFORE adding to stream
	// This prevents race conditions where messages are added but group doesn't exist yet
	if err := b.ensureConsumerGroup(ctx, streamKey); err != nil {
		b.logger.Error().Err(err).Str("stream", streamKey).Msg("failed to ensure consumer group")
		return err
	}

	jobKey := b.jobKey(j.ID)
	if err := b.client.Set(ctx, jobKey, jobData, 0).Err(); err != nil {
		b.logger.Error().Err(err).Str("job_id", j.ID).Msg("failed to store job data")
		return fmt.Errorf("%w: %v", ErrEnqueueFailed, err)
	}

	if err := b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"job_id": j.ID,
		},
	}).Err(); err != nil {
		b.logger.Error().Err(err).Str("job_id", j.ID).Str("stream", streamKey).Msg("failed to add to stream")
		return fmt.Errorf("%w: %v", ErrEnqueueFailed, err)
	}

	b.logger.Debug().
		Str("job_id", j.ID).
		Str("job_type", j.Type).
		Str("queue", j.Queue).
		Str("priority", j.Priority.String()).
		Msg("job enqueued")

	return nil
}

func (b *RedisStreamsBroker) Dequeue(ctx context.Context, queues []string, timeout time.Duration) (*job.Job, error) {
	priorities := []job.Priority{job.PriorityHigh, job.PriorityMedium, job.PriorityLow}

	var streams []string
	for _, queue := range queues {
		for _, priority := range priorities {
			streams = append(streams, b.streamKey(queue, priority))
		}
	}

	for _, stream := range streams {
		if err := b.ensureConsumerGroup(ctx, stream); err != nil {
			return nil, err
		}
	}

	for _, stream := range streams {
		args := &redis.XReadGroupArgs{
			Group:    b.groupName,
			Consumer: b.workerID,
			Streams:  []string{stream, ">"},
			Count:    1,
			Block:    -1,
			NoAck:    false,
		}

		result, err := b.client.XReadGroup(ctx, args).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrDequeueFailed, err)
		}

		if len(result) > 0 && len(result[0].Messages) > 0 {
			return b.processDequeueResult(ctx, result[0].Stream, result[0].Messages[0])
		}
	}

	args := &redis.XReadGroupArgs{
		Group:    b.groupName,
		Consumer: b.workerID,
		Streams:  append(streams, make([]string, len(streams))...),
		Count:    1,
		Block:    timeout,
		NoAck:    false,
	}
	for i := len(streams); i < len(args.Streams); i++ {
		args.Streams[i] = ">"
	}

	result, err := b.client.XReadGroup(ctx, args).Result()
	if err == redis.Nil {
		return nil, ErrQueueEmpty
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDequeueFailed, err)
	}

	if len(result) == 0 || len(result[0].Messages) == 0 {
		return nil, ErrQueueEmpty
	}

	return b.processDequeueResult(ctx, result[0].Stream, result[0].Messages[0])
}

func (b *RedisStreamsBroker) processDequeueResult(ctx context.Context, stream string, msg redis.XMessage) (*job.Job, error) {
	jobID, ok := msg.Values["job_id"].(string)
	if !ok {
		b.logger.Error().Str("stream", stream).Str("message_id", msg.ID).Msg("invalid message format: missing job_id")
		return nil, fmt.Errorf("invalid message format: missing job_id")
	}

	j, err := b.GetJob(ctx, jobID)
	if err != nil {
		b.logger.Error().Err(err).Str("job_id", jobID).Msg("failed to get job during dequeue")
		return nil, err
	}

	if j.Metadata == nil {
		j.Metadata = make(map[string]string)
	}
	j.Metadata["stream"] = stream
	j.Metadata["message_id"] = msg.ID

	if err := j.MarkStarted(); err != nil {
		return nil, err
	}

	jobData, err := json.Marshal(j)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal job: %w", err)
	}

	if err := b.client.Set(ctx, b.jobKey(j.ID), jobData, 0).Err(); err != nil {
		return nil, fmt.Errorf("failed to update job state: %w", err)
	}

	b.logger.Debug().
		Str("job_id", j.ID).
		Str("job_type", j.Type).
		Str("queue", j.Queue).
		Str("stream", stream).
		Msg("job dequeued")

	return j, nil
}

func (b *RedisStreamsBroker) Ack(ctx context.Context, j *job.Job) error {
	stream, ok := j.Metadata["stream"]
	if !ok {
		b.logger.Error().Str("job_id", j.ID).Msg("ack failed: missing stream metadata")
		return fmt.Errorf("%w: missing stream metadata", ErrAckFailed)
	}

	messageID, ok := j.Metadata["message_id"]
	if !ok {
		b.logger.Error().Str("job_id", j.ID).Msg("ack failed: missing message_id metadata")
		return fmt.Errorf("%w: missing message_id metadata", ErrAckFailed)
	}

	if err := b.client.XAck(ctx, stream, b.groupName, messageID).Err(); err != nil {
		b.logger.Error().Err(err).Str("job_id", j.ID).Str("stream", stream).Msg("failed to ack message")
		return fmt.Errorf("%w: %v", ErrAckFailed, err)
	}

	b.client.XDel(ctx, stream, messageID)

	if err := j.MarkCompleted(); err != nil {
		return err
	}

	jobData, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	if err := b.client.Set(ctx, b.jobKey(j.ID), jobData, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to update job: %w", err)
	}

	b.logger.Debug().
		Str("job_id", j.ID).
		Str("job_type", j.Type).
		Msg("job acknowledged")

	return nil
}

func (b *RedisStreamsBroker) Nack(ctx context.Context, j *job.Job, jobErr error) error {
	stream, ok := j.Metadata["stream"]
	if !ok {
		b.logger.Error().Str("job_id", j.ID).Msg("nack failed: missing stream metadata")
		return fmt.Errorf("missing stream metadata")
	}

	messageID, ok := j.Metadata["message_id"]
	if !ok {
		b.logger.Error().Str("job_id", j.ID).Msg("nack failed: missing message_id metadata")
		return fmt.Errorf("missing message_id metadata")
	}

	b.client.XAck(ctx, stream, b.groupName, messageID)
	b.client.XDel(ctx, stream, messageID)

	j.IncrementRetry(jobErr)

	if j.CanRetry() {
		backoffSeconds := 1 << j.RetryCount
		retryAt := time.Now().Add(time.Duration(backoffSeconds) * time.Second)
		j.State = job.StateRetrying

		b.logger.Warn().
			Str("job_id", j.ID).
			Str("job_type", j.Type).
			Int("retry_count", j.RetryCount).
			Int("max_retries", j.MaxRetries).
			Time("retry_at", retryAt).
			Err(jobErr).
			Msg("job failed, scheduling retry")

		return b.Schedule(ctx, j, retryAt)
	}

	if err := j.MarkDead(); err != nil {
		return err
	}

	b.logger.Error().
		Str("job_id", j.ID).
		Str("job_type", j.Type).
		Int("retry_count", j.RetryCount).
		Err(jobErr).
		Msg("job exhausted retries, marked dead")

	jobData, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	if err := b.client.Set(ctx, b.jobKey(j.ID), jobData, 7*24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to update dead job: %w", err)
	}

	return nil
}

func (b *RedisStreamsBroker) Schedule(ctx context.Context, j *job.Job, at time.Time) error {
	if err := j.Validate(); err != nil {
		return ErrInvalidJob
	}

	j.State = job.StateScheduled
	j.ScheduledAt = &at

	jobData, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	if err := b.client.Set(ctx, b.jobKey(j.ID), jobData, 0).Err(); err != nil {
		return fmt.Errorf("failed to store job: %w", err)
	}

	score := float64(at.Unix())
	if err := b.client.ZAdd(ctx, keyPrefixDelayed, redis.Z{
		Score:  score,
		Member: j.ID,
	}).Err(); err != nil {
		return fmt.Errorf("failed to schedule job: %w", err)
	}

	return nil
}

func (b *RedisStreamsBroker) GetJob(ctx context.Context, jobID string) (*job.Job, error) {
	jobKey := b.jobKey(jobID)
	data, err := b.client.Get(ctx, jobKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	var j job.Job
	if err := json.Unmarshal([]byte(data), &j); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	return &j, nil
}

func (b *RedisStreamsBroker) DeleteJob(ctx context.Context, jobID string) error {
	if err := b.client.Del(ctx, b.jobKey(jobID)).Err(); err != nil {
		return fmt.Errorf("failed to delete job: %w", err)
	}

	b.client.ZRem(ctx, keyPrefixDelayed, jobID)

	return nil
}

func (b *RedisStreamsBroker) GetQueueDepth(ctx context.Context, queue string) (int64, error) {
	var total int64

	priorities := []job.Priority{job.PriorityHigh, job.PriorityMedium, job.PriorityLow}

	for _, priority := range priorities {
		streamKey := b.streamKey(queue, priority)
		length, err := b.client.XLen(ctx, streamKey).Result()
		if err != nil && err != redis.Nil {
			return 0, fmt.Errorf("failed to get stream length: %w", err)
		}
		total += length
	}

	return total, nil
}

func (b *RedisStreamsBroker) GetDelayedJobs(ctx context.Context, until time.Time, limit int64) ([]*job.Job, error) {
	maxScore := float64(until.Unix())

	jobIDs, err := b.client.ZRangeByScore(ctx, keyPrefixDelayed, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%f", maxScore),
		Offset: 0,
		Count:  limit,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to get delayed jobs: %w", err)
	}

	jobs := make([]*job.Job, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		j, err := b.GetJob(ctx, jobID)
		if err != nil {
			continue
		}
		jobs = append(jobs, j)
	}

	return jobs, nil
}

func (b *RedisStreamsBroker) MoveDelayedToQueue(ctx context.Context, j *job.Job) error {
	if err := b.client.ZRem(ctx, keyPrefixDelayed, j.ID).Err(); err != nil {
		return fmt.Errorf("failed to remove from delayed set: %w", err)
	}

	j.ScheduledAt = nil
	return b.Enqueue(ctx, j)
}

func (b *RedisStreamsBroker) GetPendingJobs(ctx context.Context, queue string, idleTime time.Duration) ([]*job.Job, error) {
	var allJobs []*job.Job

	priorities := []job.Priority{job.PriorityHigh, job.PriorityMedium, job.PriorityLow}

	for _, priority := range priorities {
		streamKey := b.streamKey(queue, priority)

		pending, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: streamKey,
			Group:  b.groupName,
			Start:  "-",
			End:    "+",
			Count:  100,
		}).Result()

		if err != nil {
			if err == redis.Nil {
				continue
			}
			return nil, fmt.Errorf("failed to get pending messages: %w", err)
		}

		for _, msg := range pending {
			if msg.Idle < idleTime {
				continue
			}

			messages, err := b.client.XRange(ctx, streamKey, msg.ID, msg.ID).Result()
			if err != nil || len(messages) == 0 {
				continue
			}

			jobID, ok := messages[0].Values["job_id"].(string)
			if !ok {
				continue
			}

			j, err := b.GetJob(ctx, jobID)
			if err != nil {
				continue
			}

			if j.Metadata == nil {
				j.Metadata = make(map[string]string)
			}
			j.Metadata["stream"] = streamKey
			j.Metadata["message_id"] = msg.ID

			allJobs = append(allJobs, j)
		}
	}

	return allJobs, nil
}

func (b *RedisStreamsBroker) RequeueStaleJob(ctx context.Context, j *job.Job) error {
	stream, ok := j.Metadata["stream"]
	if !ok {
		return fmt.Errorf("missing stream metadata")
	}

	messageID, ok := j.Metadata["message_id"]
	if !ok {
		return fmt.Errorf("missing message_id metadata")
	}

	msgs, err := b.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   stream,
		Group:    b.groupName,
		Consumer: b.workerID,
		MinIdle:  b.claimIdle,
		Messages: []string{messageID},
	}).Result()

	if err != nil {
		return fmt.Errorf("failed to claim message: %w", err)
	}

	if len(msgs) == 0 {
		return fmt.Errorf("message %s not found or already claimed", messageID)
	}

	b.client.XAck(ctx, stream, b.groupName, messageID)
	b.client.XDel(ctx, stream, messageID)

	j.State = job.StatePending
	return b.Enqueue(ctx, j)
}

func (b *RedisStreamsBroker) Ping(ctx context.Context) error {
	return b.client.Ping(ctx).Err()
}

func (b *RedisStreamsBroker) Close() error {
	return b.client.Close()
}

func (b *RedisStreamsBroker) GetStats(ctx context.Context, queue string) (*Stats, error) {
	stats := &Stats{
		Queue:           queue,
		DepthByPriority: make(map[string]int64),
	}

	priorities := []job.Priority{job.PriorityHigh, job.PriorityMedium, job.PriorityLow}

	for _, priority := range priorities {
		streamKey := b.streamKey(queue, priority)
		length, err := b.client.XLen(ctx, streamKey).Result()
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("failed to get stream length: %w", err)
		}

		stats.DepthByPriority[priority.String()] = length
		stats.Depth += length

		pending, err := b.client.XPending(ctx, streamKey, b.groupName).Result()
		if err != nil && err != redis.Nil && !isNoGroupError(err) {
			return nil, fmt.Errorf("failed to get pending count: %w", err)
		}
		if pending != nil {
			stats.Processing += pending.Count
		}
	}

	delayed, err := b.client.ZCard(ctx, keyPrefixDelayed).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get delayed count: %w", err)
	}
	stats.Delayed = delayed
	stats.Failed = 0

	return stats, nil
}
