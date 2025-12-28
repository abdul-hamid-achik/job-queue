package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/abdul-hamid-achik/job-queue/internal/broker"
	"github.com/abdul-hamid-achik/job-queue/internal/job"
	"github.com/rs/zerolog"
)

type Worker struct {
	id           string
	broker       broker.Broker
	registry     *Registry
	queues       []string
	pollInterval time.Duration
	logger       zerolog.Logger
}

type WorkerOption func(*Worker)

func WithQueues(queues []string) WorkerOption {
	return func(w *Worker) {
		w.queues = queues
	}
}

func WithPollInterval(d time.Duration) WorkerOption {
	return func(w *Worker) {
		w.pollInterval = d
	}
}

func WithLogger(logger zerolog.Logger) WorkerOption {
	return func(w *Worker) {
		w.logger = logger
	}
}

func NewWorker(id string, b broker.Broker, registry *Registry, opts ...WorkerOption) *Worker {
	w := &Worker{
		id:           id,
		broker:       b,
		registry:     registry,
		queues:       []string{"default"},
		pollInterval: 5 * time.Second,
		logger:       zerolog.Nop(),
	}

	for _, opt := range opts {
		opt(w)
	}

	w.logger = w.logger.With().Str("worker_id", id).Logger()
	return w
}

func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info().Strs("queues", w.queues).Msg("worker started")

	for {
		select {
		case <-ctx.Done():
			w.logger.Info().Msg("worker stopping")
			return ctx.Err()
		default:
			if err := w.processOne(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				w.logger.Error().Err(err).Msg("error processing job")
			}
		}
	}
}

func (w *Worker) processOne(ctx context.Context) error {
	j, err := w.broker.Dequeue(ctx, w.queues, w.pollInterval)
	if err == broker.ErrQueueEmpty {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to dequeue job: %w", err)
	}
	return w.process(ctx, j)
}

func (w *Worker) process(ctx context.Context, j *job.Job) error {
	logger := w.logger.With().
		Str("job_id", j.ID).
		Str("job_type", j.Type).
		Str("queue", j.Queue).
		Int("retry_count", j.RetryCount).
		Logger()

	logger.Info().Msg("processing job")
	startTime := time.Now()

	handler, err := w.registry.Get(j.Type)
	if err != nil {
		logger.Error().Err(err).Msg("handler not found")
		return w.broker.Nack(ctx, j, err)
	}

	jobCtx, cancel := context.WithTimeout(ctx, j.Timeout)
	defer cancel()

	execErr := w.executeHandler(jobCtx, handler, j)

	duration := time.Since(startTime)
	logger = logger.With().Dur("duration", duration).Logger()

	if execErr != nil {
		logger.Error().Err(execErr).Msg("job failed")
		return w.broker.Nack(ctx, j, execErr)
	}

	logger.Info().Msg("job completed")
	return w.broker.Ack(ctx, j)
}

func (w *Worker) executeHandler(ctx context.Context, handler HandlerFunc, j *job.Job) (err error) {
	done := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic in handler: %v", r)
			}
		}()
		done <- handler(ctx, j)
	}()

	select {
	case err = <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("job timeout: %w", ctx.Err())
	}
}

func (w *Worker) ID() string {
	return w.id
}
