package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/broker"
	"github.com/rs/zerolog"
)

type Scheduler struct {
	broker          broker.Broker
	pollInterval    time.Duration
	batchSize       int64
	staleJobTimeout time.Duration
	logger          zerolog.Logger

	running bool
	mu      sync.RWMutex
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

type SchedulerOption func(*Scheduler)

func WithSchedulerPollInterval(d time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		s.pollInterval = d
	}
}

func WithBatchSize(n int64) SchedulerOption {
	return func(s *Scheduler) {
		s.batchSize = n
	}
}

func WithStaleJobTimeout(d time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		s.staleJobTimeout = d
	}
}

func WithSchedulerLogger(logger zerolog.Logger) SchedulerOption {
	return func(s *Scheduler) {
		s.logger = logger
	}
}

func New(b broker.Broker, opts ...SchedulerOption) *Scheduler {
	s := &Scheduler{
		broker:          b,
		pollInterval:    1 * time.Second,
		batchSize:       100,
		staleJobTimeout: 5 * time.Minute,
		logger:          zerolog.Nop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = true
	ctx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()

	s.logger.Info().
		Dur("poll_interval", s.pollInterval).
		Int64("batch_size", s.batchSize).
		Msg("scheduler started")

	s.wg.Add(2)
	go s.runDelayedJobMover(ctx)
	go s.runStaleJobDetector(ctx)

	<-ctx.Done()

	s.wg.Wait()

	s.mu.Lock()
	s.running = false
	s.mu.Unlock()

	s.logger.Info().Msg("scheduler stopped")
	return nil
}

func (s *Scheduler) Stop() {
	s.mu.RLock()
	cancel := s.cancel
	s.mu.RUnlock()

	if cancel != nil {
		cancel()
	}
}

func (s *Scheduler) runDelayedJobMover(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.processDelayedJobs(ctx); err != nil {
				s.logger.Error().Err(err).Msg("error processing delayed jobs")
			}
		}
	}
}

func (s *Scheduler) processDelayedJobs(ctx context.Context) error {
	now := time.Now().UTC()

	jobs, err := s.broker.GetDelayedJobs(ctx, now, s.batchSize)
	if err != nil {
		return err
	}

	for _, j := range jobs {
		if err := s.broker.MoveDelayedToQueue(ctx, j); err != nil {
			s.logger.Error().
				Err(err).
				Str("job_id", j.ID).
				Msg("failed to move delayed job to queue")
			continue
		}

		s.logger.Debug().
			Str("job_id", j.ID).
			Str("queue", j.Queue).
			Msg("moved delayed job to queue")
	}

	return nil
}

func (s *Scheduler) runStaleJobDetector(ctx context.Context) {
	defer s.wg.Done()

	checkInterval := s.staleJobTimeout / 2
	if checkInterval < s.pollInterval {
		checkInterval = s.pollInterval
	}

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.processStaleJobs(ctx); err != nil {
				s.logger.Error().Err(err).Msg("error processing stale jobs")
			}
		}
	}
}

func (s *Scheduler) processStaleJobs(ctx context.Context) error {
	queues := []string{"default", "critical", "low"}

	for _, queue := range queues {
		staleJobs, err := s.broker.GetPendingJobs(ctx, queue, s.staleJobTimeout)
		if err != nil {
			s.logger.Error().
				Err(err).
				Str("queue", queue).
				Msg("failed to get pending jobs")
			continue
		}

		for _, j := range staleJobs {
			if err := s.broker.RequeueStaleJob(ctx, j); err != nil {
				s.logger.Error().
					Err(err).
					Str("job_id", j.ID).
					Msg("failed to requeue stale job")
				continue
			}

			s.logger.Warn().
				Str("job_id", j.ID).
				Str("queue", queue).
				Msg("reclaimed stale job")
		}
	}

	return nil
}

func (s *Scheduler) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}
