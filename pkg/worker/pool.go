package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/broker"
	"github.com/rs/zerolog"
)

type Pool struct {
	broker          broker.Broker
	registry        *Registry
	concurrency     int
	queues          []string
	pollInterval    time.Duration
	shutdownTimeout time.Duration
	logger          zerolog.Logger

	workers []*Worker
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	running bool
	mu      sync.RWMutex
}

type PoolOption func(*Pool)

func WithConcurrency(n int) PoolOption {
	return func(p *Pool) {
		if n > 0 {
			p.concurrency = n
		}
	}
}

func WithPoolQueues(queues []string) PoolOption {
	return func(p *Pool) {
		p.queues = queues
	}
}

func WithPoolPollInterval(d time.Duration) PoolOption {
	return func(p *Pool) {
		p.pollInterval = d
	}
}

func WithShutdownTimeout(d time.Duration) PoolOption {
	return func(p *Pool) {
		p.shutdownTimeout = d
	}
}

func WithPoolLogger(logger zerolog.Logger) PoolOption {
	return func(p *Pool) {
		p.logger = logger
	}
}

func NewPool(b broker.Broker, registry *Registry, opts ...PoolOption) *Pool {
	p := &Pool{
		broker:          b,
		registry:        registry,
		concurrency:     10,
		queues:          []string{"default"},
		pollInterval:    5 * time.Second,
		shutdownTimeout: 30 * time.Second,
		logger:          zerolog.Nop(),
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (p *Pool) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return errors.New("pool already running")
	}
	p.running = true
	ctx, p.cancel = context.WithCancel(ctx)
	p.mu.Unlock()

	p.logger.Info().
		Int("concurrency", p.concurrency).
		Strs("queues", p.queues).
		Msg("starting worker pool")

	for i := 0; i < p.concurrency; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		worker := NewWorker(workerID, p.broker, p.registry,
			WithQueues(p.queues),
			WithPollInterval(p.pollInterval),
			WithLogger(p.logger),
		)
		p.workers = append(p.workers, worker)

		p.wg.Add(1)
		go func(w *Worker) {
			defer p.wg.Done()
			w.Run(ctx)
		}(worker)
	}

	p.wg.Wait()

	p.mu.Lock()
	p.running = false
	p.mu.Unlock()

	return nil
}

func (p *Pool) Stop(ctx context.Context) error {
	p.mu.RLock()
	if !p.running {
		p.mu.RUnlock()
		return nil
	}
	cancel := p.cancel
	p.mu.RUnlock()

	p.logger.Info().Msg("stopping worker pool")

	if cancel != nil {
		cancel()
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info().Msg("worker pool stopped")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.running
}

func (p *Pool) WorkerCount() int {
	return p.concurrency
}

func (p *Pool) Queues() []string {
	return p.queues
}

func (p *Pool) RegisterHandler(jobType string, handler HandlerFunc) error {
	return p.registry.Register(jobType, handler)
}

func (p *Pool) MustRegisterHandler(jobType string, handler HandlerFunc) {
	p.registry.MustRegister(jobType, handler)
}

func (p *Pool) Use(mw ...Middleware) {
	p.registry.Use(mw...)
}
