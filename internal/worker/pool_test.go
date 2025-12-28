package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abdul-hamid-achik/job-queue/internal/job"
	"github.com/abdul-hamid-achik/job-queue/internal/worker"
	"github.com/abdul-hamid-achik/job-queue/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool_Start(t *testing.T) {
	t.Run("processes jobs until context cancelled", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		registry := worker.NewRegistry()

		var processed int32
		registry.MustRegister("test.job", func(ctx context.Context, j *job.Job) error {
			atomic.AddInt32(&processed, 1)
			return nil
		})

		// Enqueue some jobs
		for i := 0; i < 5; i++ {
			broker.Enqueue(context.Background(), testutil.NewRandomTestJob())
		}

		pool := worker.NewPool(broker, registry, worker.WithConcurrency(2))

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		pool.Start(ctx)

		// All jobs should be processed
		assert.Equal(t, int32(5), atomic.LoadInt32(&processed))
	})

	t.Run("handles empty queue gracefully", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		registry := worker.NewRegistry()
		registry.MustRegister("test.job", func(ctx context.Context, j *job.Job) error {
			return nil
		})

		pool := worker.NewPool(broker, registry, worker.WithConcurrency(2))

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Should not panic or error with empty queue
		pool.Start(ctx)
	})
}

func TestPool_Stop(t *testing.T) {
	t.Run("gracefully stops workers", func(t *testing.T) {
		broker := testutil.NewMockBroker()
		registry := worker.NewRegistry()

		// Add a slow handler
		registry.MustRegister("slow", func(ctx context.Context, j *job.Job) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
				return nil
			}
		})

		// Enqueue a job
		broker.Enqueue(context.Background(), testutil.NewTestJobWithID("slow-1",
			func(j *job.Job) { j.Type = "slow" },
		))

		pool := worker.NewPool(broker, registry, worker.WithConcurrency(1))

		ctx, cancel := context.WithCancel(context.Background())

		// Start pool in goroutine
		done := make(chan error, 1)
		go func() {
			done <- pool.Start(ctx)
		}()

		// Let it start processing
		time.Sleep(50 * time.Millisecond)

		// Stop
		cancel()

		// Wait with timeout
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()

		err := pool.Stop(stopCtx)
		assert.NoError(t, err)
	})
}

func TestPool_RegisterHandler(t *testing.T) {
	broker := testutil.NewMockBroker()
	registry := worker.NewRegistry()
	pool := worker.NewPool(broker, registry)

	err := pool.RegisterHandler("test.job", func(ctx context.Context, j *job.Job) error {
		return nil
	})

	assert.NoError(t, err)
}

func TestPool_MustRegisterHandler(t *testing.T) {
	broker := testutil.NewMockBroker()
	registry := worker.NewRegistry()
	pool := worker.NewPool(broker, registry)

	// First registration should succeed
	assert.NotPanics(t, func() {
		pool.MustRegisterHandler("test", func(ctx context.Context, j *job.Job) error {
			return nil
		})
	})

	// Duplicate registration should panic
	assert.Panics(t, func() {
		pool.MustRegisterHandler("test", func(ctx context.Context, j *job.Job) error {
			return nil
		})
	})
}

func TestPool_AcksSuccessfulJobs(t *testing.T) {
	broker := testutil.NewMockBroker()
	registry := worker.NewRegistry()

	registry.MustRegister("test.job", func(ctx context.Context, j *job.Job) error {
		return nil
	})

	job := testutil.NewRandomTestJob()
	broker.Enqueue(context.Background(), job)

	pool := worker.NewPool(broker, registry, worker.WithConcurrency(1))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	pool.Start(ctx)

	// Verify job was processed (acked)
	assert.Len(t, broker.ProcessedJobs(), 1)
	assert.Len(t, broker.FailedJobs(), 0)
}

func TestPool_NacksFailedJobs(t *testing.T) {
	broker := testutil.NewMockBroker()
	registry := worker.NewRegistry()

	registry.MustRegister("test.job", func(ctx context.Context, j *job.Job) error {
		return errors.New("handler error")
	})

	job := testutil.NewRandomTestJob()
	broker.Enqueue(context.Background(), job)

	pool := worker.NewPool(broker, registry, worker.WithConcurrency(1))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	pool.Start(ctx)

	// Verify job failed (nacked)
	assert.Len(t, broker.FailedJobs(), 1)
	assert.Len(t, broker.ProcessedJobs(), 0)
}

func TestPool_HandlesPanic(t *testing.T) {
	broker := testutil.NewMockBroker()
	registry := worker.NewRegistry()

	registry.MustRegister("panicking", func(ctx context.Context, j *job.Job) error {
		panic("handler panicked!")
	})

	job := testutil.NewTestJobWithID("panic-job", func(j *job.Job) {
		j.Type = "panicking"
	})
	broker.Enqueue(context.Background(), job)

	pool := worker.NewPool(broker, registry, worker.WithConcurrency(1))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Should not crash
	assert.NotPanics(t, func() {
		pool.Start(ctx)
	})

	// Job should be nacked
	assert.Len(t, broker.FailedJobs(), 1)
}

func TestPool_WorkerCount(t *testing.T) {
	broker := testutil.NewMockBroker()
	registry := worker.NewRegistry()

	pool := worker.NewPool(broker, registry, worker.WithConcurrency(5))

	assert.Equal(t, 5, pool.WorkerCount())
}

func TestPool_Queues(t *testing.T) {
	broker := testutil.NewMockBroker()
	registry := worker.NewRegistry()

	pool := worker.NewPool(broker, registry,
		worker.WithPoolQueues([]string{"critical", "default", "low"}),
	)

	assert.Equal(t, []string{"critical", "default", "low"}, pool.Queues())
}

func TestRegistry_Register(t *testing.T) {
	t.Run("registers handler", func(t *testing.T) {
		registry := worker.NewRegistry()
		handler := func(ctx context.Context, j *job.Job) error { return nil }

		err := registry.Register("test.job", handler)
		assert.NoError(t, err)
		assert.True(t, registry.Has("test.job"))
	})

	t.Run("fails on duplicate", func(t *testing.T) {
		registry := worker.NewRegistry()
		handler := func(ctx context.Context, j *job.Job) error { return nil }

		registry.Register("test.job", handler)
		err := registry.Register("test.job", handler)

		assert.ErrorIs(t, err, worker.ErrHandlerExists)
	})
}

func TestRegistry_Get(t *testing.T) {
	t.Run("returns handler", func(t *testing.T) {
		registry := worker.NewRegistry()
		called := false
		registry.Register("test", func(ctx context.Context, j *job.Job) error {
			called = true
			return nil
		})

		handler, err := registry.Get("test")
		require.NoError(t, err)

		handler(context.Background(), nil)
		assert.True(t, called)
	})

	t.Run("returns error for unknown type", func(t *testing.T) {
		registry := worker.NewRegistry()

		_, err := registry.Get("unknown")
		assert.ErrorIs(t, err, worker.ErrHandlerNotFound)
	})
}

func TestRegistry_Types(t *testing.T) {
	registry := worker.NewRegistry()
	handler := func(ctx context.Context, j *job.Job) error { return nil }

	registry.Register("email.send", handler)
	registry.Register("image.resize", handler)
	registry.Register("notification.push", handler)

	types := registry.Types()
	assert.Len(t, types, 3)
	assert.Contains(t, types, "email.send")
	assert.Contains(t, types, "image.resize")
	assert.Contains(t, types, "notification.push")
}

func TestRegistry_Unregister(t *testing.T) {
	registry := worker.NewRegistry()
	registry.Register("test", func(ctx context.Context, j *job.Job) error { return nil })

	assert.True(t, registry.Has("test"))

	registry.Unregister("test")

	assert.False(t, registry.Has("test"))
}

func TestRegistry_Clear(t *testing.T) {
	registry := worker.NewRegistry()
	registry.Register("test1", func(ctx context.Context, j *job.Job) error { return nil })
	registry.Register("test2", func(ctx context.Context, j *job.Job) error { return nil })

	registry.Clear()

	assert.Empty(t, registry.Types())
}

func TestRegistry_Middleware(t *testing.T) {
	registry := worker.NewRegistry()

	// Track middleware calls
	var callOrder []string

	// Add middleware
	registry.Use(func(next worker.HandlerFunc) worker.HandlerFunc {
		return func(ctx context.Context, j *job.Job) error {
			callOrder = append(callOrder, "middleware1-before")
			err := next(ctx, j)
			callOrder = append(callOrder, "middleware1-after")
			return err
		}
	})

	registry.Use(func(next worker.HandlerFunc) worker.HandlerFunc {
		return func(ctx context.Context, j *job.Job) error {
			callOrder = append(callOrder, "middleware2-before")
			err := next(ctx, j)
			callOrder = append(callOrder, "middleware2-after")
			return err
		}
	})

	// Register handler
	registry.Register("test", func(ctx context.Context, j *job.Job) error {
		callOrder = append(callOrder, "handler")
		return nil
	})

	// Get and execute
	handler, _ := registry.Get("test")
	handler(context.Background(), nil)

	// Verify order: outer middleware wraps inner
	expected := []string{
		"middleware1-before",
		"middleware2-before",
		"handler",
		"middleware2-after",
		"middleware1-after",
	}
	assert.Equal(t, expected, callOrder)
}
