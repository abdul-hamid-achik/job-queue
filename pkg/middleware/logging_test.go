package middleware_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/abdul-hamid-achik/job-queue/pkg/middleware"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestLoggingMiddleware(t *testing.T) {
	t.Run("logs successful job", func(t *testing.T) {
		var buf bytes.Buffer
		logger := zerolog.New(&buf)

		mw := middleware.LoggingMiddleware(logger)
		handler := mw(func(ctx context.Context, j *job.Job) error {
			return nil
		})

		j := &job.Job{
			ID:       "test-123",
			Type:     "email.send",
			Queue:    "default",
			Priority: job.PriorityHigh,
		}

		err := handler(context.Background(), j)

		assert.NoError(t, err)
		output := buf.String()
		assert.Contains(t, output, "test-123")
		assert.Contains(t, output, "email.send")
		assert.Contains(t, output, "job started")
		assert.Contains(t, output, "job completed")
	})

	t.Run("logs failed job", func(t *testing.T) {
		var buf bytes.Buffer
		logger := zerolog.New(&buf)

		mw := middleware.LoggingMiddleware(logger)
		handler := mw(func(ctx context.Context, j *job.Job) error {
			return errors.New("handler failed")
		})

		j := &job.Job{
			ID:       "test-456",
			Type:     "payment.process",
			Queue:    "critical",
			Priority: job.PriorityHigh,
		}

		err := handler(context.Background(), j)

		assert.Error(t, err)
		output := buf.String()
		assert.Contains(t, output, "test-456")
		assert.Contains(t, output, "job failed")
	})

	t.Run("logs retry count", func(t *testing.T) {
		var buf bytes.Buffer
		logger := zerolog.New(&buf)

		mw := middleware.LoggingMiddleware(logger)
		handler := mw(func(ctx context.Context, j *job.Job) error {
			return nil
		})

		j := &job.Job{
			ID:         "test-789",
			Type:       "retry.test",
			Queue:      "default",
			Priority:   job.PriorityMedium,
			RetryCount: 3,
		}

		err := handler(context.Background(), j)

		assert.NoError(t, err)
		output := buf.String()
		assert.Contains(t, output, "retry_count")
	})
}

func TestRecoveryMiddleware(t *testing.T) {
	t.Run("recovers from panic with string", func(t *testing.T) {
		var buf bytes.Buffer
		logger := zerolog.New(&buf)

		mw := middleware.RecoveryMiddleware(logger)
		handler := mw(func(ctx context.Context, j *job.Job) error {
			panic("something went wrong")
		})

		j := &job.Job{ID: "panic-1", Type: "panic.test"}
		err := handler(context.Background(), j)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "panic")
		assert.Contains(t, err.Error(), "something went wrong")
		assert.Contains(t, buf.String(), "panicked")
	})

	t.Run("recovers from panic with error", func(t *testing.T) {
		var buf bytes.Buffer
		logger := zerolog.New(&buf)

		mw := middleware.RecoveryMiddleware(logger)
		handler := mw(func(ctx context.Context, j *job.Job) error {
			panic(errors.New("error panic"))
		})

		j := &job.Job{ID: "panic-2", Type: "panic.test"}
		err := handler(context.Background(), j)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error panic")
	})

	t.Run("recovers from panic with arbitrary value", func(t *testing.T) {
		var buf bytes.Buffer
		logger := zerolog.New(&buf)

		mw := middleware.RecoveryMiddleware(logger)
		handler := mw(func(ctx context.Context, j *job.Job) error {
			panic(12345)
		})

		j := &job.Job{ID: "panic-3", Type: "panic.test"}
		err := handler(context.Background(), j)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "panic")
	})

	t.Run("passes through normal errors", func(t *testing.T) {
		var buf bytes.Buffer
		logger := zerolog.New(&buf)

		expectedErr := errors.New("normal error")
		mw := middleware.RecoveryMiddleware(logger)
		handler := mw(func(ctx context.Context, j *job.Job) error {
			return expectedErr
		})

		j := &job.Job{ID: "no-panic", Type: "normal.test"}
		err := handler(context.Background(), j)

		assert.Equal(t, expectedErr, err)
	})

	t.Run("passes through nil error", func(t *testing.T) {
		var buf bytes.Buffer
		logger := zerolog.New(&buf)

		mw := middleware.RecoveryMiddleware(logger)
		handler := mw(func(ctx context.Context, j *job.Job) error {
			return nil
		})

		j := &job.Job{ID: "success", Type: "success.test"}
		err := handler(context.Background(), j)

		assert.NoError(t, err)
	})
}

func TestMetricsMiddleware(t *testing.T) {
	t.Run("calls collector on success", func(t *testing.T) {
		collector := &mockMetricsCollector{}
		mw := middleware.MetricsMiddleware(collector)

		handler := mw(func(ctx context.Context, j *job.Job) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		})

		j := &job.Job{ID: "metrics-1", Type: "metrics.test", Queue: "default"}
		err := handler(context.Background(), j)

		assert.NoError(t, err)
		assert.Equal(t, 1, collector.startedCount)
		assert.Equal(t, 1, collector.completedCount)
		assert.Equal(t, 0, collector.failedCount)
	})

	t.Run("calls collector on failure", func(t *testing.T) {
		collector := &mockMetricsCollector{}
		mw := middleware.MetricsMiddleware(collector)

		handler := mw(func(ctx context.Context, j *job.Job) error {
			return errors.New("failed")
		})

		j := &job.Job{ID: "metrics-2", Type: "metrics.test", Queue: "default"}
		err := handler(context.Background(), j)

		assert.Error(t, err)
		assert.Equal(t, 1, collector.startedCount)
		assert.Equal(t, 0, collector.completedCount)
		assert.Equal(t, 1, collector.failedCount)
	})

	t.Run("calls retrying when job can retry", func(t *testing.T) {
		collector := &mockMetricsCollector{}
		mw := middleware.MetricsMiddleware(collector)

		handler := mw(func(ctx context.Context, j *job.Job) error {
			return errors.New("failed")
		})

		j := &job.Job{
			ID:         "metrics-3",
			Type:       "metrics.test",
			Queue:      "default",
			MaxRetries: 3,
			RetryCount: 0,
		}
		err := handler(context.Background(), j)

		assert.Error(t, err)
		assert.Equal(t, 1, collector.retryingCount)
	})
}

func TestPanicError(t *testing.T) {
	t.Run("Error() with string value", func(t *testing.T) {
		pe := &middleware.PanicError{Value: "test panic"}
		assert.Contains(t, pe.Error(), "panic")
		assert.Contains(t, pe.Error(), "test panic")
	})

	t.Run("Error() with error value", func(t *testing.T) {
		pe := &middleware.PanicError{Value: errors.New("inner error")}
		assert.Contains(t, pe.Error(), "inner error")
	})

	t.Run("Error() with other value", func(t *testing.T) {
		pe := &middleware.PanicError{Value: 42}
		assert.Contains(t, pe.Error(), "42")
	})
}

// mockMetricsCollector implements middleware.MetricsCollector for testing
type mockMetricsCollector struct {
	startedCount   int
	completedCount int
	failedCount    int
	retryingCount  int
}

func (m *mockMetricsCollector) JobStarted(jobType, queue string) {
	m.startedCount++
}

func (m *mockMetricsCollector) JobCompleted(jobType, queue string, duration time.Duration) {
	m.completedCount++
}

func (m *mockMetricsCollector) JobFailed(jobType, queue string, duration time.Duration) {
	m.failedCount++
}

func (m *mockMetricsCollector) JobRetrying(jobType, queue string, attempt int) {
	m.retryingCount++
}
