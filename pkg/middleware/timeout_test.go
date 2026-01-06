package middleware_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/abdul-hamid-achik/job-queue/pkg/middleware"
	"github.com/stretchr/testify/assert"
)

func TestTimeoutMiddleware(t *testing.T) {
	t.Run("completes before timeout", func(t *testing.T) {
		mw := middleware.TimeoutMiddleware(1 * time.Second)

		handler := mw(func(ctx context.Context, j *job.Job) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		})

		j := &job.Job{ID: "test-1", Type: "test"}
		err := handler(context.Background(), j)

		assert.NoError(t, err)
	})

	t.Run("times out with default timeout", func(t *testing.T) {
		mw := middleware.TimeoutMiddleware(50 * time.Millisecond)

		handler := mw(func(ctx context.Context, j *job.Job) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
				return nil
			}
		})

		j := &job.Job{ID: "test-2", Type: "test"}
		err := handler(context.Background(), j)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timed out")
	})

	t.Run("uses job timeout when set", func(t *testing.T) {
		mw := middleware.TimeoutMiddleware(1 * time.Hour) // Very long default

		handler := mw(func(ctx context.Context, j *job.Job) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
				return nil
			}
		})

		j := &job.Job{ID: "test-3", Type: "test", Timeout: 50 * time.Millisecond}
		err := handler(context.Background(), j)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timed out")
	})

	t.Run("propagates handler error", func(t *testing.T) {
		mw := middleware.TimeoutMiddleware(1 * time.Second)

		expectedErr := errors.New("handler error")
		handler := mw(func(ctx context.Context, j *job.Job) error {
			return expectedErr
		})

		j := &job.Job{ID: "test-4", Type: "test"}
		err := handler(context.Background(), j)

		assert.Equal(t, expectedErr, err)
	})

	t.Run("context cancellation", func(t *testing.T) {
		mw := middleware.TimeoutMiddleware(1 * time.Second)

		handler := mw(func(ctx context.Context, j *job.Job) error {
			<-ctx.Done()
			return ctx.Err()
		})

		ctx, cancel := context.WithCancel(context.Background())
		j := &job.Job{ID: "test-5", Type: "test"}

		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		err := handler(ctx, j)
		assert.Error(t, err)
	})
}
