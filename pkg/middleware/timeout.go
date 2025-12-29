package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/abdul-hamid-achik/job-queue/pkg/worker"
)

func TimeoutMiddleware(defaultTimeout time.Duration) worker.Middleware {
	return func(next worker.HandlerFunc) worker.HandlerFunc {
		return func(ctx context.Context, j *job.Job) error {
			timeout := j.Timeout
			if timeout == 0 {
				timeout = defaultTimeout
			}

			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			done := make(chan error, 1)
			go func() {
				done <- next(ctx, j)
			}()

			select {
			case err := <-done:
				return err
			case <-ctx.Done():
				return fmt.Errorf("job timed out after %v: %w", timeout, ctx.Err())
			}
		}
	}
}
