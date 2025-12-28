package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/abdul-hamid-achik/job-queue/internal/job"
	"github.com/abdul-hamid-achik/job-queue/internal/worker"
	"github.com/rs/zerolog"
)

func LoggingMiddleware(logger zerolog.Logger) worker.Middleware {
	return func(next worker.HandlerFunc) worker.HandlerFunc {
		return func(ctx context.Context, j *job.Job) error {
			start := time.Now()

			log := logger.With().
				Str("job_id", j.ID).
				Str("job_type", j.Type).
				Str("queue", j.Queue).
				Str("priority", j.Priority.String()).
				Int("retry_count", j.RetryCount).
				Logger()

			log.Info().Msg("job started")

			err := next(ctx, j)

			duration := time.Since(start)

			if err != nil {
				log.Error().
					Err(err).
					Dur("duration", duration).
					Bool("will_retry", j.CanRetry()).
					Msg("job failed")
			} else {
				log.Info().
					Dur("duration", duration).
					Msg("job completed")
			}

			return err
		}
	}
}

type MetricsCollector interface {
	JobStarted(jobType, queue string)
	JobCompleted(jobType, queue string, duration time.Duration)
	JobFailed(jobType, queue string, duration time.Duration)
	JobRetrying(jobType, queue string, attempt int)
}

func MetricsMiddleware(collector MetricsCollector) worker.Middleware {
	return func(next worker.HandlerFunc) worker.HandlerFunc {
		return func(ctx context.Context, j *job.Job) error {
			start := time.Now()
			collector.JobStarted(j.Type, j.Queue)

			err := next(ctx, j)

			duration := time.Since(start)
			if err != nil {
				collector.JobFailed(j.Type, j.Queue, duration)
				if j.CanRetry() {
					collector.JobRetrying(j.Type, j.Queue, j.RetryCount+1)
				}
			} else {
				collector.JobCompleted(j.Type, j.Queue, duration)
			}

			return err
		}
	}
}

func RecoveryMiddleware(logger zerolog.Logger) worker.Middleware {
	return func(next worker.HandlerFunc) worker.HandlerFunc {
		return func(ctx context.Context, j *job.Job) (err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error().
						Str("job_id", j.ID).
						Str("job_type", j.Type).
						Interface("panic", r).
						Msg("job handler panicked")

					switch v := r.(type) {
					case error:
						err = v
					default:
						err = &PanicError{Value: r}
					}
				}
			}()

			return next(ctx, j)
		}
	}
}

type PanicError struct {
	Value interface{}
}

func (e *PanicError) Error() string {
	switch val := e.Value.(type) {
	case string:
		return fmt.Sprintf("panic: %s", val)
	case error:
		return fmt.Sprintf("panic: %s", val.Error())
	default:
		return fmt.Sprintf("panic: %v", e.Value)
	}
}
