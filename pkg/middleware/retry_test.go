package middleware_test

import (
	"errors"
	"testing"
	"time"

	"github.com/abdul-hamid-achik/job-queue/pkg/middleware"
	"github.com/stretchr/testify/assert"
)

func TestBackoffConfig_CalculateDelay(t *testing.T) {
	t.Run("constant backoff", func(t *testing.T) {
		config := middleware.BackoffConfig{
			Strategy:     middleware.BackoffConstant,
			BaseDelay:    100 * time.Millisecond,
			MaxDelay:     10 * time.Second,
			JitterFactor: 0, // No jitter for deterministic test
		}

		assert.Equal(t, 100*time.Millisecond, config.CalculateDelay(0))
		assert.Equal(t, 100*time.Millisecond, config.CalculateDelay(1))
		assert.Equal(t, 100*time.Millisecond, config.CalculateDelay(5))
	})

	t.Run("linear backoff", func(t *testing.T) {
		config := middleware.BackoffConfig{
			Strategy:     middleware.BackoffLinear,
			BaseDelay:    100 * time.Millisecond,
			MaxDelay:     10 * time.Second,
			JitterFactor: 0,
		}

		assert.Equal(t, 100*time.Millisecond, config.CalculateDelay(0))
		assert.Equal(t, 200*time.Millisecond, config.CalculateDelay(1))
		assert.Equal(t, 300*time.Millisecond, config.CalculateDelay(2))
		assert.Equal(t, 500*time.Millisecond, config.CalculateDelay(4))
	})

	t.Run("exponential backoff", func(t *testing.T) {
		config := middleware.BackoffConfig{
			Strategy:     middleware.BackoffExponential,
			BaseDelay:    100 * time.Millisecond,
			MaxDelay:     10 * time.Second,
			JitterFactor: 0,
			Multiplier:   2.0,
		}

		assert.Equal(t, 100*time.Millisecond, config.CalculateDelay(0))
		assert.Equal(t, 200*time.Millisecond, config.CalculateDelay(1))
		assert.Equal(t, 400*time.Millisecond, config.CalculateDelay(2))
		assert.Equal(t, 800*time.Millisecond, config.CalculateDelay(3))
		assert.Equal(t, 1600*time.Millisecond, config.CalculateDelay(4))
	})

	t.Run("respects max delay", func(t *testing.T) {
		config := middleware.BackoffConfig{
			Strategy:     middleware.BackoffExponential,
			BaseDelay:    100 * time.Millisecond,
			MaxDelay:     500 * time.Millisecond,
			JitterFactor: 0,
			Multiplier:   2.0,
		}

		assert.Equal(t, 100*time.Millisecond, config.CalculateDelay(0))
		assert.Equal(t, 200*time.Millisecond, config.CalculateDelay(1))
		assert.Equal(t, 400*time.Millisecond, config.CalculateDelay(2))
		assert.Equal(t, 500*time.Millisecond, config.CalculateDelay(3))  // Capped
		assert.Equal(t, 500*time.Millisecond, config.CalculateDelay(10)) // Still capped
	})

	t.Run("with jitter stays in range", func(t *testing.T) {
		config := middleware.BackoffConfig{
			Strategy:     middleware.BackoffExponential,
			BaseDelay:    1 * time.Second,
			MaxDelay:     1 * time.Hour,
			JitterFactor: 0.2, // ±20%
			Multiplier:   2.0,
		}

		// Test multiple times to verify jitter variance
		for i := 0; i < 100; i++ {
			delay := config.CalculateDelay(0)
			// Base delay is 1s, with ±20% jitter should be 800ms-1200ms
			assert.GreaterOrEqual(t, delay, 800*time.Millisecond)
			assert.LessOrEqual(t, delay, 1200*time.Millisecond)
		}
	})
}

func TestDefaultBackoffConfig(t *testing.T) {
	config := middleware.DefaultBackoffConfig()

	assert.Equal(t, middleware.BackoffExponential, config.Strategy)
	assert.Equal(t, 10*time.Second, config.BaseDelay)
	assert.Equal(t, 1*time.Hour, config.MaxDelay)
	assert.Equal(t, 0.2, config.JitterFactor)
	assert.Equal(t, 2.0, config.Multiplier)
}

func TestFullJitter(t *testing.T) {
	baseDelay := 100 * time.Millisecond
	maxDelay := 10 * time.Second

	// Full jitter should return value between 0 and cap
	for attempt := 0; attempt < 5; attempt++ {
		for i := 0; i < 100; i++ {
			delay := middleware.FullJitter(baseDelay, maxDelay, attempt)
			assert.GreaterOrEqual(t, delay, time.Duration(0))

			// Cap is min(maxDelay, baseDelay * 2^attempt)
			cap := baseDelay * time.Duration(1<<uint(attempt))
			if cap > maxDelay {
				cap = maxDelay
			}
			assert.LessOrEqual(t, delay, cap)
		}
	}
}

func TestEqualJitter(t *testing.T) {
	baseDelay := 100 * time.Millisecond
	maxDelay := 10 * time.Second

	// Equal jitter should return value between cap/2 and cap
	for attempt := 0; attempt < 5; attempt++ {
		for i := 0; i < 100; i++ {
			delay := middleware.EqualJitter(baseDelay, maxDelay, attempt)

			cap := baseDelay * time.Duration(1<<uint(attempt))
			if cap > maxDelay {
				cap = maxDelay
			}

			// Should be at least half of cap
			assert.GreaterOrEqual(t, delay, cap/2)
			assert.LessOrEqual(t, delay, cap)
		}
	}
}

func TestDecorrelatedJitter(t *testing.T) {
	baseDelay := 100 * time.Millisecond
	maxDelay := 10 * time.Second

	for i := 0; i < 100; i++ {
		delay := middleware.DecorrelatedJitter(baseDelay, maxDelay, 0)
		assert.GreaterOrEqual(t, delay, baseDelay)
		assert.LessOrEqual(t, delay, maxDelay)
	}
}

func TestRetryableError(t *testing.T) {
	originalErr := errors.New("temporary failure")
	retryErr := middleware.Retry(originalErr)

	assert.True(t, middleware.IsRetryable(retryErr))
	assert.False(t, middleware.IsRetryable(originalErr))
	assert.False(t, middleware.IsRetryable(nil))

	// Unwrap
	assert.Equal(t, originalErr, errors.Unwrap(retryErr))
	assert.Contains(t, retryErr.Error(), "temporary failure")
}

func TestPermanentError(t *testing.T) {
	originalErr := errors.New("invalid input")
	permErr := middleware.Permanent(originalErr)

	assert.True(t, middleware.IsPermanent(permErr))
	assert.False(t, middleware.IsPermanent(originalErr))
	assert.False(t, middleware.IsPermanent(nil))

	// Unwrap
	assert.Equal(t, originalErr, errors.Unwrap(permErr))
	assert.Contains(t, permErr.Error(), "invalid input")
}

func TestRetry_NilError(t *testing.T) {
	assert.Nil(t, middleware.Retry(nil))
}

func TestPermanent_NilError(t *testing.T) {
	assert.Nil(t, middleware.Permanent(nil))
}
