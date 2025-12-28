package middleware

import (
	"math"
	"math/rand"
	"time"
)

type BackoffStrategy int

const (
	BackoffConstant BackoffStrategy = iota
	BackoffLinear
	BackoffExponential
)

type BackoffConfig struct {
	Strategy     BackoffStrategy
	BaseDelay    time.Duration
	MaxDelay     time.Duration
	JitterFactor float64
	Multiplier   float64
}

func DefaultBackoffConfig() BackoffConfig {
	return BackoffConfig{
		Strategy:     BackoffExponential,
		BaseDelay:    10 * time.Second,
		MaxDelay:     1 * time.Hour,
		JitterFactor: 0.2,
		Multiplier:   2.0,
	}
}

func (c BackoffConfig) CalculateDelay(attempt int) time.Duration {
	var delay time.Duration

	switch c.Strategy {
	case BackoffConstant:
		delay = c.BaseDelay
	case BackoffLinear:
		delay = c.BaseDelay * time.Duration(attempt+1)
	case BackoffExponential:
		multiplier := c.Multiplier
		if multiplier == 0 {
			multiplier = 2.0
		}
		delay = time.Duration(float64(c.BaseDelay) * math.Pow(multiplier, float64(attempt)))
	default:
		delay = c.BaseDelay
	}

	if c.MaxDelay > 0 && delay > c.MaxDelay {
		delay = c.MaxDelay
	}

	if c.JitterFactor > 0 {
		delay = addJitter(delay, c.JitterFactor)
	}

	return delay
}

func addJitter(d time.Duration, factor float64) time.Duration {
	if factor <= 0 || d <= 0 {
		return d
	}

	jitterRange := float64(d) * factor
	jitter := (rand.Float64()*2 - 1) * jitterRange
	result := time.Duration(float64(d) + jitter)

	if result < 0 {
		result = 0
	}

	return result
}

func FullJitter(baseDelay, maxDelay time.Duration, attempt int) time.Duration {
	expDelay := float64(baseDelay) * math.Pow(2, float64(attempt))
	cap := math.Min(float64(maxDelay), expDelay)
	return time.Duration(rand.Float64() * cap)
}

func EqualJitter(baseDelay, maxDelay time.Duration, attempt int) time.Duration {
	expDelay := float64(baseDelay) * math.Pow(2, float64(attempt))
	cap := math.Min(float64(maxDelay), expDelay)
	return time.Duration(cap/2 + rand.Float64()*cap/2)
}

func DecorrelatedJitter(baseDelay, maxDelay, prevDelay time.Duration) time.Duration {
	if prevDelay == 0 {
		prevDelay = baseDelay
	}

	maxNext := float64(prevDelay) * 3
	delay := float64(baseDelay) + rand.Float64()*(maxNext-float64(baseDelay))

	if delay > float64(maxDelay) {
		delay = float64(maxDelay)
	}

	return time.Duration(delay)
}

type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	return e.Err.Error()
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*RetryableError)
	return ok
}

func Retry(err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Err: err}
}

type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string {
	return e.Err.Error()
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*PermanentError)
	return ok
}

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}
