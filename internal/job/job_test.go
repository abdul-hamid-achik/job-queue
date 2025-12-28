package job_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/abdul-hamid-achik/job-queue/internal/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Run("creates job with defaults", func(t *testing.T) {
		j, err := job.New("test.type", map[string]string{"key": "value"})

		require.NoError(t, err)
		assert.NotEmpty(t, j.ID)
		assert.Equal(t, "test.type", j.Type)
		assert.Equal(t, "default", j.Queue)
		assert.Equal(t, job.PriorityMedium, j.Priority)
		assert.Equal(t, 3, j.MaxRetries)
		assert.Equal(t, 5*time.Minute, j.Timeout)
		assert.Equal(t, job.StatePending, j.State)
	})

	t.Run("fails with empty type", func(t *testing.T) {
		_, err := job.New("", nil)
		assert.Error(t, err)
	})

	t.Run("handles nil payload", func(t *testing.T) {
		j, err := job.New("test.type", nil)

		require.NoError(t, err)
		assert.Equal(t, json.RawMessage("{}"), j.Payload)
	})

	t.Run("handles raw JSON payload", func(t *testing.T) {
		payload := json.RawMessage(`{"custom":"data"}`)
		j, err := job.New("test.type", payload)

		require.NoError(t, err)
		assert.Equal(t, payload, j.Payload)
	})
}

func TestNewWithOptions(t *testing.T) {
	t.Run("applies queue option", func(t *testing.T) {
		j, err := job.NewWithOptions("test", nil, job.WithQueue("critical"))

		require.NoError(t, err)
		assert.Equal(t, "critical", j.Queue)
	})

	t.Run("applies priority option", func(t *testing.T) {
		j, err := job.NewWithOptions("test", nil, job.WithPriority(job.PriorityHigh))

		require.NoError(t, err)
		assert.Equal(t, job.PriorityHigh, j.Priority)
	})

	t.Run("applies max retries option", func(t *testing.T) {
		j, err := job.NewWithOptions("test", nil, job.WithMaxRetries(5))

		require.NoError(t, err)
		assert.Equal(t, 5, j.MaxRetries)
	})

	t.Run("applies timeout option", func(t *testing.T) {
		j, err := job.NewWithOptions("test", nil, job.WithTimeout(10*time.Second))

		require.NoError(t, err)
		assert.Equal(t, 10*time.Second, j.Timeout)
	})

	t.Run("applies delay option", func(t *testing.T) {
		j, err := job.NewWithOptions("test", nil, job.WithDelay(1*time.Hour))

		require.NoError(t, err)
		assert.NotNil(t, j.ScheduledAt)
		assert.Equal(t, job.StateScheduled, j.State)
	})

	t.Run("applies scheduled at option", func(t *testing.T) {
		scheduledTime := time.Now().Add(2 * time.Hour)
		j, err := job.NewWithOptions("test", nil, job.WithScheduledAt(scheduledTime))

		require.NoError(t, err)
		assert.Equal(t, scheduledTime.Unix(), j.ScheduledAt.Unix())
		assert.Equal(t, job.StateScheduled, j.State)
	})

	t.Run("applies metadata option", func(t *testing.T) {
		j, err := job.NewWithOptions("test", nil,
			job.WithMetadata("key1", "value1"),
			job.WithMetadata("key2", "value2"),
		)

		require.NoError(t, err)
		assert.Equal(t, "value1", j.Metadata["key1"])
		assert.Equal(t, "value2", j.Metadata["key2"])
	})
}

func TestJob_Validate(t *testing.T) {
	tests := []struct {
		name    string
		job     *job.Job
		wantErr bool
	}{
		{
			name:    "valid job",
			job:     mustNewJob(t, "test.type", nil),
			wantErr: false,
		},
		{
			name:    "missing ID",
			job:     &job.Job{Type: "test", Queue: "default", Timeout: time.Minute},
			wantErr: true,
		},
		{
			name:    "missing type",
			job:     &job.Job{ID: "123", Queue: "default", Timeout: time.Minute},
			wantErr: true,
		},
		{
			name:    "missing queue",
			job:     &job.Job{ID: "123", Type: "test", Timeout: time.Minute},
			wantErr: true,
		},
		{
			name:    "zero timeout",
			job:     &job.Job{ID: "123", Type: "test", Queue: "default", Timeout: 0},
			wantErr: true,
		},
		{
			name:    "negative max retries",
			job:     &job.Job{ID: "123", Type: "test", Queue: "default", Timeout: time.Minute, MaxRetries: -1},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.job.Validate()
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestJob_CanRetry(t *testing.T) {
	t.Run("can retry when retries remaining", func(t *testing.T) {
		j := mustNewJob(t, "test", nil)
		j.MaxRetries = 3
		j.RetryCount = 2

		assert.True(t, j.CanRetry())
	})

	t.Run("cannot retry when exhausted", func(t *testing.T) {
		j := mustNewJob(t, "test", nil)
		j.MaxRetries = 3
		j.RetryCount = 3

		assert.False(t, j.CanRetry())
	})

	t.Run("cannot retry when max is zero", func(t *testing.T) {
		j := mustNewJob(t, "test", nil)
		j.MaxRetries = 0
		j.RetryCount = 0

		assert.False(t, j.CanRetry())
	})
}

func TestJob_IsDelayed(t *testing.T) {
	t.Run("not delayed when no scheduled time", func(t *testing.T) {
		j := mustNewJob(t, "test", nil)
		assert.False(t, j.IsDelayed())
	})

	t.Run("delayed when scheduled in future", func(t *testing.T) {
		j := mustNewJob(t, "test", nil)
		future := time.Now().Add(1 * time.Hour)
		j.ScheduledAt = &future

		assert.True(t, j.IsDelayed())
	})

	t.Run("not delayed when scheduled time passed", func(t *testing.T) {
		j := mustNewJob(t, "test", nil)
		past := time.Now().Add(-1 * time.Hour)
		j.ScheduledAt = &past

		assert.False(t, j.IsDelayed())
	})
}

func TestJob_Duration(t *testing.T) {
	t.Run("returns zero when not started", func(t *testing.T) {
		j := mustNewJob(t, "test", nil)
		assert.Equal(t, time.Duration(0), j.Duration())
	})

	t.Run("returns zero when not completed", func(t *testing.T) {
		j := mustNewJob(t, "test", nil)
		now := time.Now()
		j.StartedAt = &now

		assert.Equal(t, time.Duration(0), j.Duration())
	})

	t.Run("returns duration when complete", func(t *testing.T) {
		j := mustNewJob(t, "test", nil)
		start := time.Now()
		end := start.Add(5 * time.Second)
		j.StartedAt = &start
		j.CompletedAt = &end

		assert.Equal(t, 5*time.Second, j.Duration())
	})
}

func TestJob_UnmarshalPayload(t *testing.T) {
	type TestPayload struct {
		Email   string `json:"email"`
		Subject string `json:"subject"`
	}

	j, err := job.New("test", TestPayload{
		Email:   "test@example.com",
		Subject: "Hello",
	})
	require.NoError(t, err)

	var payload TestPayload
	err = j.UnmarshalPayload(&payload)

	require.NoError(t, err)
	assert.Equal(t, "test@example.com", payload.Email)
	assert.Equal(t, "Hello", payload.Subject)
}

func TestJob_Clone(t *testing.T) {
	original := mustNewJob(t, "test", map[string]string{"key": "value"})
	original.Metadata["meta"] = "data"
	now := time.Now()
	original.StartedAt = &now

	clone := original.Clone()

	// Should be equal
	assert.Equal(t, original.ID, clone.ID)
	assert.Equal(t, original.Type, clone.Type)

	// Should be independent copies
	original.Metadata["new"] = "value"
	assert.Empty(t, clone.Metadata["new"])

	// Modify clone's time
	later := time.Now().Add(time.Hour)
	clone.StartedAt = &later
	assert.NotEqual(t, original.StartedAt, clone.StartedAt)
}

func TestJob_JSONSerialization(t *testing.T) {
	original := mustNewJob(t, "test.type", map[string]string{"key": "value"})
	original.Queue = "critical"
	original.Priority = job.PriorityHigh

	// Marshal
	data, err := json.Marshal(original)
	require.NoError(t, err)

	// Unmarshal
	var decoded job.Job
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.ID, decoded.ID)
	assert.Equal(t, original.Type, decoded.Type)
	assert.Equal(t, original.Queue, decoded.Queue)
	assert.Equal(t, original.Priority, decoded.Priority)
	assert.Equal(t, original.State, decoded.State)
}

func mustNewJob(t *testing.T, jobType string, payload interface{}) *job.Job {
	t.Helper()
	j, err := job.New(jobType, payload)
	require.NoError(t, err)
	return j
}
