package job_test

import (
	"encoding/json"
	"testing"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
	"github.com/stretchr/testify/assert"
)

func TestState_String(t *testing.T) {
	tests := []struct {
		state    job.State
		expected string
	}{
		{job.StatePending, "pending"},
		{job.StateScheduled, "scheduled"},
		{job.StateQueued, "queued"},
		{job.StateProcessing, "processing"},
		{job.StateCompleted, "completed"},
		{job.StateRetrying, "retrying"},
		{job.StateFailed, "failed"},
		{job.StateDead, "dead"},
		{job.StateCancelled, "cancelled"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.state.String())
		})
	}
}

func TestParseState(t *testing.T) {
	tests := []struct {
		input    string
		expected job.State
	}{
		{"pending", job.StatePending},
		{"scheduled", job.StateScheduled},
		{"queued", job.StateQueued},
		{"processing", job.StateProcessing},
		{"completed", job.StateCompleted},
		{"retrying", job.StateRetrying},
		{"failed", job.StateFailed},
		{"dead", job.StateDead},
		{"cancelled", job.StateCancelled},
		{"unknown", job.StatePending}, // Default
		{"", job.StatePending},        // Default
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, tc.expected, job.ParseState(tc.input))
		})
	}
}

func TestState_IsTerminal(t *testing.T) {
	terminalStates := []job.State{
		job.StateCompleted,
		job.StateDead,
		job.StateCancelled,
	}

	nonTerminalStates := []job.State{
		job.StatePending,
		job.StateScheduled,
		job.StateQueued,
		job.StateProcessing,
		job.StateRetrying,
		job.StateFailed,
	}

	for _, state := range terminalStates {
		t.Run(state.String()+"_is_terminal", func(t *testing.T) {
			assert.True(t, state.IsTerminal())
		})
	}

	for _, state := range nonTerminalStates {
		t.Run(state.String()+"_is_not_terminal", func(t *testing.T) {
			assert.False(t, state.IsTerminal())
		})
	}
}

func TestState_CanTransitionTo(t *testing.T) {
	validTransitions := []struct {
		from job.State
		to   job.State
	}{
		// From Pending
		{job.StatePending, job.StateScheduled},
		{job.StatePending, job.StateQueued},
		{job.StatePending, job.StateCancelled},
		// From Scheduled
		{job.StateScheduled, job.StateQueued},
		{job.StateScheduled, job.StateCancelled},
		// From Queued
		{job.StateQueued, job.StateProcessing},
		{job.StateQueued, job.StateCancelled},
		// From Processing
		{job.StateProcessing, job.StateCompleted},
		{job.StateProcessing, job.StateFailed},
		{job.StateProcessing, job.StateRetrying},
		// From Retrying
		{job.StateRetrying, job.StateQueued},
		{job.StateRetrying, job.StateDead},
		{job.StateRetrying, job.StateCancelled},
		// From Failed
		{job.StateFailed, job.StateRetrying},
		{job.StateFailed, job.StateDead},
		// From Dead (requeue)
		{job.StateDead, job.StateQueued},
	}

	invalidTransitions := []struct {
		from job.State
		to   job.State
	}{
		// From Pending
		{job.StatePending, job.StateCompleted},
		{job.StatePending, job.StateProcessing},
		// From Completed (terminal)
		{job.StateCompleted, job.StatePending},
		{job.StateCompleted, job.StateQueued},
		// From Cancelled (terminal)
		{job.StateCancelled, job.StatePending},
		// From Processing
		{job.StateProcessing, job.StateQueued},
		{job.StateProcessing, job.StatePending},
		// From Queued
		{job.StateQueued, job.StateCompleted},
		{job.StateQueued, job.StateFailed},
	}

	for _, tc := range validTransitions {
		t.Run(tc.from.String()+"_to_"+tc.to.String()+"_valid", func(t *testing.T) {
			assert.True(t, tc.from.CanTransitionTo(tc.to))
		})
	}

	for _, tc := range invalidTransitions {
		t.Run(tc.from.String()+"_to_"+tc.to.String()+"_invalid", func(t *testing.T) {
			assert.False(t, tc.from.CanTransitionTo(tc.to))
		})
	}
}

func TestState_Transition(t *testing.T) {
	t.Run("successful transition", func(t *testing.T) {
		state := job.StatePending
		err := state.Transition(job.StateQueued)

		assert.NoError(t, err)
		assert.Equal(t, job.StateQueued, state)
	})

	t.Run("failed transition", func(t *testing.T) {
		state := job.StatePending
		err := state.Transition(job.StateCompleted)

		assert.Error(t, err)
		assert.Equal(t, job.StatePending, state) // Unchanged
	})

	t.Run("transition error type", func(t *testing.T) {
		state := job.StatePending
		err := state.Transition(job.StateCompleted)

		var transitionErr *job.StateTransitionError
		assert.ErrorAs(t, err, &transitionErr)
		assert.Equal(t, job.StatePending, transitionErr.From)
		assert.Equal(t, job.StateCompleted, transitionErr.To)
	})
}

func TestState_JSON(t *testing.T) {
	t.Run("marshal", func(t *testing.T) {
		state := job.StateProcessing
		data, err := json.Marshal(state)

		assert.NoError(t, err)
		assert.Equal(t, `"processing"`, string(data))
	})

	t.Run("unmarshal", func(t *testing.T) {
		var state job.State
		err := json.Unmarshal([]byte(`"completed"`), &state)

		assert.NoError(t, err)
		assert.Equal(t, job.StateCompleted, state)
	})

	t.Run("roundtrip", func(t *testing.T) {
		original := job.StateRetrying
		data, _ := json.Marshal(original)

		var decoded job.State
		json.Unmarshal(data, &decoded)

		assert.Equal(t, original, decoded)
	})
}

func TestStateTransitionError_Error(t *testing.T) {
	err := &job.StateTransitionError{
		From: job.StatePending,
		To:   job.StateCompleted,
	}

	assert.Contains(t, err.Error(), "pending")
	assert.Contains(t, err.Error(), "completed")
}
