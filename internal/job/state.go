package job

import (
	"encoding/json"
	"fmt"
)

type State int

const (
	StatePending State = iota
	StateScheduled
	StateQueued
	StateProcessing
	StateCompleted
	StateRetrying
	StateFailed
	StateDead
	StateCancelled
)

func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateScheduled:
		return "scheduled"
	case StateQueued:
		return "queued"
	case StateProcessing:
		return "processing"
	case StateCompleted:
		return "completed"
	case StateRetrying:
		return "retrying"
	case StateFailed:
		return "failed"
	case StateDead:
		return "dead"
	case StateCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

func ParseState(s string) State {
	switch s {
	case "pending":
		return StatePending
	case "scheduled":
		return StateScheduled
	case "queued":
		return StateQueued
	case "processing":
		return StateProcessing
	case "completed":
		return StateCompleted
	case "retrying":
		return StateRetrying
	case "failed":
		return StateFailed
	case "dead":
		return StateDead
	case "cancelled":
		return StateCancelled
	default:
		return StatePending
	}
}

func (s State) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *State) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	*s = ParseState(str)
	return nil
}

func (s State) IsTerminal() bool {
	switch s {
	case StateCompleted, StateDead, StateCancelled:
		return true
	default:
		return false
	}
}

func (s State) CanTransitionTo(target State) bool {
	validTransitions := map[State][]State{
		StatePending:    {StateScheduled, StateQueued, StateCancelled},
		StateScheduled:  {StateQueued, StateCancelled},
		StateQueued:     {StateProcessing, StateCancelled},
		StateProcessing: {StateCompleted, StateFailed, StateRetrying},
		StateRetrying:   {StateQueued, StateDead, StateCancelled},
		StateFailed:     {StateRetrying, StateDead},
		StateCompleted:  {},
		StateDead:       {StateQueued},
		StateCancelled:  {},
	}

	allowedTargets, ok := validTransitions[s]
	if !ok {
		return false
	}

	for _, allowed := range allowedTargets {
		if allowed == target {
			return true
		}
	}
	return false
}

type StateTransitionError struct {
	From State
	To   State
}

func (e *StateTransitionError) Error() string {
	return fmt.Sprintf("invalid state transition from %s to %s", e.From, e.To)
}

func (s *State) Transition(target State) error {
	if !s.CanTransitionTo(target) {
		return &StateTransitionError{From: *s, To: target}
	}
	*s = target
	return nil
}
