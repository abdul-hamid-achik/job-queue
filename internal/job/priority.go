package job

import (
	"encoding/json"
	"fmt"
)

type Priority int

const (
	PriorityLow Priority = iota
	PriorityMedium
	PriorityHigh
)

func (p Priority) String() string {
	switch p {
	case PriorityLow:
		return "low"
	case PriorityMedium:
		return "medium"
	case PriorityHigh:
		return "high"
	default:
		return fmt.Sprintf("unknown(%d)", p)
	}
}

func ParsePriority(s string) Priority {
	switch s {
	case "low":
		return PriorityLow
	case "medium", "":
		return PriorityMedium
	case "high":
		return PriorityHigh
	default:
		return PriorityMedium
	}
}

func (p Priority) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.String())
}

func (p *Priority) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*p = ParsePriority(s)
	return nil
}

func (p Priority) QueueSuffix() string {
	return p.String()
}

func AllPriorities() []Priority {
	return []Priority{PriorityHigh, PriorityMedium, PriorityLow}
}
