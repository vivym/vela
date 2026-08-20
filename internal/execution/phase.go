package execution

import (
	"database/sql/driver"
	"fmt"
)

// Phase is the backend-neutral execution phase exposed in the customer Job view.
type Phase string

const (
	PhaseQueued     Phase = "QUEUED"
	PhasePreparing  Phase = "PREPARING"
	PhaseGenerating Phase = "GENERATING"
	PhaseFinalizing Phase = "FINALIZING"
	PhaseRetryWait  Phase = "RETRY_WAIT"
)

func (phase *Phase) Scan(src any) error {
	switch value := src.(type) {
	case []byte:
		*phase = Phase(value)
	case string:
		*phase = Phase(value)
	default:
		return fmt.Errorf("unsupported scan type for execution Phase: %T", src)
	}
	return nil
}

func (phase Phase) Value() (driver.Value, error) {
	return string(phase), nil
}
