package operation

import (
	"os"
	"time"
)

// ProcessIdentity distinguishes this process from earlier and later runs of
// the service, so persisted log lines and health answers can be attributed
// to the correct process after a restart.
type ProcessIdentity struct {
	ID        string    `json:"id"`
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"started_at"`
}

// NewProcessIdentity captures the identity of the current process at startup.
func NewProcessIdentity() ProcessIdentity {
	return ProcessIdentity{
		ID:        NewID("proc"),
		PID:       os.Getpid(),
		StartedAt: time.Now().UTC(),
	}
}
