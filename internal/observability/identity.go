package observability

import "time"

// Identity answers "which process handled this operation?". A fresh instance
// identifier is generated every time the process starts, while the durable
// operation journal keeps operations locatable across restarts.
type Identity struct {
	InstanceID string
	Host       string
	PID        int
	StartedAt  time.Time
}

// NewIdentity resolves the current process identity. Host failure is not fatal:
// the instance id still distinguishes processes.
func NewIdentity(startedAt time.Time, instanceID string) Identity {
	identity := Identity{
		InstanceID: instanceID,
		PID:        processID(),
		StartedAt:  startedAt.UTC(),
	}
	identity.Host = hostName()
	return identity
}
