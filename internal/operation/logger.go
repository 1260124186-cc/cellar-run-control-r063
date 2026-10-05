package operation

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

// LogFileName is the append-only operation log kept inside the data
// directory, so a past operation stays locatable after the service restarts.
const LogFileName = "operations.log"

const (
	EventProcessStarted = "process_started"
	EventHTTPRequest    = "http_request"
	EventHTTPPanic      = "http_panic"
	EventServiceError   = "service_error"
	EventStorageFailure = "storage_failure"
)

// Fields carries structured log attributes. Only stable metadata may be
// logged: identifiers, codes, versions, states, and system error messages.
// Request payloads and free-text bodies such as run notes or observation
// notes must never be placed in Fields.
type Fields map[string]any

// Logger appends one JSON object per line and stamps every record with the
// process identity. A nil Logger is valid and drops every record, which
// keeps the wiring optional for callers such as tests and checks.
type Logger struct {
	mu       sync.Mutex
	out      io.Writer
	identity ProcessIdentity
	now      func() time.Time
}

func NewLogger(out io.Writer, identity ProcessIdentity) *Logger {
	return &Logger{
		out:      out,
		identity: identity,
		now:      func() time.Time { return time.Now().UTC() },
	}
}

// Log writes one record for the event. Writes are serialized so concurrent
// requests cannot interleave a line.
func (l *Logger) Log(event string, fields Fields) {
	if l == nil || l.out == nil {
		return
	}
	record := make(map[string]any, len(fields)+4)
	record["time"] = l.now().Format(time.RFC3339Nano)
	record["event"] = event
	record["process_id"] = l.identity.ID
	record["pid"] = l.identity.PID
	for key, value := range fields {
		record[key] = value
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.out.Write(append(encoded, '\n'))
}
