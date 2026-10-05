package observability

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const journalFileName = "operations.log"

// ErrOperationNotFound indicates that no journaled attempt carries the
// requested operation identifier.
var ErrOperationNotFound = errors.New("operation not found")

// Attempt is one durable record of a request attempt. It deliberately contains
// no request bodies, formula notes, or observation notes.
type Attempt struct {
	OperationID      string    `json:"operation_id"`
	Attempt          int       `json:"attempt"`
	InstanceID       string    `json:"instance_id"`
	ReceivedAt       time.Time `json:"received_at"`
	Method           string    `json:"method"`
	Path             string    `json:"path"`
	Status           int       `json:"status"`
	DurationMillis   int64     `json:"duration_millis"`
	ErrorCode        string    `json:"error_code,omitempty"`
	StorageAttempted bool      `json:"storage_attempted"`
	StorageFailed    bool      `json:"storage_failed,omitempty"`
	RevisionAfter    int64     `json:"revision_after,omitempty"`
}

// Sink persists request attempts so an operation stays locatable after the
// process that handled it has restarted.
type Sink interface {
	Record(attempt Attempt)
	Lookup(operationID string) ([]Attempt, error)
	Identity() Identity
	Close() error
}

// Journal is an append-only, fsynced JSON-lines log keyed by operation id.
// A mutex serializes writes; the index is rebuilt by scanning on open so a new
// process re-opens the same file and answers for old operations immediately.
type Journal struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	identity Identity
	index    map[string][]Attempt
	scanned  bool
}

// OpenJournal creates or reopens the durable operation journal inside dataDir.
func OpenJournal(dataDir string, identity Identity) (*Journal, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create journal directory: %w", err)
	}
	path := filepath.Join(dataDir, journalFileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open operation journal: %w", err)
	}
	journal := &Journal{
		file:     file,
		path:     path,
		identity: identity,
		index:    map[string][]Attempt{},
	}
	if err := journal.replay(); err != nil {
		_ = file.Close()
		return nil, err
	}
	journal.scanned = true
	return journal, nil
}

func (j *Journal) replay() error {
	handle, err := os.Open(j.path)
	if err != nil {
		return fmt.Errorf("read operation journal: %w", err)
	}
	defer handle.Close()
	scanner := bufio.NewScanner(handle)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}
		var attempt Attempt
		if err := json.Unmarshal(raw, &attempt); err != nil {
			return fmt.Errorf("operation journal line %d is corrupt: %w", line, err)
		}
		if !ValidOperationID(attempt.OperationID) {
			return fmt.Errorf("operation journal line %d has an invalid operation id", line)
		}
		j.index[attempt.OperationID] = append(j.index[attempt.OperationID], attempt)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan operation journal: %w", err)
	}
	return nil
}

// Record appends and fsyncs one attempt, then publishes it to the in-memory
// index. On failure the request itself is not failed; the error is returned to
// the caller, which logs it to stderr.
func (j *Journal) Record(attempt Attempt) {
	if j == nil {
		return
	}
	if !ValidOperationID(attempt.OperationID) || attempt.Attempt < 1 {
		return
	}
	encoded, err := json.Marshal(attempt)
	if err != nil {
		return
	}
	encoded = append(encoded, '\n')

	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.file.Write(encoded); err != nil {
		fmt.Fprintf(os.Stderr, "observability: write operation journal: %v\n", err)
		return
	}
	if err := j.file.Sync(); err != nil {
		fmt.Fprintf(os.Stderr, "observability: sync operation journal: %v\n", err)
		return
	}
	if j.scanned {
		j.index[attempt.OperationID] = append(j.index[attempt.OperationID], attempt)
	}
}

// Lookup returns every journaled attempt for an operation, oldest first.
func (j *Journal) Lookup(operationID string) ([]Attempt, error) {
	if j == nil {
		return nil, ErrOperationNotFound
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	attempts, found := j.index[operationID]
	if !found || len(attempts) == 0 {
		return nil, ErrOperationNotFound
	}
	copied := make([]Attempt, len(attempts))
	copy(copied, attempts)
	return copied, nil
}

// Identity returns the process identity that owns this journal.
func (j *Journal) Identity() Identity {
	if j == nil {
		return Identity{}
	}
	return j.identity
}

// Close releases the journal file handle.
func (j *Journal) Close() error {
	if j == nil || j.file == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	err := j.file.Close()
	j.file = nil
	return err
}
