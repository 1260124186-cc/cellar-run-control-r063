package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const (
	// OperationHeader carries the stable identifier of one operator action.
	// A terminal retry keeps the same value and increments AttemptHeader.
	OperationHeader = "X-Operation-Id"
	// AttemptHeader distinguishes repeated attempts that share one operation.
	AttemptHeader = "X-Operation-Attempt"
)

// operationIDPattern intentionally allows only lowercase hexadecimal digits so
// that a supplied value can never inject line breaks or spaces into logs.
var operationIDPattern = regexp.MustCompile(`^op_[0-9a-f]{16,32}$`)

// NewOperationID generates a cryptographically random, concurrency-safe
// operation identifier.
func NewOperationID() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("generate operation id: %w", err)
	}
	return "op_" + hex.EncodeToString(buffer), nil
}

// ValidOperationID reports whether value is safe to echo into headers, logs,
// and the durable operation journal.
func ValidOperationID(value string) bool {
	return operationIDPattern.MatchString(value)
}

// ParseAttempt reads the optional attempt counter. An empty header means 1.
func ParseAttempt(value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 1, nil
	}
	attempt, err := strconv.Atoi(value)
	if err != nil || attempt < 1 || attempt > 1_000_000 {
		return 0, fmt.Errorf("X-Operation-Attempt must be an integer from 1 to 1000000")
	}
	return attempt, nil
}

type operationContextKey struct{}
type collectorContextKey struct{}

// WithOperation stores the operation identifier on a request context.
func WithOperation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, operationContextKey{}, id)
}

// OperationFromContext returns the operation identifier bound to the context.
func OperationFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(operationContextKey{}).(string)
	return id
}

// outcomeCollector gathers per-request outcomes from layers that never see each
// other's return values. Every request owns its own collector, so concurrent
// requests cannot exchange outcomes.
type outcomeCollector struct {
	mu               sync.Mutex
	businessCode     string
	storageAttempted bool
	storageFailed    bool
	storageRevision  int64
}

// WithCollector attaches a fresh outcome collector to the context.
func WithCollector(ctx context.Context) (context.Context, Outcome) {
	collector := &outcomeCollector{}
	return context.WithValue(ctx, collectorContextKey{}, collector), collector
}

// Outcome is the per-request sink used by inner layers.
type Outcome interface {
	MarkBusinessError(code string)
	MarkStorage(revision int64, failed bool)
	Snapshot() OutcomeSnapshot
}

// OutcomeSnapshot is the finalized view consumed by the HTTP middleware.
type OutcomeSnapshot struct {
	BusinessCode     string
	StorageAttempted bool
	StorageFailed    bool
	StorageRevision  int64
}

func (c *outcomeCollector) MarkBusinessError(code string) {
	if c == nil || code == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.businessCode == "" {
		c.businessCode = code
	}
}

func (c *outcomeCollector) MarkStorage(revision int64, failed bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storageAttempted = true
	if failed {
		c.storageFailed = true
		return
	}
	c.storageRevision = revision
}

func (c *outcomeCollector) Snapshot() OutcomeSnapshot {
	if c == nil {
		return OutcomeSnapshot{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return OutcomeSnapshot{
		BusinessCode:     c.businessCode,
		StorageAttempted: c.storageAttempted,
		StorageFailed:    c.storageFailed,
		StorageRevision:  c.storageRevision,
	}
}

// MarkBusinessError records the stable machine error code produced for the
// request bound to ctx. It is a no-op outside an instrumented request.
func MarkBusinessError(ctx context.Context, code string) {
	collector, _ := ctx.Value(collectorContextKey{}).(*outcomeCollector)
	collector.MarkBusinessError(code)
}

// MarkStorage records the durable write outcome of the request bound to ctx.
func MarkStorage(ctx context.Context, revision int64, failed bool) {
	collector, _ := ctx.Value(collectorContextKey{}).(*outcomeCollector)
	collector.MarkStorage(revision, failed)
}

// OutcomeFromContext returns the request's collected outcomes.
func OutcomeFromContext(ctx context.Context) OutcomeSnapshot {
	collector, _ := ctx.Value(collectorContextKey{}).(*outcomeCollector)
	return collector.Snapshot()
}
