package workflowcheck

import "github.com/1260124186-cc/solo-0016-cellar-run-control/internal/observability"

// operationRecord mirrors the GET /v1/operations/{id} response used by the
// operation-tracing workflow check.
type operationRecord struct {
	OperationID  string                  `json:"operation_id"`
	AttemptCount int                     `json:"attempt_count"`
	Attempts     []observability.Attempt `json:"attempts"`
}

func attemptPersisted(attempt observability.Attempt) bool {
	return attempt.StorageAttempted && !attempt.StorageFailed
}
