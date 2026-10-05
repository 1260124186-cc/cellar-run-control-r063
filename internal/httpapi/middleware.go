package httpapi

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/observability"
)

// statusRecorder captures the final status code for the durable operation
// record. Explicit writes default to 200, matching net/http.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if !r.wroteHeader {
		r.status = status
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(value []byte) (int, error) {
	if !r.wroteHeader {
		r.status = http.StatusOK
		r.wroteHeader = true
	}
	return r.ResponseWriter.Write(value)
}

// withOperation binds a stable operation id to every request, echoes it back,
// journals the attempt with the business error and storage outcomes, and emits
// one structured log line. Request bodies are never read or logged.
func withOperation(sink observability.Sink, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		supplied := strings.TrimSpace(request.Header.Get(observability.OperationHeader))

		// An unparseable supplied id cannot be trusted as a key, so the server
		// issues a replacement id, journals the rejected attempt under it, and
		// returns 400. The caller retries with the replacement id.
		if supplied != "" && !observability.ValidOperationID(supplied) {
			replacement, err := observability.NewOperationID()
			if err != nil {
				log.Printf("op=- method=%s path=%q generate operation id failed: %v",
					request.Method, observability.SanitizeForLog(request.URL.Path), err)
				writeJSON(response, http.StatusInternalServerError, errorResponse{
					Error: errorBody{Code: domain.CodeStorage, Detail: "request could not be correlated"},
				})
				return
			}
			response.Header().Set(observability.OperationHeader, replacement)
			ctx, _ := observability.WithCollector(
				observability.WithOperation(request.Context(), replacement))
			recorded := request.WithContext(ctx)
			recorder := &statusRecorder{ResponseWriter: response, status: http.StatusOK}
			started := time.Now()
			writeDomainError(recorder, recorded,
				domain.FieldError(domain.CodeInvalidInput, "X-Operation-Id",
					"X-Operation-Id must match op_ followed by 16 to 32 lowercase hexadecimal digits"))
			finalizeAttempt(sink, replacement, 1, request, recorder, started, ctx)
			return
		}

		operationID := supplied
		if operationID == "" {
			generated, err := observability.NewOperationID()
			if err != nil {
				log.Printf("op=- method=%s path=%q generate operation id failed: %v",
					request.Method, observability.SanitizeForLog(request.URL.Path), err)
				writeJSON(response, http.StatusInternalServerError, errorResponse{
					Error: errorBody{
						Code:   domain.CodeStorage,
						Detail: "request could not be correlated",
					},
				})
				return
			}
			operationID = generated
		}

		// Bind the per-request collector before any further validation so even a
		// rejected attempt is correlated end to end under this operation id.
		response.Header().Set(observability.OperationHeader, operationID)
		ctx, _ := observability.WithCollector(
			observability.WithOperation(request.Context(), operationID))
		recorded := request.WithContext(ctx)
		recorder := &statusRecorder{ResponseWriter: response, status: http.StatusOK}
		started := time.Now()

		attempt, err := observability.ParseAttempt(request.Header.Get(observability.AttemptHeader))
		if err != nil {
			// The request still counts as one attempt for correlation even when
			// its attempt counter is malformed.
			attempt = 1
			writeDomainError(recorder, recorded,
				domain.FieldError(domain.CodeInvalidInput, "X-Operation-Attempt",
					err.Error()))
			finalizeAttempt(sink, operationID, attempt, request, recorder, started, ctx)
			return
		}

		// Finalize via defer so an unexpected panic inside a handler is still
		// converted to a correlated 500 and journaled under the same operation.
		defer func() {
			if recovered := recover(); recovered != nil {
				observability.MarkBusinessError(ctx, string(domain.CodeStorage))
				if !recorder.wroteHeader {
					writeJSON(recorder, http.StatusInternalServerError, errorResponse{
						Error: errorBody{
							Code:        domain.CodeStorage,
							Detail:      "request could not be completed",
							OperationID: operationID,
						},
					})
				}
				log.Printf("op=%s attempt=%d panic recovered: %v", operationID, attempt, recovered)
			}
			finalizeAttempt(sink, operationID, attempt, request, recorder, started, ctx)
		}()
		next.ServeHTTP(recorder, recorded)
	})
}

// finalizeAttempt journals the attempt and emits the structured log line. It
// runs for both served requests and pre-routing validation failures.
func finalizeAttempt(
	sink observability.Sink,
	operationID string,
	attempt int,
	request *http.Request,
	recorder *statusRecorder,
	started time.Time,
	ctx context.Context,
) {
	duration := time.Since(started)
	outcome := observability.OutcomeFromContext(ctx)
	errorCode := outcome.BusinessCode
	if errorCode == "" && recorder.status >= 500 {
		errorCode = string(domain.CodeStorage)
	}
	path := observability.SanitizeForLog(request.URL.Path)

	if sink != nil {
		identity := sink.Identity()
		sink.Record(observability.Attempt{
			OperationID:      operationID,
			Attempt:          attempt,
			InstanceID:       identity.InstanceID,
			ReceivedAt:       started.UTC(),
			Method:           request.Method,
			Path:             path,
			Status:           recorder.status,
			DurationMillis:   duration.Milliseconds(),
			ErrorCode:        errorCode,
			StorageAttempted: outcome.StorageAttempted,
			StorageFailed:    outcome.StorageFailed,
			RevisionAfter:    outcome.StorageRevision,
		})
	}

	stored := "none"
	switch {
	case outcome.StorageFailed:
		stored = "failed"
	case outcome.StorageAttempted:
		stored = "ok"
	}
	log.Printf("op=%s attempt=%d method=%s path=%q status=%d duration_ms=%d error_code=%q storage=%s revision=%d",
		operationID, attempt, request.Method, path, recorder.status,
		duration.Milliseconds(), errorCode, stored, outcome.StorageRevision)
}

// lookupOperation reads attempts for one operation from the durable journal.
func lookupOperation(ctx context.Context, sink observability.Sink, operationID string) ([]observability.Attempt, error) {
	if sink == nil {
		return nil, domain.NewError(domain.CodeOperationNotFound, "operation was not found")
	}
	if !observability.ValidOperationID(operationID) {
		return nil, domain.FieldError(domain.CodeInvalidInput, "operation_id",
			"must match op_ followed by 16 to 32 lowercase hexadecimal digits")
	}
	attempts, err := sink.Lookup(operationID)
	if err != nil {
		return nil, domain.NewError(domain.CodeOperationNotFound, "operation was not found")
	}
	return attempts, nil
}
