package httpapi

import (
	"net/http"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/operation"
)

// statusRecorder remembers the status code so the request log can report it.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// withOperation assigns one stable operation identifier per request, echoes
// it on the response, and logs the completed exchange. The identifier lives
// in the request context, so concurrent requests never share an operation
// number. A client retry may supply the same identifier to correlate both
// attempts.
func withOperation(logger *operation.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		id := operation.ResolveID(request.Header.Get(operation.HeaderName))
		response.Header().Set(operation.HeaderName, id)
		recorder := &statusRecorder{ResponseWriter: response, status: http.StatusOK}
		started := time.Now()
		next.ServeHTTP(recorder, request.WithContext(operation.WithID(request.Context(), id)))
		logger.Log(operation.EventHTTPRequest, operation.Fields{
			"operation_id": id,
			"method":       request.Method,
			"path":         request.URL.Path,
			"status":       recorder.status,
			"duration_ms":  time.Since(started).Milliseconds(),
		})
	})
}
