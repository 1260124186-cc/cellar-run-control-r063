package httpapi

import (
	"net/http"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/observability"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
)

// NewHandler builds the HTTP handler. sink records durable per-attempt
// correlation records and provides the process identity reported by healthz.
func NewHandler(app *service.Service, sink observability.Sink) http.Handler {
	mux := http.NewServeMux()
	registerFormulaRoutes(mux, app)
	registerVesselRoutes(mux, app)
	registerRunRoutes(mux, app)
	registerOperationRoutes(mux, sink)
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, request *http.Request) {
		identity := observability.Identity{}
		if sink != nil {
			identity = sink.Identity()
		}
		writeJSON(response, http.StatusOK, map[string]any{
			"status":            "ok",
			"revision":          app.Revision(),
			"instance_id":       identity.InstanceID,
			"pid":               identity.PID,
			"started_at":        identity.StartedAt.Format(time.RFC3339Nano),
			"host":              identity.Host,
			"current_operation": observability.OperationFromContext(request.Context()),
		})
	})
	// Recovery wraps operation so a panic can still be correlated and
	// journaled with the request's own operation id.
	return withRecovery(withOperation(sink, mux))
}

func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				observability.MarkBusinessError(request.Context(),
					string(domain.CodeStorage))
				writeJSON(response, http.StatusInternalServerError, errorResponse{
					Error: errorBody{
						Code:        domain.CodeStorage,
						Detail:      "request could not be completed",
						OperationID: observability.OperationFromContext(request.Context()),
					},
				})
			}
		}()
		next.ServeHTTP(response, request)
	})
}
