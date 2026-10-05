package httpapi

import (
	"fmt"
	"net/http"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/operation"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
)

func NewHandler(
	app *service.Service,
	logger *operation.Logger,
	identity operation.ProcessIdentity,
) http.Handler {
	mux := http.NewServeMux()
	registerFormulaRoutes(mux, app)
	registerVesselRoutes(mux, app)
	registerRunRoutes(mux, app)
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		writeJSON(response, http.StatusOK, map[string]any{
			"status":   "ok",
			"revision": app.Revision(),
			"process":  identity,
		})
	})
	return withOperation(logger, withRecovery(logger, mux))
}

func withRecovery(logger *operation.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Log(operation.EventHTTPPanic, operation.Fields{
					"operation_id": operation.IDFromContext(request.Context()),
					"method":       request.Method,
					"path":         request.URL.Path,
					"panic":        fmt.Sprintf("%v", recovered),
				})
				writeJSON(response, http.StatusInternalServerError, errorResponse{
					Error: errorBody{
						Code:   "internal_failure",
						Detail: "request could not be completed",
					},
				})
			}
		}()
		next.ServeHTTP(response, request)
	})
}
