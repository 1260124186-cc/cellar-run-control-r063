package httpapi

import (
	"net/http"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/observability"
)

func registerOperationRoutes(mux *http.ServeMux, sink observability.Sink) {
	mux.HandleFunc("GET /v1/operations/{id}", func(response http.ResponseWriter, request *http.Request) {
		operationID := request.PathValue("id")
		attempts, err := lookupOperation(request.Context(), sink, operationID)
		if err != nil {
			writeDomainError(response, request, err)
			return
		}
		writeJSON(response, http.StatusOK, map[string]any{
			"operation_id":  operationID,
			"attempts":      attempts,
			"attempt_count": len(attempts),
		})
	})
}
