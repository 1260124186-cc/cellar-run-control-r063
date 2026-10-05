package workflowcheck

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/httpapi"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/observability"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
)

// runOperationTracingCheck exercises the request correlation surface end to
// end: stable operation ids, terminal retries, concurrency isolation, durable
// relocation across restarts, sensitive-text exclusion, health identity, and
// storage-failure correlation.
func runOperationTracingCheck() error {
	h, err := newHarness()
	if err != nil {
		return err
	}
	defer h.close()

	if err := checkTerminalRetryCorrelation(h.api); err != nil {
		return err
	}
	if err := checkErrorBodyCarriesOperation(h.api); err != nil {
		return err
	}
	if err := checkConcurrentOperationsIsolated(h.api); err != nil {
		return err
	}
	if err := checkSensitiveTextNeverJournaled(h); err != nil {
		return err
	}
	if err := checkHealthIdentity(h.api, h.journal.Identity().InstanceID); err != nil {
		return err
	}

	firstInstance := h.journal.Identity().InstanceID
	persistedOp, err := checkStorageOutcomeRecorded(h.api)
	if err != nil {
		return err
	}
	if err := checkOperationSurvivesRestart(h, persistedOp, firstInstance); err != nil {
		return err
	}
	if err := checkStorageFailureCorrelation(h); err != nil {
		return err
	}

	fmt.Printf("workflow check passed: operation-tracing\n")
	return nil
}

// 1. A terminal retry keeps the same operation id while bumping the attempt,
// and the operation endpoint shows both attempts as one operation.
func checkTerminalRetryCorrelation(api *client) error {
	first, err := api.call("POST", "/v1/formulas",
		createFormulaPayload("Retry Correlation Ale"), nil)
	if err != nil {
		return err
	}
	if err := requireStatus(first, 201, "retry: first attempt"); err != nil {
		return err
	}
	var formula formulaView
	if err := first.decode(&formula); err != nil {
		return err
	}
	if first.operationID == "" {
		return errInvalidWorkflow("first attempt returned no operation id")
	}

	// Operator retries the same business action from the terminal.
	retry, err := api.retryWithSameOperation(first, "POST", "/v1/formulas",
		createFormulaPayload("Retry Correlation Ale"), nil)
	if err != nil {
		return err
	}
	if retry.operationID != first.operationID {
		return errInvalidWorkflow("terminal retry did not reuse the stable operation id")
	}
	if err := requireCode(retry, 409, string(domain.CodeFormulaNameConflict),
		"retry: duplicate name on retry"); err != nil {
		return err
	}

	lookup, err := api.call("GET", "/v1/operations/"+first.operationID, nil, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(lookup, 200, "retry: lookup operation"); err != nil {
		return err
	}
	var record operationRecord
	if err := lookup.decode(&record); err != nil {
		return err
	}
	if record.OperationID != first.operationID || record.AttemptCount != 2 {
		return errInvalidWorkflow("operation record did not correlate both retry attempts")
	}
	if record.Attempts[0].Attempt != 1 || record.Attempts[1].Attempt != 2 {
		return errInvalidWorkflow("retry attempts were not numbered 1 then 2")
	}
	if record.Attempts[1].ErrorCode != string(domain.CodeFormulaNameConflict) {
		return errInvalidWorkflow("business error code was not attached to the retried operation")
	}
	if !record.Attempts[0].StorageAttempted || !attemptPersisted(record.Attempts[0]) {
		return errInvalidWorkflow("successful write was not marked as durable on the operation")
	}
	if record.Attempts[0].RevisionAfter != 1 || record.Attempts[1].RevisionAfter != 0 {
		return errInvalidWorkflow("revision was attributed to the wrong attempt")
	}
	return nil
}

// 2. Error responses echo the operation id inside the JSON body too, so a
// caller can report it from a terminal that does not inspect headers.
func checkErrorBodyCarriesOperation(api *client) error {
	failed, err := api.call("GET", "/v1/formulas/formula_missing", nil, nil)
	if err != nil {
		return err
	}
	if err := requireCode(failed, 404, string(domain.CodeFormulaNotFound),
		"error body carries operation"); err != nil {
		return err
	}
	var body struct {
		Error struct {
			OperationID string `json:"operation_id"`
		} `json:"error"`
	}
	if err := failed.decode(&body); err != nil {
		return err
	}
	if body.Error.OperationID != failed.operationID {
		return errInvalidWorkflow("error body did not echo the request operation id")
	}

	// A malformed attempt counter must still be rejected under the same valid
	// operation id, and the rejection must remain locatable.
	operationID, err := observability.NewOperationID()
	if err != nil {
		return err
	}
	badAttempt, err := api.callWithRawHeaders("GET", "/healthz", nil, map[string]string{
		observability.OperationHeader: operationID,
		observability.AttemptHeader:   "0",
	})
	if err != nil {
		return err
	}
	if err := requireCode(badAttempt, 400, string(domain.CodeInvalidInput),
		"invalid attempt counter"); err != nil {
		return err
	}
	if err := badAttempt.decode(&body); err != nil {
		return err
	}
	if body.Error.OperationID != operationID || badAttempt.operationID != operationID {
		return errInvalidWorkflow("malformed attempt lost its operation id")
	}
	lookup, err := api.call("GET", "/v1/operations/"+operationID, nil, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(lookup, 200, "invalid attempt: lookup operation"); err != nil {
		return err
	}
	return nil
}

// 3. Concurrent requests never exchange operation ids: every response header,
// body operation id, and journal record matches the id sent on that wire call.
func checkConcurrentOperationsIsolated(api *client) error {
	const workers = 12
	var wg sync.WaitGroup
	errorsChannel := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			operationID, err := observability.NewOperationID()
			if err != nil {
				errorsChannel <- err
				return
			}
			response, err := api.callWithOperation("GET",
				"/v1/formulas?worker="+fmt.Sprintf("%d", index), nil, nil,
				operationID, 1)
			if err != nil {
				errorsChannel <- err
				return
			}
			if response.status != http.StatusOK {
				errorsChannel <- fmt.Errorf("concurrent op %s got status %d",
					operationID, response.status)
				return
			}
			if response.operationID != operationID {
				errorsChannel <- fmt.Errorf("concurrent op %s received header %s",
					operationID, response.operationID)
				return
			}
			lookup, err := api.call("GET", "/v1/operations/"+operationID, nil, nil)
			if err != nil {
				errorsChannel <- err
				return
			}
			var record operationRecord
			if err := lookup.decode(&record); err != nil {
				errorsChannel <- err
				return
			}
			if record.AttemptCount != 1 ||
				record.Attempts[0].OperationID != operationID {
				errorsChannel <- fmt.Errorf("concurrent op %s was cross-wired in journal",
					operationID)
			}
		}(i)
	}
	wg.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			return err
		}
	}
	return nil
}

// 4. Sensitive free text (formula style/notes and observation notes) must never
// appear in the durable operation journal, even when accepted by the API.
func checkSensitiveTextNeverJournaled(h *harness) error {
	secretFormula := "SECRET-FORMULA-TEXT-c6f8e1"
	secretObservation := "SECRET-OBSERVATION-NOTE-9a2d77"

	created, err := h.api.call("POST", "/v1/formulas", map[string]any{
		"name":                    "Sensitive Formula",
		"style":                   secretFormula,
		"target_original_gravity": 1.052,
		"target_final_gravity":    1.012,
		"max_fermentation_temp_c": 22.0,
		"minimum_days":            7,
		"maximum_days":            21,
	}, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(created, 201, "sensitive: create formula"); err != nil {
		return err
	}

	raw, err := os.ReadFile(filepath.Join(h.dataDir, "operations.log"))
	if err != nil {
		return err
	}
	content := string(raw)
	if strings.Contains(content, secretFormula) {
		return errInvalidWorkflow("formula free text leaked into operation journal")
	}
	if strings.Contains(content, secretObservation) {
		return errInvalidWorkflow("observation note leaked into operation journal")
	}
	return nil
}

// 5. The health endpoint identifies the exact serving process and echoes the
// caller's operation id.
func checkHealthIdentity(api *client, expectedInstance string) error {
	operationID, err := observability.NewOperationID()
	if err != nil {
		return err
	}
	response, err := api.callWithOperation("GET", "/healthz", nil, nil, operationID, 1)
	if err != nil {
		return err
	}
	if err := requireStatus(response, 200, "healthz"); err != nil {
		return err
	}
	var health struct {
		Status     string `json:"status"`
		InstanceID string `json:"instance_id"`
		PID        int    `json:"pid"`
		Host       string `json:"host"`
		StartedAt  string `json:"started_at"`
		CurrentOp  string `json:"current_operation"`
	}
	if err := response.decode(&health); err != nil {
		return err
	}
	if health.Status != "ok" || health.InstanceID != expectedInstance {
		return errInvalidWorkflow("healthz did not identify the current process instance")
	}
	if health.PID <= 0 || health.StartedAt == "" {
		return errInvalidWorkflow("healthz process identity is incomplete")
	}
	if health.CurrentOp != operationID {
		return errInvalidWorkflow("healthz did not carry the request operation id")
	}
	return nil
}

// 6. A successful durable write records storage outcome and revision.
func checkStorageOutcomeRecorded(api *client) (string, error) {
	response, err := api.call("POST", "/v1/vessels", map[string]any{
		"code":            "VSL-TRACE",
		"capacity_liters": 500.0,
	}, nil)
	if err != nil {
		return "", err
	}
	if err := requireStatus(response, 201, "storage: create vessel"); err != nil {
		return "", err
	}
	lookup, err := api.call("GET", "/v1/operations/"+response.operationID, nil, nil)
	if err != nil {
		return "", err
	}
	var record operationRecord
	if err := lookup.decode(&record); err != nil {
		return "", err
	}
	attempt := record.Attempts[0]
	if !attempt.StorageAttempted || attempt.StorageFailed || attempt.RevisionAfter < 1 {
		return "", errInvalidWorkflow("successful storage write was not recorded on operation")
	}
	return response.operationID, nil
}

// 7. After a simulated restart, a new process instance locates the old
// operation and shows which instance originally handled it.
func checkOperationSurvivesRestart(h *harness, operationID, firstInstance string) error {
	if err := h.reopen(); err != nil {
		return err
	}
	response, err := h.api.call("GET", "/v1/operations/"+operationID, nil, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(response, 200, "restart: lookup old operation"); err != nil {
		return err
	}
	var record operationRecord
	if err := response.decode(&record); err != nil {
		return err
	}
	if record.OperationID != operationID || record.AttemptCount < 1 {
		return errInvalidWorkflow("restarted process lost the operation record")
	}
	if record.Attempts[0].InstanceID != firstInstance {
		return errInvalidWorkflow("restarted record did not retain the original instance id")
	}
	if h.journal.Identity().InstanceID == firstInstance {
		return errInvalidWorkflow("restart reused the same process instance id")
	}
	return nil
}

// 8. When the durable snapshot write fails, the failure is correlated to the
// request's operation id (storage_failed=true), without stopping the process.
// The operation journal lives in a separate writable directory, so it survives
// the snapshot failure and still records the attempt.
func checkStorageFailureCorrelation(h *harness) error {
	operationID, err := observability.NewOperationID()
	if err != nil {
		return err
	}

	// Restart against the existing snapshot, then replace the loaded state
	// file with a directory of the same name. The store loads normally, but
	// the durable atomic rename over a directory deterministically fails,
	// independent of file permissions or the user running the check.
	h.server.Close()
	if err := h.journal.Close(); err != nil {
		return err
	}
	journalDir, err := os.MkdirTemp("", "cellar-journal-side-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(journalDir)
	instanceID, _ := observability.NewOperationID()
	journal, err := observability.OpenJournal(journalDir,
		observability.NewIdentity(time.Now().UTC(), instanceID))
	if err != nil {
		return err
	}
	store, err := storage.Open(h.dataDir)
	if err != nil {
		_ = journal.Close()
		return err
	}

	statePath := filepath.Join(h.dataDir, "state.json")
	backupPath := statePath + ".check-backup"
	if err := os.Rename(statePath, backupPath); err != nil {
		_ = journal.Close()
		return err
	}
	if err := os.Mkdir(statePath, 0o755); err != nil {
		_ = os.Rename(backupPath, statePath)
		_ = journal.Close()
		return err
	}
	defer func() {
		_ = os.RemoveAll(statePath)
		_ = os.Rename(backupPath, statePath)
	}()

	server := httptest.NewServer(httpapi.NewHandler(
		service.New(store, clock.System{}), journal))
	defer server.Close()
	api := newClient(server.URL)

	failed, err := api.callWithOperation("POST", "/v1/formulas",
		createFormulaPayload("Storage Failure Ale"), nil, operationID, 1)
	if err != nil {
		return err
	}
	if err := requireCode(failed, 500, string(domain.CodeStorage),
		"storage failure surfaces storage_error"); err != nil {
		return err
	}
	lookup, err := api.call("GET", "/v1/operations/"+operationID, nil, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(lookup, 200, "storage failure: lookup operation"); err != nil {
		return err
	}
	var record operationRecord
	if err := lookup.decode(&record); err != nil {
		return err
	}
	attempt := record.Attempts[0]
	if !attempt.StorageAttempted || !attempt.StorageFailed || attempt.Status != 500 {
		return errInvalidWorkflow("storage write failure was not correlated to the operation")
	}
	if attempt.ErrorCode != string(domain.CodeStorage) {
		return errInvalidWorkflow("storage failure did not carry the storage error code")
	}
	return journal.Close()
}
