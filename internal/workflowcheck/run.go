package workflowcheck

import (
	"fmt"
	"net/http/httptest"
	"os"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/httpapi"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/observability"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
)

// harness owns one temporary data directory shared by the snapshot and the
// durable operation journal, so checks can reopen both after a "restart".
type harness struct {
	dataDir string
	journal *observability.Journal
	server  *httptest.Server
	api     *client
}

func newHarness() (*harness, error) {
	dataDir, err := os.MkdirTemp("", "cellar-run-check-*")
	if err != nil {
		return nil, fmt.Errorf("create check directory: %w", err)
	}
	instanceID, err := observability.NewOperationID()
	if err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, err
	}
	identity := observability.NewIdentity(time.Now().UTC(), instanceID)
	journal, err := observability.OpenJournal(dataDir, identity)
	if err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, err
	}
	store, err := storage.Open(dataDir)
	if err != nil {
		_ = journal.Close()
		_ = os.RemoveAll(dataDir)
		return nil, err
	}
	app := service.New(store, clock.System{})
	server := httptest.NewServer(httpapi.NewHandler(app, journal))
	return &harness{
		dataDir: dataDir,
		journal: journal,
		server:  server,
		api:     newClient(server.URL),
	}, nil
}

// reopen simulates a process restart against the same data directory.
func (h *harness) reopen() error {
	h.server.Close()
	if err := h.journal.Close(); err != nil {
		return err
	}
	instanceID, err := observability.NewOperationID()
	if err != nil {
		return err
	}
	identity := observability.NewIdentity(time.Now().UTC(), instanceID)
	journal, err := observability.OpenJournal(h.dataDir, identity)
	if err != nil {
		return err
	}
	store, err := storage.Open(h.dataDir)
	if err != nil {
		_ = journal.Close()
		return err
	}
	app := service.New(store, clock.System{})
	server := httptest.NewServer(httpapi.NewHandler(app, journal))
	h.journal = journal
	h.server = server
	h.api = newClient(server.URL)
	return nil
}

func (h *harness) close() {
	h.server.Close()
	_ = h.journal.Close()
	_ = os.RemoveAll(h.dataDir)
}

func Run(name string) error {
	if name == "operation-tracing" {
		return runOperationTracingCheck()
	}

	h, err := newHarness()
	if err != nil {
		return err
	}
	defer h.close()
	api := h.api

	switch name {
	case "formula-approval":
		if err := formulaApproval(api); err != nil {
			return err
		}
	case "vessel-run-lifecycle":
		if err := vesselRunLifecycle(api); err != nil {
			return err
		}
	case "observations-completion":
		if err := observationsCompletion(api); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown workflow check %q", name)
	}
	fmt.Printf("workflow check passed: %s\n", name)
	return nil
}
