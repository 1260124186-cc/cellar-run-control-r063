package workflowcheck

import (
	"fmt"
	"net/http/httptest"
	"os"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/httpapi"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/operation"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
)

func Run(name string) error {
	dataDir, err := os.MkdirTemp("", "cellar-run-check-*")
	if err != nil {
		return fmt.Errorf("create check directory: %w", err)
	}
	defer os.RemoveAll(dataDir)

	identity := operation.NewProcessIdentity()
	logger := operation.NewLogger(os.Stderr, identity)
	store, err := storage.Open(dataDir, logger)
	if err != nil {
		return err
	}
	app := service.New(store, clock.System{}, logger)
	server := httptest.NewServer(httpapi.NewHandler(app, logger, identity))
	defer server.Close()

	prefix := operation.NewID("chk")
	api := newClient(server.URL, prefix)

	var runErr error
	switch name {
	case "formula-approval":
		runErr = formulaApproval(api)
	case "vessel-run-lifecycle":
		runErr = vesselRunLifecycle(api)
	case "observations-completion":
		runErr = observationsCompletion(api)
	default:
		return fmt.Errorf("unknown workflow check %q", name)
	}
	if runErr != nil {
		return fmt.Errorf("operation prefix %s: %w", prefix, runErr)
	}
	fmt.Printf("workflow check passed: %s (operation prefix %s)\n", name, prefix)
	return nil
}
