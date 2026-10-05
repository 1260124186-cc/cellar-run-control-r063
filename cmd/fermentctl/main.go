package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/config"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/httpapi"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/observability"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/workflowcheck"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "fermentctl:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 {
		return usageError()
	}
	switch arguments[0] {
	case "serve":
		if len(arguments) != 1 {
			return usageError()
		}
		return serve()
	case "check":
		if len(arguments) != 2 {
			return usageError()
		}
		return workflowcheck.Run(arguments[1])
	case "operation":
		if len(arguments) != 2 {
			return usageError()
		}
		return queryOperation(arguments[1])
	default:
		return usageError()
	}
}

func usageError() error {
	return errors.New("usage: fermentctl serve | fermentctl check <workflow> | fermentctl operation <op_id>")
}

// processAssemble opens the shared snapshot and operation journal and binds a
// fresh process identity so health checks can identify the serving process.
func processAssemble(cfg config.Config) (*service.Service, *observability.Journal, error) {
	if err := cfg.EnsureDataDir(); err != nil {
		return nil, nil, err
	}
	instanceID, err := observability.NewOperationID()
	if err != nil {
		return nil, nil, err
	}
	identity := observability.NewIdentity(time.Now().UTC(), instanceID)
	journal, err := observability.OpenJournal(cfg.DataDir, identity)
	if err != nil {
		return nil, nil, err
	}
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		_ = journal.Close()
		return nil, nil, err
	}
	return service.New(store, clock.System{}), journal, nil
}

func serve() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	app, journal, err := processAssemble(cfg)
	if err != nil {
		return err
	}
	defer journal.Close()

	server := &http.Server{
		Addr:              cfg.Address,
		Handler:           httpapi.NewHandler(app, journal),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
		}
	}()

	identity := journal.Identity()
	log.Printf("cellar run control listening on %s with data %s instance=%s pid=%d host=%s",
		cfg.Address, cfg.DataDir, identity.InstanceID, identity.PID, identity.Host)
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("serve HTTP: %w", err)
}

// queryOperation reopens the durable journal without serving HTTP, so an
// operator can relocate an operation after the handling process restarted.
func queryOperation(operationID string) error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	journal, err := observability.OpenJournal(cfg.DataDir, observability.Identity{})
	if err != nil {
		return err
	}
	defer journal.Close()
	attempts, err := journal.Lookup(operationID)
	if err != nil {
		return fmt.Errorf("operation %s: %w", operationID, err)
	}
	encoded, err := json.MarshalIndent(map[string]any{
		"operation_id":  operationID,
		"attempt_count": len(attempts),
		"attempts":      attempts,
	}, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

func init() {
	log.SetFlags(log.LstdFlags | log.LUTC)
}
