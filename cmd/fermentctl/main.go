package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/config"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/httpapi"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/operation"
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
	default:
		return usageError()
	}
}

func usageError() error {
	return errors.New("usage: fermentctl serve | fermentctl check <workflow>")
}

func serve() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	if err := cfg.EnsureDataDir(); err != nil {
		return err
	}
	identity := operation.NewProcessIdentity()
	logger, closeLogger, err := openOperationLogger(cfg.DataDir, identity)
	if err != nil {
		return err
	}
	defer func() { _ = closeLogger() }()

	store, err := storage.Open(cfg.DataDir, logger)
	if err != nil {
		return err
	}
	app := service.New(store, clock.System{}, logger)
	server := &http.Server{
		Addr:              cfg.Address,
		Handler:           httpapi.NewHandler(app, logger, identity),
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

	logger.Log(operation.EventProcessStarted, operation.Fields{
		"address":  cfg.Address,
		"data_dir": cfg.DataDir,
	})
	log.Printf("cellar run control listening on %s with data %s (process %s)",
		cfg.Address, cfg.DataDir, identity.ID)
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("serve HTTP: %w", err)
}

// openOperationLogger appends operation records to a durable log inside the
// data directory and mirrors them to stderr, so a past operation stays
// locatable after the process restarts.
func openOperationLogger(
	dataDir string,
	identity operation.ProcessIdentity,
) (*operation.Logger, func() error, error) {
	path := filepath.Join(dataDir, operation.LogFileName)
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open operation log: %w", err)
	}
	logger := operation.NewLogger(io.MultiWriter(os.Stderr, handle), identity)
	return logger, handle.Close, nil
}

func init() {
	log.SetFlags(log.LstdFlags | log.LUTC)
}
