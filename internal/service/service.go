package service

import (
	"context"
	"errors"
	"strings"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/operation"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
)

type Service struct {
	store  *storage.Store
	clock  clock.Clock
	logger *operation.Logger
}

func New(store *storage.Store, timeSource clock.Clock, logger *operation.Logger) *Service {
	if timeSource == nil {
		timeSource = clock.System{}
	}
	return &Service{store: store, clock: timeSource, logger: logger}
}

func (s *Service) Revision() int64 {
	return s.store.Revision()
}

// logFailure records one service_error event for a failed use case. Only the
// stable error code, message, and field are logged; request payloads and
// note bodies never reach the operation log.
func (s *Service) logFailure(ctx context.Context, useCase string, err error) {
	if err == nil {
		return
	}
	fields := operation.Fields{
		"operation_id": operation.IDFromContext(ctx),
		"use_case":     useCase,
		"code":         string(domain.CodeStorage),
		"detail":       err.Error(),
	}
	var domainErr *domain.Error
	if errors.As(err, &domainErr) {
		fields["code"] = string(domainErr.Code)
		fields["detail"] = domainErr.Message
		if domainErr.Field != "" {
			fields["field"] = domainErr.Field
		}
	}
	s.logger.Log(operation.EventServiceError, fields)
}

func (s *Service) checkExpected(current int64, expected *int64) error {
	if expected != nil && current != *expected {
		return domain.NewError(domain.CodeRevisionConflict,
			"the aggregate version does not match X-Expected-Version")
	}
	return nil
}

func generateEntityID(prefix string) (string, error) {
	return domain.NewID(prefix)
}

func generatedRunCode() (string, error) {
	value, err := domain.NewID("run")
	if err != nil {
		return "", err
	}
	suffix := strings.ToUpper(strings.TrimPrefix(value, "run_"))
	return "RUN-" + suffix, nil
}
