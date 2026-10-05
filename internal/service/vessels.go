package service

import (
	"context"
	"sort"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

func (s *Service) CreateVessel(
	ctx context.Context,
	input CreateVesselInput,
) (created domain.Vessel, err error) {
	defer func() { s.logFailure(ctx, "CreateVessel", err) }()
	now := s.clock.Now()
	vessel := domain.Vessel{
		Code:      domain.NormalizeVesselCode(input.Code),
		CapacityL: input.CapacityL,
		State:     domain.VesselAvailable,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := vessel.Validate(); err != nil {
		return domain.Vessel{}, err
	}
	id, err := generateEntityID("vessel")
	if err != nil {
		return domain.Vessel{}, domain.NewError(domain.CodeStorage, err.Error())
	}
	vessel.ID = id

	err = s.store.Execute(ctx, func(snapshot *domain.Snapshot) error {
		if _, exists := snapshot.VesselIndex[vessel.Code]; exists {
			return domain.NewError(domain.CodeVesselConflict,
				"a vessel with the same code already exists")
		}
		snapshot.Vessels[vessel.ID] = vessel
		snapshot.VesselIndex[vessel.Code] = vessel.ID
		return nil
	})
	if err != nil {
		return domain.Vessel{}, err
	}
	return vessel, nil
}

func (s *Service) GetVessel(
	ctx context.Context,
	id string,
) (found domain.Vessel, err error) {
	defer func() { s.logFailure(ctx, "GetVessel", err) }()
	err = s.store.View(func(snapshot domain.Snapshot) error {
		vessel, ok := snapshot.Vessels[id]
		if !ok {
			return domain.NewError(domain.CodeVesselNotFound, "vessel was not found")
		}
		found = vessel
		return nil
	})
	if err != nil {
		return domain.Vessel{}, err
	}
	return found, nil
}

func (s *Service) ListVessels(ctx context.Context) (vessels []domain.Vessel, err error) {
	defer func() { s.logFailure(ctx, "ListVessels", err) }()
	err = s.store.View(func(snapshot domain.Snapshot) error {
		vessels = make([]domain.Vessel, 0, len(snapshot.Vessels))
		for _, vessel := range snapshot.Vessels {
			vessels = append(vessels, vessel)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(vessels, func(i, j int) bool {
		if vessels[i].Code == vessels[j].Code {
			return vessels[i].ID < vessels[j].ID
		}
		return vessels[i].Code < vessels[j].Code
	})
	return vessels, nil
}

func (s *Service) FinishCleaning(
	ctx context.Context,
	id string,
	input FinishCleaningInput,
) (updated domain.Vessel, err error) {
	defer func() { s.logFailure(ctx, "FinishCleaning", err) }()
	err = s.store.Execute(ctx, func(snapshot *domain.Snapshot) error {
		vessel, ok := snapshot.Vessels[id]
		if !ok {
			return domain.NewError(domain.CodeVesselNotFound, "vessel was not found")
		}
		if err := s.checkExpected(vessel.Version, input.ExpectedVersion); err != nil {
			return err
		}
		if vessel.ActiveRunID == nil {
			return domain.NewError(domain.CodeVesselState,
				"cleaning vessel has no associated run")
		}
		run, ok := snapshot.Runs[*vessel.ActiveRunID]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound,
				"associated run was not found")
		}
		if run.State != domain.RunCompleted && run.State != domain.RunAborted {
			return domain.NewError(domain.CodeRunState,
				"associated run is still open")
		}
		if err := vessel.FinishCleaning(s.clock.Now()); err != nil {
			return err
		}
		snapshot.Vessels[id] = vessel
		updated = vessel
		return nil
	})
	if err != nil {
		return domain.Vessel{}, err
	}
	return updated, nil
}

func (s *Service) RetireVessel(
	ctx context.Context,
	id string,
	input VersionInput,
) (updated domain.Vessel, err error) {
	defer func() { s.logFailure(ctx, "RetireVessel", err) }()
	err = s.store.Execute(ctx, func(snapshot *domain.Snapshot) error {
		vessel, ok := snapshot.Vessels[id]
		if !ok {
			return domain.NewError(domain.CodeVesselNotFound, "vessel was not found")
		}
		if err := s.checkExpected(vessel.Version, input.ExpectedVersion); err != nil {
			return err
		}
		if err := vessel.Retire(s.clock.Now()); err != nil {
			return err
		}
		snapshot.Vessels[id] = vessel
		updated = vessel
		return nil
	})
	if err != nil {
		return domain.Vessel{}, err
	}
	return updated, nil
}
