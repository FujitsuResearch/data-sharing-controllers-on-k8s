// Copyright (c) 2022 Fujitsu Limited

package storage

import (
	"context"
	"time"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
)

type LifetimeInputOperations interface {
	Start(ctx context.Context) (time.Time, error)
	Stop() error

	FirstUpdate(ctx context.Context, offsetAfter time.Time) (bool, error)
	Update(ctx context.Context) (bool, error)
	Delete(deletePolicy lifetimesapi.DeletePolicy) error
}

type LifetimeOutputOperations interface {
	Start(ctx context.Context) error
	Stop() error

	Delete(deletePolicy lifetimesapi.DeletePolicy) error
}
