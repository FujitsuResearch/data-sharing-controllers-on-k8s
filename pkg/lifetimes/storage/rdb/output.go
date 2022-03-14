// Copyright (c) 2022 Fujitsu Limited

package rdb

import (
	"context"
	"time"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/storage"
)

var (
	_ storage.LifetimeOutputOperations = NewRdbLifetimerForOutputData(
		((*lifetimesapi.OutputRdbSpec)(nil)))
)

type RdbOutputLifetimer struct {
	lifetimeStatus *lifetimesapi.ConsumerStatus
}

func NewRdbLifetimerForOutputData(
	outputRdbSpec *lifetimesapi.OutputRdbSpec) *RdbOutputLifetimer {
	return &RdbOutputLifetimer{}
}

// [TODO]
func (rol *RdbOutputLifetimer) Start(ctx context.Context) error {
	return nil
}

// [TODO]
func (rol *RdbOutputLifetimer) Stop() error {
	return nil
}

// [TODO]
func (rol *RdbOutputLifetimer) FirstUpdate(
	ctx context.Context, offsetAfter time.Time) (bool, error) {
	return false, nil
}

// [TODO]
func (rol *RdbOutputLifetimer) Update(ctx context.Context) (bool, error) {
	return false, nil
}

// [TODO]
func (ril *RdbOutputLifetimer) Delete(
	deletePolicy lifetimesapi.DeletePolicy) error {
	return nil
}
