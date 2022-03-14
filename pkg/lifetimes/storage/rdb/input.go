// Copyright (c) 2022 Fujitsu Limited

package rdb

import (
	"context"
	"time"

	"k8s.io/client-go/kubernetes"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/storage"
)

var (
	_ storage.LifetimeInputOperations = NewRdbLifetimerForInputData(
		((*lifetimesapi.InputRdbSpec)(nil)), ((coreclientset.Interface)(nil)),
		((kubernetes.Interface)(nil)), ((*lifetimesapi.MessageQueueSpec)(nil)))
)

type RdbInputLifetimer struct {
	rdbSpec        *lifetimesapi.InputRdbSpec
	lifetimeStatus *lifetimesapi.ConsumerStatus
}

func NewRdbLifetimerForInputData(
	inputRdbSpec *lifetimesapi.InputRdbSpec,
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	messageQueueSpec *lifetimesapi.MessageQueueSpec) *RdbInputLifetimer {
	return &RdbInputLifetimer{
		rdbSpec: inputRdbSpec,
	}
}

// [TODO]
func (ril *RdbInputLifetimer) Start(ctx context.Context) (time.Time, error) {
	return time.Time{}, nil
}

// [TODO]
func (ril *RdbInputLifetimer) Stop() error {
	return nil
}

// [TODO]
func (ril *RdbInputLifetimer) FirstUpdate(
	ctx context.Context, offsetAfter time.Time) (bool, error) {
	return false, nil
}

// [TODO]
func (ril *RdbInputLifetimer) Update(ctx context.Context) (bool, error) {
	return false, nil
}

// [TODO]
func (ril *RdbInputLifetimer) Delete(
	deletePolicy lifetimesapi.DeletePolicy) error {
	return nil
}
