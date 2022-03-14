// Copyright (c) 2022 Fujitsu Limited

package testing

import (
	"context"

	containerdapi "github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/oci"
	prototypes "github.com/gogo/protobuf/types"

	"github.com/stretchr/testify/mock"
)

type FakeContainerdContainer struct {
	mock.Mock
}

func (fcc *FakeContainerdContainer) ID() string {
	args := fcc.Called()

	return args.Get(0).(string)
}

func (fcc *FakeContainerdContainer) Info(
	ctx context.Context, opts ...containerdapi.InfoOpts) (
	containers.Container, error) {
	args := fcc.Called(ctx, opts)

	return args.Get(0).(containers.Container), args.Error(1)
}

func (fcc *FakeContainerdContainer) Delete(
	ctx context.Context, opts ...containerdapi.DeleteOpts) error {
	args := fcc.Called(ctx, opts)

	return args.Error(0)
}

func (fcc *FakeContainerdContainer) NewTask(
	ctx context.Context, ioCreate cio.Creator,
	opts ...containerdapi.NewTaskOpts) (containerdapi.Task, error) {
	args := fcc.Called(ctx, ioCreate, opts)

	return args.Get(0).(containerdapi.Task), args.Error(1)
}

func (fcc *FakeContainerdContainer) Spec(
	ctx context.Context) (*oci.Spec, error) {
	args := fcc.Called(ctx)

	return args.Get(0).(*oci.Spec), args.Error(1)
}

func (fcc *FakeContainerdContainer) Task(
	ctx context.Context, attach cio.Attach) (containerdapi.Task, error) {
	args := fcc.Called(ctx, attach)

	return args.Get(0).(containerdapi.Task), args.Error(1)
}

func (fcc *FakeContainerdContainer) Image(
	ctx context.Context) (containerdapi.Image, error) {
	args := fcc.Called(ctx)

	return args.Get(0).(containerdapi.Image), args.Error(1)
}

func (fcc *FakeContainerdContainer) Labels(
	ctx context.Context) (map[string]string, error) {
	args := fcc.Called(ctx)

	return args.Get(0).(map[string]string), args.Error(1)
}

func (fcc *FakeContainerdContainer) SetLabels(
	ctx context.Context, labels map[string]string) (map[string]string, error) {
	args := fcc.Called(ctx, labels)

	return args.Get(0).(map[string]string), args.Error(1)
}

func (fcc *FakeContainerdContainer) Extensions(
	ctx context.Context) (map[string]prototypes.Any, error) {
	args := fcc.Called(ctx)

	return args.Get(0).(map[string]prototypes.Any), args.Error(1)
}

func (fcc *FakeContainerdContainer) Update(
	ctx context.Context, opts ...containerdapi.UpdateContainerOpts) error {
	args := fcc.Called(ctx, opts)

	return args.Error(0)
}

func (fcc *FakeContainerdContainer) Checkpoint(
	ctx context.Context, ref string,
	opts ...containerdapi.CheckpointOpts) (containerdapi.Image, error) {
	args := fcc.Called(ctx, ref, opts)

	return args.Get(0).(containerdapi.Image), args.Error(1)
}
