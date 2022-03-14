// Copyright (c) 2022 Fujitsu Limited

package testing

import (
	"context"
	"syscall"

	"github.com/containerd/containerd"
	containerdapi "github.com/containerd/containerd"
	containerdtypes "github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/oci"
	"github.com/opencontainers/runtime-spec/specs-go"

	"github.com/stretchr/testify/mock"
)

type FakeContainerdTask struct {
	mock.Mock
}

func (fct *FakeContainerdTask) ID() string {
	args := fct.Called()

	return args.Get(0).(string)
}

func (fct *FakeContainerdTask) Pid() uint32 {
	args := fct.Called()

	return args.Get(0).(uint32)
}

func (fct *FakeContainerdTask) Start(ctx context.Context) error {
	args := fct.Called(ctx)

	return args.Error(0)
}

func (fct *FakeContainerdTask) Delete(
	ctx context.Context, opts ...containerd.ProcessDeleteOpts) (
	*containerd.ExitStatus, error) {
	args := fct.Called(ctx, opts)

	return args.Get(0).(*containerd.ExitStatus), args.Error(1)
}

func (fct *FakeContainerdTask) Kill(
	ctx context.Context, s syscall.Signal, opts ...containerd.KillOpts) error {
	args := fct.Called(ctx, s, opts)

	return args.Error(0)
}

func (fct *FakeContainerdTask) Wait(
	ctx context.Context) (<-chan containerd.ExitStatus, error) {
	args := fct.Called(ctx)

	return args.Get(0).(<-chan containerd.ExitStatus), args.Error(1)
}

func (fct *FakeContainerdTask) CloseIO(
	ctx context.Context, opts ...containerdapi.IOCloserOpts) error {
	args := fct.Called(ctx, opts)

	return args.Error(0)
}

func (fct *FakeContainerdTask) Resize(
	ctx context.Context, w uint32, h uint32) error {
	args := fct.Called(ctx, w, h)

	return args.Error(0)
}

func (fct *FakeContainerdTask) IO() cio.IO {
	args := fct.Called()

	return args.Get(0).(cio.IO)
}

func (fct *FakeContainerdTask) Status(
	ctx context.Context) (containerdapi.Status, error) {
	args := fct.Called(ctx)

	return args.Get(0).(containerdapi.Status), args.Error(1)
}

func (fct *FakeContainerdTask) Pause(ctx context.Context) error {
	args := fct.Called(ctx)

	return args.Error(0)
}

func (fct *FakeContainerdTask) Resume(ctx context.Context) error {
	args := fct.Called(ctx)

	return args.Error(0)
}

func (fct *FakeContainerdTask) Exec(
	ctx context.Context, id string, spec *specs.Process,
	ioCreate cio.Creator) (containerd.Process, error) {
	args := fct.Called(ctx, id, spec, ioCreate)

	return args.Get(0).(containerd.Process), args.Error(1)
}

func (fct *FakeContainerdTask) Pids(
	ctx context.Context) ([]containerd.ProcessInfo, error) {
	args := fct.Called(ctx)

	return args.Get(0).([]containerd.ProcessInfo), args.Error(1)
}

func (fct *FakeContainerdTask) Checkpoint(
	ctx context.Context, opts ...containerdapi.CheckpointTaskOpts) (
	containerd.Image, error) {
	args := fct.Called(ctx, opts)

	return args.Get(0).(containerd.Image), args.Error(1)
}

func (fct *FakeContainerdTask) Update(
	ctx context.Context, opts ...containerdapi.UpdateTaskOpts) error {
	args := fct.Called(ctx, opts)

	return args.Error(0)
}

func (fct *FakeContainerdTask) LoadProcess(
	ctx context.Context, id string, ioAttach cio.Attach) (
	containerdapi.Process, error) {
	args := fct.Called(ctx, id, ioAttach)

	return args.Get(0).(containerdapi.Process), args.Error(1)
}

func (fct *FakeContainerdTask) Metrics(
	ctx context.Context) (*containerdtypes.Metric, error) {
	args := fct.Called(ctx)

	return args.Get(0).(*containerdtypes.Metric), args.Error(1)
}

func (fct *FakeContainerdTask) Spec(ctx context.Context) (*oci.Spec, error) {
	args := fct.Called(ctx)

	return args.Get(0).(*oci.Spec), args.Error(1)
}
