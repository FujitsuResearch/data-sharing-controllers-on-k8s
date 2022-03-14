// Copyright (c) 2022 Fujitsu Limited

package testing

import (
	"context"

	dockertypes "github.com/docker/docker/api/types"
	dockerclient "github.com/docker/docker/client"

	"github.com/stretchr/testify/mock"
)

type FakeDockerClient struct {
	mock.Mock
}

func (fdc *FakeDockerClient) NewClientWithOpts(
	opt ...dockerclient.Opt) (*dockerclient.Client, error) {
	args := fdc.Called(opt)

	return args.Get(0).(*dockerclient.Client), args.Error(1)
}

func (fdc *FakeDockerClient) Info(
	ctx context.Context) (dockertypes.Info, error) {
	args := fdc.Called(ctx)

	return args.Get(0).(dockertypes.Info), args.Error(1)
}

func (fdc *FakeDockerClient) ContainerInspect(
	ctx context.Context, containerID string) (dockertypes.ContainerJSON, error) {
	args := fdc.Called(ctx, containerID)

	return args.Get(0).(dockertypes.ContainerJSON), args.Error(1)
}

func (fdc *FakeDockerClient) Close() error {
	args := fdc.Called()

	return args.Error(0)
}
