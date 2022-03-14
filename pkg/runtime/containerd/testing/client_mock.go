// Copyright (c) 2022 Fujitsu Limited

package testing

import (
	"context"

	containerdapi "github.com/containerd/containerd"
	"github.com/containerd/containerd/services/introspection"

	"github.com/stretchr/testify/mock"
)

type FakeContainerdClient struct {
	mock.Mock
}

func (fcc *FakeContainerdClient) IntrospectionService() introspection.Service {
	args := fcc.Called()

	return args.Get(0).(introspection.Service)
}

func (fcc *FakeContainerdClient) LoadContainer(
	ctx context.Context, id string) (containerdapi.Container, error) {
	args := fcc.Called(ctx, id)

	return args.Get(0).(containerdapi.Container), args.Error(1)
}

func (fcc *FakeContainerdClient) Close() error {
	args := fcc.Called()

	return args.Error(0)
}
