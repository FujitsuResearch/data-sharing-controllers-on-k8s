// Copyright (c) 2022 Fujitsu Limited

package testing

import (
	"context"

	introspectionapi "github.com/containerd/containerd/api/services/introspection/v1"
	protobuftypes "github.com/gogo/protobuf/types"

	"github.com/stretchr/testify/mock"
)

type FakeContainerdService struct {
	mock.Mock
}

func (fcs *FakeContainerdService) Plugins(
	ctx context.Context, filters []string) (
	*introspectionapi.PluginsResponse, error) {
	args := fcs.Called(ctx, filters)

	return args.Get(0).(*introspectionapi.PluginsResponse), args.Error(1)
}

func (fcs *FakeContainerdService) Server(
	ctx context.Context, in *protobuftypes.Empty) (
	*introspectionapi.ServerResponse, error) {
	args := fcs.Called(ctx, in)

	return args.Get(0).(*introspectionapi.ServerResponse), args.Error(1)
}
