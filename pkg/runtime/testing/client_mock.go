// Copyright (c) 2022 Fujitsu Limited

package testing

import (
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime"

	"github.com/stretchr/testify/mock"
)

type FakeRuntimeClient struct {
	mock.Mock
}

func (frc *FakeRuntimeClient) GetContainerInfo(
	containerId string) (*runtime.ContainerInfo, error) {
	args := frc.Called(containerId)

	return args.Get(0).(*runtime.ContainerInfo), args.Error(1)
}

func (frc *FakeRuntimeClient) GetHostPid(containerId string) (int, error) {
	args := frc.Called(containerId)

	return args.Get(0).(int), args.Error(1)
}

func (frc *FakeRuntimeClient) Close() error {
	args := frc.Called()

	return args.Error(0)
}
