// Copyright (c) 2022 Fujitsu Limited

package testing

import (
	"context"

	"github.com/segmentio/kafka-go"

	"github.com/stretchr/testify/mock"
)

type FakeInitPublisher struct {
	mock.Mock
}

func (fip *FakeInitPublisher) HasInitialData() (bool, error) {
	args := fip.Called()

	return args.Get(0).(bool), args.Error(1)
}

func (fip *FakeInitPublisher) CreateInitialDataMessages(
	ctx context.Context) ([]kafka.Message, error) {
	args := fip.Called(ctx)

	return args.Get(0).([]kafka.Message), args.Error(1)
}
