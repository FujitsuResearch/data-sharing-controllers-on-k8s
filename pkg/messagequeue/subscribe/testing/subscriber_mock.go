// Copyright (c) 2022 Fujitsu Limited

package testing

import (
	"github.com/segmentio/kafka-go"

	"github.com/stretchr/testify/mock"
)

type FakeInitSubscriber struct {
	mock.Mock
}

type FakeUpdateSubscriber struct {
	mock.Mock
}

func (fis *FakeInitSubscriber) HandleInitMessage(
	queueMessage *kafka.Message) (bool, error) {
	args := fis.Called(queueMessage)

	return args.Get(0).(bool), args.Error(1)
}

func (fus *FakeUpdateSubscriber) HandleUpdateMessage(
	queueMessage *kafka.Message) (bool, error) {
	args := fus.Called(queueMessage)

	return args.Get(0).(bool), args.Error(1)
}
