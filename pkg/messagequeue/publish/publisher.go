// Copyright (c) 2022 Fujitsu Limited

package publish

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type MessageQueueInitPublisherOperations interface {
	HasInitialData() (bool, error)
	CreateInitialDataMessages(ctx context.Context) ([]kafka.Message, error)
}
