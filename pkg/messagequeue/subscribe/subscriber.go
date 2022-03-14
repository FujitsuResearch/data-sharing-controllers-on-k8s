// Copyright (c) 2022 Fujitsu Limited

package subscribe

import (
	"github.com/segmentio/kafka-go"
)

type MessageQueueInitSubscriberOperations interface {
	HandleInitMessage(queueMessage *kafka.Message) (bool, error)
}

type MessageQueueUpdateSubscriberOperations interface {
	HandleUpdateMessage(queueMessage *kafka.Message) (bool, error)
}
