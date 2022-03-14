// Copyright (c) 2022 Fujitsu Limited

package subscribe

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"k8s.io/client-go/kubernetes"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
)

type MessageQueueUpdateSubscriber struct {
	updateReader *kafka.Reader
	storageOps   MessageQueueUpdateSubscriberOperations
}

func getUpdateReaderConfiguration(
	topic *messagequeue.MessageQueueTopic, spec *lifetimesapi.MessageQueueSpec,
	groupId string, brokers []string, dialer *kafka.Dialer) (
	*kafka.ReaderConfig, error) {
	topicName, err := topic.CreateUpdateTopic()
	if err != nil {
		return nil, err
	}

	updateReaderConfig := &kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topicName,
		GroupID: groupId,
		Dialer:  dialer,
	}

	if spec.MaxBatchBytes != 0 {
		updateReaderConfig.MaxBytes = spec.MaxBatchBytes
	}

	return updateReaderConfig, nil
}

func NewMessageQueueUpdateSubscriber(
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	spec *lifetimesapi.MessageQueueSpec, topic *messagequeue.MessageQueueTopic,
	groupId string) (*MessageQueueUpdateSubscriber, error) {
	messageQueueConfig, err := messagequeue.GetMessageQueueConfig(
		dataClient, kubeClient, spec.MessageQueueRef)
	if err != nil {
		return nil, err
	}

	brokers := messageQueueConfig.GetBrokers()
	if len(brokers) == 0 {
		return nil, fmt.Errorf(
			"Must give 'brokers' in a message queue configuration")
	}

	dialer, err := messageQueueConfig.CreateSaslDialer()
	if err != nil {
		return nil, err
	}

	updateReaderConfig, err := getUpdateReaderConfiguration(
		topic, spec, groupId, brokers, dialer)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to create the configuration for the 'update' topic: %v",
			err.Error())
	}

	updateReader := kafka.NewReader(*updateReaderConfig)

	return &MessageQueueUpdateSubscriber{
		updateReader: updateReader,
	}, nil
}

func (mqus *MessageQueueUpdateSubscriber) AddStorageOps(
	storageOps MessageQueueUpdateSubscriberOperations) {
	mqus.storageOps = storageOps
}

func (mqus *MessageQueueUpdateSubscriber) CloseMessageQueue() error {
	err := mqus.updateReader.Close()
	if err != nil {
		return fmt.Errorf(
			"Failed to close the 'update' subscriber: %v", err.Error())
	}

	return nil
}

func (mqus *MessageQueueUpdateSubscriber) HandleFirstQueueMessage(
	ctx context.Context, offsetTime time.Time) (bool, error) {
	for {
		message, err := mqus.updateReader.ReadMessage(ctx)
		if err != nil {
			if err == context.Canceled {
				return true, nil
			} else {
				return true, err
			}
		}

		if message.Time.UTC().After(offsetTime.UTC()) {
			return mqus.storageOps.HandleUpdateMessage(&message)
		}
	}
}

func (mqus *MessageQueueUpdateSubscriber) HandleQueueMessage(
	ctx context.Context) (bool, error) {
	message, err := mqus.updateReader.ReadMessage(ctx)
	if err != nil {
		if err == context.Canceled {
			return true, nil
		} else {
			return true, err
		}
	}

	finished, err := mqus.storageOps.HandleUpdateMessage(&message)
	if err != nil {
		return true, err
	} else if finished {
		return true, nil
	}

	return false, nil
}
