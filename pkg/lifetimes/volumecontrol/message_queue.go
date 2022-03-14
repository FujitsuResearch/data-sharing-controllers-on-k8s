// Copyright (c) 2022 Fujitsu Limited

package volumecontrol

import (
	"fmt"
	"reflect"

	"k8s.io/client-go/kubernetes"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	volumecontrolapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
	mqpub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
	volumemq "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"
)

type lifetimer struct {
	dataClient         coreclientset.Interface
	kubeClient         kubernetes.Interface
	mqCompressionCodec string
}

func CreateGrpcMessageQueue(
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	messageQueueSpec *lifetimesapi.PublishMessageQueueSpec, topic string,
	messageQueuePublisherOptions *mqpub.MessageQueuePublisherOptions) (
	*volumecontrolapi.MessageQueue, error) {
	messageQueueConfig, err := messagequeue.GetMessageQueueConfig(
		dataClient, kubeClient, messageQueueSpec.MessageQueueRef)
	if err != nil {
		return nil, err
	}

	return &volumecontrolapi.MessageQueue{
		Brokers:          messageQueueConfig.GetBrokers(),
		User:             messageQueueConfig.GetUser(),
		Password:         messageQueueConfig.GetPassword(),
		Topic:            topic,
		CompressionCodec: messageQueuePublisherOptions.CompressionCodec,
		MaxBatchBytes:    fmt.Sprintf("%d", messageQueueSpec.MaxBatchBytes),
		UpdatePublishChannelBufferSize: fmt.Sprintf(
			"%d", messageQueueSpec.UpdatePublishChannelBufferSize),
	}, nil
}

func newLifetimer(
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	messageQueuePublisherOptions *mqpub.MessageQueuePublisherOptions) *lifetimer {
	return &lifetimer{
		dataClient:         dataClient,
		kubeClient:         kubeClient,
		mqCompressionCodec: messageQueuePublisherOptions.CompressionCodec,
	}
}

func (l *lifetimer) getMessageQueueConfig(
	outputDataSpec []lifetimesapi.OutputDataSpec) (
	map[string]*volumecontrolapi.MessageQueue, error) {
	messageQueueConfig := map[string]*volumecontrolapi.MessageQueue{}

	for _, dataSpec := range outputDataSpec {
		if dataSpec.FileSystemSpec == nil {
			continue
		}

		pvcKey := util.ConcatenateNamespaceAndName(
			dataSpec.FileSystemSpec.PersistentVolumeClaimRef.Namespace,
			dataSpec.FileSystemSpec.PersistentVolumeClaimRef.Name)

		if dataSpec.MessageQueuePublisher == nil {
			messageQueueConfig[pvcKey] = nil
		} else {
			mqConfig, err := messagequeue.GetMessageQueueConfig(
				l.dataClient, l.kubeClient,
				dataSpec.MessageQueuePublisher.MessageQueueRef)
			if err != nil {
				return nil, err
			}
			messageQueueTopic := volumemq.GetMessageQueueTopic(
				dataSpec.FileSystemSpec.PersistentVolumeClaimRef.Namespace,
				dataSpec.FileSystemSpec.PersistentVolumeClaimRef.Name)
			updateTopic, err := messageQueueTopic.CreateUpdateTopic()

			maxBatchBytes := "0"
			if dataSpec.MessageQueuePublisher.MaxBatchBytes != 0 {
				maxBatchBytes = fmt.Sprintf(
					"%d", dataSpec.MessageQueuePublisher.MaxBatchBytes)
			}

			messageQueueConfig[pvcKey] = &volumecontrolapi.MessageQueue{
				Brokers:          mqConfig.GetBrokers(),
				User:             mqConfig.GetUser(),
				Password:         mqConfig.GetPassword(),
				Topic:            updateTopic,
				CompressionCodec: l.mqCompressionCodec,
				MaxBatchBytes:    maxBatchBytes,
			}
		}
	}

	return messageQueueConfig, nil
}

func getMessageQueueConfigDiffs(
	oldMessageQueueConfig map[string]*volumecontrolapi.MessageQueue,
	newMessageQueueConfig map[string]*volumecontrolapi.MessageQueue) (
	map[string]*volumecontrolapi.MessageQueue, error) {
	messageQueueDiffs := map[string]*volumecontrolapi.MessageQueue{}

	for podVolume, newMqConfig := range newMessageQueueConfig {
		oldMqConfig, ok := oldMessageQueueConfig[podVolume]
		if !ok {
			continue
		}

		if reflect.DeepEqual(oldMqConfig, newMqConfig) {
			continue
		}

		if oldMqConfig != nil && newMqConfig != nil {
			return nil, fmt.Errorf(
				"Currently, not support changes in the message queue "+
					"configuration: (old: %+v, new: %+v)",
				oldMqConfig, newMqConfig)
		}

		messageQueueDiffs[podVolume] = newMqConfig
	}

	return messageQueueDiffs, nil
}

func CreateGrpcMessageQueueDiffs(
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	messageQueuePublisherOptions *mqpub.MessageQueuePublisherOptions,
	oldOutputDataSpec []lifetimesapi.OutputDataSpec,
	newOutputDataSpec []lifetimesapi.OutputDataSpec) (
	map[string]*volumecontrolapi.MessageQueue, error) {
	lifetimer := newLifetimer(
		dataClient, kubeClient, messageQueuePublisherOptions)

	oldMessageQueueConfig, err := lifetimer.getMessageQueueConfig(
		oldOutputDataSpec)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to get a message queue configuration form a "+
				"old 'DataLifetime': %v", err.Error())
	}

	newMessageQueueConfig, err := lifetimer.getMessageQueueConfig(
		newOutputDataSpec)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to get a message queue configuration form a "+
				"new 'DataLifetime': %v", err.Error())
	}

	return getMessageQueueConfigDiffs(
		oldMessageQueueConfig, newMessageQueueConfig)
}
