// Copyright (c) 2022 Fujitsu Limited

package publish

import (
	"context"

	"k8s.io/client-go/kubernetes"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	messagepub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	volumemq "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"
)

type VolumeMessageQueueInitPublisher struct {
	messageQueue *messagepub.MessageQueueInitPublisher
	volume       *messageQueuePublisher
}

func NewVolumeMessageQueueInitPublisher(
	fileSystemSpec *lifetimesapi.OutputFileSystemSpec,
	publishOptions *messagepub.MessageQueuePublisherOptions,
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	messageQueueSpec *lifetimesapi.MessageQueueSpec, volumeRootPath string) (
	*VolumeMessageQueueInitPublisher, error) {
	messageQueueTopic := volumemq.GetMessageQueueTopic(
		fileSystemSpec.PersistentVolumeClaimRef.Namespace,
		fileSystemSpec.PersistentVolumeClaimRef.Name)

	messageQueuePublisher, err := messagepub.NewMessageQueueInitPublisher(
		publishOptions, dataClient, kubeClient, messageQueueSpec,
		messageQueueTopic)
	if err != nil {
		return nil, err
	}

	maxBatchSize := messageQueuePublisher.GetMaxBatchSize()
	volumePublisher := newMessageQueuePublisher(volumeRootPath, maxBatchSize)

	messageQueuePublisher.AddStorageOps(volumePublisher)

	return &VolumeMessageQueueInitPublisher{
		messageQueue: messageQueuePublisher,
		volume:       volumePublisher,
	}, nil
}

func (vmqip *VolumeMessageQueueInitPublisher) CloseMessageQueues() error {
	return vmqip.messageQueue.CloseMessageQueues()
}

func (vmqip *VolumeMessageQueueInitPublisher) HasInitialData() (bool, error) {
	return vmqip.volume.HasInitialData()
}

func (vmqip *VolumeMessageQueueInitPublisher) HandleInitQueueMessages(
	ctx context.Context) error {
	return vmqip.messageQueue.HandleInitQueueMessages(ctx)
}
