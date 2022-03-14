// Copyright (c) 2022 Fujitsu Limited

package subscribe

import (
	"context"
	"time"

	"k8s.io/client-go/kubernetes"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	messagesub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/subscribe"
	volumemq "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"
)

type VolumeMessageQueueInitSubscriber struct {
	messageQueue *messagesub.MessageQueueInitSubscriber
	volume       *messageQueueSubscriber
}

func NewVolumeMessageQueueInitSubscriber(
	fileSystemSpec *lifetimesapi.InputFileSystemSpec,
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	messageQueueSpec *lifetimesapi.MessageQueueSpec, volumeRootPath string) (
	*VolumeMessageQueueInitSubscriber, error) {
	topic := volumemq.GetMessageQueueTopic(
		fileSystemSpec.FromPersistentVolumeClaimRef.Namespace,
		fileSystemSpec.FromPersistentVolumeClaimRef.Name)
	groupId := volumemq.GetGroupId(fileSystemSpec.ToPersistentVolumeClaimRef)

	messageQueueSubscriber, err := messagesub.NewMessageQueueInitSubscriber(
		dataClient, kubeClient, messageQueueSpec, topic, groupId)
	if err != nil {
		return nil, err
	}

	volumeSubscriber := newMessageQueueSubscriber(volumeRootPath)

	messageQueueSubscriber.AddStorageOps(volumeSubscriber)

	return &VolumeMessageQueueInitSubscriber{
		messageQueue: messageQueueSubscriber,
		volume:       volumeSubscriber,
	}, nil
}

func (vmqis *VolumeMessageQueueInitSubscriber) CloseMessageQueues() error {
	return vmqis.messageQueue.CloseMessageQueues()
}

func (vmqis *VolumeMessageQueueInitSubscriber) HandleQueueMessages(
	ctx context.Context) (time.Time, error) {
	return vmqis.messageQueue.HandleQueueMessages(ctx)
}
