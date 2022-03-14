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

type VolumeMessageQueueUpdateSubscriber struct {
	messageQueue *messagesub.MessageQueueUpdateSubscriber
	volume       *messageQueueSubscriber
}

func NewVolumeMessageQueueUpdateSubscriber(
	fileSystemSpec *lifetimesapi.InputFileSystemSpec,
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	messageQueueSpec *lifetimesapi.MessageQueueSpec, volumeRootPath string) (
	*VolumeMessageQueueUpdateSubscriber, error) {
	topic := volumemq.GetMessageQueueTopic(
		fileSystemSpec.FromPersistentVolumeClaimRef.Namespace,
		fileSystemSpec.FromPersistentVolumeClaimRef.Name)
	groupId := volumemq.GetGroupId(fileSystemSpec.ToPersistentVolumeClaimRef)

	messageQueueSubscriber, err := messagesub.NewMessageQueueUpdateSubscriber(
		dataClient, kubeClient, messageQueueSpec, topic, groupId)
	if err != nil {
		return nil, err
	}

	volumeSubscriber := newMessageQueueSubscriber(volumeRootPath)

	messageQueueSubscriber.AddStorageOps(volumeSubscriber)

	return &VolumeMessageQueueUpdateSubscriber{
		messageQueue: messageQueueSubscriber,
		volume:       volumeSubscriber,
	}, nil
}

func (vmqus *VolumeMessageQueueUpdateSubscriber) CloseMessageQueue() error {
	return vmqus.messageQueue.CloseMessageQueue()
}

func (vmqus *VolumeMessageQueueUpdateSubscriber) HandleFirstQueueMessage(
	ctx context.Context, offsetTime time.Time) (bool, error) {
	return vmqus.messageQueue.HandleFirstQueueMessage(ctx, offsetTime)
}

func (vmqus *VolumeMessageQueueUpdateSubscriber) HandleQueueMessage(
	ctx context.Context) (bool, error) {
	return vmqus.messageQueue.HandleQueueMessage(ctx)
}
