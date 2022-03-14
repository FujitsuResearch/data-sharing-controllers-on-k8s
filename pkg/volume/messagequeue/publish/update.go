// Copyright (c) 2022 Fujitsu Limited

package publish

import (
	"fmt"

	"github.com/segmentio/kafka-go"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	volumecontrolapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	lifetimesvolumectl "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/volumecontrol"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
	messagepub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	volumemq "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"
)

type updateMessagesChannel struct {
	method   int
	messages []kafka.Message
}

type VolumeMessageQueueUpdatePublisher struct {
	messageQueue     *messagepub.MessageQueueUpdatePublisher
	volume           *messageQueuePublisher
	updateMessagesCh chan *updateMessagesChannel
}

func CreateVolumeMessageQueueUpdatePublisherConfiguration(
	fileSystemSpec *lifetimesapi.OutputFileSystemSpec,
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	messageQueueSpec *lifetimesapi.PublishMessageQueueSpec,
	messageQueuePublisherOptions *messagepub.MessageQueuePublisherOptions) (
	*volumecontrolapi.MessageQueue, error) {
	messageQueueTopic := volumemq.GetMessageQueueTopic(
		fileSystemSpec.PersistentVolumeClaimRef.Namespace,
		fileSystemSpec.PersistentVolumeClaimRef.Name)
	updateTopic, err := messageQueueTopic.CreateUpdateTopic()
	if err != nil {
		return nil, err
	}

	return lifetimesvolumectl.CreateGrpcMessageQueue(
		dataClient, kubeClient, messageQueueSpec, updateTopic,
		messageQueuePublisherOptions)
}

func NewVolumeMessageQueueUpdatePublisher(
	messageQueueConfig *messagequeue.MessageQueueConfig, topic string,
	compressionCodecOption string, volumeRootPath string, maxBatchBytes int,
	channelBufferSize int) (
	*VolumeMessageQueueUpdatePublisher, error) {
	messageQueuePublisher, err := messagepub.NewMessageQueueUpdatePublisher(
		messageQueueConfig, topic, compressionCodecOption, maxBatchBytes)
	if err != nil {
		return nil, err
	}

	maxBatchSize := messageQueuePublisher.GetMaxBatchSize()
	volumePublisher := newMessageQueuePublisher(volumeRootPath, maxBatchSize)

	chanBufferSize := messagequeue.DefaultUpdatePublishChannelBufferSize
	if channelBufferSize != 0 {
		chanBufferSize = channelBufferSize
	}

	updateMessagesCh := make(chan *updateMessagesChannel, chanBufferSize)

	publisher := &VolumeMessageQueueUpdatePublisher{
		messageQueue:     messageQueuePublisher,
		volume:           volumePublisher,
		updateMessagesCh: updateMessagesCh,
	}

	go publisher.start()

	return publisher, nil
}

func (vmqup *VolumeMessageQueueUpdatePublisher) CloseMessageQueue() error {
	close(vmqup.updateMessagesCh)

	return vmqup.messageQueue.CloseMessageQueue()
}

func (vmqup *VolumeMessageQueueUpdatePublisher) CreateUpdateFileMessages(
	path string, offset int64, contents []byte) error {
	duplicatedContents := make([]byte, len(contents))
	copy(duplicatedContents, contents)

	messages, err := vmqup.volume.createUpdateFileMessages(
		path, offset, duplicatedContents)
	if err != nil {
		return err
	}

	vmqup.updateMessagesCh <- &updateMessagesChannel{
		method:   volumemq.MessageMethodAdd,
		messages: messages,
	}

	return nil
}

func (vmqup *VolumeMessageQueueUpdatePublisher) CreateDeleteFileMessages(
	path string) error {
	messages, err := vmqup.volume.createDeleteFileMessages(path)
	if err != nil {
		return err
	}

	vmqup.updateMessagesCh <- &updateMessagesChannel{
		method:   volumemq.MessageMethodDelete,
		messages: messages,
	}

	return nil
}

func (vmqup *VolumeMessageQueueUpdatePublisher) CreateRenameFileMessages(
	newPath string, oldPath string) error {
	messages, err := vmqup.volume.createRenameFileMessages(newPath, oldPath)
	if err != nil {
		return err
	}

	vmqup.updateMessagesCh <- &updateMessagesChannel{
		method:   volumemq.MessageMethodRename,
		messages: messages,
	}

	return nil
}

func (vmqup *VolumeMessageQueueUpdatePublisher) writeUpdateFileMessages(
	writeMessages []kafka.Message) error {
	err := vmqup.messageQueue.WriteQueueMessages(writeMessages)
	if err != nil {
		return fmt.Errorf(
			"Failed to send the 'update-file' message: %v", err.Error())
	}

	return nil
}

func (vmqup *VolumeMessageQueueUpdatePublisher) writeDeleteFileMessages(
	deleteMessages []kafka.Message) error {
	err := vmqup.messageQueue.WriteQueueMessages(deleteMessages)
	if err != nil {
		return fmt.Errorf(
			"Failed to send the 'delete-file' message: %v", err.Error())
	}

	return nil
}

func (vmqup *VolumeMessageQueueUpdatePublisher) writeRenameFileMessages(
	renameMessages []kafka.Message) error {
	err := vmqup.messageQueue.WriteQueueMessages(renameMessages)
	if err != nil {
		return fmt.Errorf(
			"Failed to send the 'rename-file' message: %v", err.Error())
	}

	return nil
}

func (vmqup *VolumeMessageQueueUpdatePublisher) handleUpdateMessage(
	updateMessages *updateMessagesChannel) {
	switch updateMessages.method {
	case volumemq.MessageMethodAdd:
		err := vmqup.writeUpdateFileMessages(updateMessages.messages)
		if err != nil {
			klog.Errorf("Failed to update message for 'write()': %v",
				err.Error())
		}

	case volumemq.MessageMethodDelete:
		err := vmqup.writeDeleteFileMessages(updateMessages.messages)
		if err != nil {
			klog.Errorf("Failed to update message for 'unlink()': %v",
				err.Error())
		}

	case volumemq.MessageMethodRename:
		err := vmqup.writeRenameFileMessages(updateMessages.messages)
		if err != nil {
			klog.Errorf("Failed to update message for 'rename()': %v",
				err.Error())
		}

	default:
		klog.Errorf(
			"Not support such a update publish message method: %+v",
			*updateMessages)
	}
}

func (vmqup *VolumeMessageQueueUpdatePublisher) start() {
outerLoop:
	for {
		select {
		case updateMessages, ok := <-vmqup.updateMessagesCh:
			if !ok {
				err := vmqup.messageQueue.CloseMessageQueue()
				if err != nil {
					klog.Errorf(
						"Failed to close update the message queue : %v",
						err.Error())
				}
				break outerLoop
			}

			vmqup.handleUpdateMessage(updateMessages)
		}
	}

	klog.Info("Finished volume message queue update publisher()")
}
