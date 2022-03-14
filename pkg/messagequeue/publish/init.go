// Copyright (c) 2022 Fujitsu Limited

package publish

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
)

const (
	topicsCheckMaxIteration = 5
)

type initConfigurations struct {
	request  *kafka.ReaderConfig
	response *kafka.WriterConfig
	init     *kafka.WriterConfig
}

type MessageQueueInitPublisher struct {
	requester  *kafka.Reader
	responder  *kafka.Writer
	initConfig *kafka.WriterConfig

	initWriter  *kafka.Writer
	subscribers map[string]struct{}

	storageOps MessageQueueInitPublisherOperations
}

func getInitConfigurations(
	topic *messagequeue.MessageQueueTopic,
	messageQueueSpec *lifetimesapi.MessageQueueSpec, brokers []string,
	dialer *kafka.Dialer) (*initConfigurations, error) {
	topicNames, err := topic.CreateInitTopics()
	if err != nil {
		return nil, err
	}

	requestConfig := &kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topicNames.Request,
		Dialer:  dialer,
	}
	responseConfig := &kafka.WriterConfig{
		Brokers: brokers,
		Topic:   topicNames.Response,
		Dialer:  dialer,
	}
	initWriterConfig := &kafka.WriterConfig{
		Brokers: brokers,
		Topic:   topicNames.Init,
		Dialer:  dialer,
	}

	if messageQueueSpec.MaxBatchBytes != 0 {
		initWriterConfig.BatchBytes = messageQueueSpec.MaxBatchBytes
	} else {
		initWriterConfig.BatchBytes = messagequeue.DefaultMaxBatchBytes
	}

	return &initConfigurations{
		request:  requestConfig,
		response: responseConfig,
		init:     initWriterConfig,
	}, nil
}

func NewMessageQueueInitPublisher(
	publishOptions *MessageQueuePublisherOptions,
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	messageQueueSpec *lifetimesapi.MessageQueueSpec,
	topic *messagequeue.MessageQueueTopic) (
	*MessageQueueInitPublisher, error) {
	messageQueueConfig, err := messagequeue.GetMessageQueueConfig(
		dataClient, kubeClient, messageQueueSpec.MessageQueueRef)
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

	configurations, err := getInitConfigurations(
		topic, messageQueueSpec, brokers, dialer)
	if err != nil {
		return nil, err
	}

	brokerAddress := configurations.request.Brokers[0]
	topics := []string{
		configurations.request.Topic,
		configurations.response.Topic,
	}

	err = messagequeue.CreateTopics(brokerAddress, dialer, topics)
	if err != nil {
		klog.Errorf("err: %v", err)
		return nil, err
	}

	return &MessageQueueInitPublisher{
		requester: kafka.NewReader(*configurations.request),
		// [TODO] NewWriter() is deprecated
		responder: kafka.NewWriter(*configurations.response),

		initConfig: configurations.init,

		subscribers: map[string]struct{}{},
	}, nil
}

func (mqip *MessageQueueInitPublisher) GetMaxBatchSize() int {
	return mqip.initConfig.BatchBytes
}

func (mqip *MessageQueueInitPublisher) AddStorageOps(
	storageOps MessageQueueInitPublisherOperations) {
	mqip.storageOps = storageOps
}

func (mqip *MessageQueueInitPublisher) CloseMessageQueues() error {
	err := mqip.requester.Close()
	if err != nil {
		return fmt.Errorf(
			"Failed to close the 'init' request publisher: %v", err.Error())
	}

	err = mqip.responder.Close()
	if err != nil {
		return fmt.Errorf(
			"Failed to close the 'init' response publisher: %v", err.Error())
	}

	return nil
}

func (mqip *MessageQueueInitPublisher) writeInitResponse(
	ctx context.Context, message *kafka.Message) error {
	err := mqip.responder.WriteMessages(ctx, *message)
	if err != nil {
		if err == context.Canceled {
			return err
		}

		return fmt.Errorf(
			"Failed to write the 'init' respose message: %v", err.Error())
	}

	return nil
}

func (mqip *MessageQueueInitPublisher) receiveFirstInitRequest(
	ctx context.Context) (*messagequeue.InitCommandMessage, error) {
	startTime := time.Now().UTC()

	var message kafka.Message
	for {
		var err error
		message, err = mqip.requester.ReadMessage(ctx)
		if err != nil {
			if err == context.Canceled {
				return nil, err
			}

			return nil, fmt.Errorf(
				"Failed to read the 'init start' request message: %v",
				err.Error())
		}

		if !message.Time.UTC().Before(startTime) {
			break
		}
	}

	command, err := messagequeue.DecodeInitCommand(&message)
	if err != nil {
		return nil, fmt.Errorf(
			"For 'start' request: %v", err.Error())
	}

	return command, nil
}

func (mqip *MessageQueueInitPublisher) receiveInitRequest(
	ctx context.Context) (*messagequeue.InitCommandMessage, error) {
	message, err := mqip.requester.ReadMessage(ctx)
	if err != nil {
		if err == context.Canceled {
			return nil, err
		}

		return nil, fmt.Errorf(
			"Failed to read the 'init start' request message: %v",
			err.Error())
	}

	command, err := messagequeue.DecodeInitCommand(&message)
	if err != nil {
		return nil, fmt.Errorf(
			"For 'start' request: %v", err.Error())
	}

	return command, nil
}

func (mqip *MessageQueueInitPublisher) writeInitialData(
	ctx context.Context, initCmd *messagequeue.InitCommand) {
	messages, err := mqip.storageOps.CreateInitialDataMessages(ctx)
	if err != nil {
		if err != context.Canceled {
			klog.Errorf(
				"Failed to create initial data message %q: %v",
				mqip.initConfig.Topic, err.Error())
		}
		return
	}

	err = mqip.initWriter.WriteMessages(ctx, messages...)
	if err != nil {
		if err != context.Canceled {
			klog.Errorf(
				"Failed to write initial data message %q: %v",
				mqip.initConfig.Topic, err.Error())
		}
	}
}

func validateInitTopic(
	brokerAddress string, dialer *kafka.Dialer, topics []string) bool {
	found := false
	for i := 0; i < topicsCheckMaxIteration; i++ {
		exist, err := messagequeue.HasTopics(brokerAddress, dialer, topics)
		if exist {
			found = true
			break
		} else if err != nil {
			klog.Infof("Invalid init topic: %v", err.Error())
		}
		time.Sleep(100 * time.Millisecond)
	}

	return found
}

func (mqip *MessageQueueInitPublisher) sendInitialData(
	ctx context.Context, initCmd *messagequeue.InitCommand) error {
	var topicErr error
	errMessage := ""

	if len(mqip.subscribers) == 0 {
		brokerAddress := mqip.initConfig.Brokers[0]
		dialer := mqip.initConfig.Dialer
		topics := []string{mqip.initConfig.Topic}
		topicErr = messagequeue.CreateTopics(brokerAddress, dialer, topics)
		if topicErr != nil {
			errMessage = fmt.Sprintf(
				"Publisher internal error: %v", topicErr.Error())
			klog.Errorf("Created an 'init' topic: %q", mqip.initConfig.Topic)
		} else {
			klog.Infof("Created an 'init' topic: %q", mqip.initConfig.Topic)

			found := validateInitTopic(brokerAddress, dialer, topics)
			if !found {
				errMessage = fmt.Sprintf(
					"Publisher internal error: not find topics: %v", topics)
			} else {
				groupId := initCmd.GetGroupId()
				mqip.subscribers[groupId] = struct{}{}
				if mqip.initWriter == nil {
					// [TODO] NewWriter() is deprecated
					mqip.initWriter = kafka.NewWriter(*mqip.initConfig)
				}

				go mqip.writeInitialData(ctx, initCmd)
			}
		}
	} else {
		groupId := initCmd.GetGroupId()
		mqip.subscribers[groupId] = struct{}{}
	}

	startCmd, err := initCmd.CreateInitResponseForCreateTopic(errMessage)
	if err != nil {
		return err
	}

	err = mqip.writeInitResponse(ctx, startCmd)

	if topicErr != nil {
		return topicErr
	} else if err != nil {
		groupId := initCmd.GetGroupId()
		delete(mqip.subscribers, groupId)

		return err
	}

	return nil
}

func (mqip *MessageQueueInitPublisher) writeNotFindDataResponse(
	ctx context.Context, initCmd *messagequeue.InitCommand) error {
	startCmd, err := initCmd.CreateInitResponseForNotFindData()
	if err != nil {
		return err
	}

	return mqip.writeInitResponse(ctx, startCmd)
}

func (mqip *MessageQueueInitPublisher) handleStartRequest(
	ctx context.Context, initCmd *messagequeue.InitCommand) error {
	klog.Infof(
		"Sending initial data through topic %q ...", mqip.initConfig.Topic)
	hasData, err := mqip.storageOps.HasInitialData()
	if err != nil {
		return err
	}

	if hasData {
		klog.Info("Message queue publisher: Initial data existed")
		err := mqip.sendInitialData(ctx, initCmd)
		if err != nil {
			return err
		}
	} else {
		klog.Info("Message queue publisher: Initial data did NOT exist")
		err := mqip.writeNotFindDataResponse(ctx, initCmd)
		if err != nil {
			return err
		}
	}
	klog.Infof(
		"... Has sent initial data through topic %q", mqip.initConfig.Topic)

	return nil
}

func (mqip *MessageQueueInitPublisher) handleFinishRequest(
	ctx context.Context, initCmd *messagequeue.InitCommand) error {
	if mqip.initWriter == nil {
		return nil
	}

	groupId := initCmd.GetGroupId()
	delete(mqip.subscribers, groupId)

	if len(mqip.subscribers) == 0 {
		err := mqip.initWriter.Close()
		if err != nil {
			return err
		} else {
			mqip.initWriter = nil
		}

		brokerAddress := mqip.initConfig.Brokers[0]
		dialer := mqip.initConfig.Dialer
		topics := []string{mqip.initConfig.Topic}
		err = messagequeue.DeleteTopics(brokerAddress, dialer, topics)
		if err != nil {
			return err
		}
	}

	return nil
}

func (mqip *MessageQueueInitPublisher) handleInitRequest(
	ctx context.Context, message *messagequeue.InitCommandMessage) error {
	initCmd := messagequeue.NewInitCommand(message.GroupId)

	switch {
	case messagequeue.IsInitRequestforStart(message.Command):
		return mqip.handleStartRequest(ctx, initCmd)
	case messagequeue.IsInitRequestforFinish(message.Command):
		return mqip.handleFinishRequest(ctx, initCmd)
	default:
		return fmt.Errorf("Not support the command: '%v'", message.Command)
	}
}

func (mqip *MessageQueueInitPublisher) handleFirstInitQueueMessages(
	ctx context.Context) error {
	message, err := mqip.receiveFirstInitRequest(ctx)
	if err != nil {
		return err
	}

	return mqip.handleInitRequest(ctx, message)
}

func (mqip *MessageQueueInitPublisher) HandleInitQueueMessages(
	ctx context.Context) error {
	err := mqip.handleFirstInitQueueMessages(ctx)
	if err != nil {
		return err
	}

	for {
		message, err := mqip.receiveInitRequest(ctx)
		if err != nil {
			return err
		}

		err = mqip.handleInitRequest(ctx, message)
		if err != nil {
			return err
		}
	}
}
