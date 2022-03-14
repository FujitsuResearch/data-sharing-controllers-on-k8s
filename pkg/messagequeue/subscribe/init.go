// Copyright (c) 2022 Fujitsu Limited

package subscribe

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

type initConfigurations struct {
	request  *kafka.WriterConfig
	response *kafka.ReaderConfig
	init     *kafka.ReaderConfig
}

type MessageQueueInitSubscriber struct {
	requester  *kafka.Writer
	responder  *kafka.Reader
	initConfig *kafka.ReaderConfig

	storageOps MessageQueueInitSubscriberOperations
}

type initTopicCreationChannel struct {
	created bool
	err     error
}

func getInitConfigurations(
	topic *messagequeue.MessageQueueTopic, spec *lifetimesapi.MessageQueueSpec,
	groupId string, brokers []string, dialer *kafka.Dialer) (
	*initConfigurations, error) {
	topicNames, err := topic.CreateInitTopics()
	if err != nil {
		return nil, err
	}

	requestConfig := &kafka.WriterConfig{
		Brokers: brokers,
		Topic:   topicNames.Request,
		Dialer:  dialer,
	}
	responseConfig := &kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topicNames.Response,
		GroupID: groupId,
		Dialer:  dialer,
	}
	initReaderConfig := &kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topicNames.Init,
		GroupID: groupId,
		Dialer:  dialer,
	}

	if spec.MaxBatchBytes != 0 {
		initReaderConfig.MaxBytes = spec.MaxBatchBytes
	}

	return &initConfigurations{
		request:  requestConfig,
		response: responseConfig,
		init:     initReaderConfig,
	}, nil
}

func NewMessageQueueInitSubscriber(
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	spec *lifetimesapi.MessageQueueSpec, topic *messagequeue.MessageQueueTopic,
	groupId string) (*MessageQueueInitSubscriber, error) {
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

	configurations, err := getInitConfigurations(
		topic, spec, groupId, brokers, dialer)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to create the configuration for the 'init' topic: %v",
			err.Error())
	}

	return &MessageQueueInitSubscriber{
		// [TODO] NewWriter() is deprecated
		requester:  kafka.NewWriter(*configurations.request),
		responder:  kafka.NewReader(*configurations.response),
		initConfig: configurations.init,
	}, nil
}

func (mqis *MessageQueueInitSubscriber) AddStorageOps(
	storageOps MessageQueueInitSubscriberOperations) {
	mqis.storageOps = storageOps
}

func (mqis *MessageQueueInitSubscriber) CloseMessageQueues() error {
	err := mqis.requester.Close()
	if err != nil {
		return fmt.Errorf(
			"Failed to close the 'init' request subscriber: %v", err.Error())
	}

	err = mqis.responder.Close()
	if err != nil {
		return fmt.Errorf(
			"Failed to close the 'init' response subscriber: %v", err.Error())
	}

	return nil
}

func (mqis *MessageQueueInitSubscriber) sendInitRequest(
	ctx context.Context, message *kafka.Message) error {
	err := mqis.requester.WriteMessages(ctx, *message)
	if err != nil {
		if err == context.Canceled {
			return err
		}

		return fmt.Errorf(
			"Failed to write the 'init' request message: %v", err.Error())
	}

	return err
}

func (mqis *MessageQueueInitSubscriber) receivedStartResponse(
	ctx context.Context, initCmd *messagequeue.InitCommand,
	topicCh chan<- initTopicCreationChannel) {
	for {
		message, err := mqis.responder.ReadMessage(ctx)
		if err != nil {
			if err == context.Canceled {
				topicCh <- initTopicCreationChannel{
					err: err,
				}
				return
			}

			topicCh <- initTopicCreationChannel{
				err: fmt.Errorf(
					"Failed to write the 'init start' request message: %v",
					err.Error()),
			}
			return
		}

		created, err := initCmd.IsInitResponseforCreateTopic(&message)
		if err != nil {
			topicCh <- initTopicCreationChannel{
				err: fmt.Errorf(
					"Failed to read the 'init create-topic' response "+
						"message: %v", err.Error()),
			}
			return
		} else if created {
			topicCh <- initTopicCreationChannel{
				created: true,
			}
			return
		}

		notFound, err := initCmd.IsInitResponseforNotFindData(&message)
		if err != nil {
			topicCh <- initTopicCreationChannel{
				err: fmt.Errorf(
					"Failed to read the 'init not-find-data' response "+
						"message: %v", err.Error()),
			}
			return
		} else if notFound {
			topicCh <- initTopicCreationChannel{
				created: false,
			}
			return
		}
	}
}

func (mqis *MessageQueueInitSubscriber) hasInitData(
	ctx context.Context, initCmd *messagequeue.InitCommand) (bool, error) {
	topicCh := make(chan initTopicCreationChannel)
	go mqis.receivedStartResponse(ctx, initCmd, topicCh)

	responseTicker := time.NewTicker(messagequeue.InitResponseTimeout)
	select {
	case initTopic := <-topicCh:
		return initTopic.created, initTopic.err
	case <-responseTicker.C:
		return false, fmt.Errorf(
			"Waited too long to receive the 'init start' response")
	}
}

func (mqis *MessageQueueInitSubscriber) handleStartRequest(
	ctx context.Context, initCmd *messagequeue.InitCommand) (
	bool, bool, error) {
	startCmd, err := initCmd.CreateInitRequestforStart()
	if err != nil {
		return false, false, err
	}

	err = mqis.sendInitRequest(ctx, startCmd)
	if err != nil {
		return false, false, err
	}

	created, err := mqis.hasInitData(ctx, initCmd)

	return created, true, err
}

func (mqis *MessageQueueInitSubscriber) handleFinishRequest(
	ctx context.Context, initCmd *messagequeue.InitCommand) error {
	finishCmd, err := initCmd.CreateInitRequestForFinish()
	if err != nil {
		return err
	}

	return mqis.sendInitRequest(ctx, finishCmd)
}

func (mqis *MessageQueueInitSubscriber) handleInitReads(
	ctx context.Context) (time.Time, error) {
	initReader := kafka.NewReader(*mqis.initConfig)
	defer func() {
		err := initReader.Close()
		if err != nil {
			klog.Errorf(
				"Failed to close the 'init' subscriber: %v", err.Error())
		}
	}()

	startTime := time.Time{}

	klog.Infof(
		"Receiving initial data through topic %q ...", mqis.initConfig.Topic)
	for {
		message, err := initReader.ReadMessage(ctx)
		if err != nil {
			if err == context.Canceled {
				return startTime, err
			}

			return startTime, fmt.Errorf(
				"Failed to read the 'init' message: %v", err.Error())
		}

		if startTime.IsZero() {
			startTime = message.Time
		}

		finished, err := mqis.storageOps.HandleInitMessage(&message)
		if err != nil {
			return startTime, fmt.Errorf(
				"Failed to handle 'init' message: %v", err.Error())
		} else if finished {
			break
		}
	}
	klog.Infof(
		"... Has received initial data through topic %q",
		mqis.initConfig.Topic)

	return startTime, nil
}

func (mqis *MessageQueueInitSubscriber) HandleQueueMessages(
	ctx context.Context) (time.Time, error) {
	groupId := mqis.responder.Config().GroupID
	initCmd := messagequeue.NewInitCommand(groupId)
	startTime := time.Time{}

	initTopicCreated, initSent, err := mqis.handleStartRequest(ctx, initCmd)
	if err != nil {
		if initSent {
			mqis.handleFinishRequest(ctx, initCmd)
		}
		return startTime, err
	}

	if initTopicCreated {
		klog.Info("Message queue suscriber: Initial data existed")
		startTime, err = mqis.handleInitReads(ctx)
		if err != nil {
			mqis.handleFinishRequest(ctx, initCmd)
			return startTime, err
		}
	} else {
		klog.Info("Message queue suscriber: Initial data did NOT exist")
	}

	return startTime, mqis.handleFinishRequest(ctx, initCmd)
}
