// Copyright (c) 2022 Fujitsu Limited

package subscribe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"
	corev1 "k8s.io/api/core/v1"
	k8scorev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	coreapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/core/v1alpha1"
	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	corefake "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned/fake"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
	subscribefake "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/subscribe/testing"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
	volumemq "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	kafkaBrokerAddressEnv = "KAFKA_BROKER_ADDRESS"

	// [REF] github.com/segmentio/kafka-go/docker-compose.yml
	saslUser     = "adminscram"
	saslPassword = "admin-secret-512"

	numOfSubscribers = 2
)

type fakeInitPublisher struct {
	requester *kafka.Reader
	responder *kafka.Writer

	initTopic string

	subscribers map[string]struct{}
	initWriter  *kafka.Writer
}

func newMessageQueue(
	namespace string, name string, brokers []string, saslNamespace string,
	saslName string) *coreapi.MessageQueue {
	return &coreapi.MessageQueue{
		TypeMeta: metav1.TypeMeta{
			APIVersion: coreapi.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: coreapi.MessageQueueSpec{
			Brokers: brokers,
			SaslRef: &corev1.ObjectReference{
				Name:      saslName,
				Namespace: saslNamespace,
			},
		},
	}
}

func newSecret(
	namespace string, name string, data map[string][]byte) *k8scorev1.Secret {
	return &k8scorev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: k8scorev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: data,
	}
}

func newFakeMessageQueueInitSubscriber(
	brokerAddress string, messageQueueTopic *messagequeue.MessageQueueTopic,
	groupId string) (*MessageQueueInitSubscriber, error) {
	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"
	brokers := []string{brokerAddress}
	saslNamespace := metav1.NamespaceDefault
	saslName := saslUser
	messageQueueObject := newMessageQueue(
		mqNamespace, mqName, brokers, saslNamespace, saslName)
	messageQueueDataObjects := []runtime.Object{
		messageQueueObject,
	}
	messageQueueDataClient := corefake.NewSimpleClientset(
		messageQueueDataObjects...)

	secretNamespace := saslNamespace
	secretName := saslName
	saslData := map[string][]byte{
		"password": []byte(saslPassword),
	}
	secretObject := newSecret(secretNamespace, secretName, saslData)

	kubeObjects := []runtime.Object{
		secretObject,
	}
	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	messageQueueSpec := &lifetimesapi.MessageQueueSpec{
		MessageQueueRef: &corev1.ObjectReference{
			Namespace: mqNamespace,
			Name:      mqName,
		},
	}

	return NewMessageQueueInitSubscriber(
		messageQueueDataClient, kubeClient, messageQueueSpec,
		messageQueueTopic, groupId)
}

func TestCloseInitMessageQueues(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	topic := messagequeue.NewMessageQueueFileSystemTopic(
		[]string{"test.sub.init0"})

	initTopics, err := topic.CreateInitTopics()
	assert.NoError(t, err)

	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	topics := []string{
		initTopics.Request,
		initTopics.Response,
	}
	err = messagequeue.CreateTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	defer func(broker string, d *kafka.Dialer, ts []string) {
		err := messagequeue.DeleteTopics(broker, d, ts)
		assert.NoError(t, err)
	}(brokerAddress, dialer, topics)

	found, err := messagequeue.HasTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	assert.True(t, found)

	groupId := "subscriber-1"
	subscriber, err := newFakeMessageQueueInitSubscriber(
		brokerAddress, topic, groupId)
	assert.NoError(t, err)

	err = subscriber.CloseMessageQueues()
	assert.NoError(t, err)
}

func newFakePublishReader(
	t *testing.T, brokerAddress string, topic string,
	groupId string) *kafka.Reader {
	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)

	readerConfig := kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		GroupID: groupId,
		Topic:   topic,
		Dialer:  dialer,
	}

	return kafka.NewReader(readerConfig)
}

func newFakePublishWriter(
	t *testing.T, brokerAddress string, topic string) *kafka.Writer {
	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)

	writerConfig := kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		Dialer:  dialer,
	}

	return kafka.NewWriter(writerConfig)
}

func newFakeInitPublisher(
	t *testing.T, brokerAddress string,
	topics *messagequeue.MessageQueueInitTopics,
	groupId string) *fakeInitPublisher {
	requester := newFakePublishReader(
		t, brokerAddress, topics.Request, groupId)
	responder := newFakePublishWriter(t, brokerAddress, topics.Response)

	return &fakeInitPublisher{
		requester: requester,
		responder: responder,

		initTopic: topics.Init,

		subscribers: map[string]struct{}{},
	}
}

func (fip *fakeInitPublisher) Close(t *testing.T) {
	err := fip.requester.Close()
	assert.NoError(t, err)

	err = fip.responder.Close()
	assert.NoError(t, err)
}

func createFinishMessage(t *testing.T) *kafka.Message {
	message := &volumemq.Message{
		Method: volumemq.MessageMethodFinish,
	}
	encodedMessage, err := json.Marshal(message)
	assert.NoError(t, err)

	return &kafka.Message{
		Value: encodedMessage,
	}
}

func (fip *fakeInitPublisher) sendInitialData(
	t *testing.T, ctx context.Context, initCmd *messagequeue.InitCommand,
	initialDataMessage *kafka.Message) bool {
	if len(fip.subscribers) == 0 {
		brokerAddress := fip.requester.Config().Brokers[0]
		dialer := fip.requester.Config().Dialer
		topics := []string{fip.initTopic}
		err := messagequeue.CreateTopics(brokerAddress, dialer, topics)
		assert.NoError(t, err)

		groupId := initCmd.GetGroupId()
		fip.subscribers[groupId] = struct{}{}
		if fip.initWriter == nil {
			fip.initWriter = newFakePublishWriter(
				t, brokerAddress, fip.initTopic)
		}

		finishMessage := createFinishMessage(t)
		messages := []kafka.Message{
			*initialDataMessage,
			*finishMessage,
		}
		err = fip.initWriter.WriteMessages(ctx, messages...)
		if err == context.Canceled {
			return true
		}
	} else {
		groupId := initCmd.GetGroupId()
		fip.subscribers[groupId] = struct{}{}
	}

	startCmd, err := initCmd.CreateInitResponseForCreateTopic("")
	assert.NoError(t, err)

	err = fip.responder.WriteMessages(ctx, *startCmd)
	if err == context.Canceled {
		return true
	}

	return false
}

func (fip *fakeInitPublisher) writeNotFindDataResponse(
	t *testing.T, ctx context.Context,
	initCmd *messagequeue.InitCommand) bool {
	startCmd, err := initCmd.CreateInitResponseForNotFindData()
	assert.NoError(t, err)

	err = fip.responder.WriteMessages(ctx, *startCmd)
	if err != nil {
		if err == context.Canceled {
			return true
		}
	}

	return false
}

func (fip *fakeInitPublisher) handleStartRequest(
	t *testing.T, ctx context.Context, initCmd *messagequeue.InitCommand,
	initialDataMessage *kafka.Message) bool {
	if initialDataMessage != nil {
		return fip.sendInitialData(t, ctx, initCmd, initialDataMessage)
	} else {
		return fip.writeNotFindDataResponse(t, ctx, initCmd)
	}
}

func (fip *fakeInitPublisher) handleFinishRequest(
	t *testing.T, ctx context.Context, initCmd *messagequeue.InitCommand) {
	if fip.initWriter == nil {
		return
	}

	groupId := initCmd.GetGroupId()
	delete(fip.subscribers, groupId)

	if len(fip.subscribers) == 0 {
		err := fip.initWriter.Close()
		assert.NoError(t, err)

		brokerAddress := fip.initWriter.Addr.String()
		dialer := fip.requester.Config().Dialer
		topics := []string{fip.initTopic}
		err = messagequeue.DeleteTopics(brokerAddress, dialer, topics)
		assert.NoError(t, err)

		assert.NoError(t, err)
	}
}

func (fip *fakeInitPublisher) handleInitQueueMessages(
	t *testing.T, ctx context.Context, initialDataMessage *kafka.Message) {
	for {
		message, err := fip.requester.ReadMessage(ctx)
		if err == context.Canceled {
			break
		}
		assert.NoError(t, err)

		decodedMessage, err := messagequeue.DecodeInitCommand(&message)
		assert.NoError(t, err)

		initCmd := messagequeue.NewInitCommand(decodedMessage.GroupId)
		switch {
		case messagequeue.IsInitRequestforStart(decodedMessage.Command):
			stopped := fip.handleStartRequest(
				t, ctx, initCmd, initialDataMessage)
			if stopped {
				break
			}
		case messagequeue.IsInitRequestforFinish(decodedMessage.Command):
			fip.handleFinishRequest(t, ctx, initCmd)
		}
	}
}

func fakeInitPublisherRuns(
	t *testing.T, ctx context.Context, brokerAddress string,
	topics *messagequeue.MessageQueueInitTopics,
	initialDataMessage *kafka.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	groupId := "publisher-1"
	publisher := newFakeInitPublisher(t, brokerAddress, topics, groupId)
	defer publisher.Close(t)

	publisher.handleInitQueueMessages(t, ctx, initialDataMessage)
}

func initSubscriberRuns(
	t *testing.T, ctx context.Context, brokerAddress string,
	topic *messagequeue.MessageQueueTopic, groupId string,
	initialDataMessage *kafka.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	subscriber, err := newFakeMessageQueueInitSubscriber(
		brokerAddress, topic, groupId)
	assert.NoError(t, err)

	fakeStorageOps := new(subscribefake.FakeInitSubscriber)
	fakeStorageOps.On(
		"HandleInitMessage",
		mock.MatchedBy(
			func(message *kafka.Message) bool {
				return bytes.Equal(message.Value, initialDataMessage.Value)
			})).Return(false, nil).Once()
	finishMessage := createFinishMessage(t)
	fakeStorageOps.On(
		"HandleInitMessage",
		mock.MatchedBy(
			func(message *kafka.Message) bool {
				return bytes.Equal(message.Value, finishMessage.Value)
			})).Return(true, nil).Once()

	subscriber.AddStorageOps(fakeStorageOps)

	_, err = subscriber.HandleQueueMessages(ctx)
	assert.NoError(t, err)

	if initialDataMessage != nil {
		fakeStorageOps.AssertExpectations(t)
	} else {
		fakeStorageOps.AssertNumberOfCalls(t, "HandleInitMessage", 0)
	}

	err = subscriber.CloseMessageQueues()
	assert.NoError(t, err)
}

func TestHandleInitQueueMessagesForNotHaveInitialData(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	topic := messagequeue.NewMessageQueueFileSystemTopic(
		[]string{"test.sub.nodata"})

	initTopics, err := topic.CreateInitTopics()
	assert.NoError(t, err)

	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	topics := []string{
		initTopics.Request,
		initTopics.Response,
	}
	err = messagequeue.CreateTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	defer func(broker string, d *kafka.Dialer, ts []string) {
		err := messagequeue.DeleteTopics(broker, d, ts)
		assert.NoError(t, err)
	}(brokerAddress, dialer, topics)

	found, err := messagequeue.HasTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	assert.True(t, found)

	ctx, cancel := context.WithTimeout(
		context.Background(), util.DefaultMessageQueueTimeout)

	publisherWg := new(sync.WaitGroup)
	subscriberWg := new(sync.WaitGroup)

	publisherWg.Add(1)
	go fakeInitPublisherRuns(
		t, ctx, brokerAddress, initTopics, nil, publisherWg)

	for i := 0; i < numOfSubscribers; i++ {
		groupId := fmt.Sprintf("subscriber-%d", i)
		subscriberWg.Add(1)
		go initSubscriberRuns(
			t, ctx, brokerAddress, topic, groupId, nil, subscriberWg)
	}

	subscriberWg.Wait()
	cancel()

	publisherWg.Wait()
}

func TestHandleInitQueueMessagesForHasInitialData(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	topic := messagequeue.NewMessageQueueFileSystemTopic(
		[]string{"test.sub.initdata"})

	initTopics, err := topic.CreateInitTopics()
	assert.NoError(t, err)

	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	topics := []string{
		initTopics.Request,
		initTopics.Response,
	}
	err = messagequeue.CreateTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	defer func(broker string, d *kafka.Dialer, ts []string) {
		err := messagequeue.DeleteTopics(broker, d, ts)
		assert.NoError(t, err)
	}(brokerAddress, dialer, topics)

	found, err := messagequeue.HasTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	assert.True(t, found)

	ctx, cancel := context.WithTimeout(
		context.Background(), util.DefaultMessageQueueTimeout)

	publisherWg := new(sync.WaitGroup)
	subscriberWg := new(sync.WaitGroup)

	publisherWg.Add(1)
	pseudoInitialDataMessage := kafka.Message{
		Value: []byte("initial test data"),
	}
	go fakeInitPublisherRuns(
		t, ctx, brokerAddress, initTopics, &pseudoInitialDataMessage,
		publisherWg)

	for i := 0; i < numOfSubscribers; i++ {
		groupId := fmt.Sprintf("subscriber-%d", i)
		subscriberWg.Add(1)
		go initSubscriberRuns(
			t, ctx, brokerAddress, topic, groupId, &pseudoInitialDataMessage,
			subscriberWg)
	}

	subscriberWg.Wait()
	cancel()

	publisherWg.Wait()
}
