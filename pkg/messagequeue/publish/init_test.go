// Copyright (c) 2022 Fujitsu Limited

package publish

import (
	"context"
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
	publishfake "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish/testing"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"

	"github.com/stretchr/testify/assert"
)

const (
	kafkaBrokerAddressEnv = "KAFKA_BROKER_ADDRESS"

	// [REF] github.com/segmentio/kafka-go/docker-compose.yml
	saslUser     = "adminscram"
	saslPassword = "admin-secret-512"

	numOfSubscribers = 2
)

type fakeInitSubscriber struct {
	requester *kafka.Writer
	responder *kafka.Reader

	initTopic string
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

func newFakeMessageQueueInitPublisher(
	t *testing.T, brokerAddress string,
	messageQueueTopic *messagequeue.MessageQueueTopic) *MessageQueueInitPublisher {
	publishOptions := &MessageQueuePublisherOptions{}

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

	publisher, err := NewMessageQueueInitPublisher(
		publishOptions, messageQueueDataClient, kubeClient,
		messageQueueSpec, messageQueueTopic)
	assert.NoError(t, err)

	return publisher
}

func TestCloseInitMessageQueues(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	topic := messagequeue.NewMessageQueueFileSystemTopic(
		[]string{"test.pub.init0"})
	publisher := newFakeMessageQueueInitPublisher(
		t, brokerAddress, topic)

	maxBatchSize := publisher.GetMaxBatchSize()

	expectedMaxBatchSize := int(messagequeue.DefaultMaxBatchBytes)
	assert.Equal(t, expectedMaxBatchSize, maxBatchSize)

	initTopics, err := topic.CreateInitTopics()
	assert.NoError(t, err)
	topics := []string{
		initTopics.Request,
		initTopics.Response,
	}

	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)

	found, err := messagequeue.HasTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	assert.True(t, found)

	err = messagequeue.CreateTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)

	err = publisher.CloseMessageQueues()
	assert.NoError(t, err)

	err = messagequeue.DeleteTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
}

func newFakeSubscribeReader(
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

func newFakeSubscribeWriter(
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

func newFakeInitSubscriber(
	t *testing.T, brokerAddress string,
	topics *messagequeue.MessageQueueInitTopics, groupId string) *fakeInitSubscriber {
	requester := newFakeSubscribeWriter(t, brokerAddress, topics.Request)
	responder := newFakeSubscribeReader(
		t, brokerAddress, topics.Response, groupId)

	return &fakeInitSubscriber{
		requester: requester,
		responder: responder,
		initTopic: topics.Init,
	}
}

func (fis *fakeInitSubscriber) Close(t *testing.T) {
	err := fis.requester.Close()
	assert.NoError(t, err)

	err = fis.responder.Close()
	assert.NoError(t, err)
}

func (fis *fakeInitSubscriber) handleStartRequest(
	t *testing.T, ctx context.Context, initialDataMessage *kafka.Message) {
	groupId := fis.responder.Config().GroupID
	initCmd := messagequeue.NewInitCommand(groupId)

	startCmd, err := initCmd.CreateInitRequestforStart()
	assert.NoError(t, err)

	err = fis.requester.WriteMessages(ctx, *startCmd)
	assert.NoError(t, err)

	var (
		created  bool
		notFound bool
		maxTries = 3
	)

	for i := 0; i < maxTries; i++ {
		message, err := fis.responder.ReadMessage(ctx)
		assert.NoError(t, err)

		created, err = initCmd.IsInitResponseforCreateTopic(&message)
		assert.NoError(t, err)
		if created {
			break
		}

		notFound, err = initCmd.IsInitResponseforNotFindData(&message)
		assert.NoError(t, err)
		if notFound {
			break
		}
	}

	if initialDataMessage != nil {
		assert.True(t, created)

		brokerAddress := fis.responder.Config().Brokers[0]
		brokers := []string{brokerAddress}
		config := messagequeue.NewMessageQueueConfig(
			brokers, saslUser, saslPassword)
		dialer, err := config.CreateSaslDialer()
		assert.NoError(t, err)

		topics := []string{fis.initTopic}
		found, err := messagequeue.HasTopics(brokerAddress, dialer, topics)
		assert.NoError(t, err)
		assert.True(t, found)

		initReader := newFakeSubscribeReader(
			t, brokerAddress, fis.initTopic, groupId)
		defer initReader.Close()

		message, err := initReader.ReadMessage(ctx)
		assert.NoError(t, err)
		assert.Equal(t, initialDataMessage.Value, message.Value)
	} else {
		assert.True(t, notFound)
	}
}

func (fis *fakeInitSubscriber) handlefinishRequest(
	t *testing.T, ctx context.Context) {
	groupId := fis.responder.Config().GroupID
	initCmd := messagequeue.NewInitCommand(groupId)

	finishCmd, err := initCmd.CreateInitRequestForFinish()
	assert.NoError(t, err)

	err = fis.requester.WriteMessages(ctx, *finishCmd)
	assert.NoError(t, err)
}

func initPublisherRun(
	t *testing.T, ctx context.Context, brokerAddress string,
	topic *messagequeue.MessageQueueTopic,
	initialDataMessage *kafka.Message, wg *sync.WaitGroup,
	initWg *sync.WaitGroup) {
	defer wg.Done()

	publisher := newFakeMessageQueueInitPublisher(t, brokerAddress, topic)
	initWg.Done()

	fakeStorageOps := new(publishfake.FakeInitPublisher)
	if initialDataMessage != nil {
		fakeStorageOps.On("HasInitialData").Return(true, nil)
		messages := []kafka.Message{
			*initialDataMessage,
		}
		fakeStorageOps.On(
			"CreateInitialDataMessages", ctx).Return(messages, nil)
	} else {
		fakeStorageOps.On("HasInitialData").Return(false, nil)
	}

	publisher.AddStorageOps(fakeStorageOps)

	err := publisher.HandleInitQueueMessages(ctx)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)

	err = publisher.CloseMessageQueues()
	assert.NoError(t, err)

	fakeStorageOps.AssertExpectations(t)
}

func fakeInitSubscriberRun(
	t *testing.T, ctx context.Context, brokerAddress string,
	topics *messagequeue.MessageQueueInitTopics, groupId string,
	initialDataMessage *kafka.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	subscriber := newFakeInitSubscriber(
		t, brokerAddress, topics, groupId)
	defer subscriber.Close(t)

	subscriber.handleStartRequest(t, ctx, initialDataMessage)

	subscriber.handlefinishRequest(t, ctx)
}

func TestHandleInitQueueMessagesForNotHaveInitialData(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	topic := messagequeue.NewMessageQueueFileSystemTopic(
		[]string{"test.pub.init1"})

	ctx, cancel := context.WithTimeout(
		context.Background(), util.DefaultMessageQueueTimeout)

	publisherWg := new(sync.WaitGroup)
	publisherInitWg := new(sync.WaitGroup)
	subscriberWg := new(sync.WaitGroup)

	publisherWg.Add(1)
	publisherInitWg.Add(1)
	go initPublisherRun(
		t, ctx, brokerAddress, topic, nil, publisherWg, publisherInitWg)

	initTopics, err := topic.CreateInitTopics()
	assert.NoError(t, err)

	topics := []string{
		initTopics.Request,
		initTopics.Response,
	}

	defer func(broker string, ts []string) {
		brokers := []string{broker}
		config := messagequeue.NewMessageQueueConfig(
			brokers, saslUser, saslPassword)
		dialer, err := config.CreateSaslDialer()
		assert.NoError(t, err)
		err = messagequeue.DeleteTopics(brokerAddress, dialer, ts)
		assert.NoError(t, err)
	}(brokerAddress, topics)

	publisherInitWg.Wait()

	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)

	found, err := messagequeue.HasTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	assert.True(t, found)

	for i := 0; i < numOfSubscribers; i++ {
		groupId := fmt.Sprintf("subscriber-%d", i)
		subscriberWg.Add(1)
		go fakeInitSubscriberRun(
			t, ctx, brokerAddress, initTopics, groupId, nil, subscriberWg)
	}

	subscriberWg.Wait()
	cancel()

	publisherWg.Wait()
}

func TestHandleInitQueueMessagesForHasInitialData(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	topic := messagequeue.NewMessageQueueFileSystemTopic(
		[]string{"test.pub.init2"})

	ctx, cancel := context.WithTimeout(
		context.Background(), util.DefaultMessageQueueTimeout)

	pseudoInitialDataMessage := kafka.Message{
		Value: []byte("initial test data"),
	}

	publisherWg := new(sync.WaitGroup)
	publisherInitWg := new(sync.WaitGroup)
	subscriberWg := new(sync.WaitGroup)

	publisherWg.Add(1)
	publisherInitWg.Add(1)
	go initPublisherRun(
		t, ctx, brokerAddress, topic, &pseudoInitialDataMessage, publisherWg,
		publisherInitWg)

	initTopics, err := topic.CreateInitTopics()
	assert.NoError(t, err)

	topics := []string{
		initTopics.Request,
		initTopics.Response,
	}

	defer func(broker string, ts []string) {
		brokers := []string{broker}
		config := messagequeue.NewMessageQueueConfig(
			brokers, saslUser, saslPassword)
		dialer, err := config.CreateSaslDialer()
		assert.NoError(t, err)
		err = messagequeue.DeleteTopics(brokerAddress, dialer, ts)
		assert.NoError(t, err)
	}(brokerAddress, topics)

	publisherInitWg.Wait()

	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)

	found, err := messagequeue.HasTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	assert.True(t, found)

	for i := 0; i < numOfSubscribers; i++ {
		groupId := fmt.Sprintf("subscriber-%d", i)
		subscriberWg.Add(1)
		go fakeInitSubscriberRun(
			t, ctx, brokerAddress, initTopics, groupId,
			&pseudoInitialDataMessage, subscriberWg)
	}

	subscriberWg.Wait()
	cancel()

	publisherWg.Wait()
}
