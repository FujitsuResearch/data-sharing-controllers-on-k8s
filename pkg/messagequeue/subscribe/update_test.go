// Copyright (c) 2022 Fujitsu Limited

package subscribe

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	corefake "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned/fake"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
	subscribefake "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/subscribe/testing"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type fakeUpdatePublisher struct {
	writer *kafka.Writer
}

func newFakeMessageQueueUpdateSubscriber(
	brokerAddress string, messageQueueTopic *messagequeue.MessageQueueTopic,
	groupId string) (*MessageQueueUpdateSubscriber, error) {
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

	return NewMessageQueueUpdateSubscriber(
		messageQueueDataClient, kubeClient, messageQueueSpec,
		messageQueueTopic, groupId)
}

func TestCloseUpdateMessageQueue(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	topic := messagequeue.NewMessageQueueFileSystemTopic(
		[]string{"test.sub.upadate0"})

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
	subscriber, err := newFakeMessageQueueUpdateSubscriber(
		brokerAddress, topic, groupId)
	assert.NoError(t, err)

	err = subscriber.CloseMessageQueue()
	assert.NoError(t, err)
}

func newFakeUpdatePublisher(
	t *testing.T, brokerAddress string, topic string) *fakeUpdatePublisher {
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

	return &fakeUpdatePublisher{
		writer: kafka.NewWriter(writerConfig),
	}
}

func (fup *fakeUpdatePublisher) Close(t *testing.T) {
	err := fup.writer.Close()
	assert.NoError(t, err)
}

func fakeUpdatePublisherRuns(
	t *testing.T, ctx context.Context, brokerAddress string, topic string,
	messages []kafka.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	publisher := newFakeUpdatePublisher(t, brokerAddress, topic)
	defer publisher.Close(t)

	err := publisher.writer.WriteMessages(ctx, messages...)
	assert.NoError(t, err)
}

func updateSubscriberExecutesFirstReadMessage(
	t *testing.T, ctx context.Context, brokerAddress string,
	topic *messagequeue.MessageQueueTopic, groupId string,
	messages []kafka.Message, wg *sync.WaitGroup) {
	subscriber, err := newFakeMessageQueueUpdateSubscriber(
		brokerAddress, topic, groupId)
	assert.NoError(t, err)

	expectedStopped := false
	if messages == nil {
		expectedStopped = true
	}
	fakeStorageOps := new(subscribefake.FakeUpdateSubscriber)
	fakeStorageOps.On(
		"HandleUpdateMessage",
		mock.MatchedBy(
			func(message *kafka.Message) bool {
				return bytes.Equal(message.Value, message.Value)
			})).Return(expectedStopped, nil).Once()

	subscriber.AddStorageOps(fakeStorageOps)

	offsetTime := time.Now()
	stopped, err := subscriber.HandleFirstQueueMessage(
		ctx, offsetTime)
	assert.NoError(t, err)
	assert.Equal(t, expectedStopped, stopped)

	if messages != nil {
		fakeStorageOps.AssertExpectations(t)
	} else {
		fakeStorageOps.AssertNumberOfCalls(t, "HandleUpdateMessage", 0)
	}

	err = subscriber.CloseMessageQueue()
	assert.NoError(t, err)

	wg.Done()
}

func updateSubscriberExecutesReadMessages(
	t *testing.T, ctx context.Context, brokerAddress string,
	topic *messagequeue.MessageQueueTopic, groupId string,
	messages []kafka.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	subscriber, err := newFakeMessageQueueUpdateSubscriber(
		brokerAddress, topic, groupId)
	assert.NoError(t, err)

	numOfMessages := len(messages)
	fakeStorageOps := new(subscribefake.FakeUpdateSubscriber)

	if numOfMessages > 1 {
		fakeStorageOps.On(
			"HandleUpdateMessage",
			mock.MatchedBy(
				func(message *kafka.Message) bool {
					return bytes.Equal(message.Value, message.Value)
				})).Return(false, nil).Times(numOfMessages - 1)
	}

	fakeStorageOps.On(
		"HandleUpdateMessage",
		mock.MatchedBy(
			func(message *kafka.Message) bool {
				return bytes.Equal(message.Value, message.Value)
			})).Return(true, nil).Once()

	subscriber.AddStorageOps(fakeStorageOps)

	for {
		stopped, err := subscriber.HandleQueueMessage(ctx)
		assert.NoError(t, err)

		if stopped {
			break
		}
	}

	fakeStorageOps.AssertExpectations(t)

	err = subscriber.CloseMessageQueue()
	assert.NoError(t, err)
}

func TestHandleFirstQueueMessage(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)

	topic := messagequeue.NewMessageQueueFileSystemTopic(
		[]string{"test.sub.first"})
	updateTopic, err := topic.CreateUpdateTopic()
	assert.NoError(t, err)

	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)
	topics := []string{updateTopic}
	err = messagequeue.CreateTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	defer func(broker string, d *kafka.Dialer, ts []string) {
		err = messagequeue.DeleteTopics(broker, d, ts)
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
	messages := []kafka.Message{
		{
			Time:  time.Now().Add(1 * time.Minute),
			Value: []byte("test update data"),
		},
	}

	go fakeUpdatePublisherRuns(
		t, ctx, brokerAddress, updateTopic, messages, publisherWg)

	for i := 0; i < numOfSubscribers; i++ {
		groupId := fmt.Sprintf("subcriber-%d", i)
		subscriberWg.Add(1)
		go updateSubscriberExecutesFirstReadMessage(
			t, ctx, brokerAddress, topic, groupId, messages, subscriberWg)
	}

	subscriberWg.Wait()
	cancel()

	publisherWg.Wait()
}

func TestHandleUpdateQueueMessagesForDataNotFound(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)

	topic := messagequeue.NewMessageQueueFileSystemTopic(
		[]string{"test.sub.not.found"})
	updateTopic, err := topic.CreateUpdateTopic()
	assert.NoError(t, err)

	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)
	topics := []string{updateTopic}
	err = messagequeue.CreateTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	defer func(broker string, d *kafka.Dialer, ts []string) {
		err = messagequeue.DeleteTopics(broker, d, ts)
		assert.NoError(t, err)
	}(brokerAddress, dialer, topics)

	found, err := messagequeue.HasTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	assert.True(t, found)

	ctx, cancel := context.WithCancel(context.Background())

	publisherWg := new(sync.WaitGroup)
	subscriberWg := new(sync.WaitGroup)

	publisherWg.Add(1)
	messages := []kafka.Message{
		{
			Time:  time.Now().Add(-1 * time.Minute),
			Value: []byte("test update data"),
		},
	}

	go fakeUpdatePublisherRuns(
		t, ctx, brokerAddress, updateTopic, messages, publisherWg)

	for i := 0; i < numOfSubscribers; i++ {
		groupId := fmt.Sprintf("subcriber-%d", i)
		subscriberWg.Add(1)
		go updateSubscriberExecutesFirstReadMessage(
			t, ctx, brokerAddress, topic, groupId, nil, subscriberWg)
	}

	time.Sleep(2 * time.Second)
	cancel()
	subscriberWg.Wait()

	publisherWg.Wait()
}

func TestHandleUpdateQueueMessages(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)

	topic := messagequeue.NewMessageQueueFileSystemTopic(
		[]string{"test.sub.update1"})
	updateTopic, err := topic.CreateUpdateTopic()
	assert.NoError(t, err)

	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)
	topics := []string{updateTopic}
	err = messagequeue.CreateTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	defer func(broker string, d *kafka.Dialer, ts []string) {
		err = messagequeue.DeleteTopics(broker, d, ts)
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
	messages := []kafka.Message{
		{
			Value: []byte("fist test update data"),
		},
		{
			Value: []byte("second test update data"),
		},
	}

	go fakeUpdatePublisherRuns(
		t, ctx, brokerAddress, updateTopic, messages, publisherWg)

	for i := 0; i < numOfSubscribers; i++ {
		groupId := fmt.Sprintf("subcriber-%d", i)
		subscriberWg.Add(1)
		go updateSubscriberExecutesReadMessages(
			t, ctx, brokerAddress, topic, groupId, messages, subscriberWg)
	}

	subscriberWg.Wait()
	cancel()

	publisherWg.Wait()
}
