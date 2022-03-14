// Copyright (c) 2022 Fujitsu Limited

package publish

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"

	"github.com/stretchr/testify/assert"
)

func newMessageQueueUpdatePublisher(
	t *testing.T, brokerAddress string,
	topic string) *MessageQueueUpdatePublisher {
	brokers := []string{
		brokerAddress,
	}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)

	compressionCodec := CompressionCodecSnappy

	publisher, err := NewMessageQueueUpdatePublisher(
		config, topic, compressionCodec, 0)
	assert.NoError(t, err)

	return publisher
}

func TestCloseUpdateMessageQueue(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)

	topic := messagequeue.NewMessageQueueFileSystemTopic(
		[]string{"test.pub.update0"})
	updateTopic, err := topic.CreateUpdateTopic()
	assert.NoError(t, err)

	publisher := newMessageQueueUpdatePublisher(t, brokerAddress, updateTopic)

	maxBatchSize := publisher.GetMaxBatchSize()

	expectedMaxBatchSize := int(messagequeue.DefaultMaxBatchBytes)
	assert.Equal(t, expectedMaxBatchSize, maxBatchSize)

	err = publisher.CloseMessageQueue()
	assert.NoError(t, err)
}

func newFakeUpdateSubscriber(
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

func updatePublisherRun(
	t *testing.T, ctx context.Context, brokerAddress string, topic string,
	messages []kafka.Message, wg *sync.WaitGroup, initWg *sync.WaitGroup) {
	defer wg.Done()

	publisher := newMessageQueueUpdatePublisher(t, brokerAddress, topic)
	initWg.Done()

	err := publisher.WriteQueueMessages(messages)
	assert.NoError(t, err)

	err = publisher.CloseMessageQueue()
	assert.NoError(t, err)
}

func fakeUpdateSubscriberRun(
	t *testing.T, ctx context.Context, brokerAddress string, topic string,
	groupId string, updateMessage kafka.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	reader := newFakeUpdateSubscriber(
		t, brokerAddress, topic, groupId)
	defer reader.Close()

	message, err := reader.ReadMessage(ctx)
	assert.NoError(t, err)
	assert.Equal(t, updateMessage.Value, message.Value)
}

func TestWriteUpdateQueueMessages(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)

	topic := messagequeue.NewMessageQueueFileSystemTopic(
		[]string{"test.pub.update1"})
	updateTopic, err := topic.CreateUpdateTopic()
	assert.NoError(t, err)

	pseudoUpdateDataMessage := kafka.Message{
		Value: []byte("updated test data"),
	}
	messages := []kafka.Message{
		pseudoUpdateDataMessage,
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), util.DefaultMessageQueueTimeout)
	defer cancel()

	publisherWg := new(sync.WaitGroup)
	publisherInitWg := new(sync.WaitGroup)
	subscriberWg := new(sync.WaitGroup)

	publisherWg.Add(1)
	publisherInitWg.Add(1)
	go updatePublisherRun(
		t, ctx, brokerAddress, updateTopic, messages, publisherWg,
		publisherInitWg)

	publisherInitWg.Wait()

	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)

	topics := []string{updateTopic}
	found, err := messagequeue.HasTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	assert.True(t, found)

	for i := 0; i < numOfSubscribers; i++ {
		groupId := fmt.Sprintf("subscriber-%d", i)
		subscriberWg.Add(1)
		go fakeUpdateSubscriberRun(
			t, ctx, brokerAddress, updateTopic, groupId,
			pseudoUpdateDataMessage, subscriberWg)
	}

	subscriberWg.Wait()
	cancel()

	publisherWg.Wait()

	err = messagequeue.DeleteTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
}
