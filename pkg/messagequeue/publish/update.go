// Copyright (c) 2022 Fujitsu Limited

package publish

import (
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/gzip"
	"github.com/segmentio/kafka-go/lz4"
	"github.com/segmentio/kafka-go/snappy"
	"github.com/segmentio/kafka-go/zstd"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

type MessageQueueUpdatePublisher struct {
	updateWriter *kafka.Writer
	maxBatchSize int
}

func getCompressionCodec(
	compressionCodec string) (kafka.CompressionCodec, error) {
	switch compressionCodec {
	case CompressionCodecNone:
		return nil, nil
	case CompressionCodecGzip:
		return gzip.NewCompressionCodec(), nil
	case CompressionCodecSnappy:
		return snappy.NewCompressionCodec(), nil
	case CompressionCodecLz4:
		return lz4.NewCompressionCodec(), nil
	case CompressionCodecZstd:
		return zstd.NewCompressionCodec(), nil
	default:
		return nil, fmt.Errorf(
			"Not support such a compression codec: %q", compressionCodec)
	}
}

func NewMessageQueueUpdatePublisher(
	messageQueueConfig *messagequeue.MessageQueueConfig, topic string,
	compressionCodecOption string, maxBatchBytes int) (
	*MessageQueueUpdatePublisher, error) {
	dialer, err := messageQueueConfig.CreateSaslDialer()
	if err != nil {
		return nil, err
	}

	updateWriterConfig := kafka.WriterConfig{
		Brokers: messageQueueConfig.GetBrokers(),
		Topic:   topic,
		Dialer:  dialer,
	}

	if maxBatchBytes != 0 {
		updateWriterConfig.BatchBytes = maxBatchBytes
	} else {
		updateWriterConfig.BatchBytes = messagequeue.DefaultMaxBatchBytes
	}

	compressionCodec, err := getCompressionCodec(compressionCodecOption)
	if err != nil {
		return nil, err
	}

	if compressionCodec != nil {
		updateWriterConfig.CompressionCodec = compressionCodec
	}

	brokerAddress := messageQueueConfig.GetBrokers()[0]
	topics := []string{
		topic,
	}
	err = messagequeue.CreateTopics(brokerAddress, dialer, topics)
	if err != nil {
		return nil, err
	}

	return &MessageQueueUpdatePublisher{
		// [TODO] NewWriter() is deprecated
		updateWriter: kafka.NewWriter(updateWriterConfig),
		maxBatchSize: updateWriterConfig.BatchBytes,
	}, nil
}

func (mqup *MessageQueueUpdatePublisher) CloseMessageQueue() error {
	err := mqup.updateWriter.Close()
	if err != nil {
		return fmt.Errorf(
			"Failed to close the 'update' publisher: %v", err.Error())
	}

	return nil
}

func (mqup *MessageQueueUpdatePublisher) GetMaxBatchSize() int {
	return mqup.maxBatchSize
}

func (mqup *MessageQueueUpdatePublisher) WriteQueueMessages(
	messages []kafka.Message) error {
	ctx, cancel := util.GetTimeoutContext(util.DefaultMessageQueueTimeout)
	defer cancel()

	return mqup.updateWriter.WriteMessages(ctx, messages...)
}
