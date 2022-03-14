// Copyright (c) 2022 Fujitsu Limited

package publish

import (
	"testing"

	"github.com/spf13/pflag"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	publiserOptions := NewMessageQueuePublisherOptions()
	publiserOptions.AddFlags(flagSet)

	err := flagSet.Parse(nil)
	assert.NoError(t, err)

	expectedPublisherOptions := &MessageQueuePublisherOptions{
		CompressionCodec: CompressionCodecNone,
	}
	assert.Equal(t, expectedPublisherOptions, publiserOptions)
}

func TestAddMessageQueuePublisherFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	publiserOptions := NewMessageQueuePublisherOptions()
	publiserOptions.AddFlags(flagSet)

	args := []string{
		"--mq-publisher-compression-codec=snappy",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	expectedPublisherOptions := &MessageQueuePublisherOptions{
		CompressionCodec: CompressionCodecSnappy,
	}
	assert.Equal(t, expectedPublisherOptions, publiserOptions)
}
