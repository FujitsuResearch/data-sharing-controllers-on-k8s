// Copyright (c) 2022 Fujitsu Limited

package publish

import (
	"fmt"

	"github.com/spf13/pflag"
)

const (
	CompressionCodecNone   = "none"
	CompressionCodecGzip   = "gzip"
	CompressionCodecSnappy = "snappy"
	CompressionCodecLz4    = "lz4"
	CompressionCodecZstd   = "zstd"

	defaultCompressionCodec = CompressionCodecNone
)

type MessageQueuePublisherOptions struct {
	CompressionCodec string
}

func NewMessageQueuePublisherOptions() *MessageQueuePublisherOptions {
	return &MessageQueuePublisherOptions{
		CompressionCodec: defaultCompressionCodec,
	}
}

func (mqpo *MessageQueuePublisherOptions) AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&mqpo.CompressionCodec, "mq-publisher-compression-codec",
		mqpo.CompressionCodec, fmt.Sprintf(
			"Compression codec.Possible values: 'none', 'gzip', 'snappy', "+
				"'lz4', 'zstd'. The default value is %q",
			defaultCompressionCodec))
}
