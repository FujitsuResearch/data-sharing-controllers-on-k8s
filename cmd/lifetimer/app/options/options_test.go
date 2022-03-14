// Copyright (c) 2022 Fujitsu Limited

package options

import (
	"testing"
	"time"

	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	volumectloptions "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/volume-controller/app/options"
	datastoreconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore/config"
	publishconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	lifetimesOptions := NewLifetimerOptions()
	lifetimesOptions.AddFlags(flagSet)

	err := flagSet.Parse(nil)
	assert.NoError(t, err)

	expectedLifetimesOptions := &LifetimerOptions{
		SyncFrequency: metav1.Duration{
			Duration: defaultSyncFrequencyInSeconds * time.Second,
		},
		Workers: defaultWorkers,

		VolumeController: &VolumeControllerOptions{
			Endpoint: volumectloptions.DefaultVolumeControlServerEndpoint,
		},
		DataStore: &datastoreconfig.DataStoreOptions{
			Name: datastoreconfig.DataStoreRedis,
			Addr: ":6379",
		},
		MessageQueuePublisher: &publishconfig.MessageQueuePublisherOptions{
			CompressionCodec: publishconfig.CompressionCodecNone,
		},
	}

	assert.Equal(t, expectedLifetimesOptions, lifetimesOptions)
}

func TestAddFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	lifetimesOptions := NewLifetimerOptions()
	lifetimesOptions.AddFlags(flagSet)

	args := []string{
		"--consumer-kubeconfig=/tmp/consumer_kubeconfig",
		"--consumer-api-server-url=http://127.0.0.1",
		"--sync-frequency=30s",
		"--workers=3",

		"--volume-controller-endpoint=http://127.0.0.3:8080",
		"--disable-usage-control",

		"--datastore-name=datastore",
		"--datastore-addr=localhost:7000",

		"--datastore-addr=localhost:7000",

		"--mq-publisher-compression-codec=gzip",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	expectedLifetimesOptions := &LifetimerOptions{
		ConsumerKubeConfig:   "/tmp/consumer_kubeconfig",
		ConsumerApiServerUrl: "http://127.0.0.1",
		SyncFrequency:        metav1.Duration{Duration: 30 * time.Second},
		Workers:              3,

		VolumeController: &VolumeControllerOptions{
			Endpoint:            "http://127.0.0.3:8080",
			DisableUsageControl: true,
		},
		DataStore: &datastoreconfig.DataStoreOptions{
			Name: "datastore",
			Addr: "localhost:7000",
		},
		MessageQueuePublisher: &publishconfig.MessageQueuePublisherOptions{
			CompressionCodec: publishconfig.CompressionCodecGzip,
		},
	}

	assert.Equal(t, expectedLifetimesOptions, lifetimesOptions)
}
