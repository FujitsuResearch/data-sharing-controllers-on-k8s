// Copyright (c) 2022 Fujitsu Limited

package options

import (
	"time"

	"github.com/spf13/pflag"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	datastoreconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore/config"
	publishconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
)

const (
	defaultSyncFrequencyInSeconds = 60
	defaultWorkers                = 2
)

type LifetimerOptions struct {
	ConsumerKubeConfig   string
	ConsumerApiServerUrl string
	SyncFrequency        metav1.Duration
	Workers              int

	VolumeController      *VolumeControllerOptions
	DataStore             *datastoreconfig.DataStoreOptions
	MessageQueuePublisher *publishconfig.MessageQueuePublisherOptions
}

func NewLifetimerOptions() *LifetimerOptions {
	return &LifetimerOptions{
		SyncFrequency: metav1.Duration{
			Duration: defaultSyncFrequencyInSeconds * time.Second,
		},
		Workers: defaultWorkers,

		VolumeController:      newVolumeControllerOptions(),
		DataStore:             datastoreconfig.NewDataStoreOptions(),
		MessageQueuePublisher: publishconfig.NewMessageQueuePublisherOptions(),
	}
}

func (lo *LifetimerOptions) AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&lo.ConsumerKubeConfig, "consumer-kubeconfig", lo.ConsumerKubeConfig,
		"Path to the kubeconfig file with authorization and master location "+
			"information.")
	flagSet.StringVar(
		&lo.ConsumerApiServerUrl, "consumer-api-server-url",
		lo.ConsumerApiServerUrl,
		"URL of the Kubernetes API server for the data consumer.")

	flagSet.DurationVar(
		&lo.SyncFrequency.Duration, "sync-frequency",
		lo.SyncFrequency.Duration,
		"Period between target file synchronization.")

	flagSet.IntVar(
		&lo.Workers, "workers", lo.Workers, "The number of worker threads.")

	lo.VolumeController.addFlags(flagSet)

	lo.DataStore.AddFlags(flagSet)

	lo.MessageQueuePublisher.AddFlags(flagSet)
}
