// Copyright (c) 2022 Fujitsu Limited

package app

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/azure"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/lifetimer/app/options"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/controller/lifetimes"
	datastoreclient "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore/client"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

func run(lifetimesOptions *options.LifetimerOptions) error {
	consumerConfig, err := clientcmd.BuildConfigFromFlags(
		lifetimesOptions.ConsumerApiServerUrl,
		lifetimesOptions.ConsumerKubeConfig)
	if err != nil {
		return fmt.Errorf(
			"Error building kubeconfig for a data consumer: %v", err.Error())
	}

	stopCh := util.SetupSignalHandler()

	kubeClient, err := kubernetes.NewForConfig(consumerConfig)
	if err != nil {
		return fmt.Errorf(
			"Error building kubernetes clientset: %v", err.Error())
	}

	dataClient, err := coreclientset.NewForConfig(consumerConfig)
	if err != nil {
		return fmt.Errorf(
			"Error building data-core clientset: %v", err.Error())
	}

	dataStoreClient, err := datastoreclient.NewValidationDataStoreClient(
		lifetimesOptions.DataStore)
	if err != nil {
		return fmt.Errorf(
			"Error creating datasotre-client: %v", err.Error())
	}

	lcontroller, err := lifetimes.NewLifetimeController(
		kubeClient, dataClient, lifetimesOptions.SyncFrequency.Duration,
		lifetimesOptions.VolumeController,
		lifetimesOptions.MessageQueuePublisher, dataStoreClient, stopCh)
	if err != nil {
		return fmt.Errorf(
			"Error creating lifetime-controller: %v", err.Error())
	}

	return lcontroller.Run(lifetimesOptions.Workers)
}

func NewLifeTimerCommand() *cobra.Command {
	opts := options.NewLifetimerOptions()

	cmd := &cobra.Command{
		Use: "shadowy-lifetimer",
		Long: `the shadowy lifetimer controls updates for target files and ` +
			`their expiration.`,
		Run: func(cmd *cobra.Command, args []string) {
			util.PrintFlags(cmd.Flags())

			if err := run(opts); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}
		},
	}

	opts.AddFlags(cmd.Flags())

	return cmd
}
