// Copyright (c) 2022 Fujitsu Limited

package app

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/volume-controller/app/options"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/controller/volume"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

func run(usageOptions *options.VolumeControllerOptions) error {
	consumerConfig, err := clientcmd.BuildConfigFromFlags(
		usageOptions.ConsumerApiServerUrl, usageOptions.ConsumerKubeConfig)
	if err != nil {
		return fmt.Errorf(
			"Error building kubeconfig for a data consumer: %v", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(consumerConfig)
	if err != nil {
		return fmt.Errorf(
			"Error building kubernetes clientset: %v", err.Error())
	}

	stopCh := util.SetupSignalHandler()
	ucontroller, err := volume.NewVolumeController(
		usageOptions.FuseConfig, usageOptions.VolumeConfig,
		usageOptions.ContainerRuntimeConfig, kubeClient, stopCh)
	if err != nil {
		return err
	}

	ucontroller.Run(usageOptions.GrpcServerEndpoint)

	return nil
}

func NewVolumeControllerCommand() *cobra.Command {
	opts := options.NewVolumeControllerOptions()

	cmd := &cobra.Command{
		Use: "shadowy-usage-controller",
		Long: `the shadowy usage controller prevents untrusted programs from ` +
			`manipulating provided data.`,
		Run: func(cmd *cobra.Command, args []string) {
			util.PrintFlags(cmd.Flags())

			if err := options.ValidateVolumeControllerFlags(opts); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}

			if err := run(opts); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}
		},
	}

	opts.AddFlags(cmd.Flags())

	return cmd
}
