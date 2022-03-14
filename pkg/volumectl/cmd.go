// Copyright (c) 2022 Fujitsu Limited

package volumectl

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	volumectloptions "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/volume-controller/app/options"
)

type volumeCtlOptions struct {
	volumeControllerEndpoint string
}

func newVolumeCtlOptions() *volumeCtlOptions {
	return &volumeCtlOptions{
		volumeControllerEndpoint: volumectloptions.
			DefaultVolumeControlServerEndpoint,
	}
}

func runHelp(cmd *cobra.Command, args []string) {
	cmd.Help()
}

func addVolumeCtlFlags(cmd *cobra.Command, uco *volumeCtlOptions) {
	cmd.PersistentFlags().StringVar(
		&uco.volumeControllerEndpoint, "volume-controller-endpoint",
		uco.volumeControllerEndpoint,
		"Endpoint of the volume control GRPC server.")
}

func getVolumeControllerEndpoint(cmd *cobra.Command, args []string) string {
	endpoint := cmd.Flag("volume-controller-endpoint").Value.String()
	if endpoint == "" {
		fmt.Fprintln(
			os.Stderr, "Error: 'volume-controller-endpoint' is required")
		runHelp(cmd, args)
		os.Exit(1)
	}

	return endpoint
}

func NewVolumeCtlCommand() *cobra.Command {
	opts := newVolumeCtlOptions()

	cmd := &cobra.Command{
		Use:  "shadowy-volumectl",
		Long: `the shadowy volumectl sends requests to the shadowy volume controller.`,
		Run:  runHelp,
	}

	addVolumeCtlFlags(cmd, opts)

	cmd.AddCommand(newInitCommand())
	cmd.AddCommand(newStartCommand())
	cmd.AddCommand(newStopCommand())
	cmd.AddCommand(newFinalCommand())

	cmd.AddCommand(newChecksumCommand())

	return cmd
}
