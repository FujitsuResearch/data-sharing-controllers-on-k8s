// Copyright (c) 2022 Fujitsu Limited

package volumectl

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/grpc/volumecontrol"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

type finalOptions struct {
	fuseMountPointPath string
	force              bool
}

func newFinalOptions() *finalOptions {
	return &finalOptions{}
}

func addFinalFlags(cmd *cobra.Command, fo *finalOptions) {
	cmd.Flags().StringVar(
		&fo.fuseMountPointPath, "fuse-mount-point-path", fo.fuseMountPointPath,
		"Path to the fuse mount point.")
	cmd.MarkFlagRequired("fuse-mount-point-path")

	cmd.Flags().BoolVar(
		&fo.force, "force", fo.force,
		"Fuse-unmount even if the volume controller has.information on "+
			"target containers and binaries")
}

func newFinalCommand() *cobra.Command {
	opts := newFinalOptions()

	subCmd := &cobra.Command{
		Use: "final",
		Long: `the shadowy volumectl sends the 'final' request to ` +
			`the shadowy volume controller in order to fuse-unmount a volume`,
		Run: func(cmd *cobra.Command, args []string) {
			util.PrintFlags(cmd.Flags())

			volumeControllerEndpoint := getVolumeControllerEndpoint(cmd, args)

			ucClient := volumecontrol.NewClient(volumeControllerEndpoint)
			err := ucClient.Finalize(
				opts.fuseMountPointPath, opts.force)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}

			fmt.Fprintf(
				os.Stdout, "Deleted the mount point: %s\n",
				opts.fuseMountPointPath)
		},
	}

	addFinalFlags(subCmd, opts)

	return subCmd
}
