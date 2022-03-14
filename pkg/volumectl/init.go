// Copyright (c) 2022 Fujitsu Limited

package volumectl

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/grpc/volumecontrol"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

type initOptions struct {
	volumeSourcePath                 string
	externalAllowedExecutables       string
	localFuseMountsHostRootDirectory string
	csiVolumeHandle                  string
	disableUsageControl              bool
}

func newInitOptions() *initOptions {
	return &initOptions{}
}

func getExternalAllowedExecutables(
	allowedExecutablesString string) (
	[]*api.ExternalAllowedExecutable, error) {
	allowedExecutablesBytes := []byte(allowedExecutablesString)
	allowedExecutables := []*api.ExternalAllowedExecutable{}
	err := json.Unmarshal(allowedExecutablesBytes, &allowedExecutables)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not convert %q (the JSON format) into "+
				"'AllowedExecutable' struct",
			allowedExecutablesBytes)
	}

	return allowedExecutables, nil
}

func addInitFlags(cmd *cobra.Command, io *initOptions) {
	cmd.Flags().StringVar(
		&io.volumeSourcePath, "volume-source-path", io.volumeSourcePath,
		"Path to the controlled volume.  "+
			"This path is used as a fuse-mount source path.")
	cmd.MarkFlagRequired("volume-source-path")

	cmd.Flags().StringVar(
		&io.externalAllowedExecutables, "external-allowed-executables",
		io.externalAllowedExecutables,
		"External executables allowed to access for the fuse-mount point "+
			"in the JSON format.")

	cmd.Flags().StringVar(
		&io.localFuseMountsHostRootDirectory,
		"fuse-mounts-host-root-directory", io.localFuseMountsHostRootDirectory,
		"Name of the fuse-mount points root directory on the host.  "+
			"This flag is required when the local persistent volume is used.")

	cmd.Flags().StringVar(
		&io.csiVolumeHandle, "volume-handle", io.csiVolumeHandle,
		"Name of the CSI volume handle.  "+
			"This flag is required when the CSI volume is used.")

	cmd.Flags().BoolVar(
		&io.disableUsageControl, "disable-usage-control", io.disableUsageControl,
		"Disable the usage control for file system data.")
}

func newInitCommand() *cobra.Command {
	opts := newInitOptions()

	initCmd := &cobra.Command{
		Use: "init",
		Long: `the shadowy volumectl sends the 'init' request to ` +
			`the shadowy volume controller in order to fuse-mount a volume`,
		Run: func(cmd *cobra.Command, args []string) {
			util.PrintFlags(cmd.Flags())

			volumeControllerEndpoint := getVolumeControllerEndpoint(cmd, args)

			allowedExecutables, err := getExternalAllowedExecutables(
				opts.externalAllowedExecutables)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}

			ucClient := volumecontrol.NewClient(volumeControllerEndpoint)
			fuseMountPointPath, err := ucClient.Initialize(
				opts.volumeSourcePath, allowedExecutables,
				opts.csiVolumeHandle, opts.localFuseMountsHostRootDirectory,
				opts.disableUsageControl)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}

			fmt.Fprintf(
				os.Stdout,
				"Path to the fuse mount point: %s\n", fuseMountPointPath)
		},
	}

	addInitFlags(initCmd, opts)

	return initCmd
}
