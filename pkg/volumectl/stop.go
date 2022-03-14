// Copyright (c) 2022 Fujitsu Limited

package volumectl

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/grpc/volumecontrol"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

type stopOptions struct {
	PodNamespace               string
	PodName                    string
	AllowedExecutablesFilePath string
	MountPoint                 string
}

func newStopOptions() *stopOptions {
	return &stopOptions{
		PodNamespace: metav1.NamespaceDefault,
	}
}

func addStopFlags(cmd *cobra.Command, so *stopOptions) {
	cmd.Flags().StringVar(
		&so.PodNamespace, "pod-namespace", so.PodNamespace,
		"Namespace of the volume-controlled pod.")

	cmd.Flags().StringVar(
		&so.PodName, "pod-name", so.PodName,
		"Names of the volume-controlled pod.")
	cmd.MarkFlagRequired("pod-name")

	cmd.Flags().StringVar(
		&so.AllowedExecutablesFilePath, "allowed-executables-file-path",
		so.AllowedExecutablesFilePath,
		"Path to the file including executable paths per 'datalifetime'.")
	cmd.MarkFlagRequired("executable-checksums-file-path")

	cmd.Flags().StringVar(
		&so.MountPoint, "mount-point", so.MountPoint,
		"FUSE mount point, which is required for stopping "+
			"the message queue publisher.")

}

func getAllowedExecutablePathsConfigurationFromJsonFile(
	executablesFile string) (map[string]*api.ExecutablePaths, error) {
	executablesBytes, err := ioutil.ReadFile(executablesFile)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not read %s: %v", executablesFile, err.Error())
	}

	executables := map[string]*api.ExecutablePaths{}
	err = json.Unmarshal(executablesBytes, &executables)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not convert %q (the JSON format) into "+
				"'ExecutablePath' struct", executablesBytes)
	}

	return executables, nil
}

func newStopCommand() *cobra.Command {
	opts := newStopOptions()

	stopCmd := &cobra.Command{
		Use: "stop",
		Long: `the shadowy volumectl sends the 'stop' request to ` +
			`the shadowy volume controller in order to clear information ` +
			`on target containers and binaries`,
		Run: func(cmd *cobra.Command, args []string) {
			util.PrintFlags(cmd.Flags())

			volumeControllerEndpoint := getVolumeControllerEndpoint(cmd, args)

			ucClient := volumecontrol.NewClient(volumeControllerEndpoint)
			executablePaths, err := getAllowedExecutablePathsConfigurationFromJsonFile(
				opts.AllowedExecutablesFilePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}

			err = ucClient.Stop(
				opts.PodNamespace, opts.PodName, executablePaths,
				opts.MountPoint)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}
		},
	}

	addStopFlags(stopCmd, opts)

	return stopCmd
}
