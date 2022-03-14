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

type startOptions struct {
	PodNamespace                   string
	PodName                        string
	DataContainerName              string
	PersistentVolumeClaimNamespace string
	PersistentVolumeClaimName      string
	AllowedExecutablesFilePath     string
	MessageQueueFilePath           string
}

func newStartOptions() *startOptions {
	return &startOptions{
		PodNamespace: metav1.NamespaceDefault,
	}
}

func addStartFlags(cmd *cobra.Command, so *startOptions) {
	cmd.Flags().StringVar(
		&so.PodNamespace, "pod-namespace", so.PodNamespace,
		"Namespace of the volume-controlled pod.")

	cmd.Flags().StringVar(
		&so.PodName, "pod-name", so.PodName,
		"Names of the volume-controlled pod.")
	cmd.MarkFlagRequired("pod-name")

	cmd.Flags().StringVar(
		&so.DataContainerName, "data-container-name", so.DataContainerName,
		"Name of the data container.")
	cmd.MarkFlagRequired("data-container-name")

	cmd.Flags().StringVar(
		&so.PersistentVolumeClaimNamespace, "pvc-namespace",
		so.PersistentVolumeClaimNamespace,
		"Namespace of the volume-controlled persistent volume claim.  "+
			"It can be ommitted if the same pod one")

	cmd.Flags().StringVar(
		&so.PersistentVolumeClaimName, "pvc-name",
		so.PersistentVolumeClaimName,
		"Name of the volume-controlled persistent volume claim.")
	cmd.MarkFlagRequired("pvc-name")

	cmd.Flags().StringVar(
		&so.AllowedExecutablesFilePath, "allowed-executables-file-path",
		so.AllowedExecutablesFilePath,
		"Path to the file including executable checksums per 'datalifetime'.")

	cmd.Flags().StringVar(
		&so.MessageQueueFilePath, "message-queue-file-path",
		so.MessageQueueFilePath,
		"Path to the file including the configuration of a message queue "+
			"publisher.")
}

func getAllowedExecutablesConfigurationFromJsonFile(
	executablesFile string) (map[string]*api.Executable, error) {
	executablesBytes, err := ioutil.ReadFile(executablesFile)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not read %s: %v", executablesFile, err.Error())
	}

	executables := map[string]*api.Executable{}
	err = json.Unmarshal(executablesBytes, &executables)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not convert %q (the JSON format) into "+
				"'LifetimeExecutable' struct",
			executablesBytes)
	}

	return executables, nil
}

func getMessageQueueUpdatePublisherConfigurationFromJsonFile(
	messageQueueFile string) (*api.MessageQueue, error) {
	messageQueueBytes, err := ioutil.ReadFile(messageQueueFile)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not read %s: %v", messageQueueFile, err.Error())
	}

	messageQueue := api.MessageQueue{}
	err = json.Unmarshal(messageQueueBytes, &messageQueue)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not convert %q (the JSON format) into "+
				"'MessageQueue' struct",
			messageQueueBytes)
	}

	return &messageQueue, nil
}

func newStartCommand() *cobra.Command {
	opts := newStartOptions()

	startCmd := &cobra.Command{
		Use: "start",
		Long: `the shadowy volumectl sends the 'start' request to ` +
			`the shadowy volume controller in order to set up information ` +
			`on target containers and binaries`,
		Run: func(cmd *cobra.Command, args []string) {
			util.PrintFlags(cmd.Flags())

			volumeControllerEndpoint := getVolumeControllerEndpoint(cmd, args)

			pvcNamespace := opts.PersistentVolumeClaimNamespace
			if pvcNamespace == "" {
				pvcNamespace = opts.PodNamespace
			}
			pvcKey := util.ConcatenateNamespaceAndName(
				pvcNamespace, opts.PersistentVolumeClaimName)

			executables, err := getAllowedExecutablesConfigurationFromJsonFile(
				opts.AllowedExecutablesFilePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}

			messageQueue, err := getMessageQueueUpdatePublisherConfigurationFromJsonFile(
				opts.AllowedExecutablesFilePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}
			if messageQueue.MaxBatchBytes == "" {
				messageQueue.MaxBatchBytes = "0"
			}

			ucClient := volumecontrol.NewClient(volumeControllerEndpoint)
			mountPoints, err := ucClient.Start(
				opts.PodNamespace, opts.PodName, pvcKey, executables,
				opts.DataContainerName, messageQueue)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err.Error())
				os.Exit(1)
			}

			fmt.Fprintf(
				os.Stdout,
				"Target fuse mount points: %+v", mountPoints)
		},
	}

	addStartFlags(startCmd, opts)

	return startCmd
}
