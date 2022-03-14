// Copyright (c) 2022 Fujitsu Limited

package volumectl

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vctloptions "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/volume-controller/app/options"
	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"

	"github.com/stretchr/testify/assert"
)

func TestGetAllowedExecutablesConfigurationFromJsonFile(t *testing.T) {
	executablesFile := "testing/fixtures/allowed_executables.json"
	executables, err := getAllowedExecutablesConfigurationFromJsonFile(
		executablesFile)

	assert.NoError(t, err)

	expectedExecutables := map[string]*api.Executable{
		"/usr/bin/x": {
			Writable: false,
		},
		"/usr/local/bin/y": {
			Checksum: []byte{78, 90},
			Writable: true,
		},
	}

	assert.Equal(t, expectedExecutables, executables)
}

func TestGetMessageQueueConfigurationFromJsonFile(t *testing.T) {
	messageQueueFile := "testing/fixtures/message_queue.json"
	messageQueue, err := getMessageQueueUpdatePublisherConfigurationFromJsonFile(
		messageQueueFile)

	assert.NoError(t, err)

	expectedMessageQueue := &api.MessageQueue{
		Brokers:          []string{"127.0.0.1:9093"},
		User:             "user",
		Password:         "pwd",
		Topic:            "topic",
		CompressionCodec: "none",
		MaxBatchBytes:    "",
	}

	assert.Equal(t, expectedMessageQueue, messageQueue)
}

func TestStartDefaultFlags(t *testing.T) {
	cmd := NewVolumeCtlCommand()

	for _, subCmd := range cmd.Commands() {
		if subCmd.Name() != "start" {
			continue
		}

		subCmd.ParseFlags(nil)

		volumeControlEndpoint := getVolumeControllerEndpoint(subCmd, nil)
		assert.Equal(
			t, vctloptions.DefaultVolumeControlServerEndpoint,
			volumeControlEndpoint)

		podNamespace := subCmd.Flag("pod-namespace").Value.String()
		assert.Equal(t, metav1.NamespaceDefault, podNamespace)

		podName := subCmd.Flag("pod-name").Value.String()
		assert.Equal(t, "", podName)

		dataContainerName := subCmd.Flag("data-container-name").Value.String()
		assert.Equal(t, "", dataContainerName)

		pvcNamespace := subCmd.Flag("pvc-namespace").Value.String()
		assert.Equal(t, "", pvcNamespace)

		pvcName := subCmd.Flag("pvc-name").Value.String()
		assert.Equal(t, "", pvcName)

		executablesFilePath := subCmd.Flag(
			"allowed-executables-file-path").Value.String()
		assert.Equal(t, "", executablesFilePath)
	}
}

func TestStartAddFlags(t *testing.T) {
	cmd := NewVolumeCtlCommand()

	args := []string{
		"--volume-controller-endpoint=tcp:127.0.0.1:7000",
		"--pod-namespace=ns1",
		"--pod-name=pod1",
		"--pvc-namespace=ns1",
		"--pvc-name=pvc1",
		"--data-container-name=data1",
		"--allowed-executables-file-path=/allowed/exes",
	}

	for _, subCmd := range cmd.Commands() {
		if subCmd.Name() != "start" {
			continue
		}

		subCmd.ParseFlags(args)

		volumeControlEndpoint := getVolumeControllerEndpoint(subCmd, nil)
		assert.Equal(
			t, "tcp:127.0.0.1:7000", volumeControlEndpoint)

		podNamespace := subCmd.Flag("pod-namespace").Value.String()
		assert.Equal(t, "ns1", podNamespace)

		podName := subCmd.Flag("pod-name").Value.String()
		assert.Equal(t, "pod1", podName)

		dataContainerName := subCmd.Flag("data-container-name").Value.String()
		assert.Equal(t, "data1", dataContainerName)

		pvcNamespace := subCmd.Flag("pvc-namespace").Value.String()
		assert.Equal(t, "ns1", pvcNamespace)

		pvcName := subCmd.Flag("pvc-name").Value.String()
		assert.Equal(t, "pvc1", pvcName)

		executablesFilePath := subCmd.Flag(
			"allowed-executables-file-path").Value.String()
		assert.Equal(t, "/allowed/exes", executablesFilePath)
	}
}
