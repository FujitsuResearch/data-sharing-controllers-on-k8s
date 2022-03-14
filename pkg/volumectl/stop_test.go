// Copyright (c) 2022 Fujitsu Limited

package volumectl

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vctloptions "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/volume-controller/app/options"
	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"

	"github.com/stretchr/testify/assert"
)

func TestGetAllowedExecutablePathsConfigurationFromJsonFile(t *testing.T) {
	executablePathsFile := "testing/fixtures/allowed_executable_paths.json"
	executablePaths, err := getAllowedExecutablePathsConfigurationFromJsonFile(
		executablePathsFile)

	assert.NoError(t, err)

	expectedExecutablePaths := map[string]*api.ExecutablePaths{
		"mountPoint1": {
			Paths: []string{
				"/usr/bin/x",
				"/usr/local/bin/y",
			},
		},
	}

	assert.Equal(t, expectedExecutablePaths, executablePaths)
}

func TestStopDefaultFlags(t *testing.T) {
	cmd := NewVolumeCtlCommand()

	for _, subCmd := range cmd.Commands() {
		if subCmd.Name() != "stop" {
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

		executablesFilePath := subCmd.Flag(
			"allowed-executables-file-path").Value.String()
		assert.Equal(t, "", executablesFilePath)

		mountPoint := subCmd.Flag("mount-point").Value.String()
		assert.Equal(t, "", mountPoint)
	}
}

func TestStopAddFlags(t *testing.T) {
	cmd := NewVolumeCtlCommand()

	args := []string{
		"--volume-controller-endpoint=tcp:127.0.0.1:7000",
		"--pod-namespace=ns1",
		"--pod-name=pod1",
		"--allowed-executables-file-path=/allowed/exepaths",
		"--mount-point=/mnt/point001",
	}

	for _, subCmd := range cmd.Commands() {
		if subCmd.Name() != "stop" {
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

		executablesFilePath := subCmd.Flag(
			"allowed-executables-file-path").Value.String()
		assert.Equal(t, "/allowed/exepaths", executablesFilePath)

		mountPoint := subCmd.Flag("mount-point").Value.String()
		assert.Equal(t, "/mnt/point001", mountPoint)

	}
}
