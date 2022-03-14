// Copyright (c) 2022 Fujitsu Limited

package volumectl

import (
	"fmt"
	"testing"

	vctloptions "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/volume-controller/app/options"
	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"

	"github.com/stretchr/testify/assert"
)

func TestInitDefaultFlags(t *testing.T) {
	cmd := NewVolumeCtlCommand()

	for _, subCmd := range cmd.Commands() {
		if subCmd.Name() != "init" {
			continue
		}

		subCmd.ParseFlags(nil)

		volumeControlEndpoint := getVolumeControllerEndpoint(subCmd, nil)
		assert.Equal(
			t, vctloptions.DefaultVolumeControlServerEndpoint,
			volumeControlEndpoint)

		volumeSourcePath := subCmd.Flag(
			"volume-source-path").Value.String()
		assert.Equal(t, "", volumeSourcePath)

		allowedExecutables := subCmd.Flag(
			"external-allowed-executables").Value.String()
		assert.Equal(t, "", allowedExecutables)

		mountsHostRootDirectory := subCmd.
			Flag("fuse-mounts-host-root-directory").Value.String()
		assert.Equal(t, "", mountsHostRootDirectory)

		csiHandle := subCmd.Flag("volume-handle").Value.String()
		assert.Equal(t, "", csiHandle)

		disableUsageControl := subCmd.Flag("disable-usage-control").Value.String()
		assert.Equal(t, "false", disableUsageControl)
	}
}

func TestInitAddFlags(t *testing.T) {
	cmd := NewVolumeCtlCommand()

	allowedExecutablesString := "[{\"command_absolute_path\":\"/usr/bin/x\"," +
		"\"writable\":true,\"is_host_process\":true}," +
		"{\"command_absolute_path\":\"/usr/bin/y\"," +
		"\"are_all_processes_allowed_in_namespace\":true}," +
		"{\"command_absolute_path\":\"/usr/local/bin/z\"," +
		"\"external_allowed_scripts\":[" +
		"{\"absolute_path\":\"/usr/local/bin/script1\"}," +
		"{\"absolute_path\":\"/usr/local/bin/script2\"," +
		"\"writable\":true,\"relative_path_to_init_work_dir\":\"../\"}" +
		"]}]"
	args := []string{
		"--volume-controller-endpoint=tcp:127.0.0.1:7000",
		"--volume-source-path=/source/vol1",
		fmt.Sprintf(
			"--external-allowed-executables=%s", allowedExecutablesString),
		"--fuse-mounts-host-root-directory=/mnt/host",
		"--volume-handle=handle1",
		"--disable-usage-control",
	}

	for _, subCmd := range cmd.Commands() {
		if subCmd.Name() != "init" {
			continue
		}

		subCmd.ParseFlags(args)

		volumeControlEndpoint := getVolumeControllerEndpoint(subCmd, nil)
		assert.Equal(
			t, "tcp:127.0.0.1:7000", volumeControlEndpoint)

		volumeSourcePath := subCmd.Flag(
			"volume-source-path").Value.String()
		assert.Equal(t, "/source/vol1", volumeSourcePath)

		allowedExecutables := subCmd.Flag(
			"external-allowed-executables").Value.String()
		assert.Equal(t, allowedExecutablesString, allowedExecutables)

		executables, err := getExternalAllowedExecutables(allowedExecutables)
		assert.NoError(t, err)

		expectedExecutables := []*api.ExternalAllowedExecutable{
			{
				CommandAbsolutePath: "/usr/bin/x",
				Writable:            true,
				IsHostProcess:       true,
			},
			{
				CommandAbsolutePath:               "/usr/bin/y",
				AreAllProcessesAllowedInNamespace: true,
			},
			{
				CommandAbsolutePath: "/usr/local/bin/z",
				ExternalAllowedScripts: []*api.ExternalAllowedScript{
					{
						AbsolutePath: "/usr/local/bin/script1",
					},
					{
						AbsolutePath:              "/usr/local/bin/script2",
						Writable:                  true,
						RelativePathToInitWorkDir: "../",
					},
				},
			},
		}
		assert.Equal(t, expectedExecutables, executables)

		mountsHostRootDirectory := subCmd.
			Flag("fuse-mounts-host-root-directory").Value.String()
		assert.Equal(t, "/mnt/host", mountsHostRootDirectory)

		csiHandle := subCmd.Flag("volume-handle").Value.String()
		assert.Equal(t, "handle1", csiHandle)

		disableUsageControl := subCmd.Flag("disable-usage-control").Value.String()
		assert.Equal(t, "true", disableUsageControl)
	}
}
