// Copyright (c) 2022 Fujitsu Limited

package volumectl

import (
	"testing"

	vctloptions "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/volume-controller/app/options"

	"github.com/stretchr/testify/assert"
)

func TestFinalDefaultFlags(t *testing.T) {
	cmd := NewVolumeCtlCommand()

	for _, subCmd := range cmd.Commands() {
		if subCmd.Name() != "final" {
			continue
		}

		subCmd.ParseFlags(nil)

		volumeControlEndpoint := getVolumeControllerEndpoint(subCmd, nil)
		assert.Equal(
			t, vctloptions.DefaultVolumeControlServerEndpoint,
			volumeControlEndpoint)

		fuseMountPointPath := subCmd.Flag(
			"fuse-mount-point-path").Value.String()
		assert.Equal(t, "", fuseMountPointPath)

		forceType := subCmd.Flag("force").Value.Type()
		assert.Equal(t, "bool", forceType)
		force := subCmd.Flag("force").Value.String()
		assert.Equal(t, "false", force)
	}
}

func TestFinalAddFlags(t *testing.T) {
	cmd := NewVolumeCtlCommand()

	args := []string{
		"--volume-controller-endpoint=tcp:127.0.0.1:7000",
		"--fuse-mount-point-path=/mnt/fuse/targets/local/mp1",
		"--force=true",
	}

	for _, subCmd := range cmd.Commands() {
		if subCmd.Name() != "final" {
			continue
		}

		subCmd.ParseFlags(args)

		volumeControlEndpoint := getVolumeControllerEndpoint(subCmd, nil)
		assert.Equal(
			t, "tcp:127.0.0.1:7000", volumeControlEndpoint)

		fuseMountPointPath := subCmd.Flag(
			"fuse-mount-point-path").Value.String()
		assert.Equal(t, "/mnt/fuse/targets/local/mp1", fuseMountPointPath)

		forceType := subCmd.Flag("force").Value.Type()
		assert.Equal(t, "bool", forceType)
		force := subCmd.Flag("force").Value.String()
		assert.Equal(t, "true", force)
	}
}
