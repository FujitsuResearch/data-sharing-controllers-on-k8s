// Copyright (c) 2022 Fujitsu Limited

package plugins

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

func TestDefaultFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	volumeOptions := NewVolumeOptions()
	volumeOptions.AddFlags(flagSet)

	err := flagSet.Parse(nil)
	assert.NoError(t, err)

	expectedVolumeOptions := &VolumeOptions{
		CsiPluginsDir:           "/var/lib/kubelet/plugins",
		LocalFuseSourcesHostDir: "/mnt/fuse/sources/local",
	}

	assert.Equal(t, expectedVolumeOptions, volumeOptions)
}

func TestAddFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	volumeOptions := NewVolumeOptions()
	volumeOptions.AddFlags(flagSet)

	args := []string{
		"--csi-plugins-dir=/tmp/plugins",
		"--local-fuse-sources-host-dir=/mnt/fuse/sources",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	expectedVolumeOptions := &VolumeOptions{
		CsiPluginsDir:           "/tmp/plugins",
		LocalFuseSourcesHostDir: "/mnt/fuse/sources",
	}

	assert.Equal(t, expectedVolumeOptions, volumeOptions)
}
