// Copyright (c) 2022 Fujitsu Limited

package fuse

import (
	"testing"

	"github.com/spf13/pflag"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	fuseOptions := NewFuseOptions()
	fuseOptions.AddFlags(flagSet)

	err := flagSet.Parse(nil)
	assert.NoError(t, err)

	expectedFuseOptions := &FuseOptions{
		MountPointsHostRootDirectory: "/mnt/fuse/targets",
		Debug:                        false,
		ChecksumCalculationAlways:    false,
	}

	assert.Equal(t, expectedFuseOptions, fuseOptions)
}

func TestAddFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	fuseOptions := NewFuseOptions()
	fuseOptions.AddFlags(flagSet)

	args := []string{
		"--fuse-mount-points-host-root-dir=/tmp/fuse/targets",
		"--fuse-debug",
		"--strict-checksum-validation",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	expectedFuseOptions := &FuseOptions{
		MountPointsHostRootDirectory: "/tmp/fuse/targets",
		Debug:                        true,
		ChecksumCalculationAlways:    true,
	}

	assert.Equal(t, expectedFuseOptions, fuseOptions)
}
