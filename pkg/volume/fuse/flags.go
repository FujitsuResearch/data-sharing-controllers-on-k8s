// Copyright (c) 2022 Fujitsu Limited

package fuse

import (
	"github.com/spf13/pflag"
)

const (
	defaultMountPointsHostDirectory = "/mnt/fuse/targets"
)

type FuseOptions struct {
	MountPointsHostRootDirectory string
	Debug                        bool
	ChecksumCalculationAlways    bool
}

func NewFuseOptions() *FuseOptions {
	return &FuseOptions{
		MountPointsHostRootDirectory: defaultMountPointsHostDirectory,
	}
}

func (fo *FuseOptions) AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&fo.MountPointsHostRootDirectory, "fuse-mount-points-host-root-dir",
		fo.MountPointsHostRootDirectory,
		"Root directory for fuse mount points on the host.")

	flagSet.BoolVar(
		&fo.Debug, "fuse-debug", fo.Debug, "Enable the fuse debug output.")

	flagSet.BoolVar(
		&fo.ChecksumCalculationAlways, "strict-checksum-validation",
		fo.ChecksumCalculationAlways,
		"Enable the strict checksum validation.  This means that "+
			"the controller always calculates checksums for executables.  "+
			"If this option is disabled, the controller utilizes "+
			"checksum caches updated by 'inotify()'")
}
