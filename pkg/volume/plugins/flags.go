// Copyright (c) 2022 Fujitsu Limited

package plugins

import (
	"github.com/spf13/pflag"
)

const (
	defaultKubeletPluginsDirectory       = "/var/lib/kubelet/plugins"
	defaultLocalFuseSourcesHostDirectory = "/mnt/fuse/sources/local"
)

type VolumeOptions struct {
	CsiPluginsDir           string
	LocalFuseSourcesHostDir string
}

func NewVolumeOptions() *VolumeOptions {
	return &VolumeOptions{
		CsiPluginsDir:           defaultKubeletPluginsDirectory,
		LocalFuseSourcesHostDir: defaultLocalFuseSourcesHostDirectory,
	}
}

func (vo *VolumeOptions) AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&vo.CsiPluginsDir, "csi-plugins-dir",
		vo.CsiPluginsDir, "Path to the CSI plugins directory.")
	flagSet.StringVar(
		&vo.LocalFuseSourcesHostDir, "local-fuse-sources-host-dir",
		vo.LocalFuseSourcesHostDir,
		"Host path to the source root directory for local storage.")
}
