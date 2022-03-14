// Copyright (c) 2022 Fujitsu Limited

package kubelet

import (
	"github.com/spf13/pflag"
)

const (
	defaultRootDirectory      = "/var/lib/kubelet"
	defaultPodsRootDirName    = "pods"
	defaultVolumesRootDirName = "volumes"
)

type KubeletOptions struct {
	RootDirectory  string
	PodsDirName    string
	VolumesDirName string
}

func NewKubeletOptions() *KubeletOptions {
	return &KubeletOptions{
		RootDirectory:  defaultRootDirectory,
		PodsDirName:    defaultPodsRootDirName,
		VolumesDirName: defaultVolumesRootDirName,
	}
}

func (k *KubeletOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(
		&k.RootDirectory, "kubelet-root-dir", k.RootDirectory,
		"Kubelet root directory, e.g '/var/lib/kubelet'.")
	fs.StringVar(
		&k.PodsDirName, "kubelet-pods-dir-name", k.PodsDirName,
		"Name of kubelet pods directory, e.g. 'pods'.")
	fs.StringVar(
		&k.VolumesDirName, "kubelet-volumes-dir-name", k.VolumesDirName,
		"Name of kubelet volumes directory, e.g. 'volumes'.")
}
