// Copyright (c) 2022 Fujitsu Limited

package kubelet

import (
	"testing"

	"github.com/spf13/pflag"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFlags(t *testing.T) {
	fs := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	k := NewKubeletOptions()
	k.AddFlags(fs)

	fs.Parse(nil)

	expectedKo := &KubeletOptions{
		RootDirectory:  "/var/lib/kubelet",
		PodsDirName:    "pods",
		VolumesDirName: "volumes",
	}

	assert.Equal(t, expectedKo, k)
}

func TestAddFlags(t *testing.T) {
	fs := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	k := NewKubeletOptions()
	k.AddFlags(fs)

	args := []string{
		"--kubelet-root-dir=/tmp/kubelet",
		"--kubelet-pods-dir-name=pod",
		"--kubelet-volumes-dir-name=vols",
	}

	fs.Parse(args)

	expectedKo := &KubeletOptions{
		RootDirectory:  "/tmp/kubelet",
		PodsDirName:    "pod",
		VolumesDirName: "vols",
	}

	assert.Equal(t, expectedKo, k)
}
