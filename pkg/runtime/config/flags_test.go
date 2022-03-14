// Copyright (c) 2022 Fujitsu Limited

package config

import (
	"testing"

	"github.com/spf13/pflag"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/containerd"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/docker"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/kubelet"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	runtimeOptions := NewContainerRuntimeOptions()
	runtimeOptions.AddFlags(flagSet)

	err := flagSet.Parse(nil)
	assert.NoError(t, err)

	expectedRuntimeOptions := &ContainerRuntimeOptions{
		Name: "docker",
		Kubelet: &kubelet.KubeletOptions{
			RootDirectory:  "/var/lib/kubelet",
			PodsDirName:    "pods",
			VolumesDirName: "volumes",
		},
		Docker: &docker.DockerOptions{
			DataRootDirectory: "/var/lib/docker",
		},
		Containerd: &containerd.ContainerdOptions{
			DataRootDirectory: "/run/containerd",
			Namespace:         "k8s.io",
		},
	}

	assert.Equal(t, expectedRuntimeOptions, runtimeOptions)
}

func TestAddDockerFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	runtimeOptions := NewContainerRuntimeOptions()
	runtimeOptions.AddFlags(flagSet)

	args := []string{
		"--container-runtime=containerd",
		"--containerd-data-root-dir=/var/lib/containerd",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	expectedRuntimeOptions := &ContainerRuntimeOptions{
		Name: "containerd",
		Kubelet: &kubelet.KubeletOptions{
			RootDirectory:  "/var/lib/kubelet",
			PodsDirName:    "pods",
			VolumesDirName: "volumes",
		},
		Docker: &docker.DockerOptions{
			DataRootDirectory: "/var/lib/docker",
		},
		Containerd: &containerd.ContainerdOptions{
			DataRootDirectory: "/var/lib/containerd",
			Namespace:         "k8s.io",
		},
	}

	assert.Equal(t, expectedRuntimeOptions, runtimeOptions)
}

func TestAddContainerdFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	runtimeOptions := NewContainerRuntimeOptions()
	runtimeOptions.AddFlags(flagSet)

	args := []string{
		"--container-runtime=docker",
		"--runtime-endpoint=unix:///docker.sock",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	expectedRuntimeOptions := &ContainerRuntimeOptions{
		Name:     "docker",
		Endpoint: "unix:///docker.sock",
		Kubelet: &kubelet.KubeletOptions{
			RootDirectory:  "/var/lib/kubelet",
			PodsDirName:    "pods",
			VolumesDirName: "volumes",
		},
		Docker: &docker.DockerOptions{
			DataRootDirectory: "/var/lib/docker",
		},
		Containerd: &containerd.ContainerdOptions{
			DataRootDirectory: "/run/containerd",
			Namespace:         "k8s.io",
		},
	}

	assert.Equal(t, expectedRuntimeOptions, runtimeOptions)
}
