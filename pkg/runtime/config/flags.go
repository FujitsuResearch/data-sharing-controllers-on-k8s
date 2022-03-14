// Copyright (c) 2022 Fujitsu Limited

package config

import (
	"github.com/spf13/pflag"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/containerd"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/docker"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/kubelet"
)

const (
	RuntimeDocker     = "docker"
	RuntimeContainerd = "containerd"
	RuntimeCriO       = "cri-o"

	defaultRuntime = RuntimeDocker
)

type ContainerRuntimeOptions struct {
	Name     string
	Endpoint string

	Kubelet *kubelet.KubeletOptions

	Docker     *docker.DockerOptions
	Containerd *containerd.ContainerdOptions
}

func NewContainerRuntimeOptions() *ContainerRuntimeOptions {
	return &ContainerRuntimeOptions{
		Name: defaultRuntime,

		Kubelet:    kubelet.NewKubeletOptions(),
		Docker:     docker.NewDockerOptions(),
		Containerd: containerd.NewContainerdOptions(),
	}
}

func (cro *ContainerRuntimeOptions) AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&cro.Name, "container-runtime", cro.Name,
		"Container runtime to use. Possible values: 'docker', 'containerd'.")
	flagSet.StringVar(
		&cro.Endpoint, "runtime-endpoint", cro.Endpoint,
		"Container runtime endpoint, e.g. 'unix:///xxx.sock'.")

	cro.Kubelet.AddFlags(flagSet)

	cro.Docker.AddFlags(flagSet)

	cro.Containerd.AddFlags(flagSet)
}
