// Copyright (c) 2022 Fujitsu Limited

package client

import (
	"fmt"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/containerd"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/docker"
)

func NewContainerRuntimeClient(
	runtimeOptions *config.ContainerRuntimeOptions) (
	runtime.ContainerRuntimeOperations, error) {
	switch runtimeOptions.Name {
	case config.RuntimeDocker:
		return docker.NewDockerRuntimeClient(runtimeOptions)
	case config.RuntimeContainerd:
		return containerd.NewContainerdRuntimeClient(runtimeOptions)
	case config.RuntimeCriO:
		fallthrough
	default:
		return nil, fmt.Errorf(
			"Not support such a runtime name: %q", runtimeOptions.Name)
	}
}
