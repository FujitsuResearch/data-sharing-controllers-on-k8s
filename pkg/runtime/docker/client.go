// Copyright (c) 2022 Fujitsu Limited

package docker

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	dockertypes "github.com/docker/docker/api/types"
	dockerapi "github.com/docker/docker/client"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config"
	dockerconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/docker"
	kubeletconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/kubelet"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/docker/graphdriver"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/docker/graphdriver/overlay2"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
	"k8s.io/klog/v2"
)

const (
	defaultEndpoint = "unix:///var/run/docker.sock"
	runtimeName     = "docker"
)

type dockerClient interface {
	Info(ctx context.Context) (dockertypes.Info, error)
	ContainerInspect(ctx context.Context, containerId string) (
		dockertypes.ContainerJSON, error)
	Close() error
}

type DockerRuntimeClient struct {
	dockerClient    dockerClient
	rootFsDirPrefix string
	storageDriver   graphdriver.GraphDriverOperations
	kubeletConfig   *kubeletconfig.KubeletOptions
	procFs          util.ProcFsOperations
}

func newGraphDriver(name string) (graphdriver.GraphDriverOperations, error) {
	switch name {
	case graphdriver.GraphDriverOverlay:
		return nil, fmt.Errorf("Not support such a graph driver: %s", name)
	case graphdriver.GraphDriverOverlay2:
		return overlay2.NewOverlay2Driver(), nil
	default:
		return nil, fmt.Errorf("Not support such a graph driver: %s", name)
	}
}

func newDockerClient(
	endpoint string, config *dockerconfig.DockerOptions) (
	*dockerapi.Client, error) {
	if endpoint == "" {
		endpoint = defaultEndpoint
	}

	dockerClient, err := dockerapi.NewClientWithOpts(
		dockerapi.WithHost(endpoint), dockerapi.FromEnv,
		dockerapi.WithVersion(config.ApiVersion))
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize a docker client: %v", err)
	}

	return dockerClient, nil
}

func newDockerRuntimeClient(
	dockerClient dockerClient, dataRootDirectory string,
	kubeletConfig *kubeletconfig.KubeletOptions) (
	*DockerRuntimeClient, error) {
	ctx, cancel := util.GetTimeoutContext(util.DefaultRuntimeClientTimeout)
	defer cancel()

	info, err := dockerClient.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("Could not get docker info: %v", err)
	}

	graphDriver, err := newGraphDriver(info.Driver)
	if err != nil {
		return nil, err
	}

	return &DockerRuntimeClient{
		dockerClient:    dockerClient,
		rootFsDirPrefix: filepath.Join(dataRootDirectory, info.Driver),
		storageDriver:   graphDriver,
		kubeletConfig:   kubeletConfig,
		procFs:          util.NewProcFs(),
	}, nil
}

func NewDockerRuntimeClient(
	config *config.ContainerRuntimeOptions) (*DockerRuntimeClient, error) {
	klog.Infof(
		"[docker configurations] endpoint %v, %+v",
		config.Endpoint, config.Docker)

	dockerClient, err := newDockerClient(config.Endpoint, config.Docker)
	if err != nil {
		return nil, fmt.Errorf("Could not connect to docker: %v", err)
	}

	return newDockerRuntimeClient(
		dockerClient, config.Docker.DataRootDirectory, config.Kubelet)
}

func getContainerId(kubeContainerId string) string {
	return strings.TrimPrefix(
		kubeContainerId, fmt.Sprintf("%s://", runtimeName))
}

func (drc *DockerRuntimeClient) GetContainerInfo(
	kubeContainerId string) (*runtime.ContainerInfo, error) {
	containerId := getContainerId(kubeContainerId)

	ctx, cancel := util.GetTimeoutContext(util.DefaultRuntimeClientTimeout)
	defer cancel()

	containerJson, err := drc.dockerClient.ContainerInspect(ctx, containerId)
	if err != nil {
		return nil, err
	}

	rootFs, err := drc.storageDriver.GetRootFsDirectory(
		containerJson.GraphDriver)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(rootFs, drc.rootFsDirPrefix) {
		return nil, fmt.Errorf(
			"root FS %q is NOT a directory under %q",
			rootFs, drc.rootFsDirPrefix)
	}

	mountNamespace, err := drc.procFs.GetMountNamespace(
		containerJson.State.Pid)
	if err != nil {
		return nil, err
	}

	return &runtime.ContainerInfo{
		RootFs:           rootFs,
		MountNamaspaceId: mountNamespace,
	}, nil
}

func (drc *DockerRuntimeClient) Close() error {
	return drc.dockerClient.Close()
}
