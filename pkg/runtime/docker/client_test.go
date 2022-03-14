// Copyright (c) 2022 Fujitsu Limited

package docker

import (
	"os"
	"strings"
	"testing"

	dockertypes "github.com/docker/docker/api/types"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/kubelet"
	fakedocker "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/docker/testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	graphDriver = "overlay2"
)

func newFakeDockerRuntimeClient(
	t *testing.T, fakeDockerClient *fakedocker.FakeDockerClient) *DockerRuntimeClient {
	info := dockertypes.Info{
		Driver: graphDriver,
	}

	fakeDockerClient.On("Info", mock.AnythingOfType("*context.timerCtx")).
		Return(info, nil)

	dataRootDirectory := "/var/lib/docker"
	kubeletConfig := &kubelet.KubeletOptions{
		RootDirectory:  "/var/lib/kubelet",
		PodsDirName:    "pods",
		VolumesDirName: "volumes",
	}

	runtimeClient, err := newDockerRuntimeClient(
		fakeDockerClient, dataRootDirectory, kubeletConfig)
	assert.NoError(t, err)

	assert.Equal(t, "/var/lib/docker/overlay2", runtimeClient.rootFsDirPrefix)

	return runtimeClient
}

func TestGetContainerInfo(t *testing.T) {
	fakeDockerClient := new(fakedocker.FakeDockerClient)
	runtimeClient := newFakeDockerRuntimeClient(t, fakeDockerClient)

	pid := os.Getpid()
	rootfs := "/var/lib/docker/overlay2/xxx/merged"
	containerInfoJson := dockertypes.ContainerJSON{
		ContainerJSONBase: &dockertypes.ContainerJSONBase{
			GraphDriver: dockertypes.GraphDriverData{
				Name: graphDriver,
				Data: map[string]string{
					"MergedDir": rootfs,
				},
			},
			State: &dockertypes.ContainerState{
				Pid: pid,
			},
		},
	}
	fakeDockerClient.On(
		"ContainerInspect", mock.AnythingOfType("*context.timerCtx"),
		"container1").
		Return(containerInfoJson, nil)

	fakeDockerClient.On("Close").Return(nil)

	containerId := "docker://container1"
	containerInfo, err := runtimeClient.GetContainerInfo(containerId)
	assert.NoError(t, err)

	err = runtimeClient.Close()
	assert.NoError(t, err)

	fakeDockerClient.AssertExpectations(t)

	assert.Equal(t, rootfs, containerInfo.RootFs)
	assert.True(t, strings.HasPrefix(containerInfo.MountNamaspaceId, "mnt:["))
}
