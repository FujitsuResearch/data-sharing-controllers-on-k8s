// Copyright (c) 2022 Fujitsu Limited

package containerd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	introspectionapi "github.com/containerd/containerd/api/services/introspection/v1"
	"github.com/containerd/containerd/oci"
	"github.com/opencontainers/runtime-spec/specs-go"

	containerdconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/containerd"
	fakecontainerd "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/containerd/testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func newFakeContainerdRuntimeClient(
	t *testing.T,
	fakeContainerdClient *fakecontainerd.FakeContainerdClient) *ContainerdRuntimeClient {
	fakeService := new(fakecontainerd.FakeContainerdService)

	filters := []string{
		fmt.Sprintf("type==%s", taskPluginType),
		fmt.Sprintf("id==%s", taskPluginId),
	}
	response := &introspectionapi.PluginsResponse{
		Plugins: []introspectionapi.Plugin{
			{
				Type: taskPluginType,
				ID:   taskPluginId,
			},
		},
	}
	fakeService.On(
		"Plugins", mock.AnythingOfType("*context.timerCtx"), filters).
		Return(response, nil)

	fakeContainerdClient.On("IntrospectionService").
		Return(fakeService)

	containerdConfig := containerdconfig.NewContainerdOptions()

	runtimeClient, err := newContainerdRuntimeClient(
		fakeContainerdClient, containerdConfig)
	assert.NoError(t, err)

	rootfs := filepath.Join(
		containerdConfig.DataRootDirectory,
		fmt.Sprintf("%s.%s", taskPluginType, taskPluginId),
		containerdConfig.Namespace)
	assert.Equal(t, rootfs, runtimeClient.rootFsDirPrefix)

	return runtimeClient
}

func TestGetContainerInfo(t *testing.T) {
	fakeContainerdClient := new(fakecontainerd.FakeContainerdClient)
	runtimeClient := newFakeContainerdRuntimeClient(t, fakeContainerdClient)

	fakeTask := new(fakecontainerd.FakeContainerdTask)
	pid := os.Getpid()
	fakeTask.On("Pid").Return(uint32(pid))

	fakeContainer := new(fakecontainerd.FakeContainerdContainer)
	fakeContainer.On(
		"Task", mock.AnythingOfType("*context.valueCtx"),
		mock.AnythingOfType("cio.Attach")).
		Return(fakeTask, nil)

	rootfs := "rootfs"
	containerSpec := &oci.Spec{
		Root: &specs.Root{
			Path: rootfs,
		},
	}
	fakeContainer.On(
		"Spec", mock.AnythingOfType("*context.valueCtx")).
		Return(containerSpec, nil)

	fakeContainerdClient.On(
		"LoadContainer", mock.AnythingOfType("*context.valueCtx"),
		"container1").
		Return(fakeContainer, nil)

	fakeContainerdClient.On("Close").Return(nil)

	containerId := "containerd://container1"
	containerInfo, err := runtimeClient.GetContainerInfo(containerId)
	assert.NoError(t, err)

	err = runtimeClient.Close()
	assert.NoError(t, err)

	fakeContainerdClient.AssertExpectations(t)

	expectedRootFs := filepath.Join(
		runtimeClient.rootFsDirPrefix, "container1", rootfs)
	assert.Equal(t, expectedRootFs, containerInfo.RootFs)
	assert.True(t, strings.HasPrefix(containerInfo.MountNamaspaceId, "mnt:["))
}
