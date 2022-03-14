// Copyright (c) 2022 Fujitsu Limited

package containerd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd"
	containerdapi "github.com/containerd/containerd"
	introspectionapi "github.com/containerd/containerd/api/services/introspection/v1"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/services/introspection"
	"k8s.io/klog/v2"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config"
	containerdconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/containerd"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

const (
	defaultEndpoint = "/run/containerd/containerd.sock"
	runtimeName     = "containerd"
	taskPluginId    = "task"
)

var (
	taskPluginType = plugin.RuntimePluginV2.String()
)

type containerdClient interface {
	IntrospectionService() introspection.Service
	LoadContainer(ctx context.Context, id string) (containerdapi.Container, error)
	Close() error
}

type ContainerdRuntimeClient struct {
	containerdClient containerdClient
	rootFsDirPrefix  string
	namespace        string
	procFs           util.ProcFsOperations
}

func newContainerdClient(
	endpoint string, config *containerdconfig.ContainerdOptions) (
	*containerd.Client, error) {
	if endpoint == "" {
		endpoint = defaultEndpoint
	}

	containerdClient, err := containerdapi.New(endpoint)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to initialize a containerd client: %v", err)
	}

	return containerdClient, nil
}

func validateTaskPlugin(plugins []introspectionapi.Plugin) error {
	if len(plugins) == 0 {
		return fmt.Errorf(
			"Not find the plugin (type: %s, id: %s)",
			taskPluginType, taskPluginId)
	} else if len(plugins) > 1 {
		uris := make([]string, 0, len(plugins))
		for _, plugin := range plugins {
			uris = append(
				uris,
				fmt.Sprintf("%s.%s", plugin.Type, plugin.ID))
		}
		return fmt.Errorf("Find multiple plugins: %v", uris)
	}

	plugin := plugins[0]

	if plugin.Type != taskPluginType {
		return fmt.Errorf(
			"Plugin type %q is NOT equal to %q",
			plugin.Type, taskPluginType)
	}

	if plugin.ID != taskPluginId {
		return fmt.Errorf(
			"Plugin ID %q is NOT equal to %q", plugin.ID, taskPluginId)
	}

	return nil
}

func newContainerdRuntimeClient(
	containerdClient containerdClient,
	config *containerdconfig.ContainerdOptions) (
	*ContainerdRuntimeClient, error) {
	introspectionClient := containerdClient.IntrospectionService()

	ctx, cancel := util.GetTimeoutContext(util.DefaultRuntimeClientTimeout)
	defer cancel()
	filters := []string{
		fmt.Sprintf("type==%s", taskPluginType),
		fmt.Sprintf("id==%s", taskPluginId),
	}
	response, err := introspectionClient.Plugins(ctx, filters)
	if err != nil {
		return nil, err
	}

	err = validateTaskPlugin(response.Plugins)
	if err != nil {
		return nil, err
	}

	pluginUri := fmt.Sprintf("%s.%s", taskPluginType, taskPluginId)

	// [REF] rootfs: NewBundle()
	//       <github.com/containerd/containerd/runtime/v2/bundle.go>
	return &ContainerdRuntimeClient{
		containerdClient: containerdClient,
		rootFsDirPrefix: filepath.Join(
			config.DataRootDirectory, pluginUri, config.Namespace),
		namespace: config.Namespace,
		procFs:    util.NewProcFs(),
	}, nil
}

func NewContainerdRuntimeClient(
	config *config.ContainerRuntimeOptions) (*ContainerdRuntimeClient, error) {
	klog.Infof(
		"[containerd configurations] endpoint %v, %+v",
		config.Endpoint, config.Containerd)

	containerdClient, err := newContainerdClient(
		config.Endpoint, config.Containerd)
	if err != nil {
		return nil, fmt.Errorf("Could not connect to containerd: %v", err)
	}

	return newContainerdRuntimeClient(containerdClient, config.Containerd)
}

func getContainerId(kubeContainerId string) string {
	return strings.TrimPrefix(
		kubeContainerId, fmt.Sprintf("%s://", runtimeName))
}

func createContainerdContext(
	namespace string) (context.Context, context.CancelFunc) {
	ctx, cancel := util.GetTimeoutContext(util.DefaultRuntimeClientTimeout)

	containerdCtx := namespaces.WithNamespace(ctx, namespace)

	return containerdCtx, cancel
}

func (crc *ContainerdRuntimeClient) getContainer(
	containerId string) (containerdapi.Container, error) {
	ctx, cancel := createContainerdContext(crc.namespace)
	defer cancel()

	return crc.containerdClient.LoadContainer(ctx, containerId)
}

func (crc *ContainerdRuntimeClient) getRootFileSystemDirectory(
	container containerdapi.Container) (string, error) {
	ctx, cancel := createContainerdContext(crc.namespace)
	defer cancel()

	spec, err := container.Spec(ctx)
	if err != nil {
		return "", err
	}

	return spec.Root.Path, nil
}

func (crc *ContainerdRuntimeClient) getProcessId(
	container containerdapi.Container) (uint32, error) {
	ctx, cancel := createContainerdContext(crc.namespace)
	defer cancel()

	task, err := container.Task(ctx, nil)
	if err != nil {
		return 0, err
	}

	return task.Pid(), nil
}

func (crc *ContainerdRuntimeClient) getMountNamespaceId(
	container containerdapi.Container) (string, error) {
	pid, err := crc.getProcessId(container)
	if err != nil {
		return "", err
	}

	return crc.procFs.GetMountNamespace(int(pid))
}

func (crc *ContainerdRuntimeClient) GetContainerInfo(
	kubeContainerId string) (*runtime.ContainerInfo, error) {
	containerId := getContainerId(kubeContainerId)

	container, err := crc.getContainer(containerId)
	if err != nil {
		return nil, err
	}

	rootFs, err := crc.getRootFileSystemDirectory(container)
	if err != nil {
		return nil, err
	}

	if !filepath.IsAbs(rootFs) {
		rootFs = filepath.Join(crc.rootFsDirPrefix, containerId, rootFs)
	}

	if !strings.HasPrefix(rootFs, crc.rootFsDirPrefix) {
		return nil, fmt.Errorf(
			"root FS %q is NOT a directory under %q",
			rootFs, crc.rootFsDirPrefix)
	}

	mountNamespace, err := crc.getMountNamespaceId(container)
	if err != nil {
		return nil, err
	}

	return &runtime.ContainerInfo{
		RootFs:           rootFs,
		MountNamaspaceId: mountNamespace,
	}, nil
}

func (crc *ContainerdRuntimeClient) Close() error {
	return crc.containerdClient.Close()
}
