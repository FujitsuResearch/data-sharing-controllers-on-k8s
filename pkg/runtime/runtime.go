// Copyright (c) 2022 Fujitsu Limited

package runtime

type ContainerInfo struct {
	RootFs           string
	MountNamaspaceId string
}

type ContainerRuntimeOperations interface {
	GetContainerInfo(containerId string) (*ContainerInfo, error)
	Close() error
}
