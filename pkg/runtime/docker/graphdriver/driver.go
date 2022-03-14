// Copyright (c) 2022 Fujitsu Limited

package graphdriver

import (
	dockertypes "github.com/docker/docker/api/types"
)

const (
	GraphDriverOverlay  = "overlay"
	GraphDriverOverlay2 = "overlay2"
)

type GraphDriverOperations interface {
	GetRootFsDirectory(graphDriverData dockertypes.GraphDriverData) (string, error)
}
