// Copyright (c) 2022 Fujitsu Limited

package overlay2

import (
	"fmt"

	dockertypes "github.com/docker/docker/api/types"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/docker/graphdriver"
)

const (
	driverName    = "overlay2"
	metaMergedDir = "MergedDir"
)

type overlay2 struct {
}

func NewOverlay2Driver() graphdriver.GraphDriverOperations {
	return &overlay2{}
}

func (o *overlay2) GetRootFsDirectory(
	graphDriverData dockertypes.GraphDriverData) (string, error) {
	if graphDriverData.Name != driverName {
		return "", fmt.Errorf(
			"%q does not equal to %s", graphDriverData.Name, driverName)
	}

	if rootFs, ok := graphDriverData.Data[metaMergedDir]; ok {
		return rootFs, nil
	} else {
		return "", fmt.Errorf(
			"%q is not contained in the graph driver data: %+v",
			metaMergedDir, graphDriverData.Data)
	}
}
