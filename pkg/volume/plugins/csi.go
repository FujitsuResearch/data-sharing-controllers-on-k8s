// Copyright (c) 2022 Fujitsu Limited

package plugins

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
)

const (
	fuseMountPointsCsiDirName = "csi"
)

var (
	_ pluginOperations = newCsiPersistentVolume("", "")
)

type csiPersistentVolume struct {
	fuseMountPointsHostRootDir string
	csiPluginsDir              string
}

func newCsiPersistentVolume(
	fuseMountPointsHostRootDir string,
	csiPluginsDir string) *csiPersistentVolume {
	return &csiPersistentVolume{
		fuseMountPointsHostRootDir: fuseMountPointsHostRootDir,
		csiPluginsDir:              csiPluginsDir,
	}
}

func (cpv *csiPersistentVolume) generateFuseMountPointHostPath(
	req *api.InitializeRequest) (string, error) {
	if req.CsiVolumeHandle == "" {
		return "", nil
	}

	if !strings.HasPrefix(req.FuseSourcePath, cpv.csiPluginsDir) {
		return "", fmt.Errorf(
			"Fuse source path %q must be subdirectory of %q",
			req.FuseSourcePath, cpv.csiPluginsDir)
	}

	mountPointPath := filepath.Join(
		cpv.fuseMountPointsHostRootDir, fuseMountPointsCsiDirName,
		req.CsiVolumeHandle)

	if _, err := os.Stat(mountPointPath); !os.IsNotExist(err) {
		return "", fmt.Errorf("The mount point %q exists", mountPointPath)
	}

	err := os.MkdirAll(mountPointPath, 0755)
	if err != nil {
		return "", fmt.Errorf(
			"Not create the mount point path for the CSI-based storage %q: %s",
			mountPointPath, err.Error())
	}

	return mountPointPath, nil
}

func (cpv *csiPersistentVolume) getFuseMountPointHostPath(
	pv *corev1.PersistentVolume) (string, error) {
	if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle == "" {
		return "", nil
	}

	return filepath.Join(
		cpv.fuseMountPointsHostRootDir, fuseMountPointsCsiDirName,
		pv.Spec.CSI.VolumeHandle), nil
}
