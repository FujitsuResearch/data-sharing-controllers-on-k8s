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
	mountPointNameLength        = 16
	fuseMountPointsLocalDirName = "local"
)

var (
	_ pluginOperations = newLocalPersistentVolume("", "")
)

type localPersistentVolume struct {
	fuseMountPointsHostRootDir string
	fuseSourcesHostRootDir     string
}

func newLocalPersistentVolume(
	fuseMountPointsHostRootDir string,
	fuseSourcesHostRootDir string) *localPersistentVolume {
	return &localPersistentVolume{
		fuseMountPointsHostRootDir: fuseMountPointsHostRootDir,
		fuseSourcesHostRootDir:     fuseSourcesHostRootDir,
	}
}

func (lpv *localPersistentVolume) generateFuseMountPointHostPath(
	req *api.InitializeRequest) (string, error) {
	if req.LocalFuseMountsHostRootDir == "" {
		return "", nil
	}

	sourcesRootDir := filepath.Dir(req.FuseSourcePath)
	mountPointName := filepath.Base(req.FuseSourcePath)

	if sourcesRootDir != lpv.fuseSourcesHostRootDir || mountPointName == "" {
		return "", fmt.Errorf(
			"Fuse source path %q must be a subdirectory of %q",
			req.FuseSourcePath, lpv.fuseSourcesHostRootDir)
	}

	if req.LocalFuseMountsHostRootDir != lpv.fuseMountPointsHostRootDir {
		return "", fmt.Errorf(
			"Local fuse mounts host root directory %q does NOT match %q",
			req.LocalFuseMountsHostRootDir, lpv.fuseMountPointsHostRootDir)
	}

	mountPointPath := filepath.Join(
		lpv.fuseMountPointsHostRootDir, fuseMountPointsLocalDirName,
		mountPointName)

	if _, err := os.Stat(mountPointPath); !os.IsNotExist(err) {
		return "", fmt.Errorf("The mount point %q exists", mountPointPath)
	}

	err := os.MkdirAll(mountPointPath, 0755)
	if err != nil {
		return "", fmt.Errorf(
			"Not create the mount point path for the local storage %q: %s",
			mountPointPath, err.Error())
	}

	return mountPointPath, nil
}

func (lpv *localPersistentVolume) getFuseMountPointHostPath(
	pv *corev1.PersistentVolume) (string, error) {
	if pv.Spec.Local == nil || pv.Spec.Local.Path == "" {
		return "", nil
	}

	fuseMountPointLocalDir := filepath.Join(
		lpv.fuseMountPointsHostRootDir, fuseMountPointsLocalDirName)
	if !strings.HasPrefix(pv.Spec.Local.Path, fuseMountPointLocalDir) {
		return "", fmt.Errorf(
			"Local host path %q must be a directory under the root of "+
				"the fuse mount point for local storage %q",
			pv.Spec.Local.Path, fuseMountPointLocalDir)
	}

	return pv.Spec.Local.Path, nil
}
