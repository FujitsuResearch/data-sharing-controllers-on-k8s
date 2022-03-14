// Copyright (c) 2022 Fujitsu Limited

package plugins

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"

	"github.com/stretchr/testify/assert"
)

func TestGenerateFuseMountPointHostPathForLocalPV(t *testing.T) {
	currentDir, err := os.Getwd()
	assert.NoError(t, err)
	fuseMountPointRootDir := filepath.Join(currentDir, "targets")

	volumeOptions := &VolumeOptions{
		LocalFuseSourcesHostDir: "/mnt/fuse/sources/local",
	}
	plugins := NewPlugins(fuseMountPointRootDir, volumeOptions)

	initRequest := &api.InitializeRequest{
		FuseSourcePath:             "/mnt/fuse/sources/local/test1",
		LocalFuseMountsHostRootDir: filepath.Join(currentDir, "targets"),
	}
	mountPoint, err := plugins.GenerateMountPointHostPathFromInitializeRequest(
		initRequest)
	assert.NoError(t, err)

	expectedMountPoint := filepath.Join(
		fuseMountPointRootDir, fuseMountPointsLocalDirName, "test1")
	assert.Equal(t, expectedMountPoint, mountPoint)

	err = plugins.DeleteMountPoint(mountPoint)
	assert.NoError(t, err)

	localRootDir := filepath.Join(
		fuseMountPointRootDir, fuseMountPointsLocalDirName)
	_, err = os.Stat(localRootDir)
	assert.False(t, os.IsNotExist(err))

	err = os.RemoveAll(fuseMountPointRootDir)
	assert.NoError(t, err)
}

func TestGenerateFuseMountPointHostPathForLocalPVInInvalidFuseSourcePath(
	t *testing.T) {
	fuseMountPointRootDir := "/mnt/fuse/targets"
	volumeOptions := &VolumeOptions{
		LocalFuseSourcesHostDir: "/mnt/fuse/sources/local",
	}
	plugins := NewPlugins(fuseMountPointRootDir, volumeOptions)

	initRequest := &api.InitializeRequest{
		FuseSourcePath:             "/tmp/fuse/sources/local/test1",
		LocalFuseMountsHostRootDir: "/mnt/fuse/targets",
	}
	_, err := plugins.GenerateMountPointHostPathFromInitializeRequest(
		initRequest)
	assert.Error(t, err)

	errMsg := fmt.Sprintf(
		"Fuse source path %q must be a subdirectory of %q",
		initRequest.FuseSourcePath, volumeOptions.LocalFuseSourcesHostDir)
	assert.EqualError(t, err, errMsg)
}

func TestGenerateFuseMountPointHostPathForLocalPVInInvalidFuseMountPointDirectory(
	t *testing.T) {
	fuseMountPointRootDir := "/mnt/fuse/targets"
	volumeOptions := &VolumeOptions{
		LocalFuseSourcesHostDir: "/mnt/fuse/sources/local",
	}
	plugins := NewPlugins(fuseMountPointRootDir, volumeOptions)

	initRequest := &api.InitializeRequest{
		FuseSourcePath:             "/mnt/fuse/sources/local/test1",
		LocalFuseMountsHostRootDir: "/tmp/fuse/targets",
	}
	_, err := plugins.GenerateMountPointHostPathFromInitializeRequest(
		initRequest)
	assert.Error(t, err)

	errMsg := fmt.Sprintf(
		"Local fuse mounts host root directory %q does NOT match %q",
		initRequest.LocalFuseMountsHostRootDir, fuseMountPointRootDir)
	assert.EqualError(t, err, errMsg)
}

func newLocalVolumeSource(path string) corev1.PersistentVolumeSource {
	return corev1.PersistentVolumeSource{
		Local: &corev1.LocalVolumeSource{
			Path: path,
		},
	}
}

func TestGetFuseMountPointHostPathForLocalPV(t *testing.T) {
	kubeObjects := []runtime.Object{}

	pvcNamespace := metav1.NamespaceDefault
	pvcName := "pvc1"
	pvName := "pv1"

	localPath := "/mnt/fuse/targets/local/test1"
	localPv := newLocalVolumeSource(localPath)

	pv := newPersistentVolume(pvName, localPv, pvcNamespace, pvcName)
	kubeObjects = append(kubeObjects, pv)

	pvc := newPersistentVolumeClaim(pvcNamespace, pvcName, pvName)
	kubeObjects = append(kubeObjects, pvc)

	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	fuseMountPointRootDir := "/mnt/fuse/targets"
	volumeOptions := &VolumeOptions{
		LocalFuseSourcesHostDir: "/mnt/fuse/sources/local",
	}
	plugins := NewPlugins(fuseMountPointRootDir, volumeOptions)

	mountPoint, err := plugins.GetMountPointHostPathFromPvcVolumeSource(
		pvcName, kubeClient, pvcNamespace)
	assert.NoError(t, err)

	expectedMountPoint := localPath
	assert.Equal(t, expectedMountPoint, mountPoint)
}

func TestGetFuseMountPointHostPathForLocalPVInInvalidLocalPath(t *testing.T) {
	kubeObjects := []runtime.Object{}

	pvcNamespace := metav1.NamespaceDefault
	pvcName := "pvc1"
	pvName := "pv1"

	localPath := "/var/local/dsc/fuse/vol1"
	localPv := newLocalVolumeSource(localPath)

	pv := newPersistentVolume(pvName, localPv, pvcNamespace, pvcName)
	kubeObjects = append(kubeObjects, pv)

	pvc := newPersistentVolumeClaim(pvcNamespace, pvcName, pvName)
	kubeObjects = append(kubeObjects, pvc)

	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	fuseMountPointRootDir := "/mnt/fuse/targets"
	volumeOptions := &VolumeOptions{
		LocalFuseSourcesHostDir: "/mnt/fuse/sources/local",
	}
	plugins := NewPlugins(fuseMountPointRootDir, volumeOptions)

	_, err := plugins.GetMountPointHostPathFromPvcVolumeSource(
		pvcName, kubeClient, pvcNamespace)
	assert.Error(t, err)

	errMsg := fmt.Sprintf(
		"Local host path %q must be a directory under the root of "+
			"the fuse mount point for local storage %q",
		localPath,
		filepath.Join(fuseMountPointRootDir, fuseMountPointsLocalDirName))
	assert.EqualError(t, err, errMsg)
}
