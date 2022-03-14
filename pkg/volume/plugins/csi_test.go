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

func TestGenerateFuseMountPointHostPathForCsiPV(t *testing.T) {
	currentDir, err := os.Getwd()
	assert.NoError(t, err)

	fuseMountPointRootDir := filepath.Join(currentDir, "targets")
	volumeOptions := &VolumeOptions{
		CsiPluginsDir: "/var/lib/kubelet/plugins",
	}
	plugins := NewPlugins(fuseMountPointRootDir, volumeOptions)

	initRequest := &api.InitializeRequest{
		FuseSourcePath:  "/var/lib/kubelet/plugins/ceph-csi/test1",
		CsiVolumeHandle: "volume-handle-1",
	}
	mountPoint, err := plugins.GenerateMountPointHostPathFromInitializeRequest(
		initRequest)
	assert.NoError(t, err)

	expectedMountPoint := filepath.Join(
		fuseMountPointRootDir, fuseMountPointsCsiDirName,
		initRequest.CsiVolumeHandle)
	assert.Equal(t, expectedMountPoint, mountPoint)

	err = plugins.DeleteMountPoint(mountPoint)
	assert.NoError(t, err)

	csiRootDir := filepath.Join(
		fuseMountPointRootDir, fuseMountPointsCsiDirName)
	_, err = os.Stat(csiRootDir)
	assert.False(t, os.IsNotExist(err))

	err = os.RemoveAll(fuseMountPointRootDir)
	assert.NoError(t, err)
}

func TestGenerateFuseMountPointHostPathForCsiPVInInvalidFuseSourcePath(
	t *testing.T) {
	fuseMountPointRootDir := "/mnt/fuse/targets"
	volumeOptions := &VolumeOptions{
		CsiPluginsDir: "/var/lib/kubelet/plugins/",
	}
	plugins := NewPlugins(fuseMountPointRootDir, volumeOptions)

	initRequest := &api.InitializeRequest{
		FuseSourcePath:  "/mnt/fuse/sources/csi/test1",
		CsiVolumeHandle: "volume-handle-1",
	}
	_, err := plugins.GenerateMountPointHostPathFromInitializeRequest(
		initRequest)

	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"Fuse source path %q must be subdirectory of %q",
		initRequest.FuseSourcePath, volumeOptions.CsiPluginsDir)
	assert.EqualError(t, err, errMsg)
}

func newCsiVolumeSource(volumeHandle string) corev1.PersistentVolumeSource {
	return corev1.PersistentVolumeSource{
		CSI: &corev1.CSIPersistentVolumeSource{
			VolumeHandle: volumeHandle,
		},
	}
}

func TestGetFuseMountPointHostPathForCsiPV(t *testing.T) {
	kubeObjects := []runtime.Object{}

	pvcNamespace := metav1.NamespaceDefault
	pvcName := "pvc1"
	pvName := "pv1"

	volumeHandle := "volume-handle-1"
	localPv := newCsiVolumeSource(volumeHandle)

	pv := newPersistentVolume(pvName, localPv, pvcNamespace, pvcName)
	kubeObjects = append(kubeObjects, pv)

	pvc := newPersistentVolumeClaim(pvcNamespace, pvcName, pvName)
	kubeObjects = append(kubeObjects, pvc)

	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	fuseMountPointRootDir := "/mnt/fuse/targets"
	volumeOptions := &VolumeOptions{
		CsiPluginsDir: "/var/lib/kubelet/plugins/ceph-csi",
	}
	plugins := NewPlugins(fuseMountPointRootDir, volumeOptions)

	mountPoint, err := plugins.GetMountPointHostPathFromPvcVolumeSource(
		pvcName, kubeClient, pvcNamespace)
	assert.NoError(t, err)

	expectedMountPoint := filepath.Join(
		fuseMountPointRootDir, fuseMountPointsCsiDirName, volumeHandle)
	assert.Equal(t, expectedMountPoint, mountPoint)
}
