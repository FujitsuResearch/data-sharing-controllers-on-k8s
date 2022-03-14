// Copyright (c) 2022 Fujitsu Limited

package plugins

import (
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

type pluginOperations interface {
	generateFuseMountPointHostPath(req *api.InitializeRequest) (string, error)
	getFuseMountPointHostPath(pv *corev1.PersistentVolume) (string, error)
}

type Plugins struct {
	plugins []pluginOperations
}

func NewPlugins(
	fuseMountPointRootDir string, options *VolumeOptions) *Plugins {
	return &Plugins{
		plugins: []pluginOperations{
			newCsiPersistentVolume(
				fuseMountPointRootDir, options.CsiPluginsDir),
			newLocalPersistentVolume(
				fuseMountPointRootDir, options.LocalFuseSourcesHostDir),
		},
	}
}

func (p *Plugins) GenerateMountPointHostPathFromInitializeRequest(
	req *api.InitializeRequest) (string, error) {
	for _, plugin := range p.plugins {
		mountPointPath, err := plugin.generateFuseMountPointHostPath(req)
		if mountPointPath != "" {
			return mountPointPath, nil
		} else if err != nil {
			return "", err
		}
	}

	return "", fmt.Errorf(
		"Could not generate the mount point path for "+
			"the request of the volume control initialization: %+v",
		req)
}

func (p *Plugins) DeleteMountPoint(
	mountPointDir string) error {
	err := os.Remove(mountPointDir)
	if err != nil {
		return fmt.Errorf(
			"Could not delete the fuse mount point %q: %v", mountPointDir, err)
	}

	return nil
}

func (p *Plugins) GetMountPointHostPathFromPvcVolumeSource(
	claimName string, kubeClient kubernetes.Interface, namespace string) (
	string, error) {
	ctxPvc, cancelPvc := util.GetTimeoutContext(util.DefaultKubeClientTimeout)
	defer cancelPvc()

	pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(
		ctxPvc, claimName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf(
			"Not get the persistent volume claim: %v", err.Error())
	}

	ctxPv, cancelPv := util.GetTimeoutContext(util.DefaultKubeClientTimeout)
	defer cancelPv()

	pv, err := kubeClient.CoreV1().PersistentVolumes().Get(
		ctxPv, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf(
			"Not get the persistent volume: %v", err.Error())
	}

	for _, plugin := range p.plugins {
		mountPointPath, err := plugin.getFuseMountPointHostPath(pv)
		if mountPointPath != "" {
			return mountPointPath, nil
		} else if err != nil {
			return "", err
		}
	}

	return "", nil
}
