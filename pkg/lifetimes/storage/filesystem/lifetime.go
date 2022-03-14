// Copyright (c) 2022 Fujitsu Limited

package filesystem

import (
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
)

type volumeController struct {
	disableUsageControl      bool
	endpoint                 string
	podNamespace             string
	podName                  string
	dataContainerName        string
	persistentVolumeClaimKey string
}

type deletedChmodData struct {
	directories []string
	root        string
}

type deletedRemoveData struct {
	root string
}

func hasPersistentVolumeClaim(
	persistentVolumeClaimName string, pod *corev1.Pod) error {

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			claimName := volume.PersistentVolumeClaim.ClaimName
			if claimName == persistentVolumeClaimName {
				return nil
			}
		}
	}
	return fmt.Errorf(
		"Not find the persistent volume claim: %v", persistentVolumeClaimName)
}

func findDataCntainer(pod *corev1.Pod, name string) (corev1.Container, bool) {
	for _, container := range pod.Spec.Containers {
		if container.Name == name {
			return container, true
		}
	}

	return corev1.Container{}, false
}

func createPersistentVolumeClaimToIndex(
	volumes []corev1.Volume, container corev1.Container) map[string]int {
	volumeNameToPvc := map[string]string{}
	for _, volume := range volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		volumeNameToPvc[volume.Name] = volume.PersistentVolumeClaim.ClaimName
	}

	volumeMountToIndex := map[string]int{}

	for index, volumeMount := range container.VolumeMounts {
		if pvc, ok := volumeNameToPvc[volumeMount.Name]; ok {
			volumeMountToIndex[pvc] = index
		}
	}

	return volumeMountToIndex
}

func (dcd *deletedChmodData) chmodWalkFunc(
	path string, sourceInfo os.FileInfo, err error) error {
	if err != nil {
		return fmt.Errorf(
			"prevent panic by handling failure accessing a local path %q: %v",
			path, err)
	}

	if sourceInfo.IsDir() {
		if dcd.root != path {
			dcd.directories = append(dcd.directories, path)
		}

		return nil
	}

	err = os.Chmod(path, 0000)
	if err != nil {
		return fmt.Errorf("[path: %s] %s", path, err.Error())
	}

	return nil
}

func (drd *deletedRemoveData) removeFilesWalkFunc(
	path string, sourceInfo os.FileInfo, err error) error {
	if drd.root == path {
		if sourceInfo.IsDir() {
			return nil
		}
	}

	return os.RemoveAll(path)
}
