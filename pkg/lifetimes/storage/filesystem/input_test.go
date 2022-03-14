// Copyright (c) 2022 Fujitsu Limited

package filesystem

import (
	"fmt"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"

	"github.com/stretchr/testify/assert"
)

func newMessageQueueSpec(
	mqNamespace string, mqName string) *lifetimesapi.MessageQueueSpec {
	return &lifetimesapi.MessageQueueSpec{
		MessageQueueRef: &corev1.ObjectReference{
			Namespace: mqNamespace,
			Name:      mqName,
		},
	}
}

func prepareInputDataLifetimer(t *testing.T) *FileSystemInputLifetimer {
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      podSourceVolume,
			MountPath: sourceRootDirectory,
		},
		{
			Name:      podLocalVolume,
			MountPath: destinationRootDirectory,
		},
	}
	volumes := []corev1.Volume{
		{
			Name: podSourceVolume,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: providerPvcName,
				},
			},
		},
		{
			Name: podLocalVolume,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName,
				},
			},
		},
	}
	pod := newPod(
		podNamespace, podName, dataContainerName, volumeMounts, volumes)

	lifetimeFileSystemSpec := &lifetimesapi.InputFileSystemSpec{
		FromPersistentVolumeClaimRef: &corev1.ObjectReference{
			Namespace: metav1.NamespaceDefault,
			Name:      providerPvcName,
		},
		ToPersistentVolumeClaimRef: &corev1.ObjectReference{
			Namespace: metav1.NamespaceDefault,
			Name:      consumerPvcName,
		},
	}

	lifetimeStatus := lifetimesapi.ConsumerStatus{}

	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"

	dataClient := newDataClient(mqNamespace, mqName)
	kubeClient := newKubeCliet()
	messageQueueSpec := newMessageQueueSpec(mqNamespace, mqName)

	fsil, err := NewFileSystemLifetimerForInputData(
		lifetimeFileSystemSpec, pod, dataClient, kubeClient, messageQueueSpec,
		&lifetimeStatus, false, "", podNamespace, podName, dataContainerName)
	assert.NoError(t, err)

	return fsil
}

func TestInputFileSystemDeleteDirectoryForDefaultPolicy(t *testing.T) {
	fileSystemDeleteDirectory(t, "", lifetimesapi.DefaultPolicy)
}

func TestInputFileSystemDeleteDirectoryForChmodPolicy(t *testing.T) {
	fileSystemDeleteDirectory(t, "", lifetimesapi.ChmodPolicy)
}

func TestInputFileSystemDeleteDirectoryForRemovePolicy(t *testing.T) {
	fileSystemDeleteDirectory(t, "", lifetimesapi.RemovePolicy)
}

func TestInputFileSystemDeleteForDirectoryNotFound(t *testing.T) {
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      podSourceVolume,
			MountPath: sourceDummyRootDirectory,
		},
		{
			Name:      podLocalVolume,
			MountPath: destinationDummyRootDirectory,
		},
	}
	volumes := []corev1.Volume{
		{
			Name: podSourceVolume,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: providerPvcName,
				},
			},
		},
		{
			Name: podLocalVolume,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName,
				},
			},
		},
	}
	pod := newPod(
		podNamespace, podName, dataContainerName, volumeMounts, volumes)

	lifetimeFileSystemSpec := &lifetimesapi.InputFileSystemSpec{
		FromPersistentVolumeClaimRef: &corev1.ObjectReference{
			Namespace: metav1.NamespaceDefault,
			Name:      providerPvcName,
		},
		ToPersistentVolumeClaimRef: &corev1.ObjectReference{
			Namespace: metav1.NamespaceDefault,
			Name:      consumerPvcName,
		},
	}
	lifetimeStatus := lifetimesapi.ConsumerStatus{}

	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"

	dataClient := newDataClient(mqNamespace, mqName)
	kubeClient := newKubeCliet()
	messageQueueSpec := newMessageQueueSpec(mqNamespace, mqName)

	fsl, err := NewFileSystemLifetimerForInputData(
		lifetimeFileSystemSpec, pod, dataClient, kubeClient, messageQueueSpec,
		&lifetimeStatus, false, "", podNamespace, podName, dataContainerName)
	assert.NoError(t, err)

	destinationPaths := []string{
		destinationFilePath1,
		destinationFilePath2,
	}

	notExists(t, destinationPaths)

	directories := []string{
		destinationRootDirectory,
	}
	createTestFileSet(t, directories, destinationPaths)
	defer func() {
		assert.NoError(t, os.RemoveAll(destinationRootDirectory))
	}()

	err = fsl.Delete(lifetimesapi.DefaultPolicy)
	assert.Error(t, err)
	assert.EqualError(
		t, err,
		fmt.Sprintf(
			"[Failed to change the mode] prevent panic by handling failure "+
				"accessing a local path %q: lstat %s: no such file or "+
				"directory",
			destinationDummyRootDirectory, destinationDummyRootDirectory))
}
