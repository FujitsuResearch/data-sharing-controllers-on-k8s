// Copyright (c) 2022 Fujitsu Limited

package filesystem

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"

	"github.com/stretchr/testify/assert"
)

func newPublishMessageQueueSpec(
	mqNamespace string, mqName string) *lifetimesapi.PublishMessageQueueSpec {
	return &lifetimesapi.PublishMessageQueueSpec{
		MessageQueueSpec: &lifetimesapi.MessageQueueSpec{
			MessageQueueRef: &corev1.ObjectReference{
				Namespace: mqNamespace,
				Name:      mqName,
			},
		},
		UpdatePublishChannelBufferSize: 100,
	}
}

func prepareOutputDataLifetimer(
	t *testing.T, outputPvcName string) *FileSystemOutputLifetimer {
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      podOutputVolume,
			MountPath: destinationRootDirectory,
		},
	}
	volumes := []corev1.Volume{
		{
			Name: podOutputVolume,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: outputPvcName,
				},
			},
		},
	}
	pod := newPod(
		podNamespace, podName, dataContainerName, volumeMounts, volumes)

	lifetimeFileSystemSpec := &lifetimesapi.OutputFileSystemSpec{
		PersistentVolumeClaimRef: &corev1.ObjectReference{
			Namespace: metav1.NamespaceDefault,
			Name:      outputPvcName,
		},
	}

	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"

	dataClient := newDataClient(mqNamespace, mqName)
	kubeClient := newKubeCliet()
	messageQueueSpec := newPublishMessageQueueSpec(mqNamespace, mqName)
	messageQueuePublisherOptions := &publish.MessageQueuePublisherOptions{
		CompressionCodec: publish.CompressionCodecNone,
	}

	fsol, err := NewFileSystemLifetimerForOutputData(
		lifetimeFileSystemSpec, pod, dataClient, kubeClient, messageQueueSpec,
		messageQueuePublisherOptions, false, "", podNamespace, podName,
		dataContainerName)
	assert.NoError(t, err)

	return fsol
}

func TestOutputFileSystemDeleteDirectoryForDefaultPolicy(t *testing.T) {
	outputPvcName := "output_claim_1_1"
	fileSystemDeleteDirectory(t, outputPvcName, lifetimesapi.DefaultPolicy)
}

func TestOutputFileSystemDeleteDirectoryForChmodPolicy(t *testing.T) {
	outputPvcName := "output_claim_1_2"
	fileSystemDeleteDirectory(t, outputPvcName, lifetimesapi.ChmodPolicy)
}

func TestOutputFileSystemDeleteDirectoryForRemovePolicy(t *testing.T) {
	outputPvcName := "output_claim_1_3"
	fileSystemDeleteDirectory(t, outputPvcName, lifetimesapi.RemovePolicy)
}

func TestFileSystemLifetimerForOutputDataForPvcNotFound(t *testing.T) {
	podCsiVolume := "pod_csi_volume_1"
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      podCsiVolume,
			MountPath: destinationRootDirectory,
		},
	}
	volumes := []corev1.Volume{
		{
			Name: podCsiVolume,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: providerPvcName,
				},
			},
		},
	}

	pod := newPod(
		podNamespace, podName, dataContainerName, volumeMounts, volumes)

	lifetimeFileSystemSpec := &lifetimesapi.OutputFileSystemSpec{
		PersistentVolumeClaimRef: &corev1.ObjectReference{
			Namespace: metav1.NamespaceDefault,
			Name:      consumerPvcName,
		},
	}

	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"

	dataClient := newDataClient(mqNamespace, mqName)
	kubeClient := newKubeCliet()
	messageQueueSpec := newPublishMessageQueueSpec(mqNamespace, mqName)
	messageQueuePublisherOptions := &publish.MessageQueuePublisherOptions{
		CompressionCodec: publish.CompressionCodecNone,
	}

	volumeControllerEndpoint := "unix:///var/run/dsc/usage-controller.sock"
	_, err := NewFileSystemLifetimerForOutputData(
		lifetimeFileSystemSpec, pod, dataClient, kubeClient, messageQueueSpec,
		messageQueuePublisherOptions, false, volumeControllerEndpoint,
		podNamespace, podName, dataContainerName)
	assert.Error(t, err)

	assert.EqualError(
		t, err,
		fmt.Sprintf(
			"Not find the persistent volume claim: %v", consumerPvcName))
}

func TestFileSystemLifetimerForOutputDataForDataContainerNotFound(
	t *testing.T) {
	dummyDataContainer := "dummy-c"

	podCsiVolume := "pod_csi_volume_1"
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      podCsiVolume,
			MountPath: destinationRootDirectory,
		},
	}
	volumes := []corev1.Volume{
		{
			Name: podCsiVolume,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName,
				},
			},
		},
	}

	pod := newPod(
		podNamespace, podName, dummyDataContainer, volumeMounts, volumes)

	lifetimeFileSystemSpec := &lifetimesapi.OutputFileSystemSpec{
		PersistentVolumeClaimRef: &corev1.ObjectReference{
			Namespace: metav1.NamespaceDefault,
			Name:      consumerPvcName,
		},
	}

	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"

	dataClient := newDataClient(mqNamespace, mqName)
	kubeClient := newKubeCliet()
	messageQueueSpec := newPublishMessageQueueSpec(mqNamespace, mqName)
	messageQueuePublisherOptions := &publish.MessageQueuePublisherOptions{
		CompressionCodec: publish.CompressionCodecNone,
	}

	volumeControllerEndpoint := "unix:///var/run/dsc/usage-controller.sock"
	_, err := NewFileSystemLifetimerForOutputData(
		lifetimeFileSystemSpec, pod, dataClient, kubeClient, messageQueueSpec,
		messageQueuePublisherOptions, false, volumeControllerEndpoint,
		podNamespace, podName, dataContainerName)
	assert.Error(t, err)

	assert.EqualError(
		t, err,
		fmt.Sprintf(
			"The pod does not have the data contaniner %q", dataContainerName))
}

func TestFileSystemLifetimerForOutputDataForVolumeMountNotFound(t *testing.T) {
	podCsiVolume := "pod_csi_volume_1"
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      podCsiVolume,
			MountPath: destinationRootDirectory,
		},
	}
	volumes := []corev1.Volume{
		{
			Name: podCsiVolume,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: providerPvcName,
				},
			},
		},
	}

	pod := newPod(
		podNamespace, podName, dataContainerName, volumeMounts, volumes)

	lifetimeFileSystemSpec := &lifetimesapi.OutputFileSystemSpec{
		PersistentVolumeClaimRef: &corev1.ObjectReference{
			Namespace: metav1.NamespaceDefault,
			Name:      consumerPvcName,
		},
	}

	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"

	dataClient := newDataClient(mqNamespace, mqName)
	kubeClient := newKubeCliet()
	messageQueueSpec := newPublishMessageQueueSpec(mqNamespace, mqName)
	messageQueuePublisherOptions := &publish.MessageQueuePublisherOptions{
		CompressionCodec: publish.CompressionCodecNone,
	}

	volumeControllerEndpoint := "unix:///var/run/dsc/usage-controller.sock"
	_, err := NewFileSystemLifetimerForOutputData(
		lifetimeFileSystemSpec, pod, dataClient, kubeClient, messageQueueSpec,
		messageQueuePublisherOptions, false, volumeControllerEndpoint,
		podNamespace, podName, dataContainerName)
	assert.Error(t, err)

	assert.EqualError(
		t, err,
		fmt.Sprintf(
			"Not find the persistent volume claim: %v", consumerPvcName))
}
