// Copyright (c) 2022 Fujitsu Limited

package csi

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/stretchr/testify/assert"
)

func newPersistentVolumeClaim(
	namespace string, name string,
	pvName string, labels map[string]string) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: pvName,
		},
	}

	if labels != nil {
		pvc.Labels = labels
	}

	return pvc
}

func newCsiControllerClient(
	pvcNamespace string, pvcName string, pvName string,
	labels map[string]string) *CsiControllerClient {
	pvc := newPersistentVolumeClaim(pvcNamespace, pvcName, pvName, labels)
	kubeObjects := []k8sruntime.Object{
		pvc,
	}

	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	return &CsiControllerClient{
		kubeClient: kubeClient,
	}
}

func TestUsageControlAndDataExportAreEnabled(t *testing.T) {
	pvcNamespace := metav1.NamespaceDefault
	pvcName := "pvc-1"
	pvName := "pc-1"

	labels := map[string]string{
		UsageControlKey:        "true",
		MessageQueuePublishKey: "true",
	}

	csiClient := newCsiControllerClient(pvcNamespace, pvcName, pvName, labels)

	usageControl, dataExport, err := csiClient.
		AreUsageControlAndDataExportEnabled(pvcNamespace, pvcName)
	assert.NoError(t, err)

	assert.True(t, usageControl)
	assert.True(t, dataExport)
}

func TestUsageControlIsEnabled(t *testing.T) {
	pvcNamespace := metav1.NamespaceDefault
	pvcName := "pvc-1"
	pvName := "pc-1"

	labels := map[string]string{
		UsageControlKey: "true",
	}

	csiClient := newCsiControllerClient(pvcNamespace, pvcName, pvName, labels)

	usageControl, dataExport, err := csiClient.
		AreUsageControlAndDataExportEnabled(pvcNamespace, pvcName)
	assert.NoError(t, err)

	assert.True(t, usageControl)
	assert.False(t, dataExport)
}

func TestDataExportIsEnabled(t *testing.T) {
	pvcNamespace := metav1.NamespaceDefault
	pvcName := "pvc-1"
	pvName := "pc-1"

	labels := map[string]string{
		MessageQueuePublishKey: "true",
	}

	csiClient := newCsiControllerClient(pvcNamespace, pvcName, pvName, labels)

	usageControl, dataExport, err := csiClient.
		AreUsageControlAndDataExportEnabled(pvcNamespace, pvcName)
	assert.NoError(t, err)

	assert.False(t, usageControl)
	assert.True(t, dataExport)
}
