// Copyright (c) 2022 Fujitsu Limited

package plugins

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"

	"github.com/stretchr/testify/assert"
)

func TestGenerateMountPointHostPathFromInitializeRequestForPluginNotFound(
	t *testing.T) {
	fuseMountPointRootDir := "/mnt/fuse/targets"
	plugins := NewPlugins(fuseMountPointRootDir, &VolumeOptions{})

	initRequest := &api.InitializeRequest{}
	_, err := plugins.GenerateMountPointHostPathFromInitializeRequest(
		initRequest)

	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"Could not generate the mount point path for "+
			"the request of the volume control initialization: %+v",
		initRequest)
	assert.EqualError(t, err, errMsg)
}

func newFakeVolumeSource() corev1.PersistentVolumeSource {
	return corev1.PersistentVolumeSource{}
}

func newPersistentVolume(
	name string, pvSource corev1.PersistentVolumeSource, pvcNamespace string,
	pvcName string) *corev1.PersistentVolume {
	if pvcName == "" {
		return &corev1.PersistentVolume{
			TypeMeta: metav1.TypeMeta{
				APIVersion: corev1.SchemeGroupVersion.String(),
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					corev1.ResourceRequestsStorage: *resource.NewQuantity(
						5, resource.BinarySI),
				},
				PersistentVolumeSource: pvSource,
			},
		}
	} else {
		return &corev1.PersistentVolume{
			TypeMeta: metav1.TypeMeta{
				APIVersion: corev1.SchemeGroupVersion.String(),
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					corev1.ResourceRequestsStorage: *resource.NewQuantity(
						5, resource.BinarySI),
				},
				PersistentVolumeSource: pvSource,
				ClaimRef: &corev1.ObjectReference{
					APIVersion: corev1.SchemeGroupVersion.String(),
					Kind:       "PersistentVolumeClaim",
					Name:       pvcName,
					Namespace:  pvcNamespace,
				},
			},
		}
	}
}

func newPersistentVolumeClaim(
	namespace string, name string,
	pvName string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
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
}

func TestGetMountPointHostPathFromPvcVolumeSourceForPluginNotFound(
	t *testing.T) {
	kubeObjects := []runtime.Object{}

	pvcNamespace := metav1.NamespaceDefault
	pvcName := "pvc1"
	pvName := "pv1"

	fakePv := newFakeVolumeSource()

	pv := newPersistentVolume(pvName, fakePv, "", "")
	kubeObjects = append(kubeObjects, pv)

	pvc := newPersistentVolumeClaim(pvcNamespace, pvcName, pvName)
	kubeObjects = append(kubeObjects, pvc)

	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	fuseMountPointRootDir := "/mnt/fuse/targets"
	plugins := NewPlugins(fuseMountPointRootDir, &VolumeOptions{})

	mountPoint, err := plugins.GetMountPointHostPathFromPvcVolumeSource(
		pvcName, kubeClient, pvcNamespace)

	assert.NoError(t, err)
	assert.Equal(t, "", mountPoint)
}

func TestGetMountPointHostPathFromPvcVolumeSourceForPvcNotFound(t *testing.T) {
	kubeObjects := []runtime.Object{}

	pvcNamespace := metav1.NamespaceDefault
	pvcName := "pvc1"
	pvName := "pv1"

	localPath := "/var/local/dsc/fuse/vol1"
	localPv := newLocalVolumeSource(localPath)

	pv := newPersistentVolume(pvName, localPv, pvcNamespace, pvcName)
	kubeObjects = append(kubeObjects, pv)

	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	fuseMountPointRootDir := "/mnt/fuse/targets"
	plugins := NewPlugins(fuseMountPointRootDir, &VolumeOptions{})

	_, err := plugins.GetMountPointHostPathFromPvcVolumeSource(
		pvcName, kubeClient, pvcNamespace)

	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"Not get the persistent volume claim: "+
			"persistentvolumeclaims %q not found",
		pvcName)
	assert.EqualError(t, err, errMsg)
}

func TestGetMountPointHostPathFromPvcVolumeSourceForPvNotFund(t *testing.T) {
	kubeObjects := []runtime.Object{}

	pvcNamespace := metav1.NamespaceDefault
	pvcName := "pvc1"
	pvName := "pv1"

	pvc := newPersistentVolumeClaim(pvcNamespace, pvcName, pvName)
	kubeObjects = append(kubeObjects, pvc)

	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	fuseMountPointRootDir := "/mnt/fuse/targets"
	plugins := NewPlugins(fuseMountPointRootDir, &VolumeOptions{})

	_, err := plugins.GetMountPointHostPathFromPvcVolumeSource(
		pvcName, kubeClient, pvcNamespace)

	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"Not get the persistent volume: persistentvolumes %q not found",
		pvName)
	assert.EqualError(t, err, errMsg)
}
