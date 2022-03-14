// Copyright (c) 2022 Fujitsu Limited

package filesystem

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8scorev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	coreapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/core/v1alpha1"
	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	corefake "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned/fake"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"

	"github.com/stretchr/testify/assert"
)

const (
	kafkaBrokerAddressEnv = "KAFKA_BROKER_ADDRESS"

	// [REF] github.com/segmentio/kafka-go/docker-compose.yml
	saslUser     = "adminscram"
	saslPassword = "admin-secret-512"
)

var (
	podNamespace      = "ns1"
	podName           = "pod1"
	dataContainerName = "data-c"
	providerPvcName   = "provider_claim_1"
	consumerPvcName   = "consumer_claim_1"
	consumerDataName  = "consumer_data_1"

	podSourceVolume          = "pod_source_volume_1"
	podLocalVolume           = "pod_local_volume_1"
	podOutputVolume          = "pod_output_volume_1"
	sourceRootDirectory      = "testing/fixtures/src"
	sourceDummyRootDirectory = "testing/fixtures/src_dummy"
	sourceFile1              = "source_1.txt"
	sourceFile2              = "source_2.txt"
	sourceFile3Directory     = "dir"
	sourceFile3              = filepath.Join(
		sourceFile3Directory, "file_3.txt")
	destinationRootDirectory      = "testing/fixtures/dst"
	destinationDummyRootDirectory = "testing/fixtures/dst_dummy"
	destinationFilePath1          = filepath.Join(
		destinationRootDirectory, sourceFile1)
	destinationFilePath2 = filepath.Join(
		destinationRootDirectory, sourceFile2)
	destinationFilePath3 = filepath.Join(
		destinationRootDirectory, sourceFile3)
)

type fileSystemDeleter interface {
	Delete(deletePolicy lifetimesapi.DeletePolicy) error
}

func chtimes(t *testing.T, paths []string, aTime time.Time, mTime time.Time) {
	for _, path := range paths {
		err := os.Chtimes(path, aTime, mTime)
		assert.NoError(t, err)
	}
}

func exists(t *testing.T, paths []string) {
	for _, path := range paths {
		_, err := os.Stat(path)
		assert.NoError(t, err)
	}
}

func existsWithMode(t *testing.T, paths []string, mode os.FileMode) {
	for _, path := range paths {
		fileInfo, err := os.Stat(path)
		assert.NoError(t, err)
		assert.Equal(t, fileInfo.Mode(), mode)
	}
}

func notExists(t *testing.T, paths []string) {
	for _, path := range paths {
		_, err := os.Stat(path)
		assert.Error(t, err)
		assert.True(t, os.IsNotExist(err))
	}
}

func createDirectories(t *testing.T, paths []string) {
	for _, path := range paths {
		err := os.MkdirAll(path, 0755)
		assert.NoError(t, err)
	}
}

func createFiles(t *testing.T, paths []string) {
	for _, path := range paths {
		file, err := os.Create(path)
		assert.NoError(t, err)
		assert.NoError(t, file.Close())
	}
}

func chmods(t *testing.T, paths []string, mode os.FileMode) {
	for _, path := range paths {
		err := os.Chmod(path, mode)
		assert.NoError(t, err)
	}
}

func createTestFileSet(t *testing.T, directories []string, files []string) {
	createDirectories(t, directories)

	createFiles(t, files)
}

func deleteTopics(t *testing.T, outputPvcName string) {
	nameElements := []string{
		metav1.NamespaceDefault,
		outputPvcName,
	}
	fsTopic := messagequeue.NewMessageQueueFileSystemTopic(nameElements)
	initTopics, err := fsTopic.CreateInitTopics()
	assert.NoError(t, err)

	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	config := messagequeue.NewMessageQueueConfig(nil, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)
	topics := []string{
		initTopics.Request,
		initTopics.Response,
	}

	err = messagequeue.DeleteTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
}

func fileSystemDeleteDirectory(
	t *testing.T, outputPvcName string,
	deletePolicy lifetimesapi.DeletePolicy) {
	lifetimeDeletePolicy := lifetimesapi.DefaultPolicy
	if deletePolicy != lifetimesapi.DefaultPolicy {
		lifetimeDeletePolicy = deletePolicy
	}

	var fsd fileSystemDeleter
	if outputPvcName == "" {
		fsd = prepareInputDataLifetimer(t)
	} else {
		fsd = prepareOutputDataLifetimer(t, outputPvcName)
		defer deleteTopics(t, outputPvcName)
	}

	destinationPaths := []string{
		destinationFilePath1,
		destinationFilePath2,
		destinationFilePath3,
	}

	notExists(t, destinationPaths)

	err := os.MkdirAll(
		filepath.Join(destinationRootDirectory, sourceFile3Directory), 0755)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.RemoveAll(destinationRootDirectory))
	}()

	createFiles(t, destinationPaths)

	err = fsd.Delete(lifetimeDeletePolicy)
	assert.NoError(t, err)

	switch deletePolicy {
	case lifetimesapi.DefaultPolicy:
		fallthrough
	case lifetimesapi.ChmodPolicy:
		existsWithMode(
			t, []string{destinationRootDirectory}, os.FileMode(os.ModeDir|0755))

		existsWithMode(
			t,
			[]string{
				filepath.Join(destinationRootDirectory, sourceFile3Directory),
			},
			os.FileMode(os.ModeDir|0000))
		chmods(
			t,
			[]string{
				filepath.Join(destinationRootDirectory, sourceFile3Directory),
			},
			0755)

		existsWithMode(t, destinationPaths, 0000)

		chmods(t, destinationPaths, 0644)

		assert.NoError(t, os.RemoveAll(
			filepath.Join(destinationRootDirectory, sourceFile3Directory)))
		assert.NoError(t, os.Remove(destinationFilePath1))
		assert.NoError(t, os.Remove(destinationFilePath2))

	case lifetimesapi.RemovePolicy:
		exists(t, []string{destinationRootDirectory})

		notExists(
			t, []string{
				filepath.Join(destinationRootDirectory, sourceFile3Directory),
			})

		notExists(t, destinationPaths)

	default:
		assert.Failf(
			t, "Not support such a delete policy: %q", string(deletePolicy))
	}
}

func newPod(
	namespace string, name string, dataContainerName string,
	volumeMounts []corev1.VolumeMount, volumes []corev1.Volume) *corev1.Pod {
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:         dataContainerName,
					VolumeMounts: volumeMounts,
				},
			},
			Volumes: volumes,
		},
	}

	for _, volumeMount := range volumeMounts {
		pod.Spec.Volumes = append(
			pod.Spec.Volumes,
			corev1.Volume{
				Name: volumeMount.Name,
			})
	}

	return pod
}

func newMessageQueue(
	namespace string, name string, saslNamespace string,
	saslName string) *coreapi.MessageQueue {
	brokers := []string{
		os.Getenv(kafkaBrokerAddressEnv),
	}
	return &coreapi.MessageQueue{
		TypeMeta: metav1.TypeMeta{
			APIVersion: coreapi.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: coreapi.MessageQueueSpec{
			Brokers: brokers,
			SaslRef: &corev1.ObjectReference{
				Name:      saslName,
				Namespace: saslNamespace,
			},
		},
	}
}

func newDataClient(
	mqNamespace string, mqName string) coreclientset.Interface {
	saslNamespace := metav1.NamespaceDefault
	saslName := saslUser

	messageQueueObject := newMessageQueue(
		mqNamespace, mqName, saslNamespace, saslName)
	messageQueueDataObjects := []runtime.Object{
		messageQueueObject,
	}

	return corefake.NewSimpleClientset(messageQueueDataObjects...)
}

func newSecret(
	namespace string, name string, data map[string][]byte) *k8scorev1.Secret {
	return &k8scorev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: k8scorev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: data,
	}
}

func newKubeCliet() kubernetes.Interface {
	secretNamespace := metav1.NamespaceDefault
	secretName := saslUser
	saslData := map[string][]byte{
		"password": []byte(saslPassword),
	}
	secretObject := newSecret(secretNamespace, secretName, saslData)

	kubeObjects := []runtime.Object{
		secretObject,
	}

	return k8sfake.NewSimpleClientset(kubeObjects...)
}
