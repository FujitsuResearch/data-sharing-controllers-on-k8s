// Copyright (c) 2022 Fujitsu Limited

package volumecontrol

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	k8scorev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	coreapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/core/v1alpha1"
	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	volumecontrolapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	corefake "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned/fake"
	mqpub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
	volumemq "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"

	"github.com/stretchr/testify/assert"
)

func newMessageQueue(
	namespace string, name string, brokers []string, saslNamespace string,
	saslName string, topicName string) *coreapi.MessageQueue {
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
	mqNamespace string, mqName string, brokers []string,
	saslNamespace string, saslName string,
	topicName string) coreclientset.Interface {
	messageQueueObject := newMessageQueue(
		mqNamespace, mqName, brokers, saslNamespace, saslName, topicName)
	messageQueueDataObjects := []runtime.Object{
		messageQueueObject,
	}

	return corefake.NewSimpleClientset(messageQueueDataObjects...)
}

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

func newKubeCliet(
	saslNamespace string, saslName string,
	password string) kubernetes.Interface {
	secretNamespace := saslNamespace
	secretName := saslName
	saslData := map[string][]byte{
		"password": []byte(password),
	}
	secretObject := newSecret(secretNamespace, secretName, saslData)

	kubeObjects := []runtime.Object{
		secretObject,
	}

	return k8sfake.NewSimpleClientset(kubeObjects...)
}

func TestCreateMessageQueueConfig(t *testing.T) {
	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"
	saslNamespace := metav1.NamespaceDefault
	saslName := "sasl-1"
	topicName := "update.topic"
	brokers := []string{"127.0.0.1:9093"}
	password := "pwd-1"

	dataClient := newDataClient(
		mqNamespace, mqName, brokers, saslNamespace, saslName, topicName)
	kubeClient := newKubeCliet(saslNamespace, saslName, password)
	messageQueueSpec := newPublishMessageQueueSpec(mqNamespace, mqName)
	publisherOptions := &mqpub.MessageQueuePublisherOptions{
		CompressionCodec: mqpub.CompressionCodecNone,
	}

	messageQueueConfig, err := CreateGrpcMessageQueue(
		dataClient, kubeClient, messageQueueSpec, topicName, publisherOptions)
	assert.NoError(t, err)

	expectedMessageQueueConfig := &volumecontrolapi.MessageQueue{
		Brokers:                        brokers,
		User:                           saslName,
		Password:                       password,
		Topic:                          topicName,
		CompressionCodec:               mqpub.CompressionCodecNone,
		MaxBatchBytes:                  "0",
		UpdatePublishChannelBufferSize: "100",
	}

	assert.Equal(t, expectedMessageQueueConfig, messageQueueConfig)
}

func TestAddMessageQueueConfig(t *testing.T) {
	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"
	saslNamespace := metav1.NamespaceDefault
	saslName := "sasl-1"
	brokers := []string{"127.0.0.1:9093"}
	password := "pwd-1"

	pvcName := "volume-1"
	messageQueueTopic := volumemq.GetMessageQueueTopic(
		metav1.NamespaceDefault, pvcName)
	topicName, err := messageQueueTopic.CreateUpdateTopic()
	assert.NoError(t, err)

	dataClient := newDataClient(
		mqNamespace, mqName, brokers, saslNamespace, saslName, topicName)
	kubeClient := newKubeCliet(saslNamespace, saslName, password)
	publisherOptions := &mqpub.MessageQueuePublisherOptions{
		CompressionCodec: mqpub.CompressionCodecNone,
	}

	oldOutputDataSpec := []lifetimesapi.OutputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: metav1.NamespaceDefault,
					Name:      pvcName,
				},
			},
		},
	}
	newOutputDataSpec := []lifetimesapi.OutputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: metav1.NamespaceDefault,
					Name:      pvcName,
				},
			},
			MessageQueuePublisher: &lifetimesapi.PublishMessageQueueSpec{
				MessageQueueSpec: &lifetimesapi.MessageQueueSpec{
					MessageQueueRef: &corev1.ObjectReference{
						Namespace: mqNamespace,
						Name:      mqName,
					},
				},
			},
		},
	}

	diffs, err := CreateGrpcMessageQueueDiffs(
		dataClient, kubeClient, publisherOptions, oldOutputDataSpec,
		newOutputDataSpec)
	assert.NoError(t, err)

	pvcKey := util.ConcatenateNamespaceAndName(
		metav1.NamespaceDefault, pvcName)
	expectedDiffs := map[string]*volumecontrolapi.MessageQueue{
		pvcKey: {
			Brokers:          brokers,
			User:             saslName,
			Password:         password,
			Topic:            topicName,
			CompressionCodec: mqpub.CompressionCodecNone,
			MaxBatchBytes:    "0",
		},
	}
	assert.Equal(t, expectedDiffs, diffs)
}

func TestDeleteMessageQueueConfig(t *testing.T) {
	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"
	saslNamespace := metav1.NamespaceDefault
	saslName := "sasl-1"
	brokers := []string{"127.0.0.1:9093"}
	password := "pwd-1"

	pvcName := "volume-1"
	messageQueueTopic := volumemq.GetMessageQueueTopic(
		metav1.NamespaceDefault, pvcName)
	topicName, err := messageQueueTopic.CreateUpdateTopic()
	assert.NoError(t, err)

	dataClient := newDataClient(
		mqNamespace, mqName, brokers, saslNamespace, saslName, topicName)
	kubeClient := newKubeCliet(saslNamespace, saslName, password)
	publisherOptions := &mqpub.MessageQueuePublisherOptions{
		CompressionCodec: mqpub.CompressionCodecNone,
	}

	oldOutputDataSpec := []lifetimesapi.OutputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: metav1.NamespaceDefault,
					Name:      pvcName,
				},
			},
			MessageQueuePublisher: &lifetimesapi.PublishMessageQueueSpec{
				MessageQueueSpec: &lifetimesapi.MessageQueueSpec{
					MessageQueueRef: &corev1.ObjectReference{
						Namespace: mqNamespace,
						Name:      mqName,
					},
				},
			},
		},
	}
	newOutputDataSpec := []lifetimesapi.OutputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: metav1.NamespaceDefault,
					Name:      pvcName,
				},
			},
		},
	}

	diffs, err := CreateGrpcMessageQueueDiffs(
		dataClient, kubeClient, publisherOptions, oldOutputDataSpec,
		newOutputDataSpec)
	assert.NoError(t, err)

	pvcKey := util.ConcatenateNamespaceAndName(
		metav1.NamespaceDefault, pvcName)
	expectedDiffs := map[string]*volumecontrolapi.MessageQueue{
		pvcKey: nil,
	}
	assert.Equal(t, expectedDiffs, diffs)
}

func TestMessageQueueConfigNotChanged(t *testing.T) {
	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"
	saslNamespace := metav1.NamespaceDefault
	saslName := "sasl-1"
	brokers := []string{"127.0.0.1:9093"}
	password := "pwd-1"

	pvcName := "volume-1"
	messageQueueTopic := volumemq.GetMessageQueueTopic(
		metav1.NamespaceDefault, pvcName)
	topicName, err := messageQueueTopic.CreateUpdateTopic()
	assert.NoError(t, err)

	dataClient := newDataClient(
		mqNamespace, mqName, brokers, saslNamespace, saslName, topicName)
	kubeClient := newKubeCliet(saslNamespace, saslName, password)
	publisherOptions := &mqpub.MessageQueuePublisherOptions{
		CompressionCodec: mqpub.CompressionCodecNone,
	}

	outputDataSpec := []lifetimesapi.OutputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: metav1.NamespaceDefault,
					Name:      pvcName,
				},
			},
			MessageQueuePublisher: &lifetimesapi.PublishMessageQueueSpec{
				MessageQueueSpec: &lifetimesapi.MessageQueueSpec{
					MessageQueueRef: &corev1.ObjectReference{
						Namespace: mqNamespace,
						Name:      mqName,
					},
				},
			},
		},
	}

	diffs, err := CreateGrpcMessageQueueDiffs(
		dataClient, kubeClient, publisherOptions, outputDataSpec,
		outputDataSpec)
	assert.NoError(t, err)

	expectedDiffs := map[string]*volumecontrolapi.MessageQueue{}
	assert.Equal(t, expectedDiffs, diffs)
}

func TestChangeInMessageQueueConfig(t *testing.T) {
	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"
	saslNamespace := metav1.NamespaceDefault
	saslName := "sasl-1"
	brokers := []string{"127.0.0.1:9093"}
	password := "pwd-1"

	pvcName := "volume-1"
	messageQueueTopic := volumemq.GetMessageQueueTopic(
		metav1.NamespaceDefault, pvcName)
	topicName, err := messageQueueTopic.CreateUpdateTopic()
	assert.NoError(t, err)

	dataClient := newDataClient(
		mqNamespace, mqName, brokers, saslNamespace, saslName, topicName)
	kubeClient := newKubeCliet(saslNamespace, saslName, password)
	publisherOptions := &mqpub.MessageQueuePublisherOptions{
		CompressionCodec: mqpub.CompressionCodecNone,
	}

	oldOutputDataSpec := []lifetimesapi.OutputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: metav1.NamespaceDefault,
					Name:      pvcName,
				},
			},
			MessageQueuePublisher: &lifetimesapi.PublishMessageQueueSpec{
				MessageQueueSpec: &lifetimesapi.MessageQueueSpec{
					MessageQueueRef: &corev1.ObjectReference{
						Namespace: mqNamespace,
						Name:      mqName,
					},
				},
			},
		},
	}
	newOutputDataSpec := []lifetimesapi.OutputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: metav1.NamespaceDefault,
					Name:      pvcName,
				},
			},
			MessageQueuePublisher: &lifetimesapi.PublishMessageQueueSpec{
				MessageQueueSpec: &lifetimesapi.MessageQueueSpec{
					MessageQueueRef: &corev1.ObjectReference{
						Namespace: mqNamespace,
						Name:      mqName,
					},
					MaxBatchBytes: 100,
				},
			},
		},
	}

	_, err = CreateGrpcMessageQueueDiffs(
		dataClient, kubeClient, publisherOptions, oldOutputDataSpec,
		newOutputDataSpec)
	assert.Error(t, err)

	errPrefix := "Currently, not support changes in the message queue " +
		"configuration:"
	assert.True(t, strings.HasPrefix(err.Error(), errPrefix))
}
