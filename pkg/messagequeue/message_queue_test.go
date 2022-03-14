// Copyright (c) 2022 Fujitsu Limited

package messagequeue

import (
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	k8scorev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	coreapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/core/v1alpha1"
	corefake "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned/fake"

	"github.com/stretchr/testify/assert"
)

const (
	kafkaBrokerAddressEnv = "KAFKA_BROKER_ADDRESS"

	// [REF] github.com/segmentio/kafka-go/docker-compose.yml
	saslUser     = "adminscram"
	saslPassword = "admin-secret-512"
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

func TestGetMessageQueueConfig(t *testing.T) {
	mqNamespace := metav1.NamespaceDefault
	mqName := "mq-1"
	brokers := []string{"localhost:9092"}
	saslNamespace := metav1.NamespaceDefault
	saslName := "sasl-1"
	topicName := "topic-1"
	messageQueueObject := newMessageQueue(
		mqNamespace, mqName, brokers, saslNamespace, saslName, topicName)
	messageQueueDataObjects := []runtime.Object{
		messageQueueObject,
	}
	messageQueueDataClient := corefake.NewSimpleClientset(
		messageQueueDataObjects...)

	secretNamespace := saslNamespace
	secretName := saslName
	user := saslName
	password := "pwd"

	saslData := map[string][]byte{
		saslPasswordKey: []byte(password),
	}
	secretObject := newSecret(secretNamespace, secretName, saslData)

	kubeObjects := []runtime.Object{
		secretObject,
	}
	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	messageQueueRef := &corev1.ObjectReference{
		Namespace: mqNamespace,
		Name:      mqName,
	}
	config, err := GetMessageQueueConfig(
		messageQueueDataClient, kubeClient, messageQueueRef)
	assert.NoError(t, err)

	assert.Equal(t, brokers, config.GetBrokers())

	assert.Equal(t, user, config.GetUser())
	assert.Equal(t, password, config.GetPassword())
}

func TestCreateDeleteTopics(t *testing.T) {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	topics := []string{
		"test",
	}

	brokers := []string{brokerAddress}
	config := NewMessageQueueConfig(brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)

	err = CreateTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)

	found, err := HasTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	assert.True(t, found)

	err = DeleteTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
}
