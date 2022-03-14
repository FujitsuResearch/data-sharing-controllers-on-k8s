// Copyright (c) 2022 Fujitsu Limited

package messagequeue

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
)

const (
	saslPasswordKey  = "password"
	brokersDelimiter = ","

	topicConcatenater   = "."
	topicFileSystem     = "fs"
	topicRdb            = "rdb"
	topicInit           = "init"
	topicUpdate         = "update"
	topicRequest        = "request"
	topicResponse       = "response"
	groupIdConcatenater = "."

	defaultDialerTimeout                  = 10 * time.Second
	DefaultMaxBatchBytes                  = 1e6
	DefaultUpdatePublishChannelBufferSize = 1000
)

const (
	topicKindFileSystem = iota + 1
	topicKindRdb
)

type MessageQueueConfig struct {
	brokers  []string
	user     string
	password string
}

type MessageQueueTopic struct {
	kind int
	name string
}

type MessageQueueInitTopics struct {
	Init     string
	Request  string
	Response string
}

func NewMessageQueueConfig(
	brokers []string, user string, password string) *MessageQueueConfig {
	return &MessageQueueConfig{
		brokers:  brokers,
		user:     user,
		password: password,
	}
}

func GetMessageQueueConfig(
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	messageQueueRef *corev1.ObjectReference) (*MessageQueueConfig, error) {
	messageCtx, cancel := util.GetTimeoutContext(
		util.DefaultKubeClientTimeout)
	defer cancel()

	messageQueue, err := dataClient.CoreV1alpha1().
		MessageQueues(messageQueueRef.Namespace).Get(
		messageCtx, messageQueueRef.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf(
			"Could not get an instance of the 'MessageQueue' type "+
				"(namespacce: %q, name: %q): %s",
			messageQueueRef.Namespace, messageQueueRef.Name, err.Error())
	}

	saslCtx, cancel := util.GetTimeoutContext(
		util.DefaultKubeClientTimeout)
	defer cancel()

	saslConfig, err := kubeClient.CoreV1().
		Secrets(messageQueue.Spec.SaslRef.Namespace).Get(
		saslCtx, messageQueue.Spec.SaslRef.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf(
			"Could not get an instance of the 'Secret' type "+
				"(namespacce: %q, name: %q): %s",
			messageQueue.Spec.SaslRef.Namespace,
			messageQueue.Spec.SaslRef.Name, err.Error())
	}

	saslUser := messageQueue.Spec.SaslRef.Name

	saslPasswordBytes, found := saslConfig.Data[saslPasswordKey]
	if !found {
		return nil, fmt.Errorf(
			"Could not find the key %q in the 'Secret' "+
				"(namespacce: %q, name: %q)",
			saslPasswordKey, messageQueue.Spec.SaslRef.Namespace,
			messageQueue.Spec.SaslRef.Name)
	}

	return &MessageQueueConfig{
		brokers:  messageQueue.Spec.Brokers,
		user:     string(saslUser),
		password: string(saslPasswordBytes),
	}, nil
}

func (mqc *MessageQueueConfig) GetBrokers() []string {
	return mqc.brokers
}

func (mqc *MessageQueueConfig) GetUser() string {
	return mqc.user
}

func (mqc *MessageQueueConfig) GetPassword() string {
	return mqc.password
}

func (mqc *MessageQueueConfig) CreateSaslDialer() (*kafka.Dialer, error) {
	mechanism, err := scram.Mechanism(scram.SHA512, mqc.user, mqc.password)
	if err != nil {
		return nil, err
	}

	return &kafka.Dialer{
		Timeout:       defaultDialerTimeout,
		DualStack:     true,
		SASLMechanism: mechanism,
	}, nil
}

func NewMessageQueueFileSystemTopic(nameElements []string) *MessageQueueTopic {
	return &MessageQueueTopic{
		kind: topicKindFileSystem,
		name: strings.Join(nameElements, topicConcatenater),
	}
}

func NewMessageQueueRdbTopic(nameElements []string) *MessageQueueTopic {
	return &MessageQueueTopic{
		kind: topicKindRdb,
		name: strings.Join(nameElements, topicConcatenater),
	}
}

func GetGroupId(idElements []string) string {
	return strings.Join(idElements, groupIdConcatenater)
}

func getTopicKindString(topicKind int) (string, error) {
	switch topicKind {
	case topicKindFileSystem:
		return topicFileSystem, nil
	case topicKindRdb:
		return topicRdb, nil
	default:
		return "", fmt.Errorf("Not support such a topic type: %d\n", topicKind)
	}
}

func (mqt *MessageQueueTopic) CreateInitTopics() (
	*MessageQueueInitTopics, error) {
	topicKindString, err := getTopicKindString(mqt.kind)
	if err != nil {
		return nil, err
	}

	initElements := []string{
		topicKindString,
		mqt.name,
		topicInit,
	}

	requestElements := append(initElements, topicRequest)
	responseElements := append(initElements, topicResponse)

	return &MessageQueueInitTopics{
		Init:     strings.Join(initElements, topicConcatenater),
		Request:  strings.Join(requestElements, topicConcatenater),
		Response: strings.Join(responseElements, topicConcatenater),
	}, nil
}

func (mqt *MessageQueueTopic) CreateUpdateTopic() (string, error) {
	topicKindString, err := getTopicKindString(mqt.kind)
	if err != nil {
		return "", err
	}

	elements := []string{
		topicKindString,
		mqt.name,
		topicUpdate,
	}

	return strings.Join(elements, topicConcatenater), nil
}

func HasTopics(
	brokerAddress string, dialer *kafka.Dialer, topics []string) (
	bool, error) {
	connCtx, connCancel := util.GetTimeoutContext(
		util.DefaultMessageQueueTimeout)

	conn, err := dialer.DialContext(connCtx, "tcp", brokerAddress)

	connCancel()

	if err != nil {
		return false, err
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions(topics...)
	if err != nil {
		return false, err
	}

	ps := map[string]struct{}{}
	for _, partition := range partitions {
		ps[partition.Topic] = struct{}{}
	}

	for _, topic := range topics {
		delete(ps, topic)
	}

	if len(ps) != 0 {
		return false, fmt.Errorf("Not find topics: %+v", ps)
	}

	return true, nil
}

func CreateTopics(
	brokerAddress string, dialer *kafka.Dialer, topics []string) error {
	connCtx, connCancel := util.GetTimeoutContext(
		util.DefaultMessageQueueTimeout)

	conn, err := dialer.DialContext(connCtx, "tcp", brokerAddress)

	connCancel()

	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	var controllerConn *kafka.Conn
	ctlCtx, ctlCancel := util.GetTimeoutContext(
		util.DefaultMessageQueueTimeout)

	host := controller.Host
	port := strconv.Itoa(controller.Port)
	controllerConn, err = dialer.DialContext(
		ctlCtx, "tcp", net.JoinHostPort(host, port))

	ctlCancel()

	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := make([]kafka.TopicConfig, 0, len(topics))

	for _, topic := range topics {
		topicConfigs = append(
			topicConfigs,
			kafka.TopicConfig{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			})
	}

	return controllerConn.CreateTopics(topicConfigs...)
}

// [TODO?] deleteTopics() is not called in CloseMessageQueues() because
// subscribers are disconnected to the topics created by a publisher
// in the case that it restarted.
func DeleteTopics(
	brokerAddress string, dialer *kafka.Dialer, topics []string) error {
	connCtx, connCancel := util.GetTimeoutContext(
		util.DefaultMessageQueueTimeout)

	conn, err := dialer.DialContext(connCtx, "tcp", brokerAddress)

	connCancel()

	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	var controllerConn *kafka.Conn
	ctlCtx, ctlCancel := util.GetTimeoutContext(
		util.DefaultMessageQueueTimeout)

	host := controller.Host
	port := strconv.Itoa(controller.Port)
	controllerConn, err = dialer.DialContext(
		ctlCtx, "tcp", net.JoinHostPort(host, port))

	ctlCancel()

	if err != nil {
		return err
	}
	defer controllerConn.Close()

	return conn.DeleteTopics(topics...)
}
