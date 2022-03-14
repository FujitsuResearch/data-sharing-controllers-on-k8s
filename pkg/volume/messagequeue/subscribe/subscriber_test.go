// Copyright (c) 2022 Fujitsu Limited

package subscribe

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/segmentio/kafka-go"

	volumemq "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"

	"github.com/stretchr/testify/assert"
)

const (
	testDir     = "testing"
	fixturesDir = "fixtures"
)

func createKafkaMessage(
	t *testing.T, message *volumemq.Message) kafka.Message {
	encodedMessage, err := json.Marshal(message)
	assert.NoError(t, err)

	return kafka.Message{
		Value: encodedMessage,
	}
}

func TestHandleInitMessage(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	fileName := "test1.txt"
	relativePath := filepath.Join(testDir, fixturesDir, fileName)
	contentsbytes := []byte("This is test.")
	addMessage := &volumemq.Message{
		Method:   volumemq.MessageMethodAdd,
		Path:     relativePath,
		Contents: contentsbytes,
	}
	kafkaAddMessage := createKafkaMessage(t, addMessage)

	path := rootPath
	subscriber := newMessageQueueSubscriber(path)
	finished, err := subscriber.HandleInitMessage(&kafkaAddMessage)
	assert.NoError(t, err)
	assert.False(t, finished)

	contents, err := ioutil.ReadFile(relativePath)
	assert.NoError(t, err)

	assert.True(t, bytes.Equal(contentsbytes, contents))
	defer os.RemoveAll(testDir)

	finishMessage := &volumemq.Message{
		Method: volumemq.MessageMethodFinish,
	}
	kafkaFinishMessage := createKafkaMessage(t, finishMessage)

	finished, err = subscriber.HandleInitMessage(&kafkaFinishMessage)
	assert.NoError(t, err)
	assert.True(t, finished)
}

func TestHandleInitMessageForNotSupportedMethod(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	oldFileName := "test1.txt"
	newFileName := "test2.txt"
	relativeOldPath := filepath.Join(rootPath, oldFileName)
	relativeNewPath := filepath.Join(rootPath, newFileName)
	renameMessage := &volumemq.Message{
		Method:  volumemq.MessageMethodRename,
		Path:    relativeNewPath,
		OldPath: relativeOldPath,
	}
	kafkaRenameMessage := createKafkaMessage(t, renameMessage)

	path := rootPath
	subscriber := newMessageQueueSubscriber(path)
	_, err = subscriber.HandleInitMessage(&kafkaRenameMessage)
	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"Not support such a message method: %v",
		volumemq.MessageMethodRename)
	assert.EqualError(t, err, errMsg)
}

func TestHandleUpdateMessage(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	fileName := "test1.txt"
	relativePath := filepath.Join(testDir, fixturesDir, fileName)
	contentsbytes := []byte("This is test.")
	addMessage := &volumemq.Message{
		Method:   volumemq.MessageMethodAdd,
		Path:     relativePath,
		Contents: contentsbytes,
	}
	kafkaAddMessage := createKafkaMessage(t, addMessage)

	path := rootPath
	subscriber := newMessageQueueSubscriber(path)
	finished, err := subscriber.HandleUpdateMessage(&kafkaAddMessage)
	assert.NoError(t, err)
	assert.False(t, finished)

	contents, err := ioutil.ReadFile(relativePath)
	assert.NoError(t, err)

	assert.True(t, bytes.Equal(contentsbytes, contents))

	deleteMessage := &volumemq.Message{
		Method: volumemq.MessageMethodDelete,
		Path:   relativePath,
	}
	kafkaDeleteMessage := createKafkaMessage(t, deleteMessage)

	finished, err = subscriber.HandleUpdateMessage(&kafkaDeleteMessage)
	assert.NoError(t, err)
	assert.False(t, finished)

	_, err = os.Open(relativePath)
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))

	finishMessage := &volumemq.Message{
		Method: volumemq.MessageMethodFinish,
	}
	kafkaFinishMessage := createKafkaMessage(t, finishMessage)

	finished, err = subscriber.HandleUpdateMessage(&kafkaFinishMessage)
	assert.NoError(t, err)
	assert.True(t, finished)
}
