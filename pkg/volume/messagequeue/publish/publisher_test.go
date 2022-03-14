// Copyright (c) 2022 Fujitsu Limited

package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"
	volumemq "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"

	"github.com/stretchr/testify/assert"
)

const (
	relativePath = "testing/fixtures/test1.txt"
)

func TestGetMaxContentsBatchSize(t *testing.T) {
	message := &messagequeue.Message{
		Method:   messagequeue.MessageMethodAdd,
		Path:     "/tmp/test.txt",
		Contents: []byte{0x01},
	}

	maxBatchSize := 80
	batchSize, err := getMaxContentsBatchSize(message, maxBatchSize)
	assert.NoError(t, err)

	message.Contents = make([]byte, batchSize)
	encodedMessage, err := json.Marshal(message)
	assert.NoError(t, err)
	assert.True(t, maxBatchSize >= len(encodedMessage))

	message.Contents = make([]byte, batchSize+1)
	encodedMessage, err = json.Marshal(message)
	assert.NoError(t, err)
	assert.True(t, maxBatchSize < len(encodedMessage))
}

func TestCreateAddFileMessages(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 80
	publisher := newMessageQueuePublisher(rootPath, maxBatchSize)

	path := filepath.Join(rootPath, relativePath)
	messages, err := publisher.createAddFileMessages(path)
	assert.NoError(t, err)

	message := &messagequeue.Message{
		Method:   messagequeue.MessageMethodAdd,
		Path:     relativePath,
		Contents: []byte{0x01},
	}

	batchSize, err := getMaxContentsBatchSize(message, maxBatchSize)
	assert.NoError(t, err)

	file, err := os.Stat(relativePath)
	assert.NoError(t, err)
	fileSize := file.Size()
	expectedNumOfMessages := fileSize / int64(batchSize)
	if fileSize%int64(batchSize) != 0 {
		expectedNumOfMessages += 1
	}

	numOfMessages := len(messages)
	assert.Equal(t, expectedNumOfMessages, int64(numOfMessages))

	var msg volumemq.Message
	err = json.Unmarshal(messages[0].Value, &msg)
	assert.NoError(t, err)

	assert.Equal(t, messagequeue.MessageMethodAdd, int(msg.Method))
	assert.Equal(t, relativePath, msg.Path)
	assert.Equal(t, int64(0), msg.Offset)

	if numOfMessages > 1 {
		for i := 1; i < numOfMessages; i++ {
			err = json.Unmarshal(messages[i].Value, &msg)
			assert.NoError(t, err)
			assert.Equal(t, messagequeue.MessageMethodAdd, int(msg.Method))
			assert.Equal(t, relativePath, msg.Path)
			assert.Equal(t, int64(-1), msg.Offset)
		}
	}
}

func TestCreateAddFileMessagesForInvalidPath(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 80
	subDirPath := filepath.Join(rootPath, "sub")
	publisher := newMessageQueuePublisher(subDirPath, maxBatchSize)

	path := filepath.Join(rootPath, relativePath)
	_, err = publisher.createAddFileMessages(path)
	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"%q does NOT the subdirectory of %q", path, subDirPath)
	assert.EqualError(t, err, errMsg)
}

func TestCreateUpdateFileMessages(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 80
	publisher := newMessageQueuePublisher(rootPath, maxBatchSize)

	path := filepath.Join(rootPath, relativePath)

	contents, err := ioutil.ReadFile(relativePath)
	assert.NoError(t, err)

	offset := int64(5)
	assert.True(t, int(offset) < len(contents))

	messages, err := publisher.createUpdateFileMessages(
		path, offset, contents[offset:])
	assert.NoError(t, err)

	message := &messagequeue.Message{
		Method:   messagequeue.MessageMethodAdd,
		Path:     relativePath,
		Offset:   offset,
		Contents: []byte{0x01},
	}

	batchSize, err := getMaxContentsBatchSize(message, maxBatchSize)
	assert.NoError(t, err)

	file, err := os.Stat(relativePath)
	assert.NoError(t, err)
	fileSize := file.Size() - offset

	expectedNumOfMessages := fileSize / int64(batchSize)
	if fileSize%int64(batchSize) != 0 {
		expectedNumOfMessages += 1
	}

	numOfMessages := len(messages)
	assert.Equal(t, expectedNumOfMessages, int64(numOfMessages))

	var msg volumemq.Message
	err = json.Unmarshal(messages[0].Value, &msg)
	assert.NoError(t, err)

	assert.Equal(t, messagequeue.MessageMethodAdd, int(msg.Method))
	assert.Equal(t, relativePath, msg.Path)
	assert.Equal(t, int64(offset), msg.Offset)

	if numOfMessages > 1 {
		for i := 1; i < numOfMessages; i++ {
			err = json.Unmarshal(messages[i].Value, &msg)
			assert.NoError(t, err)
			assert.Equal(t, messagequeue.MessageMethodAdd, int(msg.Method))
			assert.Equal(t, relativePath, msg.Path)
			assert.Equal(t, int64(-1), msg.Offset)
		}
	}
}

func TestCreateUpdateFileMessagesForInvalidPath(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 80
	subDirPath := filepath.Join(rootPath, "sub")
	publisher := newMessageQueuePublisher(subDirPath, maxBatchSize)

	path := filepath.Join(rootPath, relativePath)

	_, err = publisher.createUpdateFileMessages(path, 0, nil)
	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"%q does NOT the subdirectory of %q", path, subDirPath)
	assert.EqualError(t, err, errMsg)
}

func TestCreateDeleteFileMessages(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 80
	publisher := newMessageQueuePublisher(rootPath, maxBatchSize)

	path := filepath.Join(rootPath, relativePath)
	messages, err := publisher.createDeleteFileMessages(path)
	assert.NoError(t, err)

	message := &messagequeue.Message{
		Method: messagequeue.MessageMethodDelete,
		Path:   relativePath,
	}

	encodedMessage, err := json.Marshal(message)
	assert.NoError(t, err)
	deleteSize := len(encodedMessage)

	expectedNumOfMessages := deleteSize / maxBatchSize
	if deleteSize%maxBatchSize != 0 {
		expectedNumOfMessages += 1
	}

	assert.Equal(t, 1, expectedNumOfMessages)
	numOfMessages := len(messages)
	assert.Equal(t, expectedNumOfMessages, numOfMessages)

	var msg volumemq.Message
	err = json.Unmarshal(messages[0].Value, &msg)
	assert.NoError(t, err)

	assert.Equal(t, messagequeue.MessageMethodDelete, int(msg.Method))
	assert.Equal(t, relativePath, msg.Path)
	assert.Equal(t, int64(0), msg.Offset)
}

func TestCreateDeleteFileMessagesForInvalidPath(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 80
	subDirPath := filepath.Join(rootPath, "sub")
	publisher := newMessageQueuePublisher(subDirPath, maxBatchSize)

	path := filepath.Join(rootPath, relativePath)

	_, err = publisher.createDeleteFileMessages(path)
	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"%q does NOT the subdirectory of %q", path, subDirPath)
	assert.EqualError(t, err, errMsg)
}

func TestCreateDeleteFileMessagesForTooSmallMaxBatchSize(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 40
	publisher := newMessageQueuePublisher(rootPath, maxBatchSize)

	message := &messagequeue.Message{
		Method: messagequeue.MessageMethodDelete,
		Path:   relativePath,
	}

	encodedMessage, err := json.Marshal(message)
	assert.NoError(t, err)
	deleteSize := len(encodedMessage)

	path := filepath.Join(rootPath, relativePath)
	_, err = publisher.createDeleteFileMessages(path)
	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"The 'delete' message '%d' is "+
			"greater than the maximum message batch size '%d'",
		deleteSize, maxBatchSize)
	assert.EqualError(t, err, errMsg)
}

func TestCreateRenameFileMessages(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 120
	publisher := newMessageQueuePublisher(rootPath, maxBatchSize)

	newRelativePath := "testing/fixtures/test2.txt"
	message := &messagequeue.Message{
		Method:  messagequeue.MessageMethodRename,
		Path:    newRelativePath,
		OldPath: relativePath,
	}

	encodedMessage, err := json.Marshal(message)
	assert.NoError(t, err)
	renameSize := len(encodedMessage)

	newPath := filepath.Join(rootPath, newRelativePath)
	oldPath := filepath.Join(rootPath, relativePath)
	messages, err := publisher.createRenameFileMessages(newPath, oldPath)
	assert.NoError(t, err)

	expectedNumOfMessages := renameSize / maxBatchSize
	if renameSize%maxBatchSize != 0 {
		expectedNumOfMessages += 1
	}

	assert.Equal(t, 1, expectedNumOfMessages)
	numOfMessages := len(messages)
	assert.Equal(t, expectedNumOfMessages, numOfMessages)

	var msg volumemq.Message
	err = json.Unmarshal(messages[0].Value, &msg)
	assert.NoError(t, err)

	assert.Equal(t, messagequeue.MessageMethodRename, int(msg.Method))
	assert.Equal(t, newRelativePath, msg.Path)
	assert.Equal(t, relativePath, msg.OldPath)
}

func TestCreateRenameFileMessagesForInvalidNewPath(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 80
	subDirPath := filepath.Join(rootPath, "sub")
	publisher := newMessageQueuePublisher(subDirPath, maxBatchSize)

	newRelativePath := "testing/fixtures/test2.txt"
	newPath := filepath.Join(rootPath, newRelativePath)
	oldPath := filepath.Join(rootPath, relativePath)
	_, err = publisher.createRenameFileMessages(newPath, oldPath)
	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"New path %q does NOT the subdirectory of %q", newPath, subDirPath)
	assert.EqualError(t, err, errMsg)
}

func TestCreateRenameFileMessagesForInvalidOldPath(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 80
	subDirPath := filepath.Join(rootPath, "sub")
	publisher := newMessageQueuePublisher(subDirPath, maxBatchSize)

	newPath := filepath.Join(subDirPath, "test2.txt")
	oldPath := filepath.Join(rootPath, relativePath)
	_, err = publisher.createRenameFileMessages(newPath, oldPath)
	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"Old path %q does NOT the subdirectory of %q", oldPath, subDirPath)
	assert.EqualError(t, err, errMsg)
}

func TestCreateRenameFileMessagesForTooSmallMaxBatchSize(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 80
	publisher := newMessageQueuePublisher(rootPath, maxBatchSize)

	newRelativePath := "testing/fixtures/test2.txt"
	message := &messagequeue.Message{
		Method:  messagequeue.MessageMethodRename,
		Path:    newRelativePath,
		OldPath: relativePath,
	}

	encodedMessage, err := json.Marshal(message)
	assert.NoError(t, err)
	renameSize := len(encodedMessage)

	newPath := filepath.Join(rootPath, newRelativePath)
	oldPath := filepath.Join(rootPath, relativePath)
	_, err = publisher.createRenameFileMessages(newPath, oldPath)
	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"The 'rename' message '%d' is "+
			"greater than the maximum message batch size '%d'",
		renameSize, maxBatchSize)
	assert.EqualError(t, err, errMsg)
}

func TestCreateFinishMessages(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 120
	publisher := newMessageQueuePublisher(rootPath, maxBatchSize)

	message := &messagequeue.Message{
		Method: messagequeue.MessageMethodFinish,
	}

	encodedMessage, err := json.Marshal(message)
	assert.NoError(t, err)
	finishSize := len(encodedMessage)

	messages, err := publisher.createFinishMessages()
	assert.NoError(t, err)

	expectedNumOfMessages := finishSize / maxBatchSize
	if finishSize%maxBatchSize != 0 {
		expectedNumOfMessages += 1
	}

	assert.Equal(t, 1, expectedNumOfMessages)
	numOfMessages := len(messages)
	assert.Equal(t, expectedNumOfMessages, numOfMessages)

	var msg volumemq.Message
	err = json.Unmarshal(messages[0].Value, &msg)
	assert.NoError(t, err)

	assert.Equal(t, messagequeue.MessageMethodFinish, int(msg.Method))
}

func TestCreateFinishMessagesForTooSmallMaxBatchSize(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	maxBatchSize := 8
	publisher := newMessageQueuePublisher(rootPath, maxBatchSize)

	message := &messagequeue.Message{
		Method: messagequeue.MessageMethodFinish,
	}

	encodedMessage, err := json.Marshal(message)
	assert.NoError(t, err)
	finishSize := len(encodedMessage)

	_, err = publisher.createFinishMessages()
	assert.Error(t, err)
	errMsg := fmt.Sprintf(
		"The 'finish' message '%d' is "+
			"greater than the maximum message batch size '%d'",
		finishSize, maxBatchSize)
	assert.EqualError(t, err, errMsg)
}

func TestHasInitialData(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	relativeDirPath := "testing/fixtures"
	dirPath := filepath.Join(rootPath, relativeDirPath)
	maxBatchSize := 8
	publisher := newMessageQueuePublisher(dirPath, maxBatchSize)

	has, err := publisher.HasInitialData()
	assert.NoError(t, err)
	assert.True(t, has)
}

func TestNotHaveInitialData(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	relativeDirPath := "testing/fixtures/test01"
	relativeSubDirPath := "test001"
	subDirPath := filepath.Join(relativeDirPath, relativeSubDirPath)
	err = os.MkdirAll(subDirPath, 0755)
	assert.NoError(t, err)
	defer os.RemoveAll(relativeDirPath)

	dirPath := filepath.Join(rootPath, relativeDirPath)
	maxBatchSize := 8
	publisher := newMessageQueuePublisher(dirPath, maxBatchSize)

	has, err := publisher.HasInitialData()
	assert.NoError(t, err)
	assert.False(t, has)
}

func TestCreateInitialDataMessages(t *testing.T) {
	rootPath, err := os.Getwd()
	assert.NoError(t, err)

	relativeDirPath := "testing/fixtures"
	dirPath := filepath.Join(rootPath, relativeDirPath)
	maxBatchSize := 80
	publisher := newMessageQueuePublisher(dirPath, maxBatchSize)

	messages, err := publisher.CreateInitialDataMessages(context.Background())
	assert.NoError(t, err)

	expectedMinNumOfMessages := 3
	numOfMessages := len(messages)
	assert.True(t, numOfMessages >= expectedMinNumOfMessages)

	for i := 0; i < numOfMessages-1; i++ {
		var msg volumemq.Message
		err = json.Unmarshal(messages[i].Value, &msg)
		assert.NoError(t, err)

		assert.Equal(t, messagequeue.MessageMethodAdd, int(msg.Method))
	}

	var msg volumemq.Message
	err = json.Unmarshal(messages[numOfMessages-1].Value, &msg)
	assert.NoError(t, err)

	assert.Equal(t, messagequeue.MessageMethodFinish, int(msg.Method))
}
