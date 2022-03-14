// Copyright (c) 2022 Fujitsu Limited

package publish

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/segmentio/kafka-go"
	messagepub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"
)

var (
	_ = (messagepub.MessageQueueInitPublisherOperations)(
		(*messageQueuePublisher)(nil))
)

type fileInfo struct {
	rootPath     string
	maxBatchSize int
	messages     []kafka.Message
}

type messageQueuePublisher struct {
	rootPath     string
	maxBatchSize int
}

func newMessageQueuePublisher(
	rootPath string, maxBatchSize int) *messageQueuePublisher {
	return &messageQueuePublisher{
		rootPath:     rootPath,
		maxBatchSize: maxBatchSize,
	}
}

func (mqp *messageQueuePublisher) HasInitialData() (bool, error) {
	err := filepath.Walk(
		mqp.rootPath,
		func(path string, info fs.FileInfo, err error) error {
			if !info.IsDir() {
				return io.EOF
			}
			return nil
		})

	if err != nil {
		if err == io.EOF {
			return true, nil
		} else {
			return false, err
		}
	} else {
		return false, nil
	}
}

func getMaxContentsBatchSize(
	message *messagequeue.Message, maxBatchSize int) (int, error) {
	encodedMessage, err := json.Marshal(message)
	if err != nil {
		return -1, fmt.Errorf(
			"Failed to marshal the message %+v: %v", message, err.Error())
	}

	init := binary.Size(encodedMessage)
	if init > maxBatchSize {
		return -1, fmt.Errorf(
			"The initial term of the arithmetic progression '%d' is "+
				"greater than the maximum message batch size '%d'",
			init, maxBatchSize)
	}

	diff := 0
	divisor := 0
	for i := 1; i < maxBatchSize; i++ {
		message.Contents = append(message.Contents, 0x01)
		encodedMessage, err := json.Marshal(message)
		if err != nil {
			return -1, fmt.Errorf(
				"Failed to marshal the message %+v: %v", message, err.Error())
		}

		binSize := binary.Size(encodedMessage)
		if init != binSize {
			diff = binSize - init
			divisor = i
			break
		}
	}

	if diff == 0 {
		return -1, fmt.Errorf(
			"Not find the difference of arithmetic progression and "+
				"the divisor for the message: %+v", message)
	}

	minContentsBatchSize := (maxBatchSize-init)/diff*divisor + 1
	maxContentsBatchSize := minContentsBatchSize + (divisor - 1)

	return maxContentsBatchSize, nil
}

func getContentsBatchesForCreation(
	file *os.File, batchSize int) ([][]byte, error) {
	contentsBatches := [][]byte{}

	for {
		buffer := make([]byte, batchSize)

		count, err := file.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf(
				"Failed to read the file %q: %v", file.Name(), err.Error())
		}

		contentsBatches = append(contentsBatches, buffer[:count])
	}

	return contentsBatches, nil
}

func (mqp *messageQueuePublisher) createAddFileMessages(
	path string) ([]kafka.Message, error) {
	relativePath, err := filepath.Rel(mqp.rootPath, path)
	if err != nil {
		return nil, fmt.Errorf("%v: %s", err.Error(), path)
	} else if strings.HasPrefix(relativePath, "../") {
		return nil, fmt.Errorf(
			"%q does NOT the subdirectory of %q", path, mqp.rootPath)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Could not open the file %q", path)
	}
	defer file.Close()

	message := &messagequeue.Message{
		Method:   messagequeue.MessageMethodAdd,
		Path:     relativePath,
		Contents: []byte{0x01},
	}

	batchSize, err := getMaxContentsBatchSize(message, mqp.maxBatchSize)
	if err != nil {
		return nil, err
	}

	contentsBatches, err := getContentsBatchesForCreation(file, batchSize)
	if err != nil {
		return nil, err
	}

	messages := []kafka.Message{}

	message = &messagequeue.Message{
		Method:   messagequeue.MessageMethodAdd,
		Path:     relativePath,
		Contents: contentsBatches[0],
	}

	encodedMessage, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to marshal the message %+v: %v", message, err.Error())
	}

	messages = append(
		messages, kafka.Message{
			Value: encodedMessage,
		})

	if len(contentsBatches) > 1 {
		for _, contentsBatch := range contentsBatches[1:] {
			message := &messagequeue.Message{
				Method:   messagequeue.MessageMethodAdd,
				Path:     relativePath,
				Offset:   -1,
				Contents: contentsBatch,
			}

			encodedMessage, err := json.Marshal(message)
			if err != nil {
				return nil, fmt.Errorf(
					"Failed to marshal the message %+v: %v", message, err.Error())
			}

			messages = append(
				messages, kafka.Message{
					Value: encodedMessage,
				})
		}
	}

	return messages, nil
}

func getContentsBatchesForUpdate(contents []byte, batchSize int) [][]byte {
	contentsBatches := make([][]byte, 0, len(contents)/batchSize+1)

	for i := 0; i < len(contents); i += batchSize {
		contentsBatch := contents[i:]
		if len(contentsBatch) > batchSize {
			contentsBatch = contentsBatch[:batchSize]
		}
		contentsBatches = append(contentsBatches, contentsBatch)
	}

	return contentsBatches
}

func (mqp *messageQueuePublisher) createUpdateFileMessages(
	path string, offset int64, contents []byte) (
	[]kafka.Message, error) {
	relativePath, err := filepath.Rel(mqp.rootPath, path)
	if err != nil {
		return nil, fmt.Errorf("%v: %s", err.Error(), path)
	} else if strings.HasPrefix(relativePath, "../") {
		return nil, fmt.Errorf(
			"%q does NOT the subdirectory of %q", path, mqp.rootPath)
	}

	message := &messagequeue.Message{
		Method:   messagequeue.MessageMethodAdd,
		Path:     relativePath,
		Contents: []byte{0x01},
	}

	if offset != 0 {
		message.Offset = offset
	}

	batchSize, err := getMaxContentsBatchSize(message, mqp.maxBatchSize)
	if err != nil {
		return nil, err
	}

	contentsBatches := getContentsBatchesForUpdate(contents, batchSize)

	messages := []kafka.Message{}

	message = &messagequeue.Message{
		Method:   messagequeue.MessageMethodAdd,
		Path:     relativePath,
		Contents: contentsBatches[0],
	}

	if offset != 0 {
		message.Offset = offset
	}

	encodedMessage, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to marshal the message %+v: %v", message, err.Error())
	}

	messages = append(
		messages, kafka.Message{
			Value: encodedMessage,
		})

	if len(contentsBatches) > 1 {
		for _, contentsBatch := range contentsBatches[1:] {
			message := messagequeue.Message{
				Method:   messagequeue.MessageMethodAdd,
				Path:     relativePath,
				Offset:   -1,
				Contents: contentsBatch,
			}

			encodedMessage, err := json.Marshal(message)
			if err != nil {
				return nil, fmt.Errorf(
					"Failed to marshal the message %+v: %v",
					message, err.Error())
			}

			messages = append(
				messages, kafka.Message{
					Value: encodedMessage,
				})
		}
	}

	return messages, nil
}

func (mqp *messageQueuePublisher) createDeleteFileMessages(
	path string) ([]kafka.Message, error) {
	relativePath, err := filepath.Rel(mqp.rootPath, path)
	if err != nil {
		return nil, fmt.Errorf("%v: %s", err.Error(), path)
	} else if strings.HasPrefix(relativePath, "../") {
		return nil, fmt.Errorf(
			"%q does NOT the subdirectory of %q", path, mqp.rootPath)
	}

	message := messagequeue.Message{
		Method: messagequeue.MessageMethodDelete,
		Path:   relativePath,
	}

	encodedMessage, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to marshal the message %+v: %v", message, err.Error())
	}

	deleteMessageSize := binary.Size(encodedMessage)
	if deleteMessageSize > mqp.maxBatchSize {
		return nil, fmt.Errorf(
			"The 'delete' message '%d' is "+
				"greater than the maximum message batch size '%d'",
			deleteMessageSize, mqp.maxBatchSize)
	}

	return []kafka.Message{
		{
			Value: encodedMessage,
		},
	}, nil
}

func (mqp *messageQueuePublisher) createRenameFileMessages(
	newPath string, oldPath string) ([]kafka.Message, error) {
	relativeNewPath, err := filepath.Rel(mqp.rootPath, newPath)
	if err != nil {
		return nil, fmt.Errorf("%v: %s", err.Error(), newPath)
	}
	if strings.HasPrefix(relativeNewPath, "../") {
		return nil, fmt.Errorf(
			"New path %q does NOT the subdirectory of %q",
			newPath, mqp.rootPath)
	}

	relativeOldPath, err := filepath.Rel(mqp.rootPath, oldPath)
	if err != nil {
		return nil, fmt.Errorf("%v: %s", err.Error(), oldPath)
	} else if strings.HasPrefix(relativeOldPath, "../") {
		return nil, fmt.Errorf(
			"Old path %q does NOT the subdirectory of %q",
			oldPath, mqp.rootPath)
	}

	message := messagequeue.Message{
		Method:  messagequeue.MessageMethodRename,
		Path:    relativeNewPath,
		OldPath: relativeOldPath,
	}

	encodedMessage, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to marshal the message %+v: %v", message, err.Error())
	}

	renameMessageSize := binary.Size(encodedMessage)
	if renameMessageSize > mqp.maxBatchSize {
		return nil, fmt.Errorf(
			"The 'rename' message '%d' is "+
				"greater than the maximum message batch size '%d'",
			renameMessageSize, mqp.maxBatchSize)
	}

	return []kafka.Message{
		{
			Value: encodedMessage,
		},
	}, nil
}

func (mqp *messageQueuePublisher) createFinishMessages() (
	[]kafka.Message, error) {
	message := &messagequeue.Message{
		Method: messagequeue.MessageMethodFinish,
	}

	encodedMessage, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to marshal the message %+v: %v", message, err.Error())
	}

	finishMessageSize := binary.Size(encodedMessage)
	if finishMessageSize > mqp.maxBatchSize {
		return nil, fmt.Errorf(
			"The 'finish' message '%d' is "+
				"greater than the maximum message batch size '%d'",
			finishMessageSize, mqp.maxBatchSize)
	}

	return []kafka.Message{
		{
			Value: encodedMessage,
		},
	}, nil
}

func (fi *fileInfo) createAddMessagesWalkFunc(
	path string, info os.FileInfo, err error) error {
	if info.IsDir() {
		return nil
	}

	publisher := newMessageQueuePublisher(fi.rootPath, fi.maxBatchSize)

	messages, err := publisher.createAddFileMessages(path)
	if err != nil {
		return err
	}

	fi.messages = append(fi.messages, messages...)

	return nil
}

func (mqp *messageQueuePublisher) CreateInitialDataMessages(
	ctx context.Context) ([]kafka.Message, error) {
	fi := &fileInfo{
		rootPath:     mqp.rootPath,
		maxBatchSize: mqp.maxBatchSize,
		messages:     []kafka.Message{},
	}

	err := filepath.Walk(mqp.rootPath, fi.createAddMessagesWalkFunc)
	if err != nil {
		return nil, err
	}

	finMessages, err := mqp.createFinishMessages()
	if err != nil {
		return nil, err
	}

	fi.messages = append(fi.messages, finMessages...)

	return fi.messages, nil
}
