// Copyright (c) 2022 Fujitsu Limited

package subscribe

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/segmentio/kafka-go"
	messagesub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/subscribe"
	volumemq "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"
)

var (
	_ = (messagesub.MessageQueueInitSubscriberOperations)(
		(*messageQueueSubscriber)(nil))
	_ = (messagesub.MessageQueueUpdateSubscriberOperations)(
		(*messageQueueSubscriber)(nil))
)

type messageQueueSubscriber struct {
	rootPath string
}

func newMessageQueueSubscriber(rootPath string) *messageQueueSubscriber {
	return &messageQueueSubscriber{
		rootPath: rootPath,
	}
}

func (mqs *messageQueueSubscriber) addFile(
	relativePath string, offset int64, contents []byte) error {
	path := filepath.Join(mqs.rootPath, relativePath)

	var (
		file *os.File
		err  error
	)
	switch offset {
	case 0:
		directory := filepath.Dir(path)
		if _, err = os.Stat(directory); err != nil {
			if os.IsNotExist(err) {
				err = os.MkdirAll(directory, 0755)
				if err != nil {
					return fmt.Errorf(
						"Failed to create %q: %v", directory, err.Error())
				}
			} else {
				return fmt.Errorf(
					"Failed to stat %q: %v", directory, err.Error())
			}
		}

		if _, err = os.Stat(path); err == nil {
			file, err = os.OpenFile(path, os.O_WRONLY, 0755)
		} else {
			if os.IsNotExist(err) {
				file, err = os.Create(path)
			} else {
				return fmt.Errorf("Failed to open %q: %v", path, err.Error())
			}
		}
	case -1:
		file, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0755)
	default:
		file, err = os.OpenFile(path, os.O_WRONLY, 0755)
		_, err = file.Seek(offset, 0)
	}

	if err != nil {
		return fmt.Errorf("Could not open the file %q", path)
	}
	defer file.Close()

	_, err = file.Write(contents)
	if err != nil {
		return fmt.Errorf("Failed to write to file %q: %v", path, err.Error())
	}

	return err
}

func (mqs *messageQueueSubscriber) deleteDirectories(path string) error {
	for directoryPath := filepath.Dir(path); directoryPath !=
		mqs.rootPath; directoryPath = filepath.Dir(directoryPath) {
		file, err := os.Open(directoryPath)
		if err != nil {
			return fmt.Errorf(
				"Could not opent %q: %v", directoryPath, err.Error())
		}

		_, err = file.Readdirnames(1)
		file.Close()

		if err != nil {
			if err == io.EOF {
				err := os.Remove(directoryPath)
				if err != nil {
					return fmt.Errorf(
						"Failed to remove %q: %v", directoryPath, err.Error())
				}
				continue
			}

			return fmt.Errorf(
				"Failed to Readdirnames() for %q: %v",
				directoryPath, err.Error())
		} else {
			return nil
		}
	}

	return nil
}

func (mqs *messageQueueSubscriber) deleteFile(relativePath string) error {
	path := filepath.Join(mqs.rootPath, relativePath)

	err := os.Remove(path)
	if err != nil {
		return err
	}

	return mqs.deleteDirectories(path)
}

func (mqs *messageQueueSubscriber) renameFile(
	relativeNewPath string, relativeOldPath string) error {
	newPath := filepath.Join(mqs.rootPath, relativeNewPath)
	oldPath := filepath.Join(mqs.rootPath, relativeOldPath)

	return os.Rename(oldPath, newPath)
}

func (mqs *messageQueueSubscriber) HandleInitMessage(
	queueMessage *kafka.Message) (bool, error) {
	var message volumemq.Message
	err := json.Unmarshal(queueMessage.Value, &message)
	if err != nil {
		return false, fmt.Errorf(
			"Failed to unmarshal a queue message: %v", err.Error())
	}

	switch message.Method {
	case volumemq.MessageMethodAdd:
		return false, mqs.addFile(
			message.Path, message.Offset, message.Contents)
	case volumemq.MessageMethodFinish:
		return true, nil
	default:
		return false, fmt.Errorf(
			"Not support such a message method: %v", message.Method)
	}
}

func (mqs *messageQueueSubscriber) HandleUpdateMessage(
	queueMessage *kafka.Message) (bool, error) {
	var message volumemq.Message
	err := json.Unmarshal(queueMessage.Value, &message)
	if err != nil {
		return false, fmt.Errorf(
			"Failed to unmarshal a queue message: %v", err.Error())
	}

	switch message.Method {
	case volumemq.MessageMethodAdd:
		return false, mqs.addFile(
			message.Path, message.Offset, message.Contents)
	case volumemq.MessageMethodDelete:
		return false, mqs.deleteFile(message.Path)
	case volumemq.MessageMethodRename:
		return false, mqs.renameFile(
			message.Path, message.OldPath)
	case volumemq.MessageMethodFinish:
		return true, nil
	default:
		return false, fmt.Errorf(
			"Not support such a message method: %v", message.Method)
	}
}
