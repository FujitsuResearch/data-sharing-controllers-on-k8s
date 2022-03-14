// Copyright (c) 2022 Fujitsu Limited

package messagequeue

import (
	"github.com/segmentio/kafka-go"
)

const (
	MessageMethodAdd = iota + 1
	MessageMethodDelete
	MessageMethodRename
	MessageMethodFinish
)

type Message struct {
	Method   int8   `json:"method"`
	Path     string `json:"path,omitempty"`
	OldPath  string `json:"oldPath,omitempty"`
	Offset   int64  `json:"offset,omitempty"`
	Contents []byte `json:"contents,omitempty"`
}

type MessageQueueUpdatePublisherOperations interface {
	CreateUpdateFileMessages(
		path string, offset int64, contents []byte) ([]kafka.Message, error)
	CreateDeleteFileMessages(path string) ([]kafka.Message, error)
	CreateRenameFileMessages(
		newPath string, oldPath string) ([]kafka.Message, error)
	CreateFinishFileMessages() ([]kafka.Message, error)
}
