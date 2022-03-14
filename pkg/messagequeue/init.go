// Copyright (c) 2022 Fujitsu Limited

package messagequeue

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	InitResponseTimeout = 1 * time.Minute
)

const (
	kindRequest = iota + 1
	kindResponse
)
const (
	commandRequestStart = iota + 1
	commandRequestFinish
)

const (
	commandResponseNotFindData = iota + 1
	commandResponseCreateTopic
)

type InitCommand struct {
	groupId string
}

type InitCommandMessage struct {
	Command    int8   `json:"command"`
	GroupId    string `json:"filePath"`
	ErrMessage string `json:"errMessage,omitempty"`
}

func NewInitCommand(groupId string) *InitCommand {
	return &InitCommand{
		groupId: groupId,
	}
}

func getRequestCommandString(command int8) string {
	switch command {
	case commandRequestStart:
		return "request: start"
	case commandRequestFinish:
		return "request: finish"
	default:
		return fmt.Sprintf("Not find such the 'request' command: %v", command)
	}
}

func getResponseCommandString(command int8) string {
	switch command {
	case commandResponseNotFindData:
		return "response: not-find-data"
	case commandResponseCreateTopic:
		return "response: create-topic"
	default:
		return fmt.Sprintf("Not find such the 'response' command: %v", command)
	}
}

func (ic *InitCommand) GetGroupId() string {
	return ic.groupId
}

func (ic *InitCommand) getCommandString(kind int8, command int8) string {
	switch kind {
	case kindRequest:
		return getRequestCommandString(command)
	case kindResponse:
		return getResponseCommandString(command)
	default:
		return fmt.Sprintf("Not find such the init 'kind': %v", kind)
	}
}

func (ic *InitCommand) encodeInitCommand(
	kind int8, command int8) (*kafka.Message, error) {
	request := InitCommandMessage{
		Command: command,
		GroupId: ic.groupId,
	}

	encodedRequest, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to encode the 'init' message: %q",
			ic.getCommandString(kind, command))
	}

	return &kafka.Message{
		Value: encodedRequest,
	}, nil
}

func (ic *InitCommand) encodeInitCommandWithErrorMessage(
	kind int8, command int8, errMessage string) (*kafka.Message, error) {
	request := InitCommandMessage{
		Command: command,
		GroupId: ic.groupId,
	}
	if errMessage != "" {
		request.ErrMessage = errMessage
	}

	encodedRequest, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to encode the 'init' message: %q",
			ic.getCommandString(kind, command))
	}

	return &kafka.Message{
		Value: encodedRequest,
	}, nil
}

func (ic *InitCommand) CreateInitRequestforStart() (*kafka.Message, error) {
	return ic.encodeInitCommand(kindRequest, commandRequestStart)
}

func (ic *InitCommand) CreateInitRequestForFinish() (*kafka.Message, error) {
	return ic.encodeInitCommand(kindRequest, commandRequestFinish)
}

func (ic *InitCommand) CreateInitResponseForNotFindData() (
	*kafka.Message, error) {
	return ic.encodeInitCommand(kindResponse, commandResponseNotFindData)
}

func (ic *InitCommand) CreateInitResponseForCreateTopic(
	errMessage string) (*kafka.Message, error) {
	return ic.encodeInitCommandWithErrorMessage(
		kindResponse, commandResponseCreateTopic, errMessage)
}

func DecodeInitCommand(
	message *kafka.Message) (*InitCommandMessage, error) {
	var command InitCommandMessage
	err := json.Unmarshal(message.Value, &command)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to decode the 'init' message")
	}

	return &command, nil
}

func (ic *InitCommand) isExpectedInitResponseCommand(
	message *kafka.Message, expectedCommand int8) (bool, error) {
	command, err := DecodeInitCommand(message)
	if err != nil {
		return false, fmt.Errorf("For 'init' response: %v", err.Error())
	}

	if command.ErrMessage != "" {
		return false, fmt.Errorf("%s", command.ErrMessage)
	}

	if command.GroupId != ic.groupId {
		return false, nil
	}

	return command.Command == expectedCommand, nil
}

func IsInitRequestforStart(command int8) bool {
	return command == commandRequestStart
}

func IsInitRequestforFinish(command int8) bool {
	return command == commandRequestFinish
}

func (ic *InitCommand) IsInitResponseforNotFindData(
	message *kafka.Message) (bool, error) {
	return ic.isExpectedInitResponseCommand(
		message, commandResponseNotFindData)
}

func (ic *InitCommand) IsInitResponseforCreateTopic(
	message *kafka.Message) (bool, error) {
	return ic.isExpectedInitResponseCommand(
		message, commandResponseCreateTopic)
}
