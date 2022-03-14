// Copyright (c) 2022 Fujitsu Limited

package testing

import (
	"fmt"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

type fakeProcess struct {
	commandPath      string
	mountNamespace   string
	commandLine      []byte
	workingDirectory string
}

type FakeProcFs struct {
	rootPath  string
	processes map[int]*fakeProcess
}

type FakeProcFsOperations interface {
	util.ProcFsOperations

	AddProcess(
		pid int, commandPath string, mountNamespace string, commandLine []byte,
		workingDirectory string)
	DeleteProcess(pid int)
}

func NewFakeProcFs(rootPath string) FakeProcFsOperations {
	return &FakeProcFs{
		rootPath:  rootPath,
		processes: map[int]*fakeProcess{},
	}
}

func (fpf *FakeProcFs) GetProcFsRootPath() string {
	return fpf.rootPath
}

func (fpf *FakeProcFs) AddProcess(
	pid int, commandPath string, mountNamespace string,
	commandLine []byte, workingDirectory string) {
	fpf.processes[pid] = &fakeProcess{
		commandPath:      commandPath,
		mountNamespace:   mountNamespace,
		commandLine:      commandLine,
		workingDirectory: workingDirectory,
	}
}

func (fpf *FakeProcFs) DeleteProcess(pid int) {
	delete(fpf.processes, pid)
}

func (fpf *FakeProcFs) GetCommandPath(pid int) (string, error) {
	process, ok := fpf.processes[pid]
	if !ok {
		return "", fmt.Errorf(
			"Not find the /proc element for the process '%d'", pid)
	}

	return process.commandPath, nil
}

func (fpf *FakeProcFs) GetMountNamespace(pid int) (string, error) {
	process, ok := fpf.processes[pid]
	if !ok {
		return "", fmt.Errorf(
			"Not find the /proc element for the process '%d'", pid)
	}

	return process.mountNamespace, nil
}

func (fpf *FakeProcFs) GetCommandLine(pid int) ([]byte, error) {
	process, ok := fpf.processes[pid]
	if !ok {
		return nil, fmt.Errorf(
			"Not find the /proc element for the process '%d'", pid)
	}

	return process.commandLine, nil
}

func (fpf *FakeProcFs) GetCurrentWorkingDirectory(pid int) (string, error) {
	process, ok := fpf.processes[pid]
	if !ok {
		return "", fmt.Errorf(
			"Not find the /proc element for the process '%d'", pid)
	}

	return process.workingDirectory, nil
}
