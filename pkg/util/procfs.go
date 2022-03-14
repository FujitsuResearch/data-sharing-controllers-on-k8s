// Copyright (c) 2022 Fujitsu Limited

package util

import (
	"os"
	"path/filepath"
	"strconv"
)

const (
	procFsRoot      = "/proc"
	hostDelegatePid = 1
)

var (
	_ ProcFsOperations = NewProcFs()
)

type ProcFs struct {
	rootPath string
}

type ProcFsOperations interface {
	GetProcFsRootPath() string
	GetCommandPath(pid int) (string, error)
	GetMountNamespace(pid int) (string, error)
	GetCommandLine(pid int) ([]byte, error)
	GetCurrentWorkingDirectory(pid int) (string, error)
}

func NewProcFs() ProcFsOperations {
	return &ProcFs{
		rootPath: procFsRoot,
	}
}

func (pf *ProcFs) GetProcFsRootPath() string {
	return pf.rootPath
}

func (pf *ProcFs) GetCommandPath(pid int) (string, error) {
	cmd := filepath.Join(pf.rootPath, strconv.Itoa(pid), "exe")

	return os.Readlink(cmd)
}

func (pf *ProcFs) GetMountNamespace(pid int) (string, error) {
	mountNamespacePath := filepath.Join(
		pf.rootPath, strconv.Itoa(pid), "ns/mnt")

	return os.Readlink(mountNamespacePath)
}

func (pf *ProcFs) GetCommandLine(pid int) ([]byte, error) {
	cmdlinePath := filepath.Join(
		pf.rootPath, strconv.Itoa(pid), "cmdline")

	return os.ReadFile(cmdlinePath)
}

func (pf *ProcFs) GetCurrentWorkingDirectory(pid int) (string, error) {
	workingDirectory := filepath.Join(
		pf.rootPath, strconv.Itoa(pid), "cwd")

	return os.Readlink(workingDirectory)
}
