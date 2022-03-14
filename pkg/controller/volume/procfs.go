// Copyright (c) 2022 Fujitsu Limited

package volume

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"k8s.io/klog/v2"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

type hostProcFs struct {
	util.ProcFsOperations
	hostDelegatePid int
}

type externalAllowedScript struct {
	relativePathToInitWorkDir string
}

type externalAllowedExecutable struct {
	commandAbsolutePath             string
	withResidentCommandAbsolutePath string
	specifiedMountNamespace         string
	isHostProcess                   bool
	scripts                         map[string]*externalAllowedScript
}

func newHostProcFs(procFs util.ProcFsOperations, hostDelegatePid int) *hostProcFs {
	return &hostProcFs{
		ProcFsOperations: procFs,
		hostDelegatePid:  hostDelegatePid,
	}
}

func (hpf *hostProcFs) getHostPidAndMountNamespace() (int, string, error) {
	mntNamespace, err := hpf.GetMountNamespace(hpf.hostDelegatePid)
	if err != nil {
		return -1, "", fmt.Errorf(
			"Could not get the mount namespace id for a host process ID "+
				"%d: %v",
			hpf.hostDelegatePid, err.Error())
	}

	return hpf.hostDelegatePid, mntNamespace, nil
}

func newExternalAllowedExecutable(
	allowedExecutable *api.ExternalAllowedExecutable) *externalAllowedExecutable {
	mountNamespaceId := ""
	if allowedExecutable.MountNamespaceId != "" {
		mountNamespaceId = allowedExecutable.MountNamespaceId
	}

	scripts := map[string]*externalAllowedScript{}
	if len(allowedExecutable.ExternalAllowedScripts) != 0 {
		for _, script := range allowedExecutable.ExternalAllowedScripts {
			scripts[script.AbsolutePath] = &externalAllowedScript{
				relativePathToInitWorkDir: script.RelativePathToInitWorkDir,
			}
		}
	}

	return &externalAllowedExecutable{
		commandAbsolutePath:             allowedExecutable.CommandAbsolutePath,
		withResidentCommandAbsolutePath: allowedExecutable.WithResidentCommandAbsolutePath,
		specifiedMountNamespace:         mountNamespaceId,
		isHostProcess:                   allowedExecutable.IsHostProcess,
		scripts:                         scripts,
	}
}

func isAllowedScript(
	pid int, scripts map[string]*externalAllowedScript,
	procFs util.ProcFsOperations) (bool, error) {
	cmdLineBytes, err := procFs.GetCommandLine(pid)
	if err != nil {
		return false, err
	}

	cmdLine := strings.Split(string(cmdLineBytes), "\x00")
	if len(cmdLine) < 2 {
		return false, nil
	}

	currentWorkingDirectory := ""
	for _, argument := range cmdLine[1:] {
		_, ok := scripts[argument]
		if ok {
			return true, nil
		} else {
			for path, scriptForRelativePath := range scripts {
				if !strings.HasSuffix(path, argument) {
					continue
				}

				if currentWorkingDirectory == "" {
					currentWorkingDirectory, err = procFs.
						GetCurrentWorkingDirectory(pid)
					if err != nil {
						return false, err
					}
				}

				if scriptForRelativePath.relativePathToInitWorkDir != "" {
					scriptPath := filepath.Join(
						currentWorkingDirectory,
						scriptForRelativePath.relativePathToInitWorkDir,
						argument)

					if path == scriptPath {
						return true, nil
					}
				}

				scriptPath := filepath.Join(
					currentWorkingDirectory, argument)
				if path != scriptPath {
					continue
				}

				return true, nil
			}
		}
	}

	return false, nil
}

func (eae *externalAllowedExecutable) getPidAndMountNamespaceForEachProcess(
	pidString string, mountNamespace string, procFs util.ProcFsOperations,
	pids []int) (int, string, error) {
	if pidString[0] < '0' || pidString[0] > '9' {
		return -1, "", nil
	}

	pid, err := strconv.Atoi(pidString)
	if err != nil {
		return -1, "", err
	}

	mntNamespace, err := procFs.GetMountNamespace(pid)
	if err != nil {
		return -1, "", nil
	}

	cmdPath, err := procFs.GetCommandPath(pid)
	if err != nil {
		return -1, "", nil
	}

	if mntNamespace == eae.specifiedMountNamespace {
		if eae.commandAbsolutePath == "" {
			return pid, eae.specifiedMountNamespace, nil
		} else if cmdPath == eae.commandAbsolutePath ||
			cmdPath == eae.withResidentCommandAbsolutePath {
			if cmdPath == eae.commandAbsolutePath && len(eae.scripts) > 0 {
				found, err := isAllowedScript(pid, eae.scripts, procFs)
				if err != nil {
					return -1, "", err
				} else if !found {
					return -1, "", nil
				}
			}

			return pid, eae.specifiedMountNamespace, nil
		}
	} else if cmdPath == eae.commandAbsolutePath ||
		cmdPath == eae.withResidentCommandAbsolutePath {
		if mountNamespace == "" {
			if eae.specifiedMountNamespace == "" {
				if cmdPath == eae.withResidentCommandAbsolutePath {
					return pid, mntNamespace, nil
				}

				if len(eae.scripts) > 0 {
					found, err := isAllowedScript(pid, eae.scripts, procFs)
					if err != nil {
						return -1, "", err
					} else if !found {
						return -1, "", nil
					}
				}

				return pid, mntNamespace, nil
			}
		} else if mntNamespace == mountNamespace {
			return pid, mntNamespace, nil
		} else {
			if cmdPath == eae.withResidentCommandAbsolutePath {
				return -1, "", nil
			}

			if eae.specifiedMountNamespace != "" {
				mountNamespace = eae.specifiedMountNamespace
			}

			return -1, "", fmt.Errorf(
				"For %q, the mount namepsace %q for a process ID %d is "+
					"different from that %q for process IDs %v",
				eae.commandAbsolutePath, mntNamespace, pid, mountNamespace,
				pids)
		}
	}

	return -1, "", nil
}

func (eae *externalAllowedExecutable) getPidAndMountNamespace(
	pids []int, mountNamespace string, hostProcFs *hostProcFs) (
	int, string, error) {
	if len(pids) > 0 {
		var (
			hostPid            int
			hostMountNamespace string
			err                error
		)

		if eae.isHostProcess {
			hostPid, hostMountNamespace, err = hostProcFs.
				getHostPidAndMountNamespace()
			if err != nil {
				return -1, "", err
			} else if mountNamespace != hostMountNamespace {
				return -1, "", fmt.Errorf(
					"For %q, the mount namepsace %q for a host process ID %d "+
						"is different from that %q for process IDs %v",
					eae.commandAbsolutePath, hostMountNamespace, hostPid, pids,
					mountNamespace)
			}
		}

		if eae.specifiedMountNamespace == "" {
			klog.V(2).Infof(
				"Running process IDs for %q are %v",
				eae.commandAbsolutePath, pids)
			return pids[0], mountNamespace, nil
		} else {
			klog.V(2).Infof(
				"Running process IDs for the mount namespace %q are %v",
				eae.specifiedMountNamespace, pids)
			return pids[0], eae.specifiedMountNamespace, nil
		}
	} else if eae.isHostProcess {
		return hostProcFs.getHostPidAndMountNamespace()
	}

	if eae.specifiedMountNamespace != "" {
		mountNamespace = eae.specifiedMountNamespace
	}

	scripts := make([]string, 0, len(eae.scripts))
	for scriptPath := range eae.scripts {
		scripts = append(scripts, scriptPath)
	}

	return -1, "", fmt.Errorf(
		"Any processes for a namespace %q, a command path %q, and "+
			"one of script paths '%v' do NOT find in running processes",
		mountNamespace, eae.commandAbsolutePath, scripts)
}

func (eae *externalAllowedExecutable) getOneOfPidsAndMountNamespace(
	procDirectory *os.File, procFs util.ProcFsOperations,
	hostProcFs *hostProcFs) (int, string, error) {
	initPidsCapacity := 50
	readEntries := 10

	mountNamespace := ""
	pids := make([]int, 0, initPidsCapacity)

	for {
		names, err := procDirectory.Readdirnames(readEntries)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return -1, "", fmt.Errorf(
					"%q: %v", procDirectory.Name(), err.Error())
			}
		}

		for _, name := range names {
			pid, mntNamespace, err := eae.
				getPidAndMountNamespaceForEachProcess(
					name, mountNamespace, procFs, pids)
			if err != nil {
				return -1, "", err
			}

			if mountNamespace == "" && mntNamespace != "" {
				mountNamespace = mntNamespace
			}

			if pid != -1 {
				pids = append(pids, pid)
			}
		}
	}

	return eae.getPidAndMountNamespace(pids, mountNamespace, hostProcFs)
}

func (eae *externalAllowedExecutable) getOneOfPidsAndMountNamespaceForExecutable() (
	int, string, error) {
	procFs := util.NewProcFs()

	procDirectory := procFs.GetProcFsRootPath()
	directory, err := os.Open(procDirectory)
	if err != nil {
		return -1, "", fmt.Errorf("%q: %v", procDirectory, err.Error())
	}
	defer directory.Close()

	hostDelegatePid := 1
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	return eae.getOneOfPidsAndMountNamespace(directory, procFs, hostProcFs)
}
