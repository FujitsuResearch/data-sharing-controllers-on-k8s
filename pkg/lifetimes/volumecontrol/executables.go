// Copyright (c) 2022 Fujitsu Limited

package volumecontrol

import (
	"encoding/hex"
	"fmt"
	"reflect"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	volumecontrolapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

type allowedScript struct {
	checksum                  string
	writable                  bool
	relativePathToInitWorkDir string
}

type allowedExecutable struct {
	checksum string
	writable bool
	scripts  map[string]*allowedScript
}

type executablesDiffs struct {
	addedAndUpdatedExecutables map[string]*allowedExecutable
	deletedPaths               []string
}

type lifetimeExecutablesDiffs map[string]*executablesDiffs

func convertAllowedScripts(
	allowedScriptsList []*lifetimesapi.AllowedScript) (
	map[string]*allowedScript, error) {
	allowedScripts := map[string]*allowedScript{}
	for _, script := range allowedScriptsList {
		if script.AbsolutePath == "" {
			return nil, fmt.Errorf(
				"'Path' must be given: %+v", script)
		}

		if script.Checksum == "" && !script.Writable &&
			script.RelativePathToInitWorkDir != "" {
			allowedScripts[script.AbsolutePath] = nil
		} else {
			allowedScripts[script.AbsolutePath] = &allowedScript{
				checksum:                  script.Checksum,
				writable:                  script.Writable,
				relativePathToInitWorkDir: script.RelativePathToInitWorkDir,
			}
		}
	}

	return allowedScripts, nil
}

func convertAllowedExecutables(
	allowedExecutablesList []*lifetimesapi.AllowedExecutable) (
	map[string]*allowedExecutable, error) {
	allowedExecutables := map[string]*allowedExecutable{}

	for _, executable := range allowedExecutablesList {
		if executable.CmdAbsolutePath == "" {
			return nil, fmt.Errorf(
				"'Path' must be given: %+v", executable)
		}

		allowedScripts := map[string]*allowedScript{}
		if len(executable.Scripts) != 0 {
			var err error

			allowedScripts, err = convertAllowedScripts(executable.Scripts)
			if err != nil {
				return nil, err
			}
		}

		if executable.Checksum == "" && !executable.Writable &&
			len(allowedScripts) == 0 {
			allowedExecutables[executable.CmdAbsolutePath] = nil
		} else {
			allowedExecutables[executable.CmdAbsolutePath] = &allowedExecutable{
				checksum: executable.Checksum,
				writable: executable.Writable,
				scripts:  allowedScripts,
			}
		}
	}

	return allowedExecutables, nil
}

func newLifetimeExecutablesDiffs() lifetimeExecutablesDiffs {
	return map[string]*executablesDiffs{}
}

func (led lifetimeExecutablesDiffs) areEmpty() bool {
	return len(led) == 0
}

func (led lifetimeExecutablesDiffs) addExecutablesDiff(
	podVolumeName string,
	addedAndUpdatedExecutables map[string]*allowedExecutable,
	deletedPaths []string) {
	led[podVolumeName] = &executablesDiffs{
		addedAndUpdatedExecutables: addedAndUpdatedExecutables,
		deletedPaths:               deletedPaths,
	}
}

func createLifetimeScripts(
	allowedScripts map[string]*allowedScript) (
	map[string]*volumecontrolapi.Script, error) {
	lifetimeScripts := map[string]*volumecontrolapi.Script{}
	for path, script := range allowedScripts {
		if script == nil {
			lifetimeScripts[path] = &volumecontrolapi.Script{}
		} else {
			checksumBytes, err := hex.DecodeString(script.checksum)
			if err != nil {
				return nil, fmt.Errorf(
					"[Script] Checksum %q: %v", script.checksum, err)
			}

			lifetimeScripts[path] = &volumecontrolapi.Script{
				Checksum:                  checksumBytes,
				Writable:                  script.writable,
				RelativePathToInitWorkDir: script.relativePathToInitWorkDir,
			}
		}
	}

	return lifetimeScripts, nil
}

func createLifetimeExecutables(
	allowedExecutables map[string]*allowedExecutable) (
	map[string]*volumecontrolapi.Executable, error) {
	lifetimeExecutables := map[string]*volumecontrolapi.Executable{}
	for path, executable := range allowedExecutables {
		if executable == nil {
			lifetimeExecutables[path] = &volumecontrolapi.Executable{}
		} else {
			checksumBytes, err := hex.DecodeString(executable.checksum)
			if err != nil {
				return nil, fmt.Errorf(
					"[Executable] Checksum %q: %v", executable.checksum, err)
			}

			allowedScripts, err := createLifetimeScripts(executable.scripts)
			if err != nil {
				return nil, err
			}

			lifetimeExecutables[path] = &volumecontrolapi.Executable{
				Checksum: checksumBytes,
				Writable: executable.writable,
				Scripts:  allowedScripts,
			}
		}
	}

	return lifetimeExecutables, nil
}

func CreateGrpcAllowedExecutables(
	allowedExecutablesList []*lifetimesapi.AllowedExecutable) (
	map[string]*volumecontrolapi.Executable, error) {
	allowedExecutables, err := convertAllowedExecutables(
		allowedExecutablesList)
	if err != nil {
		return nil, err
	}

	return createLifetimeExecutables(allowedExecutables)
}

func getAllowedExecutables(
	lifetimeSpec *lifetimesapi.LifetimeSpec) (
	map[string]map[string]*allowedExecutable, error) {
	executables := map[string]map[string]*allowedExecutable{}

	for _, dataSpec := range lifetimeSpec.InputData {
		if dataSpec.FileSystemSpec == nil {
			continue
		}

		pvcKey := util.ConcatenateNamespaceAndName(
			dataSpec.FileSystemSpec.ToPersistentVolumeClaimRef.Namespace,
			dataSpec.FileSystemSpec.ToPersistentVolumeClaimRef.Name)

		allowedExecutables, err := convertAllowedExecutables(
			dataSpec.FileSystemSpec.AllowedExecutables)
		if err != nil {
			return nil, err
		}

		executables[pvcKey] = allowedExecutables
	}

	for _, dataSpec := range lifetimeSpec.OutputData {
		if dataSpec.FileSystemSpec == nil {
			continue
		}

		pvcKey := util.ConcatenateNamespaceAndName(
			dataSpec.FileSystemSpec.PersistentVolumeClaimRef.Namespace,
			dataSpec.FileSystemSpec.PersistentVolumeClaimRef.Name)

		allowedExecutables, err := convertAllowedExecutables(
			dataSpec.FileSystemSpec.AllowedExecutables)
		if err != nil {
			return nil, err
		}

		executables[pvcKey] = allowedExecutables
	}

	return executables, nil
}

func getExecutablesDiffs(
	oldAllowedExecutables map[string]map[string]*allowedExecutable,
	newAllowedExecutables map[string]map[string]*allowedExecutable) lifetimeExecutablesDiffs {
	executablesDiffs := newLifetimeExecutablesDiffs()

	for podVolume, newExecutables := range newAllowedExecutables {
		oldExecutables, ok := oldAllowedExecutables[podVolume]
		if !ok {
			continue
		}

		addedAndUpdated := map[string]*allowedExecutable{}
		deletedPaths := []string{}

		for path, newExecutable := range newExecutables {
			oldExecutable, ok := oldExecutables[path]
			if ok {
				if !reflect.DeepEqual(oldExecutable, newExecutable) {
					addedAndUpdated[path] = newExecutable
				}
			} else {
				addedAndUpdated[path] = newExecutable
			}
		}

		for path := range oldExecutables {
			if _, ok := newExecutables[path]; !ok {
				deletedPaths = append(deletedPaths, path)
			}
		}

		changed := len(addedAndUpdated) != 0 || len(deletedPaths) != 0
		if changed {
			executablesDiffs.addExecutablesDiff(
				podVolume, addedAndUpdated, deletedPaths)
		}
	}

	return executablesDiffs
}

func getAllowedExecutablesDiffs(
	oldLifetimeSpec *lifetimesapi.LifetimeSpec,
	newLifetimeSpec *lifetimesapi.LifetimeSpec) (
	lifetimeExecutablesDiffs, error) {
	oldAllowedExecutables, err := getAllowedExecutables(oldLifetimeSpec)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to get allowed executables form a old 'DataLifetime': %v",
			err.Error())
	}

	newAllowedExecutables, err := getAllowedExecutables(newLifetimeSpec)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to get allowed executables form a new 'DataLifetime': %v",
			err.Error())
	}

	return getExecutablesDiffs(
		oldAllowedExecutables, newAllowedExecutables), nil
}

func CreateGrpcExecutablesDiffs(
	oldLifetimeSpec *lifetimesapi.LifetimeSpec,
	newLifetimeSpec *lifetimesapi.LifetimeSpec) (
	map[string]*volumecontrolapi.ExecutablesDiffs, error) {
	diffs, err := getAllowedExecutablesDiffs(oldLifetimeSpec, newLifetimeSpec)
	if err != nil {
		return nil, err
	}

	if diffs.areEmpty() {
		return nil, nil
	}

	executablesDiffs := map[string]*volumecontrolapi.ExecutablesDiffs{}

	for podVolume, execsDiffs := range diffs {
		addedAndUpdated, err := createLifetimeExecutables(
			execsDiffs.addedAndUpdatedExecutables)
		if err != nil {
			return nil, err
		}

		executablesDiffs[podVolume] = &volumecontrolapi.ExecutablesDiffs{
			AddedAndUpdated: addedAndUpdated,
			DeletedPaths:    execsDiffs.deletedPaths,
		}
	}

	return executablesDiffs, nil
}
