// Copyright (c) 2022 Fujitsu Limited

package fuse

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
)

const (
	AllowedExecutableChecksumKey = "checksum"
	AllowedExecutableWritableKey = "writable"
)

type FuseVolumeController struct {
	server *fuse.Server
	root   *volumeControlRoot
}

// key: executable path
type AllowedExecutables map[string]*allowedExecutable

type ContainerRoot struct {
	rootPath           string
	allowedExecutables AllowedExecutables
}

// key: mount namespace, [TODO] update
type ContainerRoots map[string]*ContainerRoot

// key: mount namespace
type PublishingContainers map[string]struct{}

type AllowedExecutablesDiffs struct {
	AddedAndUpdated AllowedExecutables
	DeletedPaths    []string
}

type ContainerRootString struct {
	RootPath           string
	AllowedExecutables map[string]map[string]string
}

func NewFuseController(
	mountPoint string, sourcePath string, debug bool, disableUsageControl bool,
	checksumCalculationAlways bool, stopCh <-chan struct{}) (
	*FuseVolumeController, error) {
	oneSec := time.Second
	opts := &fs.Options{
		EntryTimeout: &oneSec,
		AttrTimeout:  &oneSec,
		MountOptions: fuse.MountOptions{
			Options: []string{
				"allow_other",
			},
			Debug: debug,
		},
	}

	root, rootData, err := newVolumeControlRoot(
		sourcePath, disableUsageControl, checksumCalculationAlways, stopCh)
	if err != nil {
		return nil, fmt.Errorf("%q: %v", sourcePath, err)
	}

	server, err := fs.Mount(mountPoint, root, opts)
	if err != nil {
		return nil, err
	}

	return &FuseVolumeController{
		server: server,
		root:   rootData,
	}, nil
}

func (fvc *FuseVolumeController) Wait() {
	fvc.server.Wait()
}

func (fvc *FuseVolumeController) Unmount() error {
	return fvc.server.Unmount()
}

func createExternalAllowedScripts(
	lifetimeScripts []*api.ExternalAllowedScript) map[string]*allowedScript {
	allowedScripts := map[string]*allowedScript{}

	for _, script := range lifetimeScripts {
		allowedScripts[script.AbsolutePath] = &allowedScript{
			writable:                  script.Writable,
			relativePathToInitWorkDir: script.RelativePathToInitWorkDir,
		}
	}

	return allowedScripts
}

func CreateAllowedExecutablesForExternalAllowedExecutables(
	executables []*api.ExternalAllowedExecutable) AllowedExecutables {
	allowedExecutables := AllowedExecutables{}

	for _, executable := range executables {
		scripts := createExternalAllowedScripts(executable.ExternalAllowedScripts)

		allowedExecutables[executable.CommandAbsolutePath] = &allowedExecutable{
			writable: executable.Writable,
			scripts:  scripts,
		}
	}

	return allowedExecutables
}

func createAllowedScripts(
	lifetimeScripts map[string]*api.Script) map[string]*allowedScript {
	allowedScripts := map[string]*allowedScript{}

	for path, script := range lifetimeScripts {
		allowedScripts[path] = &allowedScript{
			checksum:                  script.Checksum,
			writable:                  script.Writable,
			relativePathToInitWorkDir: script.RelativePathToInitWorkDir,
		}
	}

	return allowedScripts
}

func CreateAllowedExecutables(
	lifetimeExecutables map[string]*api.Executable) AllowedExecutables {
	allowedExecutables := map[string]*allowedExecutable{}

	for path, executable := range lifetimeExecutables {
		scripts := createAllowedScripts(executable.Scripts)

		allowedExecutables[path] = &allowedExecutable{
			checksum: executable.Checksum,
			writable: executable.Writable,
			scripts:  scripts,
		}
	}

	return allowedExecutables
}

func CreateAllowedExecutablesDiffs(
	executablesDiffs map[string]*api.ExecutablesDiffs) map[string]*AllowedExecutablesDiffs {
	allowedExecutablesDiff := map[string]*AllowedExecutablesDiffs{}

	for pvcKey, diffs := range executablesDiffs {
		executables := CreateAllowedExecutables(diffs.AddedAndUpdated)

		allowedExecutablesDiff[pvcKey] = &AllowedExecutablesDiffs{
			AddedAndUpdated: executables,
			DeletedPaths:    diffs.DeletedPaths,
		}
	}

	return allowedExecutablesDiff
}

func (fvc *FuseVolumeController) AddAllowedExecutables(
	mountNamespace string, rootPath string,
	executables AllowedExecutables) error {
	fvc.root.rootsMutex.Lock()
	defer fvc.root.rootsMutex.Unlock()

	containerRoot, found := fvc.root.containerRoots[mountNamespace]
	if !found {
		fvc.root.containerRoots[mountNamespace] = &ContainerRoot{
			rootPath:           rootPath,
			allowedExecutables: AllowedExecutables{},
		}
		containerRoot = fvc.root.containerRoots[mountNamespace]
	} else {
		if containerRoot.rootPath != rootPath {
			return fmt.Errorf(
				"The root path %q is NOT equal to the registered one %q",
				rootPath, containerRoot.rootPath)
		}
	}

	if containerRoot.allowedExecutables != nil {
		for path, executable := range executables {
			containerRoot.allowedExecutables[path] = executable
		}
	} else {
		if executables != nil {
			containerRoot.allowedExecutables = executables
		}
	}

	return nil
}

func (fvc *FuseVolumeController) UpdateAllowedExecutables(
	mountNamespace string, executables AllowedExecutables) error {
	fvc.root.rootsMutex.Lock()
	defer fvc.root.rootsMutex.Unlock()

	containerRoot, found := fvc.root.containerRoots[mountNamespace]
	if !found {
		return fmt.Errorf(
			"Not find information on the container root for "+
				"the mount namespace %q", mountNamespace)
	}

	for path, executable := range executables {
		containerRoot.allowedExecutables[path] = executable
	}

	return nil
}

func (fvc *FuseVolumeController) DeleteAllowedExecutables(
	mountNamespace string, executablePaths []string) error {
	fvc.root.rootsMutex.Lock()
	defer fvc.root.rootsMutex.Unlock()

	if mountNamespace != "" {
		containerRoot, found := fvc.root.containerRoots[mountNamespace]
		if !found {
			return fmt.Errorf(
				"Not find information on the container root for "+
					"the mount namespace %q", mountNamespace)
		}

		if executablePaths == nil {
			for path := range containerRoot.allowedExecutables {
				delete(containerRoot.allowedExecutables, path)
			}

			delete(fvc.root.containerRoots, mountNamespace)
		} else {
			for _, path := range executablePaths {
				delete(containerRoot.allowedExecutables, path)
			}

			if len(containerRoot.allowedExecutables) == 0 {
				delete(fvc.root.containerRoots, mountNamespace)
			}
		}
	} else {
		for mntNamespace, containerRoot := range fvc.root.containerRoots {
			for path := range containerRoot.allowedExecutables {
				delete(containerRoot.allowedExecutables, path)
			}

			delete(fvc.root.containerRoots, mntNamespace)
		}
	}

	return nil
}

func (fvc *FuseVolumeController) AddMessageQueueUpdatePublisher(
	messageQueue *api.MessageQueue, volumeRootPath string,
	mountNamespaces []string) error {
	return fvc.root.addMessageQeueueUpdatePublisher(
		messageQueue, volumeRootPath, mountNamespaces)
}

func (fvc *FuseVolumeController) DeleteMessageQueueUpdatePublisher(
	mountNamespaces []string) error {
	return fvc.root.deleteMessageQeueueUpdatePublisher(mountNamespaces)
}

func (fvc *FuseVolumeController) GetContainerRootsNamespaces() map[string]struct{} {
	fvc.root.rootsMutex.RLock()
	defer fvc.root.rootsMutex.RUnlock()

	rootNamespaces := map[string]struct{}{}
	for mntNamespace := range fvc.root.containerRoots {
		rootNamespaces[mntNamespace] = struct{}{}
	}

	return rootNamespaces
}

// used by only test cases in
// <pkg/controller/volumecontrol/volume_controller_test.go>
func (fvc *FuseVolumeController) GetContainerRootString(
	mountNamespace string) ContainerRootString {
	fvc.root.rootsMutex.RLock()
	defer fvc.root.rootsMutex.RUnlock()

	root := fvc.root.containerRoots[mountNamespace]
	allowedExecutables := map[string]map[string]string{}

	for path, executable := range root.allowedExecutables {
		allowedExecutables[path] = map[string]string{
			AllowedExecutableChecksumKey: string(executable.checksum),
			AllowedExecutableWritableKey: strconv.FormatBool(
				executable.writable),
		}
	}

	return ContainerRootString{
		RootPath:           root.rootPath,
		AllowedExecutables: allowedExecutables,
	}
}
