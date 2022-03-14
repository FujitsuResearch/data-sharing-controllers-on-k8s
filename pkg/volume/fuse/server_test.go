// Copyright (c) 2022 Fujitsu Limited

package fuse

import (
	"os"
	"testing"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	mqpub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"

	"github.com/stretchr/testify/assert"
)

func newFuseVolumeController() *FuseVolumeController {
	return &FuseVolumeController{
		root: &volumeControlRoot{
			containerRoots: ContainerRoots{},
		},
	}
}

func (fvc *FuseVolumeController) getAllowedExecutables(
	mountNamespace string) (AllowedExecutables, error) {
	fvc.root.rootsMutex.RLock()
	defer fvc.root.rootsMutex.RUnlock()

	containerRoot, found := fvc.root.containerRoots[mountNamespace]
	if !found {
		return nil, nil
	}

	executables := AllowedExecutables{}
	for exePath, executable := range containerRoot.allowedExecutables {
		c := make([]byte, len(executable.checksum))
		copy(c, executable.checksum)

		scripts := map[string]*allowedScript{}
		for scrPath, script := range executable.scripts {
			c := make([]byte, len(script.checksum))
			copy(c, script.checksum)
			scripts[scrPath] = &allowedScript{
				checksum: c,
				writable: script.writable,
			}
		}

		executables[exePath] = &allowedExecutable{
			checksum: c,
			writable: executable.writable,
			scripts:  scripts,
		}
	}

	return executables, nil
}

func testAddAndDeleteExecutableChecksumsAndScripts(
	t *testing.T, isDeleteExecutablePassNil bool) {
	fuseVolumeController := newFuseVolumeController()

	mountNamespace := getMountNamespaceId(t, os.Getpid())
	rootPath := "/"
	exePath1 := "/usr/bin/x"
	exePath2 := "/usr/local/bin/y"
	allowedExecutables1 := AllowedExecutables{
		exePath1: {
			checksum: []byte{0x01, 0x23},
			writable: true,
			scripts:  map[string]*allowedScript{},
		},
		exePath2: {
			checksum: []byte{0x45, 0x67, 0x89},
			writable: true,
			scripts:  map[string]*allowedScript{},
		},
	}
	err := fuseVolumeController.AddAllowedExecutables(
		mountNamespace, rootPath, allowedExecutables1)
	assert.NoError(t, err)

	executables, err := fuseVolumeController.getAllowedExecutables(
		mountNamespace)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(executables))
	assert.Equal(t, allowedExecutables1, executables)

	exePath3 := "/usr/sbin/z"
	exePath4 := "/bin/bash"
	allowedExecutables2 := AllowedExecutables{
		exePath3: {
			checksum: []byte{0xab, 0xcd, 0xef},
			writable: false,
		},
		exePath4: {
			scripts: map[string]*allowedScript{
				"/usr/local/bin/test.sh": {
					checksum: []byte{0xaa, 0xbb, 0xcc},
				},
			},
		},
	}
	err = fuseVolumeController.AddAllowedExecutables(
		mountNamespace, rootPath, allowedExecutables2)
	assert.NoError(t, err)

	executables, err = fuseVolumeController.getAllowedExecutables(
		mountNamespace)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(executables))

	allowedExecutables12 := AllowedExecutables{
		exePath1: {
			checksum: []byte{0x01, 0x23},
			writable: true,
			scripts:  map[string]*allowedScript{},
		},
		exePath2: {
			checksum: []byte{0x45, 0x67, 0x89},
			writable: true,
			scripts:  map[string]*allowedScript{},
		},
		exePath3: {
			checksum: []byte{0xab, 0xcd, 0xef},
			writable: false,
			scripts:  map[string]*allowedScript{},
		},
		exePath4: {
			checksum: []byte{},
			writable: false,
			scripts: map[string]*allowedScript{
				"/usr/local/bin/test.sh": {
					checksum: []byte{0xaa, 0xbb, 0xcc},
				},
			},
		},
	}
	assert.Equal(t, allowedExecutables12, executables)

	executablePaths := []string(nil)
	if !isDeleteExecutablePassNil {
		executablePaths = []string{
			exePath1,
			exePath2,
			exePath3,
			exePath4,
		}
	}

	fuseVolumeController.DeleteAllowedExecutables(
		mountNamespace, executablePaths)

	executables, err = fuseVolumeController.getAllowedExecutables(
		mountNamespace)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(executables))
}

func TestAddAndDeleteExecutableChecksumsAndScripts(t *testing.T) {
	testAddAndDeleteExecutableChecksumsAndScripts(t, false)
}

func TestAddAndDeleteExecutableChecksumsAndScriptsWithAllExecutablesDeletion(
	t *testing.T) {
	testAddAndDeleteExecutableChecksumsAndScripts(t, true)
}

func TestUpdateExecutableChecksumsAndScripts(t *testing.T) {
	fuseVolumeController := newFuseVolumeController()

	mountNamespace := getMountNamespaceId(t, os.Getpid())
	rootPath := "/"
	exePath1 := "/usr/bin/x"
	exePath2 := "/usr/local/bin/y"
	exePath3 := "/bin/bash"
	allowedExecutables1 := AllowedExecutables{
		exePath1: {
			checksum: []byte{0x01, 0x23},
			writable: true,
			scripts:  map[string]*allowedScript{},
		},
		exePath2: {
			checksum: []byte{0x45, 0x67, 0x89},
			writable: true,
			scripts:  map[string]*allowedScript{},
		},
		exePath3: {
			checksum: []byte{},
			writable: true,
			scripts: map[string]*allowedScript{
				"/usr/local/bin/test1.sh": {
					checksum: []byte{0x11, 0x22, 0x33},
				},
				"/usr/local/bin/test2.sh": {
					checksum: []byte{0xaa, 0xbb, 0xcc},
				},
			},
		},
	}

	err := fuseVolumeController.AddAllowedExecutables(
		mountNamespace, rootPath, allowedExecutables1)
	assert.NoError(t, err)

	executables, err := fuseVolumeController.getAllowedExecutables(
		mountNamespace)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(executables))
	assert.Equal(t, allowedExecutables1, executables)

	exePath4 := "/usr/local/bin/z"
	allowedExecutables2 := AllowedExecutables{
		exePath2: {
			checksum: []byte{0xab, 0xcd},
			writable: false,
		},
		exePath3: {
			scripts: map[string]*allowedScript{
				"/usr/local/bin/test2.sh": {
					checksum: []byte{0xff, 0xbb, 0xcc},
				},
				"/usr/local/bin/test.py": {
					checksum: []byte{0x44, 0x55, 0x66},
				},
			},
		},
		exePath4: {
			checksum: []byte{0xef},
			writable: true,
		},
	}

	err = fuseVolumeController.UpdateAllowedExecutables(
		mountNamespace, allowedExecutables2)
	assert.NoError(t, err)

	executables, err = fuseVolumeController.getAllowedExecutables(
		mountNamespace)
	assert.NoError(t, err)
	expectedExecutables := AllowedExecutables{
		exePath1: {
			checksum: []byte{0x01, 0x23},
			writable: true,
			scripts:  map[string]*allowedScript{},
		},
		exePath2: {
			checksum: []byte{0xab, 0xcd},
			writable: false,
			scripts:  map[string]*allowedScript{},
		},
		exePath3: {
			checksum: []byte{},
			writable: false,
			scripts: map[string]*allowedScript{
				"/usr/local/bin/test2.sh": {
					checksum: []byte{0xff, 0xbb, 0xcc},
				},
				"/usr/local/bin/test.py": {
					checksum: []byte{0x44, 0x55, 0x66},
				},
			},
		},
		exePath4: {
			checksum: []byte{0xef},
			writable: true,
			scripts:  map[string]*allowedScript{},
		},
	}
	assert.Equal(t, 4, len(executables))
	assert.Equal(t, expectedExecutables, executables)

	executablePaths := []string{
		exePath1,
		exePath2,
		exePath3,
		exePath4,
	}

	err = fuseVolumeController.DeleteAllowedExecutables(
		mountNamespace, executablePaths)
	assert.NoError(t, err)

	executables, err = fuseVolumeController.getAllowedExecutables(
		mountNamespace)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(executables))
}

func TestAddAndDeleteMessageQueueUpdatePublisher(t *testing.T) {
	fuseVolumeController := newFuseVolumeController()

	messageQueueConfig := &api.MessageQueue{
		Brokers:          []string{"127.0.0.1:9093"},
		User:             "user-1",
		Password:         "pwd-1",
		Topic:            "update.topic",
		CompressionCodec: mqpub.CompressionCodecNone,
		MaxBatchBytes:    "0",
	}
	mountNamespaces := []string{
		"mns-1",
	}
	volumeRootPath := "/mnt/root"
	err := fuseVolumeController.AddMessageQueueUpdatePublisher(
		messageQueueConfig, volumeRootPath, mountNamespaces)
	assert.NoError(t, err)
	assert.NotNil(t, fuseVolumeController.root.messageQueueUpdatePublisher)

	err = fuseVolumeController.DeleteMessageQueueUpdatePublisher(
		mountNamespaces)
	assert.NoError(t, err)
	assert.Nil(t, fuseVolumeController.root.messageQueueUpdatePublisher)
}
