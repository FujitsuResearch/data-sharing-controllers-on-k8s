// Copyright (c) 2022 Fujitsu Limited

package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/posixtest"
	"github.com/segmentio/kafka-go"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
	mqpub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/fuse/internal/testutil"
	volumemq "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"

	"github.com/stretchr/testify/assert"
)

const (
	executable1      = "testing/fixtures/test1"
	copiedExecutable = "testing/fixtures/test1-1"
	executable2      = "testing/fixtures/test2"
	executable3      = "testing/fixtures/test3"
	bashScriptPath   = "/bin/bash"
	script1          = "testing/fixtures/test1.sh"
	copiedScript     = "testing/fixtures/test1-1.sh"
	script2          = "testing/fixtures/test2.sh"
	script3          = "testing/fixtures/test3.sh"
	symlinkScript    = "./test4.sh"

	kafkaBrokerAddressEnv = "KAFKA_BROKER_ADDRESS"

	// [REF] github.com/segmentio/kafka-go/docker-compose.yml
	saslUser     = "adminscram"
	saslPassword = "admin-secret-512"
)

func getMountNamespaceId(t *testing.T, pid int) string {
	mntNamespaceHandle := fmt.Sprintf("/proc/%d/ns/mnt", pid)
	mountNamespace, err := os.Readlink(mntNamespaceHandle)
	assert.NoError(t, err)

	return mountNamespace
}

func newFakeVolumeControlRoot(
	t *testing.T, executablePath string, checksumExecutablePath string,
	executableWritable bool, scriptPath string, checksumScriptPath string,
	scriptWritable bool, scriptSearchPath string,
	checksumCalculationAlways bool,
	stopCh <-chan struct{}) *volumeControlRoot {
	vcr := &volumeControlRoot{
		path:                      "",
		containerRoots:            map[string]*ContainerRoot{},
		checksumCalculationAlways: checksumCalculationAlways,
		procFsOps:                 util.NewProcFs(),
	}

	if !checksumCalculationAlways {
		vcr.checksumCaches = map[string][]byte{}
		vcr.cmdCh = make(chan commandInfo, checksumChSize)
	}

	watcher, err := fsnotify.NewWatcher()
	assert.NoError(t, err)
	go vcr.updateChecksumCaches(watcher, stopCh)

	exePath := executablePath
	if !filepath.IsAbs(executablePath) {
		cwd, err := os.Getwd()
		assert.NoError(t, err)
		exePath = filepath.Join(cwd, executablePath)
	}

	var exeChecksum []byte
	if checksumExecutablePath == "" {
		exeChecksum = nil
	} else {
		exeChecksum, err = util.CalculateChecksum(checksumExecutablePath)
		assert.NoError(t, err)
	}

	scripts := map[string]*allowedScript{}
	if scriptPath != "" {
		scrPath := scriptPath
		if !filepath.IsAbs(scriptPath) {
			cwd, err := os.Getwd()
			assert.NoError(t, err)
			scrPath = filepath.Join(cwd, scriptPath)
		}

		var scrChecksum []byte
		if checksumScriptPath == "" {
			scrChecksum = nil
		} else {
			scrChecksum, err = util.CalculateChecksum(checksumScriptPath)
			assert.NoError(t, err)
		}

		scripts[scrPath] = &allowedScript{
			checksum:                  scrChecksum,
			writable:                  scriptWritable,
			relativePathToInitWorkDir: scriptSearchPath,
		}
	}

	mountNamespace := getMountNamespaceId(t, os.Getpid())
	vcr.containerRoots[mountNamespace] = &ContainerRoot{
		allowedExecutables: AllowedExecutables{
			exePath: &allowedExecutable{
				checksum: exeChecksum,
				writable: executableWritable,
				scripts:  scripts,
			},
		},
	}

	return vcr
}

func (vcr *volumeControlRoot) setContainerRoots(
	t *testing.T, mountNamespace string, pid int) {
	containerRoot, found := vcr.containerRoots[mountNamespace]
	if found {
		containerRoot.rootPath = "/"
	} else {
		vcr.containerRoots = map[string]*ContainerRoot{
			mountNamespace: &ContainerRoot{
				rootPath: "/",
			},
		}
	}
}

func (vcr *volumeControlRoot) checkChecksumCachesForExecutable(
	t *testing.T, mountNamespace string, included bool) {
	vcr.checksumMutex.RLock()
	for cmdPath := range vcr.containerRoots[mountNamespace].
		allowedExecutables {
		_, found := vcr.checksumCaches[cmdPath]
		if included {
			assert.True(t, found)
		} else {
			assert.False(t, found)
		}
	}
	vcr.checksumMutex.RUnlock()
}

func (vcr *volumeControlRoot) checkChecksumCachesForScript(
	t *testing.T, mountNamespace string, isExecutableIncluded bool) {
	vcr.checksumMutex.RLock()
	for cmdPath, executable := range vcr.containerRoots[mountNamespace].
		allowedExecutables {
		_, found := vcr.checksumCaches[cmdPath]
		if isExecutableIncluded {
			assert.True(t, found)
		} else {
			assert.False(t, found)
		}

		for scriptPath := range executable.scripts {
			_, found := vcr.checksumCaches[scriptPath]
			assert.True(t, found)
		}
	}
	vcr.checksumMutex.RUnlock()
}

func (vcr *volumeControlRoot) checkChecksumCachesCount(
	t *testing.T, expectedCount int) {
	vcr.checksumMutex.RLock()
	assert.Equal(t, expectedCount, len(vcr.checksumCaches))
	vcr.checksumMutex.RUnlock()
}

func copyFile(t *testing.T, toFilePath string, fromPath string) {
	cwd, err := os.Getwd()
	assert.NoError(t, err)

	fromExePath := filepath.Join(cwd, fromPath)
	toExePath := filepath.Join(cwd, toFilePath)

	bytes, err := ioutil.ReadFile(fromExePath)
	assert.NoError(t, err)

	err = ioutil.WriteFile(toExePath, bytes, 0755)
	assert.NoError(t, err)
}

func removeFile(t *testing.T, filePath string) {
	cwd, err := os.Getwd()
	assert.NoError(t, err)
	exePath := filepath.Join(cwd, filePath)

	err = os.Remove(exePath)
	assert.NoError(t, err)
}

func TestAllowExecutableWithChecksumCaches(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, executable1, executable1, false, "", "", false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable1)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.True(t, allowed)

	time.Sleep(1 * time.Millisecond)
	vcr.checkChecksumCachesForExecutable(t, mountNamespace, true)
}

func TestAllowExecutableWithoutChecksumCaches(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := true
	vcr := newFakeVolumeControlRoot(
		t, executable1, executable1, false, "", "", false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable1)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.True(t, allowed)

	vcr.checkChecksumCachesCount(t, 0)
}

func TestAllowExecutableWithoutChecksum(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, executable1, "", false, "", "", false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable1)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.True(t, allowed)
}

func TestDenyMountNamespaceNotFound(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, executable1, executable1, false, "", "", false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable1)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := "dummy"
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.False(t, allowed)
}

func TestDenyExecutablePathNotFound(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, executable2, executable1, false, "", "", false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable1)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.False(t, allowed)
}

func TestDenyInvalidExecutable(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, executable1, executable3, false, "", "", false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable1)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.False(t, allowed)
}

func TestDenyWriteToReadOnlyDataForExecutable(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, executable1, executable1, false, "", "", false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable1)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), true)
	assert.NoError(t, err)
	assert.False(t, allowed)
}

func TestDenyUpdatedExecutable(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	copyFile(t, copiedExecutable, executable1)
	defer removeFile(t, copiedExecutable)

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, copiedExecutable, copiedExecutable, false, "", "", false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	cmd := exec.CommandContext(ctx, copiedExecutable)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.True(t, allowed)

	time.Sleep(1 * time.Millisecond)
	vcr.checkChecksumCachesForExecutable(t, mountNamespace, true)

	cancel()

	copyFile(t, copiedExecutable, executable2)
	vcr.checkChecksumCachesCount(t, 0)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd = exec.CommandContext(ctx, copiedExecutable)
	err = cmd.Start()
	assert.NoError(t, err)

	pid = cmd.Process.Pid
	allowed, err = vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.False(t, allowed)
	vcr.checkChecksumCachesCount(t, 0)
}

func TestAllowScriptWithChecksumCaches(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, bashScriptPath, bashScriptPath, false, script1, script1, false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, script1)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.True(t, allowed)

	time.Sleep(1 * time.Millisecond)
	vcr.checkChecksumCachesForExecutable(t, mountNamespace, true)
	vcr.checkChecksumCachesForScript(t, mountNamespace, true)
}

func TestAllowSymbolicLinkScriptWithChecksumCaches(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, bashScriptPath, bashScriptPath, false, script1, script1, false, "",
		checksumCalculationAlways, stopCh)

	err := os.Symlink(script1, symlinkScript)
	assert.NoError(t, err)
	defer os.Remove(symlinkScript)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, symlinkScript)
	err = cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.True(t, allowed)

	time.Sleep(1 * time.Millisecond)
	vcr.checkChecksumCachesForExecutable(t, mountNamespace, true)
	vcr.checkChecksumCachesForScript(t, mountNamespace, true)
}

func TestAllowScriptWithoutChecksumCaches(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := true
	vcr := newFakeVolumeControlRoot(
		t, bashScriptPath, bashScriptPath, false, script1, script1, false, "",
		checksumCalculationAlways, stopCh)

	cwd, err := os.Getwd()
	assert.NoError(t, err)
	scriptAbsolutePath := filepath.Join(cwd, script1)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, scriptAbsolutePath)
	err = cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.True(t, allowed)

	vcr.checkChecksumCachesCount(t, 0)
}

func TestAllowAbsoluteSymbolicLinkScriptWithoutChecksumCaches(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := true
	vcr := newFakeVolumeControlRoot(
		t, bashScriptPath, bashScriptPath, false, script1, script1, false, "",
		checksumCalculationAlways, stopCh)

	cwd, err := os.Getwd()
	assert.NoError(t, err)
	symlinkAbsolutePath := filepath.Join(cwd, symlinkScript)

	err = os.Symlink(script1, symlinkAbsolutePath)
	assert.NoError(t, err)
	defer os.Remove(symlinkAbsolutePath)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, symlinkAbsolutePath)
	err = cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.True(t, allowed)

	vcr.checkChecksumCachesCount(t, 0)
}

func TestAllowRelativeSymbolicLinkScriptWithoutChecksumCaches(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := true
	vcr := newFakeVolumeControlRoot(
		t, bashScriptPath, bashScriptPath, false, script1, script1, false, "",
		checksumCalculationAlways, stopCh)

	cwd, err := os.Getwd()
	assert.NoError(t, err)
	scriptAbsolutePath := filepath.Join(cwd, script1)

	err = os.Symlink(scriptAbsolutePath, symlinkScript)
	assert.NoError(t, err)
	defer os.Remove(symlinkScript)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, symlinkScript)
	err = cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.True(t, allowed)

	vcr.checkChecksumCachesCount(t, 0)
}

func TestAllowScriptWithoutChecksum(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	relativePathToInitWorkDir := "../"
	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, bashScriptPath, bashScriptPath, false, script3, script3, false,
		relativePathToInitWorkDir, checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, script3)
	err := cmd.Start()

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.True(t, allowed)
}

func TestDenyScriptPathNotFound(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, bashScriptPath, bashScriptPath, false, script2, script1, false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, script1)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.False(t, allowed)
}

func TestDenyInvalidScript(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, bashScriptPath, bashScriptPath, false, script1, script2, false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, script1)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.False(t, allowed)
}

func TestDenyWriteToReadOnlyDataForScript(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, bashScriptPath, bashScriptPath, true, script1, script1, false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, script1)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), true)
	assert.NoError(t, err)
	assert.False(t, allowed)
}

func TestDenyUpdatedScript(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
	}()

	copyFile(t, copiedScript, script1)
	defer removeFile(t, copiedScript)

	checksumCalculationAlways := false
	vcr := newFakeVolumeControlRoot(
		t, bashScriptPath, "", false, copiedScript, copiedScript, false, "",
		checksumCalculationAlways, stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	cmd := exec.CommandContext(ctx, copiedScript)
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid
	mountNamespace := getMountNamespaceId(t, pid)
	vcr.setContainerRoots(t, mountNamespace, pid)

	allowed, err := vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.True(t, allowed)

	time.Sleep(1 * time.Millisecond)
	vcr.checkChecksumCachesForExecutable(t, mountNamespace, false)
	vcr.checkChecksumCachesForScript(t, mountNamespace, false)

	cancel()

	copyFile(t, copiedScript, script2)
	time.Sleep(1 * time.Millisecond)
	vcr.checkChecksumCachesCount(t, 0)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cmd = exec.CommandContext(ctx, copiedScript)
	err = cmd.Start()
	assert.NoError(t, err)

	pid = cmd.Process.Pid
	allowed, err = vcr.allow(uint32(pid), false)
	assert.NoError(t, err)
	assert.False(t, allowed)
	vcr.checkChecksumCachesCount(t, 0)
}

/* ************************************************************************* */

// [REF] github.com/hanwen/go-fuse/fs/simple_test.go

type testCase struct {
	*testing.T

	dir     string
	origDir string
	mntDir  string

	volumeControl fs.InodeEmbedder
	vcRoot        *volumeControlRoot
	rawFS         gofuse.RawFileSystem
	server        *gofuse.Server

	stopCh chan struct{}
}

type testOptions struct {
	entryCache    bool
	attrCache     bool
	suppressDebug bool
	testDir       string
	ro            bool
}

func setupContainerRoots(
	t *testing.T, volumeControl *volumeControlRoot, writable bool,
	allowedExecutablePaths []string) {
	mntNamespaceHandle := fmt.Sprintf("/proc/self/ns/mnt")
	mountNamespace, err := os.Readlink(mntNamespaceHandle)
	assert.NoError(t, err)

	cmdPath, err := os.Readlink("/proc/self/exe")
	assert.NoError(t, err)

	checksum, err := util.CalculateChecksum(cmdPath)
	assert.NoError(t, err)

	allowedExecutables := AllowedExecutables{
		cmdPath: {
			checksum: checksum,
			writable: writable,
		},
	}

	for _, path := range allowedExecutablePaths {
		allowedExecutables[path] = &allowedExecutable{}
	}

	volumeControl.containerRoots = map[string]*ContainerRoot{
		mountNamespace: &ContainerRoot{
			rootPath:           "/",
			allowedExecutables: allowedExecutables,
		},
	}
}

func newTestCase(
	t *testing.T, opts *testOptions, allow bool, writable bool,
	allowedExecutables []string) *testCase {
	if opts == nil {
		opts = &testOptions{}
	}
	if opts.testDir == "" {
		opts.testDir = testutil.TempDir()
	}
	tc := &testCase{
		dir: opts.testDir,
		T:   t,
	}
	tc.origDir = filepath.Join(tc.dir, "orig")
	tc.mntDir = filepath.Join(tc.dir, "mnt")

	err := os.Mkdir(tc.origDir, 0755)
	assert.NoError(t, err)

	err = os.Mkdir(tc.mntDir, 0755)
	assert.NoError(t, err)

	checksumCalculationAlways := false
	stopCh := make(chan struct{})
	tc.stopCh = stopCh
	volumeControl, rootData, err := newVolumeControlRoot(
		tc.origDir, false, checksumCalculationAlways, stopCh)
	assert.NoError(t, err)
	tc.volumeControl = volumeControl
	tc.vcRoot = rootData

	if allow {
		setupContainerRoots(t, rootData, writable, allowedExecutables)
	}

	oneSec := time.Second

	attrDT := &oneSec
	if !opts.attrCache {
		attrDT = nil
	}
	entryDT := &oneSec
	if !opts.entryCache {
		entryDT = nil
	}
	tc.rawFS = fs.NewNodeFS(
		tc.volumeControl,
		&fs.Options{
			EntryTimeout: entryDT,
			AttrTimeout:  attrDT,
		})

	mOpts := &gofuse.MountOptions{}
	if !opts.suppressDebug {
		mOpts.Debug = testutil.VerboseTest()
	}
	if opts.ro {
		mOpts.Options = append(mOpts.Options, "ro")
	}

	tc.server, err = gofuse.NewServer(tc.rawFS, tc.mntDir, mOpts)
	assert.NoError(t, err)

	go tc.server.Serve()

	err = tc.server.WaitMount()
	assert.NoError(t, err)

	return tc
}

func (tc *testCase) writeOrig(
	t *testing.T, path, content string, mode os.FileMode) {
	err := ioutil.WriteFile(
		filepath.Join(tc.origDir, path), []byte(content), mode)
	assert.NoError(t, err)
}

func (tc *testCase) Clean(t *testing.T) {
	tc.stopCh <- struct{}{}

	err := tc.server.Unmount()
	assert.NoError(t, err)

	err = os.RemoveAll(tc.dir)
	assert.NoError(t, err)
}

func testBasic(t *testing.T, writable bool) {
	tc := newTestCase(
		t,
		&testOptions{
			attrCache:  false,
			entryCache: false,
		},
		true, writable, nil)
	defer tc.Clean(t)

	tc.writeOrig(t, "file", "hello", 0666)

	fn := filepath.Join(tc.mntDir, "file")
	fi, err := os.Lstat(fn)

	assert.NoError(t, err)
	assert.Equal(t, int64(5), fi.Size())

	stat := gofuse.ToStatT(fi)
	want := uint32(gofuse.S_IFREG | 0644)
	got := uint32(stat.Mode)
	assert.Equal(t, want, got)

	if writable {
		err = os.Remove(fn)
		assert.NoError(t, err)
	} else {
		fn := filepath.Join(tc.origDir, "file")
		err = os.Remove(fn)
		assert.NoError(t, err)

	}

	_, err = os.Lstat(fn)
	assert.Error(t, err)
}

func TestBasicForReadOnly(t *testing.T) {
	testBasic(t, false)
}

func TestBasicForWritable(t *testing.T) {
	testBasic(t, true)
}

func TestBasicDeny(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			attrCache:  false,
			entryCache: false,
		},
		false, false, nil)
	defer tc.Clean(t)

	tc.writeOrig(t, "file", "hello", 0666)

	fn := filepath.Join(tc.mntDir, "file")
	_, err := os.Lstat(fn)

	assert.Equal(t, syscall.EPERM, err.(*os.PathError).Err)
}

func TestDenyWriteForReadOnly(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			attrCache:  false,
			entryCache: false,
		},
		true, false, nil)
	defer tc.Clean(t)

	tc.writeOrig(t, "file", "hello", 0666)

	fn := filepath.Join(tc.mntDir, "file")
	fi, err := os.Lstat(fn)

	assert.NoError(t, err)
	assert.Equal(t, int64(5), fi.Size())

	stat := gofuse.ToStatT(fi)
	want := uint32(gofuse.S_IFREG | 0644)
	got := uint32(stat.Mode)
	assert.Equal(t, want, got)

	err = os.Remove(fn)
	assert.Equal(t, syscall.EPERM, err.(*os.PathError).Err)
}

func TestFileFdLeak(t *testing.T) {
	tc := newTestCase(
		t, &testOptions{
			suppressDebug: true,
			attrCache:     true,
			entryCache:    true,
		},
		true, true, nil)
	defer func() {
		if tc != nil {
			tc.Clean(t)
		}
	}()

	posixtest.FdLeak(t, tc.mntDir)

	tc.Clean(t)
}

func TestNotifyEntry(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			attrCache:  true,
			entryCache: true,
		},
		true, false, nil)
	defer tc.Clean(t)

	orig := filepath.Join(tc.origDir, "file")
	tc.writeOrig(t, "file", "hello", 0666)

	fn := filepath.Join(tc.mntDir, "file")
	st := syscall.Stat_t{}
	err := syscall.Lstat(fn, &st)
	assert.NoError(t, err)

	err = os.Remove(orig)
	assert.NoError(t, err)

	after := syscall.Stat_t{}
	err = syscall.Lstat(fn, &after)

	assert.True(t, assert.ObjectsAreEqual(st, after))

	errno := tc.volumeControl.EmbeddedInode().NotifyEntry("file")
	assert.Equal(t, syscall.Errno(0x0), errno)

	err = syscall.Lstat(fn, &after)
	assert.Equal(t, syscall.ENOENT, err)
}

func TestReadDirStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 'TestReadDirStress()' in short mode")
	}

	tc := newTestCase(
		t,
		&testOptions{
			suppressDebug: true,
			attrCache:     true,
			entryCache:    true,
		},
		true, true, nil)
	defer tc.Clean(t)

	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("file%036x", i)
		err := ioutil.WriteFile(
			filepath.Join(tc.mntDir, name), []byte("hello"), 0666)
		assert.NoError(t, err)
	}

	wg := new(sync.WaitGroup)
	stress := func(gr int) {
		defer wg.Done()
		for i := 1; i < 50; i++ {
			f, err := os.Open(tc.mntDir)
			assert.NoError(t, err)

			_, err = f.Readdirnames(-1)
			assert.NoError(t, err)

			f.Close()
		}
	}

	n := 3
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go stress(i)
	}
	wg.Wait()
}

// This test is racy. If an external process consumes space while this
// runs, we may see spurious differences between the two statfs() calls.
func TestStatFs(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			attrCache:  true,
			entryCache: true,
		},
		true, false, nil)
	defer tc.Clean(t)

	empty := syscall.Statfs_t{}
	orig := empty
	err := syscall.Statfs(tc.origDir, &orig)
	assert.NoError(t, err)

	mnt := syscall.Statfs_t{}
	err = syscall.Statfs(tc.mntDir, &mnt)
	assert.NoError(t, err)

	var (
		mntFuse  gofuse.StatfsOut
		origFuse gofuse.StatfsOut
	)
	mntFuse.FromStatfsT(&mnt)
	origFuse.FromStatfsT(&orig)

	assert.True(t, assert.ObjectsAreEqual(origFuse, mntFuse))
}

func TestGetAttrParallel(t *testing.T) {
	// We grab a file-handle to provide to the API so rename+fstat
	// can be handled correctly. Here, test that closing and
	// (f)stat in parallel don't lead to fstat on closed files.
	// We can only test that if we switch off caching
	tc := newTestCase(
		t,
		&testOptions{
			suppressDebug: true,
		},
		true, false, nil)
	defer tc.Clean(t)

	N := 100

	var fds []int
	var fns []string
	for i := 0; i < N; i++ {
		fn := fmt.Sprintf("file%d", i)
		tc.writeOrig(t, fn, "ello", 0666)

		fn = filepath.Join(tc.mntDir, fn)
		fns = append(fns, fn)

		fd, err := syscall.Open(fn, syscall.O_RDONLY, 0)
		assert.NoError(t, err)
		fds = append(fds, fd)
	}

	wg := new(sync.WaitGroup)
	wg.Add(2 * N)
	for i := 0; i < N; i++ {
		go func(i int) {
			if err := syscall.Close(fds[i]); err != nil {
				t.Errorf("close %d: %v", i, err)
			}
			wg.Done()
		}(i)

		go func(i int) {
			var st syscall.Stat_t
			if err := syscall.Lstat(fns[i], &st); err != nil {
				t.Errorf("lstat %d: %v", i, err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestMknod(t *testing.T) {
	tc := newTestCase(t, &testOptions{}, true, true, nil)
	defer tc.Clean(t)

	modes := map[string]uint32{
		"regular": syscall.S_IFREG,
		"socket":  syscall.S_IFSOCK,
		"fifo":    syscall.S_IFIFO,
	}

	for nm, mode := range modes {
		t.Run(nm, func(t *testing.T) {
			p := filepath.Join(tc.mntDir, nm)
			err := syscall.Mknod(p, mode|0755, (8<<8)|0)
			assert.NoError(t, err)

			var st syscall.Stat_t
			if err := syscall.Stat(p, &st); err != nil {
				got := st.Mode &^ 07777

				assert.Equal(t, mode, got)
			}

			// We could test if the files can be
			// read/written but: The kernel handles FIFOs
			// without talking to FUSE at all. Presumably,
			// this also holds for sockets.  Regular files
			// are tested extensively elsewhere.
		})
	}
}

func TestPosix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 'TestPosix()' in short mode")
	}

	noisy := map[string]bool{
		"ParallelFileOpen": true,
		"ReadDir":          true,
	}

	for nm, fn := range posixtest.All {
		t.Run(nm, func(t *testing.T) {
			tc := newTestCase(
				t,
				&testOptions{
					suppressDebug: noisy[nm],
					attrCache:     true,
					entryCache:    true},
				true, true, nil)
			defer tc.Clean(t)

			fn(t, tc.mntDir)
		})
	}
}

func TestOpenDirectIO(t *testing.T) {
	// Apparently, tmpfs does not allow O_DIRECT, so try to create
	// a test temp directory in the "testing/fixtures" directory.
	ext4Dir := filepath.Join("testing", "fixtures", ".go-fuse-test")
	err := os.MkdirAll(ext4Dir, 0755)
	assert.NoError(t, err)
	defer os.RemoveAll(ext4Dir)

	posixtest.DirectIO(t, ext4Dir)
	if t.Failed() {
		t.Skip("DirectIO failed on underlying FS")
	}

	opts := testOptions{
		testDir:    ext4Dir,
		attrCache:  true,
		entryCache: true,
	}

	tc := newTestCase(t, &opts, true, true, nil)
	defer tc.Clean(t)
	posixtest.DirectIO(t, tc.mntDir)
}

// TestFsstress is loosely modeled after xfstest's fsstress. It performs rapid
// parallel removes / creates / readdirs. Coupled with inode reuse, this test
// used to deadlock go-fuse quite quickly.
//
// Note: Run as
//
//     TMPDIR=/var/tmp go test -run TestFsstress
//
// to make sure the backing filesystem is ext4. /tmp is tmpfs on modern Linux
// distributions, and tmpfs does not reuse inode numbers, hiding the problem.
func TestFsstress(t *testing.T) {
	lsPath, err := exec.LookPath("ls")
	assert.NoError(t, err)

	paths := []string{
		lsPath,
	}
	tc := newTestCase(
		t,
		&testOptions{
			suppressDebug: true,
			attrCache:     true,
			entryCache:    true,
		},
		true, true, paths)
	defer tc.Clean(t)

	{
		old := runtime.GOMAXPROCS(100)
		defer runtime.GOMAXPROCS(old)
	}

	const concurrency = 10
	wg := new(sync.WaitGroup)
	ctx, cancel := context.WithCancel(context.Background())

	// operations taking 1 path argument
	ops1 := map[string]func(string) error{
		"mkdir": func(p string) error { return syscall.Mkdir(p, 0700) },
		"rmdir": func(p string) error { return syscall.Rmdir(p) },
		"mknod_reg": func(p string) error {
			return syscall.Mknod(p, 0700|syscall.S_IFREG, 0)
		},
		"remove": os.Remove,
		"unlink": syscall.Unlink,
		"mknod_sock": func(p string) error {
			return syscall.Mknod(p, 0700|syscall.S_IFSOCK, 0)
		},
		"mknod_fifo": func(p string) error {
			return syscall.Mknod(p, 0700|syscall.S_IFIFO, 0)
		},
		"mkfifo":  func(p string) error { return syscall.Mkfifo(p, 0700) },
		"symlink": func(p string) error { return syscall.Symlink("foo", p) },
		"creat": func(p string) error {
			fd, err := syscall.Open(p, syscall.O_CREAT|syscall.O_EXCL, 0700)
			if err == nil {
				syscall.Close(fd)
			}
			return err
		},
	}
	// operations taking 2 path arguments
	ops2 := map[string]func(string, string) error{
		"rename": syscall.Rename,
		"link":   syscall.Link,
	}

	type opStats struct {
		ok   *int64
		fail *int64
		hung *int64
	}
	stats := make(map[string]opStats)

	// pathN() returns something like /var/tmp/TestFsstress/TestFsstress.4
	pathN := func(n int) string {
		return fmt.Sprintf("%s/%s.%d", tc.mntDir, t.Name(), n)
	}

	opLoop := func(k string, n int) {
		defer wg.Done()
		op := ops1[k]
		for {
			p := pathN(1)
			atomic.AddInt64(stats[k].hung, 1)
			err := op(p)
			atomic.AddInt64(stats[k].hung, -1)
			if err != nil {
				atomic.AddInt64(stats[k].fail, 1)
			} else {
				atomic.AddInt64(stats[k].ok, 1)
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}

	op2Loop := func(k string, n int) {
		defer wg.Done()
		op := ops2[k]
		n2 := (n + 1) % concurrency
		for {
			p1 := pathN(n)
			p2 := pathN(n2)
			atomic.AddInt64(stats[k].hung, 1)
			err := op(p1, p2)
			atomic.AddInt64(stats[k].hung, -1)
			if err != nil {
				atomic.AddInt64(stats[k].fail, 1)
			} else {
				atomic.AddInt64(stats[k].ok, 1)
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}

	readdirLoop := func(k string) {
		defer wg.Done()
		for {
			atomic.AddInt64(stats[k].hung, 1)
			f, err := os.Open(tc.mntDir)
			if err != nil {
				panic(err)
			}
			_, err = f.Readdir(0)
			if err != nil {
				atomic.AddInt64(stats[k].fail, 1)
			} else {
				atomic.AddInt64(stats[k].ok, 1)
			}
			f.Close()
			atomic.AddInt64(stats[k].hung, -1)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}

	// prepare stats map
	var allOps []string
	for k := range ops1 {
		allOps = append(allOps, k)
	}
	for k := range ops2 {
		allOps = append(allOps, k)
	}
	allOps = append(allOps, "readdir")
	for _, k := range allOps {
		var i1, i2, i3 int64
		stats[k] = opStats{ok: &i1, fail: &i2, hung: &i3}
	}

	// spawn worker goroutines
	for i := 0; i < concurrency; i++ {
		for k := range ops1 {
			wg.Add(1)
			go opLoop(k, i)
		}
		for k := range ops2 {
			wg.Add(1)
			go op2Loop(k, i)
		}
	}
	{
		k := "readdir"
		wg.Add(1)
		go readdirLoop(k)
	}

	// spawn ls loop
	//
	// An external "ls" loop has a destructive effect that I am unable to
	// reproduce through in-process operations.
	if strings.ContainsAny(tc.mntDir, "'\\") {
		// But let's not enable shell injection.
		log.Panicf("shell injection attempt? mntDir=%q", tc.mntDir)
	}
	// --color=always enables xattr lookups for extra stress
	cmd := exec.Command("bash", "-c", "while true ; do ls -l --color=always '"+tc.mntDir+"'; done")
	err = cmd.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()

	// Run the test for 1 second. If it deadlocks, it usually does within 20ms.
	time.Sleep(1 * time.Second)

	cancel()

	// waitTimeout waits for the waitgroup for the specified max timeout.
	// Returns true if waiting timed out.
	waitTimeout := func(wg *sync.WaitGroup, timeout time.Duration) bool {
		c := make(chan struct{})
		go func() {
			defer close(c)
			wg.Wait()
		}()
		select {
		case <-c:
			return false // completed normally
		case <-time.After(timeout):
			return true // timed out
		}
	}

	if waitTimeout(wg, time.Second) {
		t.Errorf("timeout waiting for goroutines to exit (deadlocked?)")
	}

	// Print operation statistics
	var keys []string
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	t.Logf("Operation statistics:")
	for _, k := range keys {
		v := stats[k]
		t.Logf("%10s: %5d ok, %6d fail, %2d hung", k, *v.ok, *v.fail, *v.hung)
	}
}

/* ************************************************************************* */

func newMessageQueueSubscrber(
	brokerAddress string, topic string) (*kafka.Reader, error) {
	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	if err != nil {
		return nil, err
	}

	readerConfig := kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		Dialer:  dialer,
	}

	return kafka.NewReader(readerConfig), nil
}

func TestForwardWrittenFileData(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			suppressDebug: true,
			attrCache:     false,
			entryCache:    false,
		},
		true, true, nil)
	defer tc.Clean(t)

	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	topic := "update.test"

	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)
	topics := []string{topic}
	err = messagequeue.CreateTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	defer func(broker string, d *kafka.Dialer, ts []string) {
		err = messagequeue.DeleteTopics(broker, d, ts)
		assert.NoError(t, err)
	}(brokerAddress, dialer, topics)

	messageQueueConfig := &api.MessageQueue{
		Brokers:                        []string{brokerAddress},
		User:                           saslUser,
		Password:                       saslPassword,
		Topic:                          topic,
		CompressionCodec:               mqpub.CompressionCodecSnappy,
		MaxBatchBytes:                  "0",
		UpdatePublishChannelBufferSize: "10",
	}
	volumeRootPath := tc.origDir
	mountNamespaces := []string{
		"mns-1",
	}
	err = tc.vcRoot.addMessageQeueueUpdatePublisher(
		messageQueueConfig, volumeRootPath, mountNamespaces)
	assert.NoError(t, err)

	defer func(namespaces []string) {
		err := tc.vcRoot.deleteMessageQeueueUpdatePublisher(namespaces)
		assert.NoError(t, err)
	}(mountNamespaces)

	fileName := "test1.txt"
	path := filepath.Join(tc.mntDir, fileName)
	contents := []byte("This is test.")
	err = ioutil.WriteFile(path, contents, 0666)
	assert.NoError(t, err)

	reader, err := newMessageQueueSubscrber(brokerAddress, topic)
	assert.NoError(t, err)
	defer reader.Close()

	ctx := context.Background()
	message, err := reader.ReadMessage(ctx)

	addMessage := &volumemq.Message{
		Method:   volumemq.MessageMethodAdd,
		Path:     fileName,
		Contents: contents,
	}
	expectedMessageValue, err := json.Marshal(addMessage)
	assert.NoError(t, err)

	assert.Equal(t, expectedMessageValue, message.Value)
}

func TestForwardDeletedFileData(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			suppressDebug: true,
			attrCache:     false,
			entryCache:    false,
		},
		true, false, nil)
	defer tc.Clean(t)

	fileName := "test1.txt"
	tc.writeOrig(t, fileName, "hello", 0666)

	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	topic := "update.test"

	brokers := []string{brokerAddress}
	config := messagequeue.NewMessageQueueConfig(
		brokers, saslUser, saslPassword)
	dialer, err := config.CreateSaslDialer()
	assert.NoError(t, err)
	topics := []string{topic}
	err = messagequeue.CreateTopics(brokerAddress, dialer, topics)
	assert.NoError(t, err)
	defer func(broker string, d *kafka.Dialer, ts []string) {
		err = messagequeue.DeleteTopics(broker, d, ts)
		assert.NoError(t, err)
	}(brokerAddress, dialer, topics)

	messageQueueConfig := &api.MessageQueue{
		Brokers:                        []string{brokerAddress},
		User:                           saslUser,
		Password:                       saslPassword,
		Topic:                          topic,
		CompressionCodec:               mqpub.CompressionCodecSnappy,
		MaxBatchBytes:                  "0",
		UpdatePublishChannelBufferSize: "10",
	}
	volumeRootPath := tc.origDir
	mountNamespaces := []string{
		"mns-1",
	}
	err = tc.vcRoot.addMessageQeueueUpdatePublisher(
		messageQueueConfig, volumeRootPath, mountNamespaces)
	assert.NoError(t, err)

	defer func(namespaces []string) {
		err := tc.vcRoot.deleteMessageQeueueUpdatePublisher(namespaces)
		assert.NoError(t, err)
	}(mountNamespaces)

	path := filepath.Join(tc.mntDir, fileName)
	err = os.Remove(path)
	assert.NoError(t, err)

	reader, err := newMessageQueueSubscrber(brokerAddress, topic)
	assert.NoError(t, err)
	defer reader.Close()

	ctx := context.Background()
	message, err := reader.ReadMessage(ctx)

	deleteMessage := &volumemq.Message{
		Method: volumemq.MessageMethodDelete,
		Path:   fileName,
	}
	expectedMessageValue, err := json.Marshal(deleteMessage)
	assert.NoError(t, err)

	assert.Equal(t, expectedMessageValue, message.Value)
}
