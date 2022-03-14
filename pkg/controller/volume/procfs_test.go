// Copyright (c) 2022 Fujitsu Limited

package volume

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	fakeprocfs "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util/testing"

	"github.com/stretchr/testify/assert"
)

const (
	fakeProcFsRootDir = "testing/fixtures/proc"
)

func createFakeProcFsRootDirectory(t *testing.T) {
	err := os.MkdirAll(fakeProcFsRootDir, os.ModePerm)
	assert.NoError(t, err)
}

func createFakeProcessDirectories(t *testing.T, processes int) {
	for i := 1; i <= processes; i++ {
		processDir := filepath.Join(fakeProcFsRootDir, strconv.Itoa(i))
		err := os.Mkdir(processDir, os.ModePerm)
		assert.NoError(t, err)
	}
}

func deleteFakeProcFsRootDirectory(t *testing.T) {
	err := os.RemoveAll(fakeProcFsRootDir)
	assert.NoError(t, err)
}

func TestGetOneOfPidsAndMountNamespace(t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(
		1, "/wd1/cmd1", "mns1", []byte("cmd1\x00-x\x00script1"), "/wd1")
	procFs.AddProcess(2, "/wd2/cmd2", "mns2", []byte("cmd2"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath: "/wd2/cmd2",
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, 2, pid)
	assert.Equal(t, "mns2", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForScript(t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		2, "/wd2/cmd2", "mns2", []byte("cmd2\x00-x\x00script1"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath: "/wd2/cmd2",
		scripts: map[string]*externalAllowedScript{
			"/wd2/script1": &externalAllowedScript{},
		},
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, 2, pid)
	assert.Equal(t, "mns2", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForScriptWithRelativePath(t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		2, "/wd2/cmd2", "mns2", []byte("cmd2\x00-x\x00script1"), "/wd2/test")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath: "/wd2/cmd2",
		scripts: map[string]*externalAllowedScript{
			"/wd2/script1": &externalAllowedScript{
				relativePathToInitWorkDir: "../",
			},
		},
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, 2, pid)
	assert.Equal(t, "mns2", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForScriptNotFound(t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		2, "/wd2/cmd2", "mns2", []byte("cmd2\x00-x\x00script1"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath: "/wd2/cmd2",
		scripts: map[string]*externalAllowedScript{
			"/wd2/script2": &externalAllowedScript{},
		},
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.Error(t, err)
	assert.Equal(t, -1, pid)
	assert.Equal(t, "", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForNamespace(t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		2, "/wd2/cmd2", "mns2", []byte("cmd2\x00-x\x00script1"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		specifiedMountNamespace: "mns3",
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, 4, pid)
	assert.Equal(t, "mns3", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForCommandPathWithNamespace(t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(
		1, "/wd1/cmd1", "mns1", []byte("cmd1\x00-x\x00script1"), "/wd1")
	procFs.AddProcess(2, "/wd2/cmd2", "mns2", []byte("cmd2"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath:     "/wd2/cmd2",
		specifiedMountNamespace: "mns2",
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, 2, pid)
	assert.Equal(t, "mns2", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForScriptWithNamespace(t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(
		1, "/wd1/cmd1", "mns1", []byte("cmd1\x00-x\x00script1"), "/wd1")
	procFs.AddProcess(2, "/wd2/cmd2", "mns2", []byte("cmd2"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath:     "/wd1/cmd1",
		specifiedMountNamespace: "mns1",
		scripts: map[string]*externalAllowedScript{
			"/wd1/script1": &externalAllowedScript{},
		},
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, 1, pid)
	assert.Equal(t, "mns1", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForScriptNotFoundWithNamespace(t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(
		1, "/wd1/cmd1", "mns1", []byte("cmd1\x00-x\x00script1"), "/wd1")
	procFs.AddProcess(2, "/wd2/cmd2", "mns2", []byte("cmd2"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath:     "/wd1/cmd1",
		specifiedMountNamespace: "mns1",
		scripts: map[string]*externalAllowedScript{
			"/wd1/script2": &externalAllowedScript{},
		},
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.Error(t, err)
	assert.Equal(t, -1, pid)
	assert.Equal(t, "", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForWithResidentCommandPath(t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		2, "/wd2/cmd2", "mns2", []byte("cmd2\x00-x\x00script1"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath:             "/wd3/cmd6",
		withResidentCommandAbsolutePath: "/wd3/cmd3",
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, 3, pid)
	assert.Equal(t, "mns1", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForScriptAndWithResidentCommandPath(
	t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		2, "/wd2/cmd2", "mns2", []byte("cmd2\x00-x\x00script1"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath:             "/wd3/cmd6",
		withResidentCommandAbsolutePath: "/wd3/cmd3",
		scripts: map[string]*externalAllowedScript{
			"/wd3/script6": &externalAllowedScript{},
		},
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, 3, pid)
	assert.Equal(t, "mns1", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForScriptAndNamespaceAndWithResidentCommandPath(
	t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		2, "/wd2/cmd2", "mns2", []byte("cmd2\x00-x\x00script1"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath:             "/wd3/cmd6",
		specifiedMountNamespace:         "mns1",
		withResidentCommandAbsolutePath: "/wd3/cmd3",
		scripts: map[string]*externalAllowedScript{
			"/wd3/script6": &externalAllowedScript{},
		},
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, 3, pid)
	assert.Equal(t, "mns1", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForNamespaceAndWithResidentCommandPath(
	t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		2, "/wd2/cmd2", "mns2", []byte("cmd2\x00-x\x00script1"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd3", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath:             "/wd4/cmd6",
		withResidentCommandAbsolutePath: "/wd4/cmd3",
		specifiedMountNamespace:         "mns3",
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, 4, pid)
	assert.Equal(t, "mns3", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForHostNamespace(t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		2, "/wd2/cmd2", "hns", []byte("cmd2\x00-x\x00script1"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	processDir := filepath.Join(
		fakeProcFsRootDir, strconv.Itoa(hostDelegatePid))
	err = os.Mkdir(processDir, os.ModePerm)
	assert.NoError(t, err)
	procFs.AddProcess(
		hostDelegatePid, "/wd6/cmd6", "hns", []byte("cmd6\x00script3"), "/wd6")

	eae := &externalAllowedExecutable{
		isHostProcess: true,
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, hostDelegatePid, pid)
	assert.Equal(t, "hns", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
	procFs.DeleteProcess(hostDelegatePid)
}

func TestGetOneOfPidsAndMountNamespaceForCommandPathWithHostNamespace(
	t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(
		1, "/wd1/cmd1", "mns1", []byte("cmd1\x00-x\x00script1"), "/wd1")
	procFs.AddProcess(2, "/wd2/cmd2", "hns", []byte("cmd2"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	processDir := filepath.Join(
		fakeProcFsRootDir, strconv.Itoa(hostDelegatePid))
	err = os.Mkdir(processDir, os.ModePerm)
	assert.NoError(t, err)
	procFs.AddProcess(
		hostDelegatePid, "/wd6/cmd6", "hns", []byte("cmd6\x00script3"), "/wd6")

	eae := &externalAllowedExecutable{
		commandAbsolutePath: "/wd2/cmd2",
		isHostProcess:       true,
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, 2, pid)
	assert.Equal(t, "hns", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
	procFs.DeleteProcess(hostDelegatePid)
}

func TestGetOneOfPidsAndMountNamespaceForNotResidentCommandPathWithHostNamespace(
	t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		2, "/wd2/cmd2", "hns", []byte("cmd2\x00-x\x00script1"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd4/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	processDir := filepath.Join(
		fakeProcFsRootDir, strconv.Itoa(hostDelegatePid))
	err = os.Mkdir(processDir, os.ModePerm)
	assert.NoError(t, err)
	procFs.AddProcess(
		hostDelegatePid, "/wd6/cmd6", "hns", []byte("cmd6\x00-f"), "/wd6")

	eae := &externalAllowedExecutable{
		commandAbsolutePath: "/wd6/cmd6",
		isHostProcess:       true,
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.NoError(t, err)
	assert.Equal(t, hostDelegatePid, pid)
	assert.Equal(t, "hns", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
	procFs.DeleteProcess(hostDelegatePid)
}

func TestGetOneOfPidsAndMountNamespaceForCommandPathNotFound(t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		2, "/wd2/cmd2", "mns2", []byte("cmd2\x00-x\x00script1"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath: "/wd6/cmd6",
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.Error(t, err)
	assert.Equal(t, -1, pid)
	assert.Equal(t, "", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForCommandPathWithNamespaceNotFound(
	t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		2, "/wd2/cmd2", "mns2", []byte("cmd2\x00-x\x00script1"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd4/cmd4", "mns3", []byte("cmd4\x00-y=1"), "/wd4")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath:     "/wd3/cmd3",
		specifiedMountNamespace: "mns2",
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.Error(t, err)
	assert.Equal(t, -1, pid)
	assert.Equal(t, "", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}

func TestGetOneOfPidsAndMountNamespaceForCommandPathsInDifferentNamespaces(
	t *testing.T) {
	createFakeProcFsRootDirectory(t)
	defer deleteFakeProcFsRootDirectory(t)

	procFs := fakeprocfs.NewFakeProcFs(fakeProcFsRootDir)

	procFs.AddProcess(1, "/wd1/cmd1", "mns1", []byte("cmd1"), "/wd1")
	procFs.AddProcess(
		23, "/wd2/cmd2", "mns2", []byte("cmd2\x00-x\x00script1"), "/wd2")
	procFs.AddProcess(
		3, "/wd3/cmd3", "mns1", []byte("cmd3\x00-y\x001"), "/wd3")
	procFs.AddProcess(4, "/wd3/cmd3", "mns3", []byte("cmd4\x00-y=1"), "/wd3")
	procFs.AddProcess(
		5, "/wd5/cmd5", "mns2", []byte("cmd5\x00--zz\x00script2"), "/wd5")
	processes := 5

	createFakeProcessDirectories(t, processes)

	directory, err := os.Open(fakeProcFsRootDir)
	assert.NoError(t, err)
	defer directory.Close()

	hostDelegatePid := os.Getppid()
	hostProcFs := newHostProcFs(procFs, hostDelegatePid)

	eae := &externalAllowedExecutable{
		commandAbsolutePath: "/wd3/cmd3",
	}

	pid, namespace, err := eae.getOneOfPidsAndMountNamespace(
		directory, procFs, hostProcFs)
	assert.Error(t, err)
	assert.Equal(t, -1, pid)
	assert.Equal(t, "", namespace)

	for i := 1; i <= processes; i++ {
		procFs.DeleteProcess(i)
	}
}
