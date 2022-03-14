// Copyright (c) 2022 Fujitsu Limited

package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/kylelemons/godebug/pretty"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sys/unix"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
	mqpub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/fuse/internal/testutil"
	volumemq "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"

	"github.com/stretchr/testify/assert"
)

/* ************************************************************************* */

// [REF] github.com/hanwen/go-fuse/fs/loopback_linux_test.go

func TestRenameExchange(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			attrCache:  true,
			entryCache: true,
		},
		true, true, nil)
	defer tc.Clean(t)

	err := os.Mkdir(tc.origDir+"/dir", 0755)
	assert.NoError(t, err)

	tc.writeOrig(t, "file", "hello", 0666)
	tc.writeOrig(t, "dir/file", "x", 0666)

	f1, err := syscall.Open(
		filepath.Join(tc.mntDir, "/"), syscall.O_DIRECTORY, 0)
	assert.NoError(t, err)
	defer syscall.Close(f1)

	f2, err := syscall.Open(
		filepath.Join(tc.mntDir, "dir"), syscall.O_DIRECTORY, 0)
	assert.NoError(t, err)
	defer syscall.Close(f2)

	var (
		before1 unix.Stat_t
		before2 unix.Stat_t
	)

	err = unix.Fstatat(f1, "file", &before1, 0)
	assert.NoError(t, err)

	err = unix.Fstatat(f2, "file", &before2, 0)
	assert.NoError(t, err)

	err = unix.Renameat2(f1, "file", f2, "file", unix.RENAME_EXCHANGE)
	assert.NoError(t, err)

	var (
		after1 unix.Stat_t
		after2 unix.Stat_t
	)

	err = unix.Fstatat(f1, "file", &after1, 0)
	assert.NoError(t, err)

	err = unix.Fstatat(f2, "file", &after2, 0)
	assert.NoError(t, err)

	clearCtime := func(s *unix.Stat_t) {
		s.Ctim.Sec = 0
		s.Ctim.Nsec = 0
	}

	clearCtime(&after1)
	clearCtime(&after2)
	clearCtime(&before2)
	clearCtime(&before1)

	diff := pretty.Compare(after1, before2)
	assert.Equal(t, "", diff)
	assert.True(t, assert.ObjectsAreEqual(after2, before1))
}

func TestRenameNoOverwrite(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			attrCache:  true,
			entryCache: true},
		true, false, nil)
	defer tc.Clean(t)

	err := os.Mkdir(filepath.Join(tc.origDir, "dir"), 0755)
	assert.NoError(t, err)

	tc.writeOrig(t, "file", "hello", 0666)
	tc.writeOrig(t, "dir/file", "x", 0666)

	f1, err := syscall.Open(
		filepath.Join(tc.mntDir, "/"), syscall.O_DIRECTORY, 0)
	assert.NoError(t, err)
	defer syscall.Close(f1)

	f2, err := syscall.Open(
		filepath.Join(tc.mntDir, "dir"), syscall.O_DIRECTORY, 0)
	assert.NoError(t, err)
	defer syscall.Close(f2)

	err = unix.Renameat2(f1, "file", f2, "file", unix.RENAME_NOREPLACE)
	assert.Equal(t, syscall.EEXIST, err)
}

func TestXAttr(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			attrCache:  true,
			entryCache: true,
		},
		true, true, nil)
	defer tc.Clean(t)

	tc.writeOrig(t, "file", "", 0644)

	buf := make([]byte, 1024)
	attr := "user.xattrtest"
	_, err := syscall.Getxattr(filepath.Join(tc.mntDir, "file"), attr, buf)
	if err == syscall.ENOTSUP {
		t.Skip("$TMP does not support xattrs. Rerun this test with a $TMPDIR override")
	}

	_, err = syscall.Getxattr(filepath.Join(tc.mntDir, "file"), attr, buf)
	assert.Equal(t, syscall.ENODATA, err)

	value := []byte("value")
	err = syscall.Setxattr(filepath.Join(tc.mntDir, "file"), attr, value, 0)
	assert.NoError(t, err)

	sz, err := syscall.Listxattr(filepath.Join(tc.mntDir, "/file"), nil)
	assert.NoError(t, err)

	buf = make([]byte, sz)
	_, err = syscall.Listxattr(filepath.Join(tc.mntDir, "file"), buf)
	assert.NoError(t, err)

	attributes := bytes.Split(buf[:sz], []byte{0})
	found := false
	for _, a := range attributes {
		if string(a) == attr {
			found = true
			break
		}
	}

	assert.True(t, found)

	sz, err = syscall.Getxattr(filepath.Join(tc.mntDir, "file"), attr, buf)
	assert.NoError(t, err)

	assert.Equal(t, 0, bytes.Compare(buf[:sz], value))

	err = syscall.Removexattr(filepath.Join(tc.mntDir, "file"), attr)
	assert.NoError(t, err)

	_, err = syscall.Getxattr(filepath.Join(tc.mntDir, "file"), attr, buf)
	assert.Equal(t, syscall.ENODATA, err)
}

// TestXAttrSymlink verifies that we did not forget to use Lgetxattr instead
// of Getxattr. This test is Linux-specific because it depends on the behavoir
// of the `security` namespace.
//
// On Linux, symlinks can not have xattrs in the `user` namespace, so we
// try to read something from `security`. Writing would need root rights,
// so don't even bother. See `man 7 xattr` for more info.
func TestXAttrSymlink(t *testing.T) {
	tc := newTestCase(t, nil, true, true, nil)
	defer tc.Clean(t)

	path := filepath.Join(tc.mntDir, "symlink")
	err := syscall.Symlink("target/does/not/exist", path)
	assert.NoError(t, err)

	buf := make([]byte, 10)
	_, err = unix.Lgetxattr(path, "security.foo", buf)
	assert.Equal(t, syscall.ENODATA, err)
}

func TestCopyFileRange(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			attrCache:  false,
			entryCache: false,
		},
		true, true, nil)
	defer tc.Clean(t)

	if !tc.server.KernelSettings().SupportsVersion(7, 28) {
		t.Skip("need v7.28 for CopyFileRange")
	}

	tc.writeOrig(t, "src", "01234567890123456789", 0666)
	tc.writeOrig(t, "dst", "abcdefghijabcdefghij", 0666)

	f1, err := syscall.Open(
		filepath.Join(tc.mntDir, "src"), syscall.O_RDONLY, 0)
	assert.NoError(t, err)
	defer func() {
		// syscall.Close() is treacherous; because fds are
		// reused, a double close can cause serious havoc
		if f1 > 0 {
			syscall.Close(f1)
		}
	}()

	f2, err := syscall.Open(filepath.Join(tc.mntDir, "dst"), syscall.O_RDWR, 0)
	assert.NoError(t, err)
	defer func() {
		if f2 > 0 {
			defer syscall.Close(f2)
		}
	}()

	srcOff := int64(5)
	dstOff := int64(7)
	sz, err := unix.CopyFileRange(f1, &srcOff, f2, &dstOff, 3, 0)
	assert.NoError(t, err)
	assert.Equal(t, 3, sz)

	err = syscall.Close(f1)
	f1 = 0
	assert.NoError(t, err)

	err = syscall.Close(f2)
	f2 = 0
	assert.NoError(t, err)

	c, err := ioutil.ReadFile(filepath.Join(tc.mntDir, "dst"))
	assert.NoError(t, err)

	want := "abcdefg567abcdefghij"
	got := string(c)
	assert.Equal(t, want, got)
}

// Wait for a change in /proc/self/mounts. Efficient through the use of
// unix.Poll().
func waitProcMountsChange() error {
	fd, err := syscall.Open("/proc/self/mounts", syscall.O_RDONLY, 0)
	defer syscall.Close(fd)
	if err != nil {
		return err
	}
	pollFds := []unix.PollFd{
		{
			Fd:     int32(fd),
			Events: unix.POLLPRI,
		},
	}
	_, err = unix.Poll(pollFds, 1000)
	return err
}

// Wait until mountpoint "mnt" shows up /proc/self/mounts
func waitMount(mnt string) error {
	for {
		err := waitProcMountsChange()
		if err != nil {
			return err
		}
		content, err := ioutil.ReadFile("/proc/self/mounts")
		if err != nil {
			return err
		}
		if bytes.Contains(content, []byte(mnt)) {
			return nil
		}
	}
}

// There is a hang that appears when enabling CAP_PARALLEL_DIROPS on Linux
// 4.15.0: https://github.com/hanwen/go-fuse/issues/281
// The hang was originally triggered by gvfs-udisks2-volume-monitor. This
// test emulates what gvfs-udisks2-volume-monitor does.
func TestParallelDiropsHang(t *testing.T) {
	// We do NOT want to use newTestCase() here because we need to know the
	// mnt path before the filesystem is mounted
	dir := testutil.TempDir()
	orig := dir + "/orig"
	mnt := dir + "/mnt"
	if err := os.Mkdir(orig, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(mnt, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Unblock the goroutines onces the mount shows up in /proc/self/mounts
	wait := make(chan struct{})
	go func() {
		err := waitMount(mnt)
		if err != nil {
			t.Error(err)
		}
		// Unblock the goroutines regardless of an error. We don't want to hang
		// the test.
		close(wait)
	}()

	// gvfs-udisks2-volume-monitor hits the mount with three threads - we try to
	// emulate exactly what it does acc. to an strace log.
	var wg sync.WaitGroup
	wg.Add(3)
	// [pid  2117] lstat(".../mnt/autorun.inf",  <unfinished ...>
	go func() {
		defer wg.Done()
		<-wait
		var st unix.Stat_t
		unix.Lstat(mnt+"/autorun.inf", &st)
	}()
	// [pid  2116] open(".../mnt/.xdg-volume-info", O_RDONLY <unfinished ...>
	go func() {
		defer wg.Done()
		<-wait
		syscall.Open(mnt+"/.xdg-volume-info", syscall.O_RDONLY, 0)
	}()
	// 25 times this:
	// [pid  1874] open(".../mnt", O_RDONLY|O_NONBLOCK|O_DIRECTORY|O_CLOEXEC <unfinished ...>
	// [pid  1874] fstat(11, {st_mode=S_IFDIR|0775, st_size=4096, ...}) = 0
	// [pid  1874] getdents(11, /* 2 entries */, 32768) = 48
	// [pid  1874] close(11)                   = 0
	go func() {
		defer wg.Done()
		<-wait
		for i := 1; i <= 25; i++ {
			f, err := os.Open(mnt)
			if err != nil {
				t.Error(err)
				return
			}
			_, err = f.Stat()
			if err != nil {
				t.Error(err)
				f.Close()
				return
			}
			_, err = f.Readdirnames(-1)
			if err != nil {
				t.Errorf("iteration %d: fd %d: %v", i, f.Fd(), err)
				return
			}
			f.Close()
		}
	}()

	checksumCalculationAlways := false
	stopCh := make(chan struct{})
	volumeControl, rootData, err := newVolumeControlRoot(
		orig, false, checksumCalculationAlways, stopCh)
	assert.NoError(t, err)
	setupContainerRoots(t, rootData, true, nil)

	sec := time.Second
	opts := &fs.Options{
		AttrTimeout:  &sec,
		EntryTimeout: &sec,
	}
	opts.Debug = testutil.VerboseTest()

	rawFS := fs.NewNodeFS(volumeControl, opts)
	server, err := fuse.NewServer(rawFS, mnt, &opts.MountOptions)
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve()

	wg.Wait()
	server.Unmount()
}

func TestRoMount(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			ro: true,
		},
		true, false, nil)
	defer tc.Clean(t)
}

/* ************************************************************************* */

func TestForwardRenameFileData(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			suppressDebug: true,
			attrCache:     false,
			entryCache:    false,
		},
		true, true, nil)
	defer tc.Clean(t)

	oldFileName := "test1.txt"
	tc.writeOrig(t, oldFileName, "hello", 0666)

	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	topic := "update.test"

	config := messagequeue.NewMessageQueueConfig(nil, saslUser, saslPassword)
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

	oldPath := filepath.Join(tc.mntDir, oldFileName)
	newFileName := "test2.txt"
	newPath := filepath.Join(tc.mntDir, newFileName)
	err = os.Rename(oldPath, newPath)
	assert.NoError(t, err)

	reader, err := newMessageQueueSubscrber(brokerAddress, topic)
	assert.NoError(t, err)
	defer reader.Close()

	ctx := context.Background()
	message, err := reader.ReadMessage(ctx)

	renameMessage := &volumemq.Message{
		Method:  volumemq.MessageMethodRename,
		Path:    newFileName,
		OldPath: oldFileName,
	}
	expectedMessageValue, err := json.Marshal(renameMessage)
	assert.NoError(t, err)

	assert.Equal(t, expectedMessageValue, message.Value)
}

func TestForwardCopyFileRangeData(t *testing.T) {
	tc := newTestCase(
		t,
		&testOptions{
			attrCache:  false,
			entryCache: false,
		},
		true, true, nil)
	defer tc.Clean(t)

	if !tc.server.KernelSettings().SupportsVersion(7, 28) {
		t.Skip("need v7.28 for CopyFileRange")
	}

	tc.writeOrig(t, "src", "01234567890123456789", 0666)
	tc.writeOrig(t, "dst", "abcdefghijabcdefghij", 0666)

	sourceFileName := "src"
	f1, err := syscall.Open(
		filepath.Join(tc.mntDir, sourceFileName), syscall.O_RDONLY, 0)
	assert.NoError(t, err)
	defer func() {
		// syscall.Close() is treacherous; because fds are
		// reused, a double close can cause serious havoc
		if f1 > 0 {
			syscall.Close(f1)
		}
	}()

	destinationFileName := "dst"
	f2, err := syscall.Open(
		filepath.Join(tc.mntDir, destinationFileName), syscall.O_RDWR, 0)
	assert.NoError(t, err)
	defer func() {
		if f2 > 0 {
			defer syscall.Close(f2)
		}
	}()

	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	topic := "update.test"

	config := messagequeue.NewMessageQueueConfig(nil, saslUser, saslPassword)
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

	srcOff := int64(5)
	dstOff := int64(7)
	sz, err := unix.CopyFileRange(f1, &srcOff, f2, &dstOff, 3, 0)
	assert.NoError(t, err)
	assert.Equal(t, 3, sz)

	err = syscall.Close(f1)
	f1 = 0
	assert.NoError(t, err)

	err = syscall.Close(f2)
	f2 = 0
	assert.NoError(t, err)

	c, err := ioutil.ReadFile(filepath.Join(tc.mntDir, "dst"))
	assert.NoError(t, err)

	want := "abcdefg567abcdefghij"
	got := string(c)
	assert.Equal(t, want, got)

	reader, err := newMessageQueueSubscrber(brokerAddress, topic)
	assert.NoError(t, err)
	defer reader.Close()

	ctx := context.Background()
	message, err := reader.ReadMessage(ctx)

	addMessage := &volumemq.Message{
		Method:   volumemq.MessageMethodAdd,
		Path:     destinationFileName,
		Offset:   dstOff,
		Contents: []byte("fgh"),
	}
	expectedMessageValue, err := json.Marshal(addMessage)
	assert.NoError(t, err)

	assert.Equal(t, expectedMessageValue, message.Value)
}
