// Copyright (c) 2022 Fujitsu Limited

package fuse

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"k8s.io/klog/v2"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
	volumepub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue/publish"
)

// [REF] github.com/hanwen/go-fuse/fs/loopback.go

const (
	checksumChSize = 5
)

var (
	_ = (fs.NodeStatfser)((*volumeControlNode)(nil))
	_ = (fs.NodeGetattrer)((*volumeControlNode)(nil))
	_ = (fs.NodeSetattrer)((*volumeControlNode)(nil))
	_ = (fs.NodeGetxattrer)((*volumeControlNode)(nil))
	_ = (fs.NodeSetxattrer)((*volumeControlNode)(nil))
	_ = (fs.NodeRemovexattrer)((*volumeControlNode)(nil))
	_ = (fs.NodeListxattrer)((*volumeControlNode)(nil))
	_ = (fs.NodeReadlinker)((*volumeControlNode)(nil))
	_ = (fs.NodeCreater)((*volumeControlNode)(nil))
	_ = (fs.NodeOpener)((*volumeControlNode)(nil))
	_ = (fs.NodeCopyFileRanger)((*volumeControlNode)(nil))
	_ = (fs.NodeLookuper)((*volumeControlNode)(nil))
	_ = (fs.NodeOpendirer)((*volumeControlNode)(nil))
	_ = (fs.NodeReaddirer)((*volumeControlNode)(nil))
	_ = (fs.NodeMkdirer)((*volumeControlNode)(nil))
	_ = (fs.NodeMknoder)((*volumeControlNode)(nil))
	_ = (fs.NodeLinker)((*volumeControlNode)(nil))
	_ = (fs.NodeSymlinker)((*volumeControlNode)(nil))
	_ = (fs.NodeUnlinker)((*volumeControlNode)(nil))
	_ = (fs.NodeRmdirer)((*volumeControlNode)(nil))
	_ = (fs.NodeRenamer)((*volumeControlNode)(nil))
)

type allowedScript struct {
	checksum                  []byte
	writable                  bool
	relativePathToInitWorkDir string
}

type allowedExecutable struct {
	checksum []byte
	writable bool
	scripts  map[string]*allowedScript
}

type commandInfo struct {
	path     string
	checksum []byte
}

type volumeControlRoot struct {
	// The path to the root of the underlying file system.
	path string

	// The device on which the Path resides. This must be set if
	// the underlying filesystem crosses file systems.
	dev uint64

	newNodeFunc func(
		rootData *volumeControlRoot, parent *fs.Inode, name string,
		st *syscall.Stat_t) fs.InodeEmbedder

	rootsMutex     sync.RWMutex
	containerRoots ContainerRoots

	usageControlDisabled      bool
	checksumCalculationAlways bool
	checksumMutex             sync.RWMutex
	checksumCaches            map[string][]byte
	cmdCh                     chan commandInfo

	procFsOps util.ProcFsOperations

	publishingMutex             sync.RWMutex
	publishingContainers        PublishingContainers
	messageQueueUpdatePublisher *volumepub.VolumeMessageQueueUpdatePublisher
}

type volumeControlNode struct {
	fs.Inode

	rootData *volumeControlRoot
}

func (vcr *volumeControlRoot) newNode(
	parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
	if vcr.newNodeFunc != nil {
		return vcr.newNodeFunc(vcr, parent, name, st)
	}

	return &volumeControlNode{
		rootData: vcr,
	}
}

func (vcn *volumeControlNode) path() string {
	path := vcn.Path(vcn.Root())
	return filepath.Join(vcn.rootData.path, path)
}

func newVolumeControlRoot(
	sourcePath string, disableUsageControl bool,
	checksumCalculationAlways bool, stopCh <-chan struct{}) (
	fs.InodeEmbedder, *volumeControlRoot, error) {
	var stat syscall.Stat_t
	err := syscall.Stat(sourcePath, &stat)
	if err != nil {
		return nil, nil, fmt.Errorf("%q: %v", sourcePath, err)
	}

	ucRoot := &volumeControlRoot{
		path:                      sourcePath,
		dev:                       uint64(stat.Dev),
		containerRoots:            ContainerRoots{},
		usageControlDisabled:      disableUsageControl,
		checksumCalculationAlways: checksumCalculationAlways,

		procFsOps: util.NewProcFs(),
	}

	if !checksumCalculationAlways {
		ucRoot.checksumCaches = map[string][]byte{}
		ucRoot.cmdCh = make(chan commandInfo, checksumChSize)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, nil, fmt.Errorf(
			"Could not create an inotify watcher: %v", err)
	}
	go ucRoot.updateChecksumCaches(watcher, stopCh)

	return ucRoot.newNode(nil, "", &stat), ucRoot, nil
}

func (vcr *volumeControlRoot) addMessageQeueueUpdatePublisher(
	messageQueue *api.MessageQueue, volumeRootPath string,
	mountNamespaces []string) error {
	vcr.publishingMutex.Lock()
	defer vcr.publishingMutex.Unlock()

	if vcr.messageQueueUpdatePublisher == nil {
		messageQueueConfig := messagequeue.NewMessageQueueConfig(
			messageQueue.Brokers, messageQueue.User, messageQueue.Password)

		maxBatchBytes, err := strconv.Atoi(messageQueue.MaxBatchBytes)
		if err != nil {
			return fmt.Errorf("Failed to convert 'MaxBatchBytes' %q to integer",
				messageQueue.MaxBatchBytes)
		}

		channelBufferSize, err := strconv.Atoi(
			messageQueue.UpdatePublishChannelBufferSize)
		if err != nil {
			return fmt.Errorf(
				"Failed to convert 'UpdatePublishChannelBufferSize' %q "+
					"to integer", messageQueue.UpdatePublishChannelBufferSize)
		}

		publisher, err := volumepub.NewVolumeMessageQueueUpdatePublisher(
			messageQueueConfig, messageQueue.Topic, messageQueue.CompressionCodec,
			volumeRootPath, maxBatchBytes, channelBufferSize)
		if err != nil {
			return err
		}

		vcr.messageQueueUpdatePublisher = publisher
		vcr.publishingContainers = map[string]struct{}{}
	}

	for _, namespace := range mountNamespaces {
		vcr.publishingContainers[namespace] = struct{}{}
	}

	return nil
}

func (vcr *volumeControlRoot) deleteMessageQeueueUpdatePublisher(
	mountNamespaces []string) error {
	vcr.publishingMutex.Lock()
	defer vcr.publishingMutex.Unlock()

	for _, namespace := range mountNamespaces {
		delete(vcr.publishingContainers, namespace)
	}

	if len(vcr.publishingContainers) != 0 {
		return nil
	}

	err := vcr.messageQueueUpdatePublisher.CloseMessageQueue()
	if err != nil {
		return err
	}

	vcr.messageQueueUpdatePublisher = nil

	return nil
}

func (vcr *volumeControlRoot) deleteChecksumCaches(
	watcher *fsnotify.Watcher, stopCh <-chan struct{}) {
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				klog.Warningf("The inotify watcher could not receive an event")
				return
			}

			if event.Op&(fsnotify.Write|fsnotify.Rename|fsnotify.Remove) != 0 {
				vcr.checksumMutex.Lock()
				delete(vcr.checksumCaches, event.Name)
				vcr.checksumMutex.Unlock()

				klog.Infof("Remove %q from the checksum caches", event.Name)

				if event.Op&(fsnotify.Write|fsnotify.Rename) != 0 {
					err := watcher.Remove(event.Name)
					if err != nil {
						klog.Errorf("Could not remove %q: %v", event.Name, err)
					}
					klog.Infof(
						"Remove %q from the inotify watcher", event.Name)
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				klog.Warningf("The inotify watcher could not receive an error")
				return
			}

			klog.Errorf("Caught the error via the inotify watcher: %v", err)
		case <-stopCh:
			return
		}
	}
}

func (vcr *volumeControlRoot) updateChecksumCaches(
	watcher *fsnotify.Watcher, stopCh <-chan struct{}) {
	defer watcher.Close()

	go vcr.deleteChecksumCaches(watcher, stopCh)

	for {
		select {
		case cmd := <-vcr.cmdCh:
			vcr.checksumMutex.Lock()
			vcr.checksumCaches[cmd.path] = cmd.checksum
			vcr.checksumMutex.Unlock()

			err := watcher.Add(cmd.path)
			if err != nil {
				klog.Errorf(
					"Could not add %q to the inotify watcher: %v",
					cmd.path, err)
			}

			klog.Infof("Add %q to the checksum caches", cmd.path)
		case <-stopCh:
			break
		}
	}
}

func (vcr *volumeControlRoot) isValidChecksum(
	cmdPath string, allowedChecksum []byte) (bool, error) {
	validChecksum, err := util.CalculateChecksum(cmdPath)
	if err != nil {
		klog.Errorf("Could not calculate the checksum for %q", cmdPath)
		return false, err
	}

	cmp := bytes.Compare(validChecksum, allowedChecksum)
	if cmp != 0 {
		klog.Errorf(
			"Checksum %x for %q does not equal to %x",
			validChecksum, cmdPath, allowedChecksum)
		return false, nil
	}

	return true, nil
}

func (vcr *volumeControlRoot) isValidChecksumWithCaches(
	cmdPath string, allowedChecksum []byte) (bool, error) {
	vcr.checksumMutex.RLock()
	checksum, ok := vcr.checksumCaches[cmdPath]
	vcr.checksumMutex.RUnlock()

	if !ok {
		var err error
		checksum, err = util.CalculateChecksum(cmdPath)
		if err != nil {
			klog.Errorf("Could not calculate the checksum for %q", cmdPath)
			return false, err
		}
	}

	cmp := bytes.Compare(checksum, allowedChecksum)
	if cmp != 0 {
		klog.Errorf(
			"Checksum %x for %q does not equal to %x",
			checksum, cmdPath, allowedChecksum)
		return false, nil
	}

	if !ok {
		vcr.cmdCh <- commandInfo{
			path:     cmdPath,
			checksum: checksum,
		}
	}

	return true, nil
}

func (vcr *volumeControlRoot) isValidScript(
	containerScriptPath string, writeFlag bool, allowedScript *allowedScript) (
	bool, error) {
	if writeFlag {
		if !allowedScript.writable {
			klog.Errorf(
				"The script %q does not have writable permission",
				containerScriptPath)
			return false, nil
		}
	}

	if allowedScript.checksum == nil {
		return true, nil
	}

	if vcr.checksumCalculationAlways {
		return vcr.isValidChecksum(containerScriptPath, allowedScript.checksum)
	} else {
		return vcr.isValidChecksumWithCaches(
			containerScriptPath, allowedScript.checksum)
	}
}

func (vcr *volumeControlRoot) allowAbsoluteScriptPath(
	pid int, path string, writeFlag bool, scripts map[string]*allowedScript,
	containerRoot *ContainerRoot) (bool, error) {
	containerScriptPath := filepath.Join(containerRoot.rootPath, path)
	fileInfo, err := os.Lstat(containerScriptPath)
	if err != nil {
		return false, err
	}

	realPath := path
	if fileInfo.Mode()&os.ModeSymlink == os.ModeSymlink {
		containerScriptPath, err = os.Readlink(containerScriptPath)
		if err != nil {
			return false, err
		} else if containerScriptPath[0] != '/' {
			currentWorkingDirectory, err := vcr.procFsOps.
				GetCurrentWorkingDirectory(pid)
			if err != nil {
				return false, err
			}

			containerScriptPath = filepath.Join(
				containerRoot.rootPath, currentWorkingDirectory,
				containerScriptPath)
		}

		if containerRoot.rootPath == "/" {
			realPath = containerScriptPath
		} else {
			realPath = strings.TrimPrefix(
				containerScriptPath, containerRoot.rootPath)
		}
	}

	scriptForAbsolutePath, ok := scripts[realPath]
	if ok {
		allow, err := vcr.isValidScript(
			containerScriptPath, writeFlag, scriptForAbsolutePath)

		return allow, err
	}

	return false, nil
}

func (vcr *volumeControlRoot) allowRelativeScriptPath(
	pid int, path string, writeFlag bool, scripts map[string]*allowedScript,
	containerRoot *ContainerRoot, currentWorkingDirectory string) (
	bool, error) {
	for scriptPath, scriptForRelativePath := range scripts {
		if scriptForRelativePath.relativePathToInitWorkDir != "" {
			scrPath := filepath.Join(
				currentWorkingDirectory,
				scriptForRelativePath.relativePathToInitWorkDir, path)

			containerScriptPath := filepath.Join(containerRoot.rootPath,
				scrPath)
			fileInfo, err := os.Lstat(containerScriptPath)
			if err == nil {
				if fileInfo.Mode()&os.ModeSymlink == os.ModeSymlink {
					containerScriptPath, err = os.Readlink(containerScriptPath)
					if err != nil {
						return false, err
					} else if containerScriptPath[0] != '/' {
						containerScriptPath = filepath.Join(
							containerRoot.rootPath, currentWorkingDirectory,
							scriptForRelativePath.relativePathToInitWorkDir,
							containerScriptPath)
					}

					if containerRoot.rootPath == "/" {
						scrPath = containerScriptPath
					} else {
						scrPath = strings.TrimPrefix(
							containerScriptPath, containerRoot.rootPath)
					}
				}

				if scriptPath == scrPath {
					allow, err := vcr.isValidScript(
						containerScriptPath, writeFlag, scriptForRelativePath)
					return allow, err
				}
			}
		}

		scrPath := filepath.Join(currentWorkingDirectory, path)

		containerScriptPath := filepath.Join(containerRoot.rootPath, scrPath)
		fileInfo, err := os.Lstat(containerScriptPath)
		if err != nil {
			continue
		}

		if fileInfo.Mode()&os.ModeSymlink == os.ModeSymlink {
			containerScriptPath, err = os.Readlink(containerScriptPath)
			if err != nil {
				return false, err
			} else if containerScriptPath[0] != '/' {
				containerScriptPath = filepath.Join(
					containerRoot.rootPath, currentWorkingDirectory,
					containerScriptPath)
			}

			if containerRoot.rootPath == "/" {
				scrPath = containerScriptPath
			} else {
				scrPath = strings.TrimPrefix(
					containerScriptPath, containerRoot.rootPath)
			}
		}

		if scriptPath == scrPath {
			allow, err := vcr.isValidScript(
				containerScriptPath, writeFlag, scriptForRelativePath)
			return allow, err
		}
	}

	return false, nil
}

func (vcr *volumeControlRoot) allowScript(
	pid int, writeFlag bool, scripts map[string]*allowedScript,
	containerRoot *ContainerRoot) (bool, error) {
	cmdLineBytes, err := vcr.procFsOps.GetCommandLine(pid)
	if err != nil {
		return false, err
	}

	cmdLineString := strings.TrimSuffix(string(cmdLineBytes), "\x00")
	cmdLine := strings.Split(cmdLineString, "\x00")
	if len(cmdLine) < 2 {
		return false, nil
	}

	currentWorkingDirectory := ""
	for _, argument := range cmdLine[1:] {
		var (
			allow bool
			err   error
		)

		if argument[0] == '/' {
			allow, err = vcr.allowAbsoluteScriptPath(
				pid, argument, writeFlag, scripts, containerRoot)
			if allow {
				return allow, err
			}
		} else {
			if currentWorkingDirectory == "" {
				currentWorkingDirectory, err = vcr.procFsOps.
					GetCurrentWorkingDirectory(pid)
				if err != nil {
					return false, err
				}
			}

			allow, err = vcr.allowRelativeScriptPath(
				pid, argument, writeFlag, scripts, containerRoot,
				currentWorkingDirectory)
			if allow {
				return allow, err
			}
		}
	}

	klog.Errorf("Denied scripts: %q", cmdLine)

	return false, nil
}

func (vcr *volumeControlRoot) allow(pid uint32, writeFlag bool) (bool, error) {
	vcr.rootsMutex.RLock()
	defer vcr.rootsMutex.RUnlock()

	mountNamespace, err := vcr.procFsOps.GetMountNamespace(int(pid))
	if err != nil {
		return false, err
	}

	containerRoot, found := vcr.containerRoots[mountNamespace]
	if !found {
		cmdPath, err := vcr.procFsOps.GetCommandPath(int(pid))
		if err != nil {
			klog.Errorf("Not get the command path: %v", err.Error())
		}

		klog.Errorf(
			"Pid '%d' with a mount namespace %q and a command %q executed "+
				"a file operation",
			pid, mountNamespace, cmdPath)
		return false, nil
	} else if containerRoot.rootPath == "" {
		return true, nil
	}

	cmdPath, err := vcr.procFsOps.GetCommandPath(int(pid))
	if err != nil {
		return false, fmt.Errorf("Not get the command path: %v", err.Error())
	}

	executable, ok := containerRoot.allowedExecutables[cmdPath]
	if !ok {
		klog.Errorf("Denied the command path: %q", cmdPath)
		return false, nil
	}

	if writeFlag {
		if !executable.writable {
			klog.Errorf(
				"The command %q does not have writable permission", cmdPath)
			return false, nil
		}
	}

	if executable.checksum != nil {
		containerCmdPath := filepath.Join(containerRoot.rootPath, cmdPath)
		var (
			valid bool
			err   error
		)

		if vcr.checksumCalculationAlways {
			valid, err = vcr.isValidChecksum(
				containerCmdPath, executable.checksum)
			if !valid || err != nil {
				return valid, err
			}
		} else {
			valid, err = vcr.isValidChecksumWithCaches(
				containerCmdPath, executable.checksum)
			if !valid || err != nil {
				return valid, err
			}
		}
	}

	if len(executable.scripts) != 0 {
		return vcr.allowScript(
			int(pid), writeFlag, executable.scripts, containerRoot)
	}

	return true, nil
}

func (vcr *volumeControlRoot) idFromStat(stat *syscall.Stat_t) fs.StableAttr {
	// We compose an inode number by the underlying inode, and
	// mixing in the device number. In traditional filesystems,
	// the inode numbers are small. The device numbers are also
	// small (typically 16 bit). Finally, we mask out the root
	// device number of the root, so a loopback FS that does not
	// encompass multiple mounts will reflect the inode numbers of
	// the underlying filesystem
	swapped := (uint64(stat.Dev) << 32) | (uint64(stat.Dev) >> 32)
	swappedRootDev := (vcr.dev << 32) | (vcr.dev >> 32)
	return fs.StableAttr{
		Mode: uint32(stat.Mode),
		Gen:  1,
		// This should work well for traditional backing FSes,
		// not so much for other go-fuse FS-es
		Ino: (swapped ^ swappedRootDev) ^ stat.Ino,
	}
}

func (vcn *volumeControlNode) isProcessAllowed(
	ctx context.Context, writeFlag bool) error {
	if vcn.rootData.usageControlDisabled {
		return nil
	}

	caller, ok := fuse.FromContext(ctx)
	if !ok {
		return fmt.Errorf("Failed to call FromContext()")
	}

	ok, err := vcn.rootData.allow(caller.Pid, writeFlag)
	if err != nil {
		return err
	} else if !ok {
		return os.ErrPermission
	}

	return nil
}

// preserveOwner sets uid and gid of `path` according to the caller information
// in `ctx`.
func (vcn *volumeControlNode) preserveOwner(
	ctx context.Context, path string) error {
	if os.Getuid() != 0 {
		return nil
	}

	caller, ok := fuse.FromContext(ctx)
	if !ok {
		return nil
	}

	return syscall.Lchown(path, int(caller.Uid), int(caller.Gid))
}

func (vcn *volumeControlNode) Statfs(
	ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	err := vcn.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("statfs(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	s := syscall.Statfs_t{}
	err = syscall.Statfs(vcn.path(), &s)
	if err != nil {
		return fs.ToErrno(err)
	}

	out.FromStatfsT(&s)

	return fs.OK
}

func (vcn *volumeControlNode) Getattr(
	ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	err := vcn.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("getattr(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	if fh != nil {
		return fh.(fs.FileGetattrer).Getattr(ctx, out)
	}

	path := vcn.path()

	stat := syscall.Stat_t{}
	if &vcn.Inode == vcn.Root() {
		err = syscall.Stat(path, &stat)
	} else {
		err = syscall.Lstat(path, &stat)
	}
	if err != nil {
		return fs.ToErrno(err)
	}
	out.FromStat(&stat)

	return fs.OK
}

func (vcn *volumeControlNode) Setattr(
	ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn,
	out *fuse.AttrOut) syscall.Errno {
	err := vcn.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("setattr(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	path := vcn.path()
	fsa, ok := fh.(fs.FileSetattrer)
	if ok && fsa != nil {
		fsa.Setattr(ctx, in, out)
	} else {
		if mode, ok := in.GetMode(); ok {
			if err := syscall.Chmod(path, mode); err != nil {
				return fs.ToErrno(err)
			}
		}

		uid, uOk := in.GetUID()
		gid, gOk := in.GetGID()
		if uOk || gOk {
			suid := -1
			sgid := -1
			if uOk {
				suid = int(uid)
			}
			if gOk {
				sgid = int(gid)
			}
			if err := syscall.Chown(path, suid, sgid); err != nil {
				return fs.ToErrno(err)
			}
		}

		mTime, mOk := in.GetMTime()
		aTime, aOk := in.GetATime()

		if mOk || aOk {
			aTimeP := &aTime
			mTimeP := &mTime
			if !aOk {
				aTimeP = nil
			}
			if !mOk {
				mTimeP = nil
			}
			var ts [2]syscall.Timespec
			ts[0] = fuse.UtimeToTimespec(aTimeP)
			ts[1] = fuse.UtimeToTimespec(mTimeP)

			if err := syscall.UtimesNano(path, ts[:]); err != nil {
				return fs.ToErrno(err)
			}
		}

		if size, ok := in.GetSize(); ok {
			if err := syscall.Truncate(path, int64(size)); err != nil {
				return fs.ToErrno(err)
			}
		}
	}

	fga, ok := fh.(fs.FileGetattrer)
	if ok && fga != nil {
		fga.Getattr(ctx, out)
	} else {
		stat := syscall.Stat_t{}
		err := syscall.Lstat(path, &stat)
		if err != nil {
			return fs.ToErrno(err)
		}
		out.FromStat(&stat)
	}
	return fs.OK
}

func (vcn *volumeControlNode) Readlink(
	ctx context.Context) ([]byte, syscall.Errno) {
	err := vcn.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("readlink(): %s", err.Error())
		}
		return nil, fs.ToErrno(err)
	}

	path := vcn.path()

	for length := 256; ; length *= 2 {
		buf := make([]byte, length)
		size, err := syscall.Readlink(path, buf)
		if err != nil {
			return nil, fs.ToErrno(err)
		}

		if size < len(buf) {
			return buf[:size], 0
		}
	}
}

func (vcn *volumeControlNode) Create(
	ctx context.Context, name string, flags uint32, mode uint32,
	out *fuse.EntryOut) (
	inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	err := vcn.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("create(): %s", err.Error())
		}
		return nil, nil, 0, fs.ToErrno(err)
	}

	path := filepath.Join(vcn.path(), name)
	flags = flags &^ syscall.O_APPEND
	fd, err := syscall.Open(path, int(flags)|os.O_CREATE, mode)
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}

	err = vcn.preserveOwner(ctx, path)
	if err != nil {
		klog.Errorf("create(): %s", err.Error())
		return nil, nil, 0, fs.ToErrno(err)
	}

	stat := syscall.Stat_t{}
	if err := syscall.Fstat(fd, &stat); err != nil {
		syscall.Close(fd)
		return nil, nil, 0, fs.ToErrno(err)
	}

	node := vcn.rootData.newNode(vcn.EmbeddedInode(), name, &stat)
	childInode := vcn.NewInode(ctx, node, vcn.rootData.idFromStat(&stat))
	volumeControlFile := NewVolumeControlFile(fd, path, vcn.rootData)

	out.FromStat(&stat)

	return childInode, volumeControlFile, 0, 0
}

func (vcn *volumeControlNode) Open(
	ctx context.Context, flags uint32) (
	fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {

	writeFlag := false
	if int(flags)&(os.O_WRONLY|os.O_RDWR) != 0 {
		writeFlag = true
	}

	err := vcn.isProcessAllowed(ctx, writeFlag)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("open(): %s", err.Error())
		}
		return nil, 0, fs.ToErrno(err)
	}

	flags = flags &^ syscall.O_APPEND
	path := vcn.path()
	fd, err := syscall.Open(path, int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}

	volumeControlFile := NewVolumeControlFile(fd, path, vcn.rootData)

	return volumeControlFile, 0, 0
}

func (vcn *volumeControlNode) Lookup(
	ctx context.Context, name string, out *fuse.EntryOut) (
	*fs.Inode, syscall.Errno) {
	err := vcn.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("lookup(): %s", err.Error())
		}
		return nil, fs.ToErrno(err)
	}

	path := filepath.Join(vcn.path(), name)

	stat := syscall.Stat_t{}
	err = syscall.Lstat(path, &stat)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&stat)
	node := vcn.rootData.newNode(vcn.EmbeddedInode(), name, &stat)
	childInode := vcn.NewInode(ctx, node, vcn.rootData.idFromStat(&stat))

	return childInode, 0
}

func (vcn *volumeControlNode) Opendir(ctx context.Context) syscall.Errno {
	err := vcn.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("opendir(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	fd, err := syscall.Open(vcn.path(), syscall.O_DIRECTORY, 0755)
	if err != nil {
		return fs.ToErrno(err)
	}
	syscall.Close(fd)
	return fs.OK
}

func (vcn *volumeControlNode) Readdir(
	ctx context.Context) (fs.DirStream, syscall.Errno) {
	return NewLoopbackDirStream(vcn.path())
}

func (vcn *volumeControlNode) Mkdir(
	ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (
	*fs.Inode, syscall.Errno) {
	err := vcn.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("mkdir(): %s", err.Error())
		}
		return nil, fs.ToErrno(err)
	}

	path := filepath.Join(vcn.path(), name)
	err = os.Mkdir(path, os.FileMode(mode))
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	err = vcn.preserveOwner(ctx, path)
	if err != nil {
		klog.Errorf("mkdir(): %s", err.Error())
		return nil, fs.ToErrno(err)
	}

	stat := syscall.Stat_t{}
	if err := syscall.Lstat(path, &stat); err != nil {
		syscall.Rmdir(path)
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&stat)

	node := vcn.rootData.newNode(vcn.EmbeddedInode(), name, &stat)
	childInode := vcn.NewInode(ctx, node, vcn.rootData.idFromStat(&stat))

	return childInode, 0
}

func (vcn *volumeControlNode) Mknod(
	ctx context.Context, name string, mode, rdev uint32, out *fuse.EntryOut) (
	*fs.Inode, syscall.Errno) {
	err := vcn.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("mknod(): %s", err.Error())
		}
		return nil, fs.ToErrno(err)
	}

	path := filepath.Join(vcn.path(), name)
	err = syscall.Mknod(path, mode, int(rdev))
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	err = vcn.preserveOwner(ctx, path)
	if err != nil {
		klog.Errorf("mknod(): %s", err.Error())
		return nil, fs.ToErrno(err)
	}

	stat := syscall.Stat_t{}
	if err := syscall.Lstat(path, &stat); err != nil {
		syscall.Rmdir(path)
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&stat)

	node := vcn.rootData.newNode(vcn.EmbeddedInode(), name, &stat)
	childInode := vcn.NewInode(ctx, node, vcn.rootData.idFromStat(&stat))

	return childInode, 0
}

func (vcn *volumeControlNode) Link(
	ctx context.Context, target fs.InodeEmbedder, name string,
	out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	err := vcn.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("link(): %s", err.Error())
		}
		return nil, fs.ToErrno(err)
	}

	newPath := filepath.Join(vcn.path(), name)

	oldPath := filepath.Join(
		vcn.rootData.path, target.EmbeddedInode().Path(nil))
	err = syscall.Link(oldPath, newPath)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	stat := syscall.Stat_t{}
	if err := syscall.Lstat(newPath, &stat); err != nil {
		syscall.Unlink(newPath)
		return nil, fs.ToErrno(err)
	}
	node := vcn.rootData.newNode(vcn.EmbeddedInode(), name, &stat)
	childInode := vcn.NewInode(ctx, node, vcn.rootData.idFromStat(&stat))

	out.Attr.FromStat(&stat)

	return childInode, 0
}

func (vcn *volumeControlNode) Symlink(
	ctx context.Context, target, name string, out *fuse.EntryOut) (
	*fs.Inode, syscall.Errno) {
	err := vcn.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("symlink(): %s", err.Error())
		}
		return nil, fs.ToErrno(err)
	}

	path := filepath.Join(vcn.path(), name)
	err = syscall.Symlink(target, path)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	err = vcn.preserveOwner(ctx, path)
	if err != nil {
		klog.Errorf("symlink(): %s", err.Error())
		return nil, fs.ToErrno(err)
	}

	stat := syscall.Stat_t{}
	if err := syscall.Lstat(path, &stat); err != nil {
		syscall.Unlink(path)
		return nil, fs.ToErrno(err)
	}
	node := vcn.rootData.newNode(vcn.EmbeddedInode(), name, &stat)
	childInode := vcn.NewInode(ctx, node, vcn.rootData.idFromStat(&stat))

	out.Attr.FromStat(&stat)

	return childInode, 0
}

func (vcn *volumeControlNode) Unlink(
	ctx context.Context, name string) syscall.Errno {
	err := vcn.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("unlink(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	path := filepath.Join(vcn.path(), name)

	err = syscall.Unlink(path)
	if err == nil {
		if vcn.rootData.messageQueueUpdatePublisher != nil {
			err := vcn.rootData.messageQueueUpdatePublisher.
				CreateDeleteFileMessages(path)
			if err != nil {
				return fs.ToErrno(err)
			}
		}
	}

	return fs.ToErrno(err)
}

func (vcn *volumeControlNode) Rmdir(
	ctx context.Context, name string) syscall.Errno {
	err := vcn.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("rmdir(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	path := filepath.Join(vcn.path(), name)
	err = syscall.Rmdir(path)

	return fs.ToErrno(err)
}

func (vcn *volumeControlNode) Rename(
	ctx context.Context, name string, newParent fs.InodeEmbedder,
	newName string, flags uint32) syscall.Errno {
	err := vcn.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("rename(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	if flags&fs.RENAME_EXCHANGE != 0 {
		return vcn.renameExchange(name, newParent, newName)
	}

	oldPath := filepath.Join(vcn.path(), name)
	newPath := filepath.Join(
		vcn.rootData.path, newParent.EmbeddedInode().EmbeddedInode().Path(nil),
		newName)

	err = syscall.Rename(oldPath, newPath)
	if err == nil {
		if vcn.rootData.messageQueueUpdatePublisher != nil {
			err := vcn.rootData.messageQueueUpdatePublisher.
				CreateRenameFileMessages(newPath, oldPath)
			if err != nil {
				return fs.ToErrno(err)
			}
		}
	}

	return fs.ToErrno(err)
}
