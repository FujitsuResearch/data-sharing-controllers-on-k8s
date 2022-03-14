// Copyright (c) 2022 Fujitsu Limited

package fuse

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

// [REF] github.com/hanwen/go-fuse/fs/files.go

const (
	_OFD_GETLK  = 36
	_OFD_SETLK  = 37
	_OFD_SETLKW = 38
)

var (
	_ = (fs.FileHandle)((*volumeControlFile)(nil))
	_ = (fs.FileReleaser)((*volumeControlFile)(nil))
	_ = (fs.FileGetattrer)((*volumeControlFile)(nil))
	_ = (fs.FileSetattrer)((*volumeControlFile)(nil))
	_ = (fs.FileReader)((*volumeControlFile)(nil))
	_ = (fs.FileWriter)((*volumeControlFile)(nil))
	_ = (fs.FileGetlker)((*volumeControlFile)(nil))
	_ = (fs.FileSetlker)((*volumeControlFile)(nil))
	_ = (fs.FileSetlkwer)((*volumeControlFile)(nil))
	_ = (fs.FileLseeker)((*volumeControlFile)(nil))
	_ = (fs.FileFlusher)((*volumeControlFile)(nil))
	_ = (fs.FileFsyncer)((*volumeControlFile)(nil))
	_ = (fs.FileAllocater)((*volumeControlFile)(nil))
)

type volumeControlFile struct {
	mutex sync.Mutex
	fd    int
	path  string

	rootData *volumeControlRoot
}

func NewVolumeControlFile(
	fd int, path string, rootData *volumeControlRoot) fs.FileHandle {
	return &volumeControlFile{
		fd:       fd,
		path:     path,
		rootData: rootData,
	}
}

func (vcf *volumeControlFile) isProcessAllowed(
	ctx context.Context, writeFlag bool) error {
	caller, ok := fuse.FromContext(ctx)
	if !ok {
		return fmt.Errorf("Failed to call FromContext()")
	}

	ok, err := vcf.rootData.allow(caller.Pid, writeFlag)
	if err != nil {
		return err
	} else if !ok {
		return os.ErrPermission
	}

	return nil
}

func (vcf *volumeControlFile) isProcessAllowedExceptForPid0(
	ctx context.Context, writeFlag bool) error {
	caller, ok := fuse.FromContext(ctx)
	if !ok {
		return fmt.Errorf("Failed to call FromContext()")
	}

	if caller.Pid == 0 {
		return nil
	}

	ok, err := vcf.rootData.allow(caller.Pid, writeFlag)
	if err != nil {
		return err
	} else if !ok {
		return os.ErrPermission
	}

	return nil
}

func (vcf *volumeControlFile) Release(ctx context.Context) syscall.Errno {
	// pid '0' can call (a kernel process ?)
	err := vcf.isProcessAllowedExceptForPid0(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("release(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	vcf.mutex.Lock()
	defer vcf.mutex.Unlock()

	if vcf.fd != -1 {
		err := syscall.Close(vcf.fd)
		vcf.fd = -1
		return fs.ToErrno(err)
	}

	return syscall.EBADF
}

func (vcf *volumeControlFile) Getattr(
	ctx context.Context, attrOut *fuse.AttrOut) syscall.Errno {
	err := vcf.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("getattr(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	vcf.mutex.Lock()
	defer vcf.mutex.Unlock()

	stat := syscall.Stat_t{}
	err = syscall.Fstat(vcf.fd, &stat)
	if err != nil {
		return fs.ToErrno(err)
	}
	attrOut.FromStat(&stat)

	return fs.OK
}

func (vcf *volumeControlFile) setAttr(
	ctx context.Context, in *fuse.SetAttrIn) syscall.Errno {
	vcf.mutex.Lock()
	defer vcf.mutex.Unlock()

	var errno syscall.Errno
	if mode, ok := in.GetMode(); ok {
		errno = fs.ToErrno(syscall.Fchmod(vcf.fd, mode))
		if errno != 0 {
			return errno
		}
	}

	uid32, uOk := in.GetUID()
	gid32, gOk := in.GetGID()
	if uOk || gOk {
		uid := -1
		gid := -1

		if uOk {
			uid = int(uid32)
		}
		if gOk {
			gid = int(gid32)
		}
		errno = fs.ToErrno(syscall.Fchown(vcf.fd, uid, gid))
		if errno != 0 {
			return errno
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
		errno = vcf.utimens(aTimeP, mTimeP)
		if errno != 0 {
			return errno
		}
	}

	if size, ok := in.GetSize(); ok {
		errno = fs.ToErrno(syscall.Ftruncate(vcf.fd, int64(size)))
		if errno != 0 {
			return errno
		}
	}
	return fs.OK
}

func (vcf *volumeControlFile) Setattr(
	ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	err := vcf.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("setattr(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	if errno := vcf.setAttr(ctx, in); errno != 0 {
		return errno
	}

	return vcf.Getattr(ctx, out)
}

func (vcf *volumeControlFile) Read(
	ctx context.Context, buf []byte, offset int64) (
	res fuse.ReadResult, errno syscall.Errno) {
	err := vcf.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("read(): %s", err.Error())
		}
		return nil, fs.ToErrno(err)
	}

	vcf.mutex.Lock()
	defer vcf.mutex.Unlock()

	readResult := fuse.ReadResultFd(uintptr(vcf.fd), offset, len(buf))

	return readResult, fs.OK
}

func (vcf *volumeControlFile) Write(
	ctx context.Context, data []byte, offset int64) (uint32, syscall.Errno) {
	err := vcf.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("write(): %s", err.Error())
		}
		return 0, fs.ToErrno(err)
	}

	vcf.mutex.Lock()
	defer vcf.mutex.Unlock()

	writtenSize, err := syscall.Pwrite(vcf.fd, data, offset)
	if err == nil {
		if vcf.rootData.messageQueueUpdatePublisher != nil {
			err := vcf.rootData.messageQueueUpdatePublisher.
				CreateUpdateFileMessages(vcf.path, offset, data)
			if err != nil {
				return 0, fs.ToErrno(err)
			}
		}
	}

	return uint32(writtenSize), fs.ToErrno(err)
}

func (vcf *volumeControlFile) Getlk(
	ctx context.Context, owner uint64, fileLock *fuse.FileLock, flags uint32,
	out *fuse.FileLock) (errno syscall.Errno) {
	err := vcf.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("getlock(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	vcf.mutex.Lock()
	defer vcf.mutex.Unlock()

	flock := syscall.Flock_t{}
	fileLock.ToFlockT(&flock)

	errno = fs.ToErrno(syscall.FcntlFlock(uintptr(vcf.fd), _OFD_GETLK, &flock))

	out.FromFlockT(&flock)

	return
}

func (vcf *volumeControlFile) setLock(
	ctx context.Context, owner uint64, fileLock *fuse.FileLock, flags uint32,
	blocking bool) (errno syscall.Errno) {
	err := vcf.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("setlock(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	vcf.mutex.Lock()
	defer vcf.mutex.Unlock()

	if (flags & fuse.FUSE_LK_FLOCK) != 0 {
		var op int
		switch fileLock.Typ {
		case syscall.F_RDLCK:
			op = syscall.LOCK_SH
		case syscall.F_WRLCK:
			op = syscall.LOCK_EX
		case syscall.F_UNLCK:
			op = syscall.LOCK_UN
		default:
			return syscall.EINVAL
		}

		if !blocking {
			op |= syscall.LOCK_NB
		}

		return fs.ToErrno(syscall.Flock(vcf.fd, op))
	} else {
		flock := syscall.Flock_t{}
		fileLock.ToFlockT(&flock)

		var op int
		if blocking {
			op = _OFD_SETLKW
		} else {
			op = _OFD_SETLK
		}

		return fs.ToErrno(syscall.FcntlFlock(uintptr(vcf.fd), op, &flock))
	}
}

func (vcf *volumeControlFile) Setlk(
	ctx context.Context, owner uint64, fileLock *fuse.FileLock, flags uint32) (
	errno syscall.Errno) {
	return vcf.setLock(ctx, owner, fileLock, flags, false)
}

func (vcf *volumeControlFile) Setlkw(
	ctx context.Context, owner uint64, fileLock *fuse.FileLock, flags uint32) (
	errno syscall.Errno) {
	return vcf.setLock(ctx, owner, fileLock, flags, true)
}

func (vcf *volumeControlFile) Lseek(
	ctx context.Context, offset uint64, whence uint32) (uint64, syscall.Errno) {
	err := vcf.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("lseek(): %s", err.Error())
		}
		return 0, fs.ToErrno(err)
	}

	vcf.mutex.Lock()
	defer vcf.mutex.Unlock()

	offsetResult, err := unix.Seek(vcf.fd, int64(offset), int(whence))

	return uint64(offsetResult), fs.ToErrno(err)
}

func (vcf *volumeControlFile) Flush(ctx context.Context) syscall.Errno {
	err := vcf.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("flush(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	vcf.mutex.Lock()
	defer vcf.mutex.Unlock()

	// Since Flush() may be called for each dup'd fd, we don't
	// want to really close the file, we just want to flush. This
	// is achieved by closing a dup'd fd.
	newFd, err := syscall.Dup(vcf.fd)

	if err != nil {
		return fs.ToErrno(err)
	}
	err = syscall.Close(newFd)

	return fs.ToErrno(err)
}

func (vcf *volumeControlFile) Fsync(
	ctx context.Context, flags uint32) (errno syscall.Errno) {
	err := vcf.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("fsync(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	vcf.mutex.Lock()
	defer vcf.mutex.Unlock()

	fErr := fs.ToErrno(syscall.Fsync(vcf.fd))

	return fErr
}
