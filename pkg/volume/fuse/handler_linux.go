// Copyright (c) 2022 Fujitsu Limited

package fuse

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

// [REF] github.com/hanwen/go-fuse/fs/loopback_linux.go

func (vcn *volumeControlNode) renameExchange(
	name string, newParent fs.InodeEmbedder, newName string) syscall.Errno {
	oldFd, err := syscall.Open(vcn.path(), syscall.O_DIRECTORY, 0)
	if err != nil {
		return fs.ToErrno(err)
	}
	defer syscall.Close(oldFd)

	path := filepath.Join(
		vcn.rootData.path, newParent.EmbeddedInode().Path(nil))
	newFd, err := syscall.Open(path, syscall.O_DIRECTORY, 0)
	if err != nil {
		return fs.ToErrno(err)
	}
	defer syscall.Close(newFd)

	var stat syscall.Stat_t
	if err := syscall.Fstat(oldFd, &stat); err != nil {
		return fs.ToErrno(err)
	}

	// Double check that nodes didn't change from under us.
	inode := &vcn.Inode
	if inode.Root() != inode &&
		inode.StableAttr().Ino != vcn.rootData.idFromStat(&stat).Ino {
		return syscall.EBUSY
	}
	if err := syscall.Fstat(newFd, &stat); err != nil {
		return fs.ToErrno(err)
	}

	newInode := newParent.EmbeddedInode()
	if newInode.Root() != newInode &&
		newInode.StableAttr().Ino != vcn.rootData.idFromStat(&stat).Ino {
		return syscall.EBUSY
	}

	return fs.ToErrno(
		unix.Renameat2(oldFd, name, newFd, newName, unix.RENAME_EXCHANGE))
}

func (vcn *volumeControlNode) Getxattr(
	ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	err := vcn.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("getxattr(): %s", err.Error())
		}
		return 0, fs.ToErrno(err)
	}

	size, err := unix.Lgetxattr(vcn.path(), attr, dest)

	return uint32(size), fs.ToErrno(err)
}

func (vcn *volumeControlNode) Setxattr(
	ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	err := vcn.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("setxattr(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	err = unix.Lsetxattr(vcn.path(), attr, data, int(flags))

	return fs.ToErrno(err)
}

func (vcn *volumeControlNode) Removexattr(
	ctx context.Context, attr string) syscall.Errno {
	err := vcn.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("removexattr(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	err = unix.Lremovexattr(vcn.path(), attr)

	return fs.ToErrno(err)
}

func (vcn *volumeControlNode) Listxattr(
	ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	err := vcn.isProcessAllowed(ctx, false)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("listxattr(): %s", err.Error())
		}
		return 0, fs.ToErrno(err)
	}

	size, err := unix.Llistxattr(vcn.path(), dest)

	return uint32(size), fs.ToErrno(err)
}

func getInputContents(
	ucfIn *volumeControlFile, offset int64, count int) ([]byte, error) {
	inFd, err := syscall.Open(ucfIn.path, syscall.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(inFd)

	var contents []byte
	readCount, err := syscall.Pread(inFd, contents, offset)
	if err != nil {
		return nil, err
	} else if readCount != count {
		return nil, fmt.Errorf(
			"Internal error: the returned count (%d) is "+
				"different from the argument one (%d) in pread()",
			readCount, count)
	}

	return contents, nil
}

func (vcn *volumeControlNode) CopyFileRange(
	ctx context.Context, fhIn fs.FileHandle, offIn uint64, out *fs.Inode,
	fhOut fs.FileHandle, offOut uint64, len uint64, flags uint64) (
	uint32, syscall.Errno) {
	err := vcn.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("copyfilerange(): %s", err.Error())
		}
		return 0, fs.ToErrno(err)
	}

	ucfIn, ok := fhIn.(*volumeControlFile)
	if !ok {
		return 0, syscall.ENOTSUP
	}
	ucfOut, ok := fhOut.(*volumeControlFile)
	if !ok {
		return 0, syscall.ENOTSUP
	}

	signedOffIn := int64(offIn)
	signedOffOut := int64(offOut)
	count, err := unix.CopyFileRange(
		ucfIn.fd, &signedOffIn, ucfOut.fd, &signedOffOut, int(len), int(flags))
	if err == nil {
		if vcn.rootData.messageQueueUpdatePublisher != nil {
			inFd, err := syscall.Open(ucfIn.path, syscall.O_RDONLY, 0)
			if err != nil {
				return 0, fs.ToErrno(err)
			}
			defer syscall.Close(inFd)

			data, err := getInputContents(ucfIn, signedOffIn, int(len))
			if err != nil {
				return 0, fs.ToErrno(err)
			}

			err = vcn.rootData.messageQueueUpdatePublisher.
				CreateUpdateFileMessages(ucfOut.path, signedOffOut, data)
			if err != nil {
				return 0, fs.ToErrno(err)
			}
		}
	}

	return uint32(count), fs.ToErrno(err)
}
