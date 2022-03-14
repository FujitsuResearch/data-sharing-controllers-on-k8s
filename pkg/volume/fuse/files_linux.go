// Copyright (c) 2022 Fujitsu Limited

package fuse

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"k8s.io/klog/v2"
)

// [REF] github.com/hanwen/go-fuse/fs/files_linux.go

// Utimens - file handle based version of loopbackFileSystem.Utimens()
func (vcf *volumeControlFile) utimens(
	a *time.Time, m *time.Time) syscall.Errno {
	var ts [2]syscall.Timespec
	ts[0] = fuse.UtimeToTimespec(a)
	ts[1] = fuse.UtimeToTimespec(m)
	err := futimens(int(vcf.fd), &ts)

	return fs.ToErrno(err)
}

func (vcf *volumeControlFile) Allocate(
	ctx context.Context, offset uint64, size uint64,
	mode uint32) syscall.Errno {
	err := vcf.isProcessAllowed(ctx, true)
	if err != nil {
		if err == os.ErrPermission {
			klog.Errorf("fallocate(): %s", err.Error())
		}
		return fs.ToErrno(err)
	}

	vcf.mutex.Lock()
	defer vcf.mutex.Unlock()

	err = syscall.Fallocate(vcf.fd, mode, int64(offset), int64(size))
	if err != nil {
		return fs.ToErrno(err)
	}

	return fs.OK
}
