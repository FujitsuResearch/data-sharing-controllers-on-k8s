// Copyright (c) 2022 Fujitsu Limited

package fuse

import (
	"sync"
	"syscall"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// [REF] github.com/hanwen/go-fuse/fs/dirstream_linux.go

type loopbackDirStream struct {
	buf  []byte
	todo []byte

	// Protects fd so we can guard against double close
	mutex sync.Mutex
	fd    int
}

func (lds *loopbackDirStream) load() syscall.Errno {
	if len(lds.todo) > 0 {
		return fs.OK
	}

	n, err := syscall.Getdents(lds.fd, lds.buf)
	if err != nil {
		return fs.ToErrno(err)
	}
	lds.todo = lds.buf[:n]

	return fs.OK
}

// NewLoopbackDirStream open a directory for reading as a DirStream
func NewLoopbackDirStream(name string) (fs.DirStream, syscall.Errno) {
	fd, err := syscall.Open(name, syscall.O_DIRECTORY, 0755)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	lds := &loopbackDirStream{
		buf: make([]byte, 4096),
		fd:  fd,
	}

	if err := lds.load(); err != 0 {
		lds.Close()
		return nil, err
	}

	return lds, fs.OK
}

func (lds *loopbackDirStream) Close() {
	lds.mutex.Lock()
	defer lds.mutex.Unlock()

	if lds.fd != -1 {
		syscall.Close(lds.fd)
		lds.fd = -1
	}
}

func (lds *loopbackDirStream) HasNext() bool {
	lds.mutex.Lock()
	defer lds.mutex.Unlock()

	return len(lds.todo) > 0
}

func (lds *loopbackDirStream) Next() (fuse.DirEntry, syscall.Errno) {
	lds.mutex.Lock()
	defer lds.mutex.Unlock()

	de := (*syscall.Dirent)(unsafe.Pointer(&lds.todo[0]))

	nameBytes := lds.todo[unsafe.Offsetof(syscall.Dirent{}.Name):de.Reclen]
	lds.todo = lds.todo[de.Reclen:]

	// After the loop, l contains the index of the first '\0'.
	l := 0
	for l = range nameBytes {
		if nameBytes[l] == 0 {
			break
		}
	}
	nameBytes = nameBytes[:l]
	result := fuse.DirEntry{
		Ino:  de.Ino,
		Mode: (uint32(de.Type) << 12),
		Name: string(nameBytes),
	}

	return result, lds.load()
}
