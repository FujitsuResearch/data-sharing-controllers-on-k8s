// Copyright (c) 2022 Fujitsu Limited

package testutil

import (
	"bytes"
	"flag"
	"runtime"
)

// [REF] github.com/hanwen/go-fuse/internal/testutil/verbose.go

func VerboseTest() bool {
	var buf [2048]byte
	n := runtime.Stack(buf[:], false)
	if bytes.Index(buf[:n], []byte("TestNonVerbose")) != -1 {
		return false
	}

	flag := flag.Lookup("test.v")

	return flag != nil && flag.Value.String() == "true"
}
