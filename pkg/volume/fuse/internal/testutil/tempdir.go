// Copyright (c) 2022 Fujitsu Limited

package testutil

import (
	"io/ioutil"
	"log"
	"runtime"
	"strings"
)

// [REF] github.com/hanwen/go-fuse/internal/testutil/tempdir.go

func TempDir() string {
	frames := make([]uintptr, 10) // at least 1 entry needed
	n := runtime.Callers(1, frames)

	lastName := ""
	for _, pc := range frames[:n] {
		f := runtime.FuncForPC(pc)
		name := f.Name()
		i := strings.LastIndex(name, ".")
		if i >= 0 {
			name = name[i+1:]
		}
		if strings.HasPrefix(name, "Test") {
			lastName = name
		}
	}

	dir, err := ioutil.TempDir("", lastName)
	if err != nil {
		log.Panicf("TempDir(%s): %v", lastName, err)
	}

	return dir
}
