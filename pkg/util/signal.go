// Copyright (c) 2022 Fujitsu Limited

package util

import (
	"os"
	"os/signal"
	"syscall"
)

func SetupSignalHandler() <-chan struct{} {
	stop := make(chan struct{})

	c := make(chan os.Signal, 2)
	shutdownSignals := []os.Signal{
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGKILL,
	}
	signal.Notify(c, shutdownSignals...)

	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1)
	}()

	return stop
}
