// Copyright (c) 2022 Fujitsu Limited

package main

import (
	"math/rand"
	"os"
	"time"

	"k8s.io/component-base/logs"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volumectl"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	command := volumectl.NewVolumeCtlCommand()

	logs.InitLogs()
	defer logs.FlushLogs()

	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
