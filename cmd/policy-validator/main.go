// Copyright (c) 2022 Fujitsu Limited

package main

import (
	"math/rand"
	"os"
	"runtime"
	"time"

	"k8s.io/component-base/logs"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/policy-validator/app"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())

	command := app.NewPolicyValidatorCommand()

	logs.InitLogs()
	defer logs.FlushLogs()

	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
