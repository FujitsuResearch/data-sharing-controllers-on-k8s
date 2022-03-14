// Copyright (c) 2022 Fujitsu Limited

package docker

import (
	"fmt"

	"github.com/spf13/pflag"
)

const (
	defaultDataRoot = "/var/lib/docker"
)

type DockerOptions struct {
	DataRootDirectory string
	ApiVersion        string
}

func NewDockerOptions() *DockerOptions {
	return &DockerOptions{
		DataRootDirectory: defaultDataRoot,
	}
}

func (do *DockerOptions) AddFlags(flagSet *pflag.FlagSet) {
	dockerOnlyWarning := "This docker-specific flag only works " +
		"when container-runtime is set to 'docker'."

	flagSet.StringVar(
		&do.DataRootDirectory, "docker-data-root-dir", do.DataRootDirectory,
		fmt.Sprintf(
			"Root directry for persistent docker states, "+
				"e.g. '/var/lib/docker'. %s",
			dockerOnlyWarning))

	flagSet.StringVar(
		&do.ApiVersion, "docker-api-version", do.ApiVersion,
		fmt.Sprintf("Docker client API version. %s", dockerOnlyWarning))
}
