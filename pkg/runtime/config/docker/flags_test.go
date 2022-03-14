// Copyright (c) 2022 Fujitsu Limited

package docker

import (
	"testing"

	"github.com/spf13/pflag"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFlags(t *testing.T) {
	fs := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	d := NewDockerOptions()
	d.AddFlags(fs)

	fs.Parse(nil)

	expectedKo := &DockerOptions{
		DataRootDirectory: "/var/lib/docker",
	}

	assert.Equal(t, expectedKo, d)
}

func TestAddFlags(t *testing.T) {
	fs := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	d := NewDockerOptions()
	d.AddFlags(fs)

	args := []string{
		"--docker-data-root-dir=/var/lib/docker/data",
		"--docker-api-version=1.0",
	}

	fs.Parse(args)

	expectedKo := &DockerOptions{
		DataRootDirectory: "/var/lib/docker/data",
		ApiVersion:        "1.0",
	}

	assert.Equal(t, expectedKo, d)
}
