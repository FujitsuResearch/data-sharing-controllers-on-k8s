// Copyright (c) 2022 Fujitsu Limited

package containerd

import (
	"testing"

	"github.com/spf13/pflag"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFlags(t *testing.T) {
	fs := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	d := NewContainerdOptions()
	d.AddFlags(fs)

	fs.Parse(nil)

	expectedCo := &ContainerdOptions{
		DataRootDirectory: "/run/containerd",
		Namespace:         "k8s.io",
	}

	assert.Equal(t, expectedCo, d)
}

func TestAddFlags(t *testing.T) {
	fs := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	d := NewContainerdOptions()
	d.AddFlags(fs)

	args := []string{
		"--containerd-data-root-dir=/var/lib/containerd",
		"--containerd-namespace=dsc",
	}

	fs.Parse(args)

	expectedCo := &ContainerdOptions{
		DataRootDirectory: "/var/lib/containerd",
		Namespace:         "dsc",
	}

	assert.Equal(t, expectedCo, d)
}
