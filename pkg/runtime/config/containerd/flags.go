// Copyright (c) 2022 Fujitsu Limited

package containerd

import (
	"fmt"

	containerddefaults "github.com/containerd/containerd/defaults"
	containerdconstants "github.com/containerd/containerd/pkg/cri/constants"
	"github.com/spf13/pflag"
)

const (
	k8sContainerdNamespace = containerdconstants.K8sContainerdNamespace
)

type ContainerdOptions struct {
	DataRootDirectory string
	Namespace         string
}

func NewContainerdOptions() *ContainerdOptions {
	return &ContainerdOptions{
		DataRootDirectory: containerddefaults.DefaultStateDir,
		Namespace:         k8sContainerdNamespace,
	}
}

func (co *ContainerdOptions) AddFlags(flagSet *pflag.FlagSet) {
	containerdOnlyWarning := "This containerd-specific flag only works " +
		"when container-runtime is set to 'containerd'."

	flagSet.StringVar(
		&co.DataRootDirectory, "containerd-data-root-dir",
		co.DataRootDirectory,
		fmt.Sprintf(
			"Root directry for persistent containerd states, "+
				"e.g. '/run/containerd'. %s", containerdOnlyWarning))

	flagSet.StringVar(
		&co.Namespace, "containerd-namespace", co.Namespace,
		fmt.Sprintf("Namespace in containerd. %s", containerdOnlyWarning))
}
