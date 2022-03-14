// Copyright (c) 2022 Fujitsu Limited

package options

import (
	"github.com/spf13/pflag"
)

type KubernetesOptions struct {
	KubeConfig   string
	ApiServerUrl string
}

func newKubernetesOptions() *KubernetesOptions {
	return &KubernetesOptions{}
}

func (ko *KubernetesOptions) AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&ko.KubeConfig, "kubeconfig", ko.KubeConfig,
		"Path to the kubeconfig file with authorization and master location "+
			"information.")
	flagSet.StringVar(
		&ko.ApiServerUrl, "api-server-url", ko.ApiServerUrl,
		"URL of the Kubernetes API server.")
}
