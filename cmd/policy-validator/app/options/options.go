// Copyright (c) 2022 Fujitsu Limited

package options

import (
	"github.com/spf13/pflag"

	datastoreconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore/config"
)

const (
	defaultEndpoint = ":8443"
	defaultPath     = "/lcm/policy"
)

type PolicyValidatorOptions struct {
	Endpoint string
	Path     string

	TlsConfig *WebhookTlsOptions

	DataStore *datastoreconfig.DataStoreOptions

	KubernetesConfig *KubernetesOptions
}

func NewPolicyValidatorOptions() *PolicyValidatorOptions {
	return &PolicyValidatorOptions{
		Endpoint: defaultEndpoint,
		Path:     defaultPath,

		TlsConfig: newWebhookTlsOptions(),

		DataStore: datastoreconfig.NewDataStoreOptions(),

		KubernetesConfig: newKubernetesOptions(),
	}
}

func (pvo *PolicyValidatorOptions) AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&pvo.Endpoint, "endpoint", pvo.Endpoint,
		"Endpoint of the policy validator.  "+
			"The endpoint format is URL or k8s 'service' name.")
	flagSet.StringVar(
		&pvo.Path, "api-path", pvo.Path,
		"API path for the policy validator.")

	pvo.TlsConfig.addFlags(flagSet)

	pvo.DataStore.AddFlags(flagSet)

	pvo.KubernetesConfig.AddFlags(flagSet)
}
