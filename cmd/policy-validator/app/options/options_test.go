// Copyright (c) 2022 Fujitsu Limited

package options

import (
	"testing"

	"github.com/spf13/pflag"

	datastoreconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore/config"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	policyOptions := NewPolicyValidatorOptions()
	policyOptions.AddFlags(flagSet)

	err := flagSet.Parse(nil)
	assert.NoError(t, err)

	expectedPolicyOptions := &PolicyValidatorOptions{
		Endpoint: defaultEndpoint,
		Path:     defaultPath,

		TlsConfig: &WebhookTlsOptions{
			TlsDirectory:        defaultTlsDirectory,
			CertificateFileName: defaultCertificateFileName,
			KeyFileName:         defaultKeyFileName,
		},

		DataStore: &datastoreconfig.DataStoreOptions{
			Name: datastoreconfig.DataStoreRedis,
			Addr: ":6379",
		},

		KubernetesConfig: &KubernetesOptions{
			KubeConfig:   "",
			ApiServerUrl: "",
		},
	}

	assert.Equal(t, expectedPolicyOptions, policyOptions)
}

func TestAddFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	policyOptions := NewPolicyValidatorOptions()
	policyOptions.AddFlags(flagSet)

	args := []string{
		"--endpoint=service-name:8088",
		"--api-path=/dscpolicy",

		"--webhook-tls-dir=/var/run/tls",
		"--webhook-tls-cert-file=server.crt",
		"--webhook-tls-key-file=server.key",

		"--datastore-name=datastore",
		"--datastore-addr=localhost:7000",

		"--kubeconfig=/tmp/kubeconfig",
		"--api-server-url=http://127.0.0.1",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	expectedPolicyOptions := &PolicyValidatorOptions{
		Endpoint: "service-name:8088",
		Path:     "/dscpolicy",

		TlsConfig: &WebhookTlsOptions{
			TlsDirectory:        "/var/run/tls",
			CertificateFileName: "server.crt",
			KeyFileName:         "server.key",
		},

		DataStore: &datastoreconfig.DataStoreOptions{
			Name: "datastore",
			Addr: "localhost:7000",
		},

		KubernetesConfig: &KubernetesOptions{
			KubeConfig:   "/tmp/kubeconfig",
			ApiServerUrl: "http://127.0.0.1",
		},
	}

	assert.Equal(t, expectedPolicyOptions, policyOptions)
}
