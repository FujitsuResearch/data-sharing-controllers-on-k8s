// Copyright (c) 2022 Fujitsu Limited

package options

import (
	"github.com/spf13/pflag"
)

const (
	defaultTlsDirectory        = "/var/run/secrets/tls"
	defaultCertificateFileName = "tls.crt"
	defaultKeyFileName         = "tls.key"
)

type WebhookTlsOptions struct {
	TlsDirectory        string
	CertificateFileName string
	KeyFileName         string
}

func newWebhookTlsOptions() *WebhookTlsOptions {
	return &WebhookTlsOptions{
		TlsDirectory:        defaultTlsDirectory,
		CertificateFileName: defaultCertificateFileName,
		KeyFileName:         defaultKeyFileName,
	}
}

func (wto *WebhookTlsOptions) addFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&wto.TlsDirectory, "webhook-tls-dir", wto.TlsDirectory,
		"Directory to store a TLS certificate and the key")
	flagSet.StringVar(
		&wto.CertificateFileName, "webhook-tls-cert-file",
		wto.CertificateFileName,
		"Name of a TLS certificate file")
	flagSet.StringVar(
		&wto.KeyFileName, "webhook-tls-key-file", wto.KeyFileName,
		"Name of a TLS private key")
}
