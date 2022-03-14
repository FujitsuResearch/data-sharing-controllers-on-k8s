// Copyright (c) 2022 Fujitsu Limited

package options

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	publishconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	runtimeconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config"
	fuseconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/fuse"
	volumeconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/plugins"
)

const (
	DefaultVolumeControlServerEndpoint = "unix:///var/run/dsc/volume-controller.sock"
	GrpcEndpointSeparator              = "://"
)

type VolumeControllerOptions struct {
	ConsumerKubeConfig   string
	ConsumerApiServerUrl string

	GrpcServerEndpoint string

	FuseConfig   *fuseconfig.FuseOptions
	VolumeConfig *volumeconfig.VolumeOptions

	ContainerRuntimeConfig *runtimeconfig.ContainerRuntimeOptions

	MessageQueuePublisher *publishconfig.MessageQueuePublisherOptions
}

func NewVolumeControllerOptions() *VolumeControllerOptions {
	return &VolumeControllerOptions{
		GrpcServerEndpoint:     DefaultVolumeControlServerEndpoint,
		FuseConfig:             fuseconfig.NewFuseOptions(),
		VolumeConfig:           volumeconfig.NewVolumeOptions(),
		ContainerRuntimeConfig: runtimeconfig.NewContainerRuntimeOptions(),
		MessageQueuePublisher:  publishconfig.NewMessageQueuePublisherOptions(),
	}
}

func (vco *VolumeControllerOptions) AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&vco.ConsumerKubeConfig, "consumer-kubeconfig", vco.ConsumerKubeConfig,
		"Path to the kubeconfig file with authorization and master location "+
			"information.")
	flagSet.StringVar(
		&vco.ConsumerApiServerUrl, "consumer-api-server-url",
		vco.ConsumerApiServerUrl,
		"URL of the Kubernetes API server for the data consumer.")

	flagSet.StringVar(
		&vco.GrpcServerEndpoint, "grpc-server-endpoint",
		vco.GrpcServerEndpoint,
		"Endpoint for the GRPC server specified by '[network]://[address]'.  "+
			"'network' must be 'tcp', 'tcp4', 'tcp6', 'unix', or 'unixpacket'")

	vco.FuseConfig.AddFlags(flagSet)
	vco.VolumeConfig.AddFlags(flagSet)
	vco.ContainerRuntimeConfig.AddFlags(flagSet)
	vco.MessageQueuePublisher.AddFlags(flagSet)
}

func ValidateVolumeControllerFlags(vco *VolumeControllerOptions) error {
	endpoint := strings.SplitN(
		vco.GrpcServerEndpoint, GrpcEndpointSeparator, 2)
	switch endpoint[0] {
	case "tcp":
		fallthrough
	case "tcp4":
		fallthrough
	case "tcp6":
		fallthrough
	case "unix":
		fallthrough
	case "unixpacket":
		return nil

	default:
		return fmt.Errorf("Not suppor such a network type: %s", endpoint[0])
	}
}
