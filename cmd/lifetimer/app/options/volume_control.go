// Copyright (c) 2022 Fujitsu Limited

package options

import (
	"github.com/spf13/pflag"

	volumectloptions "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/volume-controller/app/options"
)

type VolumeControllerOptions struct {
	Endpoint            string
	DisableUsageControl bool
}

func newVolumeControllerOptions() *VolumeControllerOptions {
	return &VolumeControllerOptions{
		Endpoint: volumectloptions.DefaultVolumeControlServerEndpoint,
	}
}

func (vco *VolumeControllerOptions) addFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&vco.Endpoint, "volume-controller-endpoint", vco.Endpoint,
		"Endpoint of the volume control GRPC server.")
	flagSet.BoolVar(
		&vco.DisableUsageControl, "disable-usage-control",
		vco.DisableUsageControl,
		"Disable the usage control for file system data.")
}
