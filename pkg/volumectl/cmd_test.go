// Copyright (c) 2022 Fujitsu Limited

package volumectl

import (
	"testing"

	vctloptions "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/volume-controller/app/options"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFlags(t *testing.T) {
	cmd := NewVolumeCtlCommand()
	cmd.Flags().Parse(nil)

	endpoint := cmd.Flag("volume-controller-endpoint").Value.String()
	assert.Equal(t, vctloptions.DefaultVolumeControlServerEndpoint, endpoint)
}

func TestAddFlags(t *testing.T) {
	cmd := NewVolumeCtlCommand()
	cmd.Flags().Parse(nil)

	args := []string{
		"--volume-controller-endpoint=http://localhost",
	}
	cmd.PersistentFlags().Parse(args)

	endpoint := cmd.Flag("volume-controller-endpoint").Value.String()
	assert.Equal(t, "http://localhost", endpoint)
}
