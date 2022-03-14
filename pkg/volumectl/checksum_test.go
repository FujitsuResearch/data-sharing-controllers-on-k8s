// Copyright (c) 2022 Fujitsu Limited

package volumectl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChecksumDefaultFlags(t *testing.T) {
	cmd := NewVolumeCtlCommand()

	for _, subCmd := range cmd.Commands() {
		if subCmd.Name() != "calc-checksum" {
			continue
		}

		subCmd.ParseFlags(nil)

		filePath := subCmd.Flag("file-path").Value.String()
		assert.Equal(t, "", filePath)
	}
}

func TestChecksumAddFlags(t *testing.T) {
	cmd := NewVolumeCtlCommand()

	args := []string{
		"--file-path=/usr/bin/x",
	}

	for _, subCmd := range cmd.Commands() {
		if subCmd.Name() != "calc-checksum" {
			continue
		}

		subCmd.ParseFlags(args)

		filePath := subCmd.Flag("file-path").Value.String()
		assert.Equal(t, "/usr/bin/x", filePath)
	}
}
