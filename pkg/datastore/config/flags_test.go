// Copyright (c) 2022 Fujitsu Limited

package config

import (
	"testing"

	"github.com/spf13/pflag"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	dataStoreOptions := NewDataStoreOptions()
	dataStoreOptions.AddFlags(flagSet)

	err := flagSet.Parse(nil)
	assert.NoError(t, err)

	expectedDataStoreOptions := &DataStoreOptions{
		Name: defaultName,
		Addr: defaultAddr,
	}

	assert.Equal(t, expectedDataStoreOptions, dataStoreOptions)
}

func TestAddFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	dataStoreOptions := NewDataStoreOptions()
	dataStoreOptions.AddFlags(flagSet)

	args := []string{
		"--datastore-name=datastore",
		"--datastore-addr=localhost:7000",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	expectedDataStoreOptions := &DataStoreOptions{
		Name: "datastore",
		Addr: "localhost:7000",
	}

	assert.Equal(t, expectedDataStoreOptions, dataStoreOptions)
}
