// Copyright (c) 2022 Fujitsu Limited

package config

import (
	"fmt"

	"github.com/spf13/pflag"
)

const (
	DataStoreRedis = "redis"
	defaultName    = DataStoreRedis

	defaultAddr = ":6379"
)

type DataStoreOptions struct {
	Name string
	Addr string
}

func NewDataStoreOptions() *DataStoreOptions {
	return &DataStoreOptions{
		Name: defaultName,
		Addr: defaultAddr,
	}
}

func (dso *DataStoreOptions) AddFlags(flagSet *pflag.FlagSet) {
	flagSet.StringVar(
		&dso.Name, "datastore-name", dso.Name,
		"Name of data store including information on policy validation. "+
			"Possible values: 'redis'.")

	flagSet.StringVar(
		&dso.Addr, "datastore-addr", dso.Addr,
		fmt.Sprintf(
			"Data store address. The default value is %q", defaultAddr))
}
