// Copyright (c) 2022 Fujitsu Limited

package util

import (
	"github.com/spf13/pflag"
	"k8s.io/klog/v2"
)

func PrintFlags(flags *pflag.FlagSet) {
	flags.VisitAll(func(flag *pflag.Flag) {
		klog.V(1).Infof("FLAG: --%s=%q", flag.Name, flag.Value)
	})
}
