// Copyright (c) 2022 Fujitsu Limited

package util

import (
	"context"
	"time"
)

const (
	DefaultRuntimeClientTimeout = 2*time.Minute - 1*time.Second
	DefaultKubeClientTimeout    = 2 * time.Minute

	DefaultDataStoreClientTimeout = 1 * time.Minute

	DefaultMessageQueueTimeout = 1 * time.Minute
)

func GetTimeoutContext(
	timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}
