// Copyright (c) 2022 Fujitsu Limited

package client

import (
	"fmt"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore/config"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore/redis"
)

func NewValidationDataStoreClient(
	dataStoreOptions *config.DataStoreOptions) (
	datastore.ValidationDataStoreOperations, error) {
	switch dataStoreOptions.Name {
	case config.DataStoreRedis:
		return redis.NewRedisValidationClient(dataStoreOptions.Addr)
	default:
		return nil, fmt.Errorf(
			"Not support such a data store name: %q", dataStoreOptions.Name)
	}
}
