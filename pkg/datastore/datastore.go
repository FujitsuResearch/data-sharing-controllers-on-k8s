// Copyright (c) 2022 Fujitsu Limited

package datastore

const (
	LifetimeCachePrefix = "ltc%"

	DataLifetimePrefix = "dlt-"
	CsiPrefix          = "csi-"
)

type LifetimeCache struct {
	EndTime                string              `json:"endTime"`
	SuccessorLifetimes     map[string]struct{} `json:"successorLifetimes,omitempty"`
	SuccessorPodTerminated bool                `json:"successorPodTerminated,omitempty"`
}

type DataStoreOperations interface {
	Close() error
}

type ValidationDataStoreOperations interface {
	DataStoreOperations

	GetDataLifetimeKeys() ([]string, error)
	GetDataLifetime(key string) (*LifetimeCache, error)

	AddDataLifetime(key string, endTime string) (*LifetimeCache, error)
	DeleteDataLifetime(key string) error

	AddSuccessorDataLifetimeCustomResource(key string, successor string) error
	DeleteSuccessorDataLifetimeCustomResource(key string, successor string) error
}
