// Copyright (c) 2022 Fujitsu Limited

package datastore

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

const (
	fileSystemPrefix = "fs"
	rdbPrefix        = "rdb"
	keyConcatenater  = "#"
)

func getFileSystemDataLifetimeKey(
	persistentVolumeClaimRef *corev1.ObjectReference) string {
	namespaceAndName := util.ConcatenateNamespaceAndName(
		persistentVolumeClaimRef.Namespace, persistentVolumeClaimRef.Name)
	return fmt.Sprintf(
		"%s%s%s%s",
		DataLifetimePrefix, fileSystemPrefix, keyConcatenater,
		namespaceAndName)
}

// [TODO]
func getRdbDataLifetimeKey(dbName string) string {
	return fmt.Sprintf(
		"%s%s%s%s", DataLifetimePrefix, rdbPrefix, keyConcatenater, dbName)
}

func GetDataLifetimeKeyFromInputData(
	predecessor lifetimesapi.InputDataSpec) (string, error) {
	switch {
	case predecessor.FileSystemSpec != nil:
		pvcRef := predecessor.FileSystemSpec.FromPersistentVolumeClaimRef

		return getFileSystemDataLifetimeKey(pvcRef), nil
	case predecessor.RdbSpec != nil:
		return getRdbDataLifetimeKey(predecessor.RdbSpec.SourceDb), nil
	default:
		return "", fmt.Errorf(
			"Not find one of the storage validator [filesystem, rdb]")
	}
}

func GetDataLifetimeKeyFromOutputData(
	successor lifetimesapi.OutputDataSpec) (
	string, error) {
	switch {
	case successor.FileSystemSpec != nil:
		return getFileSystemDataLifetimeKey(
			successor.FileSystemSpec.PersistentVolumeClaimRef), nil
	case successor.RdbSpec != nil:
		return getRdbDataLifetimeKey(successor.RdbSpec.DestinationDb), nil
	default:
		return "", fmt.Errorf(
			"Not find one of the storage validator [filesystem, rdb]")
	}
}
