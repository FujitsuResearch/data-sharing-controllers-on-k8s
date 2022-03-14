// Copyright (c) 2022 Fujitsu Limited

package datastore

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"

	"github.com/stretchr/testify/assert"
)

func TestGetFileSystemDataLifetimeKeyFromInputData(t *testing.T) {
	predecessor := lifetimesapi.InputDataSpec{
		FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
			FromPersistentVolumeClaimRef: &corev1.ObjectReference{
				Namespace: "ns1",
				Name:      "pvc1",
			},
		},
	}

	key, err := GetDataLifetimeKeyFromInputData(predecessor)
	assert.NoError(t, err)
	expectedKey := fmt.Sprintf(
		"%sfs%sns1/pvc1", DataLifetimePrefix, keyConcatenater)
	assert.Equal(t, expectedKey, key)
}

func TestNotGetDataLifetimeKeyFromInputData(t *testing.T) {
	predecessor := lifetimesapi.InputDataSpec{}

	_, err := GetDataLifetimeKeyFromInputData(predecessor)
	assert.Error(t, err)
	assert.EqualError(
		t, err, "Not find one of the storage validator [filesystem, rdb]")
}

func TestGetFileSystemDataLifetimeKeyFromOutputData(t *testing.T) {
	successor := lifetimesapi.OutputDataSpec{
		FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
			PersistentVolumeClaimRef: &corev1.ObjectReference{
				Namespace: "ns1",
				Name:      "pvc1",
			},
		},
	}

	key, err := GetDataLifetimeKeyFromOutputData(successor)
	assert.NoError(t, err)
	expectedKey := fmt.Sprintf(
		"%sfs%sns1/pvc1", DataLifetimePrefix, keyConcatenater)
	assert.Equal(t, expectedKey, key)
}

func TestNotGetDataLifetimeKeyFromOutputData(t *testing.T) {
	successor := lifetimesapi.OutputDataSpec{}

	_, err := GetDataLifetimeKeyFromOutputData(successor)
	assert.Error(t, err)
	assert.EqualError(
		t, err, "Not find one of the storage validator [filesystem, rdb]")
}
