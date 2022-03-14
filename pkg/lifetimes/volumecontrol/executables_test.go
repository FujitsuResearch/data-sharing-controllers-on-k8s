// Copyright (c) 2022 Fujitsu Limited

package volumecontrol

import (
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	volumecontrolapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"

	"github.com/stretchr/testify/assert"
)

func TestCreateLifetimeExecutables(t *testing.T) {
	allowedExecutables := []*lifetimesapi.AllowedExecutable{
		{
			CmdAbsolutePath: "/usr/bin/x",
			Checksum:        "1234abcd",
			Writable:        false,
		},
		{
			CmdAbsolutePath: "/usr/local/bin/y",
			Checksum:        "ef567890",
			Writable:        true,
		},
		{
			CmdAbsolutePath: "/usr/local/bin/s",
			Scripts: []*lifetimesapi.AllowedScript{
				{
					AbsolutePath:              "/usr/local/bin/test.sh",
					Checksum:                  "01234567",
					RelativePathToInitWorkDir: "/tmp/",
				},
			},
		},
		{
			CmdAbsolutePath: "/usr/local/bin/z",
		},
	}

	executables, err := CreateGrpcAllowedExecutables(allowedExecutables)
	assert.NoError(t, err)

	expectedExecutables := map[string]*volumecontrolapi.Executable{
		"/usr/bin/x": {
			Checksum: []byte{
				0x12, 0x34, 0xab, 0xcd,
			},
			Writable: false,
			Scripts:  map[string]*volumecontrolapi.Script{},
		},
		"/usr/local/bin/y": {
			Checksum: []byte{
				0xef, 0x56, 0x78, 0x90,
			},
			Writable: true,
			Scripts:  map[string]*volumecontrolapi.Script{},
		},
		"/usr/local/bin/s": {
			Checksum: []byte{},
			Scripts: map[string]*volumecontrolapi.Script{
				"/usr/local/bin/test.sh": {
					Checksum: []byte{
						0x01, 0x23, 0x45, 0x67,
					},
					Writable:                  false,
					RelativePathToInitWorkDir: "/tmp/",
				},
			},
		},
		"/usr/local/bin/z": {
			Checksum: nil,
			Writable: false,
		},
	}
	assert.Equal(t, expectedExecutables, executables)
}

func TestAddAndDeleteExecutables(t *testing.T) {
	oldKey := "namespace1/pod1"

	namespace, _, err := cache.SplitMetaNamespaceKey(oldKey)
	assert.NoError(t, err)

	consumerPvcName1 := "consumer-pvc-1"
	consumerPvcName2 := "consumer-pvc-2"
	outputPvcName1 := "output-pvc-1"
	outputPvcName2 := "output-pvc-2"
	oldLifetimeSpec := &lifetimesapi.LifetimeSpec{
		InputData: []lifetimesapi.InputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
					ToPersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      consumerPvcName1,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/bin/x",
							Checksum:        "12345678",
							Writable:        false,
						},
						{
							CmdAbsolutePath: "/usr/local/bin/y",
							Checksum:        "abcdef09",
							Writable:        false,
						},
						{
							CmdAbsolutePath: "/usr/local/sbin/a",
							Checksum:        "654321ab",
							Writable:        true,
						},
					},
				},
			},
			{
				FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
					ToPersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      consumerPvcName2,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/local/sbin/a",
							Checksum:        "abcdef",
							Writable:        true,
						},
					},
				},
			},
		},
		OutputData: []lifetimesapi.OutputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
					PersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      outputPvcName1,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/local/sbin/b",
							Checksum:        "fedcba",
							Writable:        true,
						},
					},
				},
			},
			{
				FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
					PersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      outputPvcName2,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/local/sbin/c",
							Checksum:        "987654",
							Writable:        true,
						},
					},
				},
			},
		},
	}

	newLifetimeSpec := &lifetimesapi.LifetimeSpec{
		InputData: []lifetimesapi.InputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
					ToPersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      consumerPvcName1,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/bin/x",
							Checksum:        "12345678",
							Writable:        true,
						},
						{
							CmdAbsolutePath: "/usr/local/bin/y",
							Checksum:        "90abcdef",
							Writable:        false,
						},
						{
							CmdAbsolutePath: "/usr/sbin/z",
							Checksum:        "12abcd",
							Writable:        false,
						},
					},
				},
			},
			{
				FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
					ToPersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      consumerPvcName2,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/local/sbin/a",
							Checksum:        "abcdef",
							Writable:        false,
						},
					},
				},
			},
		},
		OutputData: []lifetimesapi.OutputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
					PersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      outputPvcName1,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/local/bin/a",
							Checksum:        "3210fe",
							Writable:        true,
							Scripts: []*lifetimesapi.AllowedScript{
								{
									AbsolutePath:              "/usr/local/bin/test.sh",
									Checksum:                  "01234567",
									RelativePathToInitWorkDir: "/tmp/",
								},
							},
						},
					},
				},
			},
			{
				FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
					PersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      outputPvcName2,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/local/sbin/c",
							Checksum:        "987654",
							Writable:        true,
						},
					},
				},
			},
		},
	}

	executablesDiffs, err := CreateGrpcExecutablesDiffs(
		oldLifetimeSpec, newLifetimeSpec)
	assert.NoError(t, err)

	inputPvcKey1 := util.ConcatenateNamespaceAndName(
		namespace, consumerPvcName1)
	inputPvcKey2 := util.ConcatenateNamespaceAndName(
		namespace, consumerPvcName2)
	outputPvcKey := util.ConcatenateNamespaceAndName(namespace, outputPvcName1)
	expectedExecutablesDiffs := map[string]*volumecontrolapi.ExecutablesDiffs{
		inputPvcKey1: {
			AddedAndUpdated: map[string]*volumecontrolapi.Executable{
				"/usr/bin/x": {
					Checksum: []byte{0x12, 0x34, 0x56, 0x78},
					Writable: true,
					Scripts:  map[string]*volumecontrolapi.Script{},
				},
				"/usr/local/bin/y": {
					Checksum: []byte{0x90, 0xab, 0xcd, 0xef},
					Writable: false,
					Scripts:  map[string]*volumecontrolapi.Script{},
				},
				"/usr/sbin/z": {
					Checksum: []byte{0x12, 0xab, 0xcd},
					Writable: false,
					Scripts:  map[string]*volumecontrolapi.Script{},
				},
			},
			DeletedPaths: []string{
				"/usr/local/sbin/a",
			},
		},
		inputPvcKey2: {
			AddedAndUpdated: map[string]*volumecontrolapi.Executable{
				"/usr/local/sbin/a": {
					Checksum: []byte{0xab, 0xcd, 0xef},
					Writable: false,
					Scripts:  map[string]*volumecontrolapi.Script{},
				},
			},
			DeletedPaths: []string{},
		},
		outputPvcKey: {
			AddedAndUpdated: map[string]*volumecontrolapi.Executable{
				"/usr/local/bin/a": {
					Checksum: []byte{0x32, 0x10, 0xfe},
					Writable: true,
					Scripts: map[string]*volumecontrolapi.Script{
						"/usr/local/bin/test.sh": {
							Checksum: []byte{
								0x01, 0x23, 0x45, 0x67,
							},
							Writable:                  false,
							RelativePathToInitWorkDir: "/tmp/",
						},
					},
				},
			},
			DeletedPaths: []string{
				"/usr/local/sbin/b",
			},
		},
	}

	assert.Equal(t, expectedExecutablesDiffs, executablesDiffs)
}

func TestAddExecutables(t *testing.T) {
	oldKey := "namespace1/pod1"

	namespace, _, err := cache.SplitMetaNamespaceKey(oldKey)
	assert.NoError(t, err)

	consumerPvcName := "consumer-pvc-1"
	outputPvcName := "output-pvc-1"
	oldLifetimeSpec := &lifetimesapi.LifetimeSpec{
		InputData: []lifetimesapi.InputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
					ToPersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      consumerPvcName,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/bin/x",
							Checksum:        "12345678",
							Writable:        false,
						},
					},
				},
			},
		},
		OutputData: []lifetimesapi.OutputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
					PersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      outputPvcName,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/local/bin/y",
							Checksum:        "abcdef09",
							Writable:        false,
						},
					},
				},
			},
		},
	}

	newLifetimeSpec := &lifetimesapi.LifetimeSpec{
		InputData: []lifetimesapi.InputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
					ToPersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      consumerPvcName,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/bin/x",
							Checksum:        "12345678",
							Writable:        true,
						},
						{
							CmdAbsolutePath: "/usr/sbin/z",
							Checksum:        "12abcd",
							Writable:        false,
						},
						{
							CmdAbsolutePath: "/usr/bin/a",
						},
					},
				},
			},
		},
		OutputData: []lifetimesapi.OutputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
					PersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      outputPvcName,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/local/bin/y",
							Checksum:        "90abcdef",
							Writable:        false,
						},
						{
							CmdAbsolutePath: "/usr/local/bin/s",
							Scripts: []*lifetimesapi.AllowedScript{
								{
									AbsolutePath:              "/usr/local/bin/test.sh",
									Checksum:                  "01234567",
									RelativePathToInitWorkDir: "/tmp/",
								},
							},
						},
					},
				},
			},
		},
	}

	executablesDiffs, err := CreateGrpcExecutablesDiffs(
		oldLifetimeSpec, newLifetimeSpec)
	assert.NoError(t, err)

	inputPvcKey := util.ConcatenateNamespaceAndName(namespace, consumerPvcName)
	outputPvcKey := util.ConcatenateNamespaceAndName(namespace, outputPvcName)
	expectedExecutablesDiffs := map[string]*volumecontrolapi.ExecutablesDiffs{
		inputPvcKey: {
			AddedAndUpdated: map[string]*volumecontrolapi.Executable{
				"/usr/bin/x": {
					Checksum: []byte{0x12, 0x34, 0x56, 0x78},
					Writable: true,
					Scripts:  map[string]*volumecontrolapi.Script{},
				},
				"/usr/sbin/z": {
					Checksum: []byte{0x12, 0xab, 0xcd},
					Writable: false,
					Scripts:  map[string]*volumecontrolapi.Script{},
				},
				"/usr/bin/a": {
					Checksum: nil,
					Writable: false,
				},
			},
			DeletedPaths: []string{},
		},
		outputPvcKey: {
			AddedAndUpdated: map[string]*volumecontrolapi.Executable{
				"/usr/local/bin/y": {
					Checksum: []byte{0x90, 0xab, 0xcd, 0xef},
					Writable: false,
					Scripts:  map[string]*volumecontrolapi.Script{},
				},
				"/usr/local/bin/s": {
					Checksum: []byte{},
					Scripts: map[string]*volumecontrolapi.Script{
						"/usr/local/bin/test.sh": {
							Checksum: []byte{
								0x01, 0x23, 0x45, 0x67,
							},
							Writable:                  false,
							RelativePathToInitWorkDir: "/tmp/",
						},
					},
				},
			},
			DeletedPaths: []string{},
		},
	}

	assert.Equal(t, expectedExecutablesDiffs, executablesDiffs)
}

func TestDeleteExecutables(t *testing.T) {
	oldKey := "namespace1/pod1"

	namespace, _, err := cache.SplitMetaNamespaceKey(oldKey)
	assert.NoError(t, err)

	consumerPvcName := "consumer-pvc-1"
	outputPvcName := "output-pvc-1"
	oldLifetimeSpec := &lifetimesapi.LifetimeSpec{
		InputData: []lifetimesapi.InputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
					ToPersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      consumerPvcName,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/bin/x",
							Checksum:        "12345678",
							Writable:        false,
						},
						{
							CmdAbsolutePath: "/usr/local/sbin/a",
							Checksum:        "654321ab",
							Writable:        true,
						},
						{
							CmdAbsolutePath: "/usr/bin/b",
						},
					},
				},
			},
		},
		OutputData: []lifetimesapi.OutputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
					PersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      outputPvcName,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/bin/c",
						},
					},
				},
			},
		},
	}

	newLifetimeSpec := &lifetimesapi.LifetimeSpec{
		InputData: []lifetimesapi.InputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
					ToPersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      consumerPvcName,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/bin/x",
							Checksum:        "12345678",
							Writable:        false,
						},
					},
				},
			},
		},
		OutputData: []lifetimesapi.OutputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
					PersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      outputPvcName,
					},
				},
			},
		},
	}

	executablesDiffs, err := CreateGrpcExecutablesDiffs(
		oldLifetimeSpec, newLifetimeSpec)
	assert.NoError(t, err)

	inputPvcKey := util.ConcatenateNamespaceAndName(namespace, consumerPvcName)
	outputPvcKey := util.ConcatenateNamespaceAndName(namespace, outputPvcName)
	expectedExecutablesDiffs := map[string]*volumecontrolapi.ExecutablesDiffs{
		inputPvcKey: {
			AddedAndUpdated: map[string]*volumecontrolapi.Executable{},
			DeletedPaths: []string{
				"/usr/bin/b",
				"/usr/local/sbin/a",
			},
		},
		outputPvcKey: {
			AddedAndUpdated: map[string]*volumecontrolapi.Executable{},
			DeletedPaths: []string{
				"/usr/bin/c",
			},
		},
	}

	sort.Strings(executablesDiffs[inputPvcKey].DeletedPaths)
	assert.Equal(t, expectedExecutablesDiffs, executablesDiffs)
}

func TestExecutablesNotChanged(t *testing.T) {
	oldKey := "namespace1/pod1"

	namespace, _, err := cache.SplitMetaNamespaceKey(oldKey)
	assert.NoError(t, err)

	consumerPvcName := "consumer-pvc-1"
	outputPvcName := "output-pvc-1"
	oldLifetimeSpec := &lifetimesapi.LifetimeSpec{
		InputData: []lifetimesapi.InputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
					ToPersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      consumerPvcName,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/bin/x",
							Checksum:        "12345678",
							Writable:        false,
						},
						{
							CmdAbsolutePath: "/usr/sbin/a",
							Scripts: []*lifetimesapi.AllowedScript{
								{
									AbsolutePath:              "/usr/local/bin/test.sh",
									Checksum:                  "01234567",
									RelativePathToInitWorkDir: "/tmp/",
								},
							},
						},
					},
				},
			},
		},
		OutputData: []lifetimesapi.OutputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
					PersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      outputPvcName,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/bin/x",
							Checksum:        "12345678",
							Writable:        true,
						},
					},
				},
			},
		},
	}

	newLifetimeSpec := &lifetimesapi.LifetimeSpec{
		InputData: []lifetimesapi.InputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
					ToPersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      consumerPvcName,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/bin/x",
							Checksum:        "12345678",
							Writable:        false,
						},
						{
							CmdAbsolutePath: "/usr/sbin/a",
							Scripts: []*lifetimesapi.AllowedScript{
								{
									AbsolutePath:              "/usr/local/bin/test.sh",
									Checksum:                  "01234567",
									RelativePathToInitWorkDir: "/tmp/",
								},
							},
						},
					},
				},
			},
		},
		OutputData: []lifetimesapi.OutputDataSpec{
			{
				FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
					PersistentVolumeClaimRef: &corev1.ObjectReference{
						Namespace: namespace,
						Name:      outputPvcName,
					},
					AllowedExecutables: []*lifetimesapi.AllowedExecutable{
						{
							CmdAbsolutePath: "/usr/bin/x",
							Checksum:        "12345678",
							Writable:        true,
						},
					},
				},
			},
		},
	}

	executablesDiffs, err := CreateGrpcExecutablesDiffs(
		oldLifetimeSpec, newLifetimeSpec)
	assert.NoError(t, err)

	assert.Nil(t, executablesDiffs)
}
