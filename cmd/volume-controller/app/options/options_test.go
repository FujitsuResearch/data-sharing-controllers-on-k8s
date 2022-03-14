// Copyright (c) 2022 Fujitsu Limited

package options

import (
	"testing"

	containerddefaults "github.com/containerd/containerd/defaults"
	containerdconstants "github.com/containerd/containerd/pkg/cri/constants"
	"github.com/spf13/pflag"

	publishconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	runtimeconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/containerd"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/docker"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config/kubelet"
	fuseconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/fuse"
	volumeconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/plugins"

	"github.com/stretchr/testify/assert"
)

func TestDefaultFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	volumeControlOptions := NewVolumeControllerOptions()
	volumeControlOptions.AddFlags(flagSet)

	err := flagSet.Parse(nil)
	assert.NoError(t, err)

	expectedVolumeControlOptions := &VolumeControllerOptions{
		GrpcServerEndpoint: DefaultVolumeControlServerEndpoint,
		FuseConfig: &fuseconfig.FuseOptions{
			MountPointsHostRootDirectory: "/mnt/fuse/targets",
			Debug:                        false,
		},
		VolumeConfig: &volumeconfig.VolumeOptions{
			CsiPluginsDir:           "/var/lib/kubelet/plugins",
			LocalFuseSourcesHostDir: "/mnt/fuse/sources/local",
		},
		ContainerRuntimeConfig: &runtimeconfig.ContainerRuntimeOptions{
			Name: "docker",
			Kubelet: &kubelet.KubeletOptions{
				RootDirectory:  "/var/lib/kubelet",
				PodsDirName:    "pods",
				VolumesDirName: "volumes",
			},
			Docker: &docker.DockerOptions{
				DataRootDirectory: "/var/lib/docker",
			},
			Containerd: &containerd.ContainerdOptions{
				DataRootDirectory: containerddefaults.DefaultStateDir,
				Namespace:         containerdconstants.K8sContainerdNamespace,
			},
		},
		MessageQueuePublisher: &publishconfig.MessageQueuePublisherOptions{
			CompressionCodec: publishconfig.CompressionCodecNone,
		},
	}

	assert.Equal(t, expectedVolumeControlOptions, volumeControlOptions)
}

func TestAddFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	volumeControlOptions := NewVolumeControllerOptions()
	volumeControlOptions.AddFlags(flagSet)

	args := []string{
		"--consumer-kubeconfig=/tmp/kubeconfig",
		"--consumer-api-server-url=http://127.0.0.1",
		"--grpc-server-endpoint=tcp://:50051",
		"--fuse-mount-points-host-root-dir=/mnt/fuse",
		"--fuse-debug",

		"--csi-plugins-dir=/tmp/plugins",
		"--local-fuse-sources-host-dir=/mnt/fuse/sources",

		"--container-runtime=docker",
		"--runtime-endpoint=unix:///docker.sock",

		"--kubelet-root-dir=/tmp/kubelet",
		"--kubelet-pods-dir-name=pod",
		"--kubelet-volumes-dir-name=vols",

		"--docker-data-root-dir=/var/lib/docker/data",
		"--docker-api-version=1.0",

		"--containerd-data-root-dir=/var/run/containerd",
		"--containerd-namespace=containerd.ns",

		"--mq-publisher-compression-codec=lz4",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	expectedVolumeControlOptions := &VolumeControllerOptions{
		ConsumerKubeConfig:   "/tmp/kubeconfig",
		ConsumerApiServerUrl: "http://127.0.0.1",
		GrpcServerEndpoint:   "tcp://:50051",
		FuseConfig: &fuseconfig.FuseOptions{
			MountPointsHostRootDirectory: "/mnt/fuse",
			Debug:                        true,
		},
		VolumeConfig: &volumeconfig.VolumeOptions{
			CsiPluginsDir:           "/tmp/plugins",
			LocalFuseSourcesHostDir: "/mnt/fuse/sources",
		},
		ContainerRuntimeConfig: &runtimeconfig.ContainerRuntimeOptions{
			Name:     "docker",
			Endpoint: "unix:///docker.sock",
			Kubelet: &kubelet.KubeletOptions{
				RootDirectory:  "/tmp/kubelet",
				PodsDirName:    "pod",
				VolumesDirName: "vols",
			},
			Docker: &docker.DockerOptions{
				DataRootDirectory: "/var/lib/docker/data",
				ApiVersion:        "1.0",
			},
			Containerd: &containerd.ContainerdOptions{
				DataRootDirectory: "/var/run/containerd",
				Namespace:         "containerd.ns",
			},
		},
		MessageQueuePublisher: &publishconfig.MessageQueuePublisherOptions{
			CompressionCodec: publishconfig.CompressionCodecLz4,
		},
	}

	assert.Equal(t, expectedVolumeControlOptions, volumeControlOptions)
}

func TestAddInvalidFlags(t *testing.T) {
	flagSet := pflag.NewFlagSet("addflagstest", pflag.ContinueOnError)

	volumeControlOptions := NewVolumeControllerOptions()
	volumeControlOptions.AddFlags(flagSet)

	args := []string{
		"--grpc-server-endpoint=udp://:50051",
	}

	err := flagSet.Parse(args)
	assert.NoError(t, err)

	err = ValidateVolumeControllerFlags(volumeControlOptions)
	assert.EqualError(t, err, "Not suppor such a network type: udp")
}
