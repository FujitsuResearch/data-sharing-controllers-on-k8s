// Copyright (c) 2022 Fujitsu Limited

package volumecontrol

import (
	"fmt"
	"time"

	"google.golang.org/grpc"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

const (
	defaultTimeout = 2 * time.Minute
)

type VolumeControl struct {
	endpoint string
}

func NewClient(endpoint string) *VolumeControl {
	return &VolumeControl{
		endpoint: endpoint,
	}
}

func (vc *VolumeControl) Initialize(
	fuseSourcePath string, allowedExecutables []*api.ExternalAllowedExecutable,
	csiVolumeHandle string, localFuseMountsHostRootDirectory string,
	disableUsageControl bool) (string, error) {
	req := api.InitializeRequest{
		FuseSourcePath:             fuseSourcePath,
		ExternalAllowedExecutables: allowedExecutables,
		CsiVolumeHandle:            csiVolumeHandle,
		LocalFuseMountsHostRootDir: localFuseMountsHostRootDirectory,
		DisableUsageControl:        disableUsageControl,
	}

	conn, err := grpc.Dial(vc.endpoint, grpc.WithInsecure())
	if err != nil {
		return "", err
	}
	defer conn.Close()

	timeout := defaultTimeout
	ctx, cancel := util.GetTimeoutContext(timeout)
	defer cancel()

	vcClient := api.NewVolumeControlClient(conn)
	res, err := vcClient.Initialize(ctx, &req)
	if err != nil {
		return "", fmt.Errorf(
			"Failed to fuse-mount volumes: %v", err.Error())
	}

	return res.FuseMountPointDir, nil
}

func (vc *VolumeControl) Start(
	podNamespace string, podName string,
	pvcKey string, allowedExecutables map[string]*api.Executable,
	dataContainerName string, messageQueueConfig *api.MessageQueue) (
	*api.StartResponse, error) {
	req := api.StartRequest{
		PodNamespace:                podNamespace,
		PodName:                     podName,
		DataContainerName:           dataContainerName,
		PvcKey:                      pvcKey,
		AllowedExecutables:          allowedExecutables,
		MessageQueueUpdatePublisher: messageQueueConfig,
	}

	conn, err := grpc.Dial(vc.endpoint, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	timeout := defaultTimeout
	ctx, cancel := util.GetTimeoutContext(timeout)
	defer cancel()

	vcClient := api.NewVolumeControlClient(conn)
	mountPointInfo, err := vcClient.Start(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to set up information on target containers "+
				"and binaries: %v", err.Error())
	}

	return mountPointInfo, nil
}

func (vc *VolumeControl) UpdateAllowedExecutables(
	podNamespace string, podName string,
	executablesDiffs map[string]*api.ExecutablesDiffs) (
	map[string]string, error) {
	req := api.UpdateAllowedExecutablesRequest{
		PodNamespace:     podNamespace,
		PodName:          podName,
		ExecutablesDiffs: executablesDiffs,
	}

	conn, err := grpc.Dial(vc.endpoint, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	timeout := defaultTimeout
	ctx, cancel := util.GetTimeoutContext(timeout)
	defer cancel()

	vcClient := api.NewVolumeControlClient(conn)

	res, err := vcClient.UpdateAllowedExecutables(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to set up the allowed executables configuration on "+
				"target containers and binaries: %v", err.Error())
	}

	return res.PvcKeyToMountPoint, nil
}

func (vc *VolumeControl) UpdateMessageQueueConfigurations(
	podNamespace string, podName string,
	executablesDiffs map[string]*api.MessageQueue) (
	map[string]string, error) {
	req := api.UpdateMessageQueueConfigRequest{
		PodNamespace:      podNamespace,
		PodName:           podName,
		MessageQueueDiffs: executablesDiffs,
	}

	conn, err := grpc.Dial(vc.endpoint, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	timeout := defaultTimeout
	ctx, cancel := util.GetTimeoutContext(timeout)
	defer cancel()

	vcClient := api.NewVolumeControlClient(conn)

	res, err := vcClient.UpdateMessageQueueConfig(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to set up the message queue configuration on "+
				"target containers and binaries: %v", err.Error())
	}

	return res.PvcKeyToMountPoint, nil
}

func (vc *VolumeControl) Stop(
	podNamespace string, podName string,
	mountPointToExecutablePaths map[string]*api.ExecutablePaths,
	mountPoint string) error {
	req := api.StopRequest{
		PodNamespace:                podNamespace,
		PodName:                     podName,
		MountPointToExecutablePaths: mountPointToExecutablePaths,
		MountPoint:                  mountPoint,
	}

	conn, err := grpc.Dial(vc.endpoint, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	timeout := defaultTimeout
	ctx, cancel := util.GetTimeoutContext(timeout)
	defer cancel()

	vcClient := api.NewVolumeControlClient(conn)
	_, err = vcClient.Stop(ctx, &req)
	if err != nil {
		return fmt.Errorf(
			"Failed to clear information on target containers "+
				"and binaries: %v", err.Error())

	}

	return nil
}

func (vc *VolumeControl) Finalize(
	fuseMountPointDir string, force bool) error {
	req := api.FinalizeRequest{
		FuseMountPointDir: fuseMountPointDir,
		Force:             force,
	}

	conn, err := grpc.Dial(vc.endpoint, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	timeout := defaultTimeout
	ctx, cancel := util.GetTimeoutContext(timeout)
	defer cancel()

	vcClient := api.NewVolumeControlClient(conn)
	_, err = vcClient.Finalize(ctx, &req)
	if err != nil {
		return fmt.Errorf("Failed to fuse-unmount: %v", err.Error())
	}

	return nil
}
