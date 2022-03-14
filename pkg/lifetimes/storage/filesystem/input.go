// Copyright (c) 2022 Fujitsu Limited

package filesystem

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	volumecontrolapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/grpc/volumecontrol"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/storage"
	lifetimesvolumectl "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/volumecontrol"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue/subscribe"
)

var (
	_ = (storage.LifetimeInputOperations)((*FileSystemInputLifetimer)(nil))
)

type inputFileSpec struct {
	localPath string
}

type FileSystemInputLifetimer struct {
	fileSpec           *inputFileSpec
	allowedExecutables []*lifetimesapi.AllowedExecutable

	messageQueueInitSubscriber   *subscribe.VolumeMessageQueueInitSubscriber
	messageQueueUpdateSubscriber *subscribe.VolumeMessageQueueUpdateSubscriber

	volumeController *volumeController

	usageControlPvcKeyToMountPoint map[string]string

	lifetimeStatus *lifetimesapi.ConsumerStatus
}

func createInputFileSpec(
	fileSystemSpec *lifetimesapi.InputFileSystemSpec, pod *corev1.Pod,
	dataContainerName string) (*inputFileSpec, error) {
	err := hasPersistentVolumeClaim(
		fileSystemSpec.ToPersistentVolumeClaimRef.Name, pod)
	if err != nil {
		return nil, err
	}

	container, found := findDataCntainer(pod, dataContainerName)
	if !found {
		return nil, fmt.Errorf(
			"The pod does not have the data contaniner %q", dataContainerName)
	}

	volumeMountToIndex := createPersistentVolumeClaimToIndex(
		pod.Spec.Volumes, container)

	localPvcName := fileSystemSpec.ToPersistentVolumeClaimRef.Name
	localIndex, ok := volumeMountToIndex[localPvcName]
	if !ok {
		return nil, fmt.Errorf(
			"The data container %q does not have the local persistent "+
				"volume claim %q",
			dataContainerName, localPvcName)
	}

	return &inputFileSpec{
		localPath: container.VolumeMounts[localIndex].MountPath,
	}, nil
}

func NewFileSystemLifetimerForInputData(
	fileSystemSpec *lifetimesapi.InputFileSystemSpec, pod *corev1.Pod,
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	messageQueueSpec *lifetimesapi.MessageQueueSpec,
	lifetimeStatus *lifetimesapi.ConsumerStatus, disableUsageControl bool,
	volumeControllerEndpoint string, podNamespace string, podName string,
	dataContainerName string) (*FileSystemInputLifetimer, error) {
	inputFileSpec, err := createInputFileSpec(
		fileSystemSpec, pod, dataContainerName)
	if err != nil {
		return nil, err
	}

	initSubscriber, err := subscribe.NewVolumeMessageQueueInitSubscriber(
		fileSystemSpec, dataClient, kubeClient, messageQueueSpec,
		inputFileSpec.localPath)
	if err != nil {
		return nil, err
	}

	updateSubscriber, err := subscribe.NewVolumeMessageQueueUpdateSubscriber(
		fileSystemSpec, dataClient, kubeClient, messageQueueSpec,
		inputFileSpec.localPath)
	if err != nil {
		return nil, err
	}

	pvcKey := util.ConcatenateNamespaceAndName(
		fileSystemSpec.ToPersistentVolumeClaimRef.Namespace,
		fileSystemSpec.ToPersistentVolumeClaimRef.Name)

	lifetimer := &FileSystemInputLifetimer{
		fileSpec:                     inputFileSpec,
		allowedExecutables:           fileSystemSpec.AllowedExecutables,
		messageQueueInitSubscriber:   initSubscriber,
		messageQueueUpdateSubscriber: updateSubscriber,
		volumeController: &volumeController{
			disableUsageControl:      disableUsageControl,
			endpoint:                 volumeControllerEndpoint,
			podNamespace:             podNamespace,
			podName:                  podName,
			persistentVolumeClaimKey: pvcKey,
			dataContainerName:        dataContainerName,
		},
		usageControlPvcKeyToMountPoint: map[string]string{},
		lifetimeStatus:                 lifetimeStatus,
	}

	return lifetimer, nil
}

func (fsil *FileSystemInputLifetimer) Start(
	ctx context.Context) (time.Time, error) {
	if !fsil.volumeController.disableUsageControl {
		executables, err := lifetimesvolumectl.CreateGrpcAllowedExecutables(
			fsil.allowedExecutables)
		if err != nil {
			return time.Time{}, err
		}

		ucClient := volumecontrol.NewClient(fsil.volumeController.endpoint)
		klog.Infof(
			"Send 'start' request to the volume controller: %+v", executables)
		mountPointInfo, err := ucClient.Start(
			fsil.volumeController.podNamespace, fsil.volumeController.podName,
			fsil.volumeController.persistentVolumeClaimKey, executables,
			fsil.volumeController.dataContainerName,
			(*volumecontrolapi.MessageQueue)(nil))
		if err != nil {
			return time.Time{}, fmt.Errorf(
				"Failed the 'start' of the volume cntroller client: %v\n",
				err.Error())
		}

		if mountPointInfo.UsageControlEnabled {
			pvcKey := fsil.volumeController.persistentVolumeClaimKey
			fsil.usageControlPvcKeyToMountPoint[pvcKey] = mountPointInfo.MountPoint
		}

		klog.Infof("Start the volume controller: %+v", mountPointInfo)
	}

	klog.Info("Receiving initial data through the message queue subscriber ...")
	startTime, err := fsil.messageQueueInitSubscriber.HandleQueueMessages(ctx)
	if err != nil {
		return startTime, err
	}
	klog.Info(
		"... Has received initial data through the message queue subscriber")

	err = fsil.messageQueueInitSubscriber.CloseMessageQueues()
	if err != nil {
		return time.Time{}, err
	}

	return startTime, nil
}

func (fsil *FileSystemInputLifetimer) createMountPointToExecutablePaths() map[string]*volumecontrolapi.ExecutablePaths {
	pvcKey := fsil.volumeController.persistentVolumeClaimKey
	mountPoint := fsil.usageControlPvcKeyToMountPoint[pvcKey]

	executablePaths := make([]string, 0, len(fsil.allowedExecutables))
	for _, executable := range fsil.allowedExecutables {
		executablePaths = append(executablePaths, executable.CmdAbsolutePath)
	}

	return map[string]*volumecontrolapi.ExecutablePaths{
		mountPoint: &volumecontrolapi.ExecutablePaths{
			Paths: executablePaths,
		},
	}
}

func (fsil *FileSystemInputLifetimer) Stop() error {
	err := fsil.messageQueueUpdateSubscriber.CloseMessageQueue()
	if err != nil {
		return err
	}

	if fsil.volumeController.disableUsageControl {
		return nil
	}

	ucClient := volumecontrol.NewClient(fsil.volumeController.endpoint)

	mountPointToExecutablePaths := fsil.createMountPointToExecutablePaths()

	klog.Infof(
		"Send 'stop' request to the volume controller: %+v",
		mountPointToExecutablePaths)

	return ucClient.Stop(
		fsil.volumeController.podNamespace, fsil.volumeController.podName,
		mountPointToExecutablePaths, "")
}

func (fsil *FileSystemInputLifetimer) FirstUpdate(
	ctx context.Context, offsetTime time.Time) (bool, error) {
	return fsil.messageQueueUpdateSubscriber.HandleFirstQueueMessage(
		ctx, offsetTime)
}

func (fsil *FileSystemInputLifetimer) Update(
	ctx context.Context) (bool, error) {
	return fsil.messageQueueUpdateSubscriber.HandleQueueMessage(ctx)
}

func (fsil *FileSystemInputLifetimer) chmodFiles() error {
	errs := []string{}

	targetPath := fsil.fileSpec.localPath

	dcd := &deletedChmodData{
		directories: []string{},
		root:        targetPath,
	}

	err := filepath.Walk(targetPath, dcd.chmodWalkFunc)
	if err != nil {
		errs = append(errs, err.Error())
	}

	for i := len(dcd.directories) - 1; i >= 0; i-- {
		err := os.Chmod(dcd.directories[i], 0000)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(
			"[Failed to change the mode] %s", strings.Join(errs, ", "))
	}

	return nil
}

func (fsil *FileSystemInputLifetimer) removeFiles() error {
	errs := []string{}

	targetPath := fsil.fileSpec.localPath

	drd := &deletedRemoveData{
		root: targetPath,
	}

	err := filepath.Walk(targetPath, drd.removeFilesWalkFunc)
	if err != nil {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return fmt.Errorf(
			"[Failed to remove data] %s", strings.Join(errs, ", "))
	}

	return nil
}

func (fsil *FileSystemInputLifetimer) Delete(
	deletePolicy lifetimesapi.DeletePolicy) error {
	switch deletePolicy {
	case lifetimesapi.DefaultPolicy:
		fallthrough
	case lifetimesapi.ChmodPolicy:
		return fsil.chmodFiles()

	case lifetimesapi.RemovePolicy:
		return fsil.removeFiles()

	case lifetimesapi.NotifyPolicy:
		return fmt.Errorf(
			"Not implement the delete method for the '%v' policy",
			deletePolicy)

	default:
		return fmt.Errorf("Not support sutch a delte policy: %v", deletePolicy)
	}
}
