// Copyright (c) 2022 Fujitsu Limited

package filesystem

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	volumecontrolapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/grpc/volumecontrol"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/storage"
	lifetimesvolumectl "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/volumecontrol"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
	volumepub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue/publish"
)

var (
	_ = (storage.LifetimeOutputOperations)((*FileSystemOutputLifetimer)(nil))
)

type outputFileSpec struct {
	path string
}

type mountPointInfo struct {
	mountPoint                   string
	usageControlEnabled          bool
	messageQueuePublisherEnabled bool
}

type FileSystemOutputLifetimer struct {
	fileSpec           *outputFileSpec
	allowedExecutables []*lifetimesapi.AllowedExecutable

	messageQueueInitPublisher         *volumepub.VolumeMessageQueueInitPublisher
	messageQueueUpdatePublisherConfig *volumecontrolapi.MessageQueue

	volumeController *volumeController

	volumeControlPvcKeyToMountPoint map[string]*volumecontrolapi.StartResponse

	lifetimeStatus *lifetimesapi.ConsumerStatus
}

func createOutputFileSpec(
	fileSystemSpec *lifetimesapi.OutputFileSystemSpec,
	pod *corev1.Pod, dataContainerName string) (*outputFileSpec, error) {
	err := hasPersistentVolumeClaim(
		fileSystemSpec.PersistentVolumeClaimRef.Name, pod)
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

	outputPvcName := fileSystemSpec.PersistentVolumeClaimRef.Name
	index, ok := volumeMountToIndex[outputPvcName]
	if !ok {
		return nil, fmt.Errorf(
			"The data container %q does not have the output persistent "+
				"volume claim %q",
			dataContainerName, outputPvcName)
	}

	return &outputFileSpec{
		path: container.VolumeMounts[index].MountPath,
	}, nil
}

func NewFileSystemLifetimerForOutputData(
	fileSystemSpec *lifetimesapi.OutputFileSystemSpec, pod *corev1.Pod,
	dataClient coreclientset.Interface, kubeClient kubernetes.Interface,
	messageQueueSpec *lifetimesapi.PublishMessageQueueSpec,
	messageQueuePublisherOptions *publish.MessageQueuePublisherOptions,
	disableUsageControl bool, volumeControllerEndpoint string,
	podNamespace string, podName string, dataContainerName string) (
	*FileSystemOutputLifetimer, error) {
	outputFilespec, err := createOutputFileSpec(
		fileSystemSpec, pod, dataContainerName)
	if err != nil {
		return nil, err
	}

	pvcKey := util.ConcatenateNamespaceAndName(
		fileSystemSpec.PersistentVolumeClaimRef.Namespace,
		fileSystemSpec.PersistentVolumeClaimRef.Name)

	lifetimer := &FileSystemOutputLifetimer{
		fileSpec:           outputFilespec,
		allowedExecutables: fileSystemSpec.AllowedExecutables,
		volumeController: &volumeController{
			disableUsageControl:      disableUsageControl,
			endpoint:                 volumeControllerEndpoint,
			podNamespace:             podNamespace,
			podName:                  podName,
			persistentVolumeClaimKey: pvcKey,
			dataContainerName:        dataContainerName,
		},
		volumeControlPvcKeyToMountPoint: map[string]*volumecontrolapi.StartResponse{},
	}

	if messageQueueSpec != nil {
		initPublisher, err := volumepub.NewVolumeMessageQueueInitPublisher(
			fileSystemSpec, messageQueuePublisherOptions, dataClient, kubeClient,
			messageQueueSpec.MessageQueueSpec, outputFilespec.path)
		if err != nil {
			return nil, err
		}

		updatePublisherConfig, err := volumepub.
			CreateVolumeMessageQueueUpdatePublisherConfiguration(
				fileSystemSpec, dataClient, kubeClient, messageQueueSpec,
				messageQueuePublisherOptions)
		if err != nil {
			return nil, err
		}

		lifetimer.messageQueueInitPublisher = initPublisher
		lifetimer.
			messageQueueUpdatePublisherConfig = updatePublisherConfig
	}

	return lifetimer, nil
}

func (fsol *FileSystemOutputLifetimer) Start(ctx context.Context) error {
	executables := map[string]*volumecontrolapi.Executable(nil)
	if !fsol.volumeController.disableUsageControl {
		var err error
		executables, err = lifetimesvolumectl.CreateGrpcAllowedExecutables(
			fsol.allowedExecutables)
		if err != nil {
			return err
		}
	} else if fsol.messageQueueUpdatePublisherConfig == nil {
		return nil
	}

	ucClient := volumecontrol.NewClient(fsol.volumeController.endpoint)
	klog.Infof(
		"Send 'start' request to the volume controller: %+v", executables)
	mountPointInfo, err := ucClient.Start(
		fsol.volumeController.podNamespace, fsol.volumeController.podName,
		fsol.volumeController.persistentVolumeClaimKey,
		executables, fsol.volumeController.dataContainerName,
		fsol.messageQueueUpdatePublisherConfig)
	if err != nil {
		return fmt.Errorf(
			"Failed the 'start' of the volume cntroller client: %v\n",
			err.Error())
	}

	if mountPointInfo.UsageControlEnabled !=
		!fsol.volumeController.disableUsageControl {
		return fmt.Errorf(
			"The configuration of the Usage control (%t) does not match "+
				"the return value of the volume controller 'Start()' (%t)",
			!fsol.volumeController.disableUsageControl,
			mountPointInfo.UsageControlEnabled)
	}

	pvcKey := fsol.volumeController.persistentVolumeClaimKey
	fsol.volumeControlPvcKeyToMountPoint[pvcKey] = mountPointInfo

	if fsol.messageQueueInitPublisher != nil {
		go func(c context.Context) {
			klog.Info(
				"Start handling initial data through the message " +
					"queue publisher")

			err := fsol.messageQueueInitPublisher.HandleInitQueueMessages(c)
			if err != nil {
				klog.Errorf(
					"Failed handling about the init message queue: %v",
					err.Error())
			}

			err = fsol.messageQueueInitPublisher.CloseMessageQueues()
			if err != nil {
				klog.Errorf(
					"Failed to close the init message queue: %v", err.Error())
			}
		}(ctx)
	}

	klog.Infof("Start the volume controller: %+v", mountPointInfo)

	return nil
}

func (fsol *FileSystemOutputLifetimer) createMountPointToExecutablePaths() map[string]*volumecontrolapi.ExecutablePaths {
	pvcKey := fsol.volumeController.persistentVolumeClaimKey
	mountPoint := fsol.volumeControlPvcKeyToMountPoint[pvcKey]

	executablePaths := make([]string, 0, len(fsol.allowedExecutables))
	for _, executable := range fsol.allowedExecutables {
		executablePaths = append(executablePaths, executable.CmdAbsolutePath)
	}

	return map[string]*volumecontrolapi.ExecutablePaths{
		mountPoint.MountPoint: &volumecontrolapi.ExecutablePaths{
			Paths: executablePaths,
		},
	}
}

func (fsol *FileSystemOutputLifetimer) Stop() error {
	pvcKey := fsol.volumeController.persistentVolumeClaimKey
	mountPoint, found := fsol.volumeControlPvcKeyToMountPoint[pvcKey]
	if !found {
		return nil
	}

	mountPointToExecutablePaths := (map[string]*volumecontrolapi.
		ExecutablePaths)(nil)
	if mountPoint.UsageControlEnabled {
		mountPointToExecutablePaths = fsol.createMountPointToExecutablePaths()
	}

	stopMountPoint := ""
	if mountPoint.MessageQueuePublisherEnabled {
		stopMountPoint = mountPoint.MountPoint
	}

	ucClient := volumecontrol.NewClient(fsol.volumeController.endpoint)

	klog.Infof(
		"Send 'stop' request to the volume controller: %+v",
		mountPointToExecutablePaths)

	return ucClient.Stop(
		fsol.volumeController.podNamespace, fsol.volumeController.podName,
		mountPointToExecutablePaths, stopMountPoint)
}

func (fsol *FileSystemOutputLifetimer) chmodFiles() error {
	errs := []string{}

	targetPath := fsol.fileSpec.path

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

func (fsol *FileSystemOutputLifetimer) removeFiles() error {
	errs := []string{}

	targetPath := fsol.fileSpec.path

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

func (fsol *FileSystemOutputLifetimer) Delete(
	deletePolicy lifetimesapi.DeletePolicy) error {
	switch deletePolicy {
	case lifetimesapi.DefaultPolicy:
		fallthrough
	case lifetimesapi.ChmodPolicy:
		return fsol.chmodFiles()

	case lifetimesapi.RemovePolicy:
		return fsol.removeFiles()

	case lifetimesapi.NotifyPolicy:
		return fmt.Errorf(
			"Not implement the delete method for the '%v' policy",
			deletePolicy)

	default:
		return fmt.Errorf("Not support sutch a delte policy: %v", deletePolicy)
	}
}
