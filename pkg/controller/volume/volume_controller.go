// Copyright (c) 2022 Fujitsu Limited

package volume

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/volume-controller/app/options"
	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime"
	runtimeclient "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/client"
	runtimeconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/config"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/fuse"
	volumeplugins "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/plugins"
)

const (
	defaultHostPid                 = 1
	fuseControllersChannelCapacity = 10
	unixDomainSocket               = "unix"
	dummyRootPath                  = "//"
	hostRootPath                   = "/"
)

type fuseControllerFin struct {
	mountPoint string
}

type fuseController struct {
	volumeController       *fuse.FuseVolumeController
	sourcePath             string
	mountNamespaces        []string
	allowedMountNamespaces map[string]*namespaceInfo
}

type VolumeController struct {
	kubeClient    kubernetes.Interface
	runtimeClient runtime.ContainerRuntimeOperations

	fuseConfig *fuse.FuseOptions

	fuseMutex         sync.Mutex
	fuseControllers   map[string]*fuseController // key: fuse mount point
	fuseControllersCh chan fuseControllerFin

	volumePlugins *volumeplugins.Plugins

	stopCh <-chan struct{}
}

type processInfo struct {
	path string
	pid  int
}

type processesInfo struct {
	rootPath  string
	processes []*processInfo
}

type namespaceInfo struct {
	rootPath    string
	executables fuse.AllowedExecutables
}

func newPodManager(controller *VolumeController) {
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(
		controller.kubeClient, 60*time.Second)

	podInformer := kubeInformerFactory.Core().V1().Pods()
	podInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(old interface{}, new interface{}) {
				// [TODO]
			},
		})

	kubeInformerFactory.Start(controller.stopCh)
}

func NewVolumeController(
	fuseOptions *fuse.FuseOptions, volumeOptions *volumeplugins.VolumeOptions,
	runtimeOptions *runtimeconfig.ContainerRuntimeOptions,
	kubeClient kubernetes.Interface, stopCh <-chan struct{}) (
	*VolumeController, error) {
	runtimeClient, err := runtimeclient.NewContainerRuntimeClient(
		runtimeOptions)
	if err != nil {
		return nil, err
	}

	klog.Infof("runtimeOptions: %+v", runtimeOptions)

	controller := &VolumeController{
		kubeClient:    kubeClient,
		runtimeClient: runtimeClient,

		fuseConfig: fuseOptions,

		fuseControllers: map[string]*fuseController{},

		volumePlugins: volumeplugins.NewPlugins(
			fuseOptions.MountPointsHostRootDirectory, volumeOptions),

		stopCh: stopCh,
	}

	newPodManager(controller)

	return controller, nil
}

func getMountNamespacesInfo(
	externalAllowedExecutables []*api.ExternalAllowedExecutable,
	mountNamespaceToProcesses map[string]*processesInfo,
	mountNamespaceToExecutableIndices map[string][]int) map[string]*namespaceInfo {
	namespacesInfo := map[string]*namespaceInfo{}

	for mountNamespace, indices := range mountNamespaceToExecutableIndices {
		if indices == nil {
			namespacesInfo[mountNamespace] = nil
		} else {
			filteredAllowedExecutables := make(
				[]*api.ExternalAllowedExecutable, 0, len(indices))
			for _, index := range indices {
				filteredAllowedExecutables = append(
					filteredAllowedExecutables,
					externalAllowedExecutables[index])
			}

			allowedExecutables := fuse.
				CreateAllowedExecutablesForExternalAllowedExecutables(
					filteredAllowedExecutables)

			namespacesInfo[mountNamespace] = &namespaceInfo{
				rootPath: mountNamespaceToProcesses[mountNamespace].
					rootPath,
				executables: allowedExecutables,
			}
		}
	}

	return namespacesInfo
}

func getMountNamespacesInfoForExternalAllowedExecutables(
	externalAllowedExecutables []*api.ExternalAllowedExecutable) (
	map[string]*namespaceInfo, error) {
	mountNamespaceToProcesses := map[string]*processesInfo{}
	mountNamespaceToExecutableIndices := map[string][]int{}

	for executableIndex, allowedExecutable := range externalAllowedExecutables {
		executable := newExternalAllowedExecutable(allowedExecutable)
		pid, mountNamespace, err := executable.
			getOneOfPidsAndMountNamespaceForExecutable()
		if err != nil {
			return nil, err
		}

		procsInfo, found := mountNamespaceToProcesses[mountNamespace]
		if found {
			if allowedExecutable.IsHostProcess {
				if procsInfo.rootPath != hostRootPath {
					return nil, fmt.Errorf(
						"%q is expected as a 'container' process",
						allowedExecutable.CommandAbsolutePath)
				}
			} else {
				if procsInfo.rootPath == hostRootPath {
					return nil, fmt.Errorf(
						"%q is expected as a 'host' process",
						allowedExecutable.CommandAbsolutePath)
				}
			}

			if allowedExecutable.AreAllProcessesAllowedInNamespace {
				if procsInfo != nil {
					return nil, fmt.Errorf(
						"%v is the namespace-grain control, but %q is the "+
							"process-grain control",
						procsInfo, allowedExecutable.CommandAbsolutePath)

				}
			} else {
				newProcessInfo := &processInfo{
					path: allowedExecutable.CommandAbsolutePath,
					pid:  pid,
				}

				processes := mountNamespaceToProcesses[mountNamespace].processes
				processes = append(processes, newProcessInfo)
				mountNamespaceToExecutableIndices[mountNamespace] = append(
					mountNamespaceToExecutableIndices[mountNamespace],
					executableIndex)
			}
		} else {
			rootPath := hostRootPath
			if !allowedExecutable.IsHostProcess {
				// [TODO] get a root path from pid
				// through a container runtime client
				rootPath = dummyRootPath
			}
			newProcessesInfo := &processesInfo{
				rootPath: rootPath,
			}

			if allowedExecutable.AreAllProcessesAllowedInNamespace {
				mountNamespaceToProcesses[mountNamespace] = newProcessesInfo
				mountNamespaceToExecutableIndices[mountNamespace] = nil
			} else {
				newProcessInfo := &processInfo{
					path: allowedExecutable.CommandAbsolutePath,
					pid:  pid,
				}
				newProcessesInfo.processes = []*processInfo{
					newProcessInfo,
				}

				mountNamespaceToProcesses[mountNamespace] = newProcessesInfo
				mountNamespaceToExecutableIndices[mountNamespace] = []int{
					executableIndex,
				}
			}
		}
	}

	namespacesInfo := getMountNamespacesInfo(
		externalAllowedExecutables, mountNamespaceToProcesses,
		mountNamespaceToExecutableIndices)

	return namespacesInfo, nil
}

func addAllowedExecutablesToFuseController(
	controller *fuse.FuseVolumeController,
	mountNamespacesInfo map[string]*namespaceInfo) error {
	addedMountNamespaces := make([]string, 0, len(mountNamespacesInfo))

	for mountNamespace, info := range mountNamespacesInfo {
		var allowedExecutables fuse.AllowedExecutables
		var rootPath string

		if info != nil {
			allowedExecutables = info.executables
			rootPath = info.rootPath
		} else {
			allowedExecutables = nil
			rootPath = ""
		}

		err := controller.AddAllowedExecutables(
			mountNamespace, rootPath, allowedExecutables)
		if err != nil {
			for _, namespace := range addedMountNamespaces {
				controller.DeleteAllowedExecutables(namespace, nil)
			}

			return err
		} else {
			addedMountNamespaces = append(addedMountNamespaces, mountNamespace)
		}
	}

	return nil
}

func (vc *VolumeController) createFuseServer(
	mountPoint string, sourcePath string, disableUsageControl bool,
	mountNamespacesInfo map[string]*namespaceInfo,
	errorCh chan<- error) *fuse.FuseVolumeController {
	vc.fuseMutex.Lock()
	defer vc.fuseMutex.Unlock()

	if _, found := vc.fuseControllers[mountPoint]; found {
		errMsg := fmt.Sprintf(
			"%q has already registered in the fuse controllers: %+v",
			mountPoint, vc.fuseControllers)

		errorCh <- fmt.Errorf(errMsg)

		return nil
	}

	controller, err := fuse.NewFuseController(
		mountPoint, sourcePath, vc.fuseConfig.Debug, disableUsageControl,
		vc.fuseConfig.ChecksumCalculationAlways, vc.stopCh)
	if err != nil {
		errMsg := fmt.Sprintf(
			"Not create a fuse server for %q: %v", mountPoint, err.Error())

		err := vc.volumePlugins.DeleteMountPoint(mountPoint)
		if err != nil {
			errMsg = fmt.Sprintf(
				"%s\n\tdelete the mount point %q: %v", errMsg, mountPoint, err)
		}

		errorCh <- fmt.Errorf(errMsg)

		return nil
	} else {
		errorCh <- err
	}

	err = addAllowedExecutablesToFuseController(
		controller, mountNamespacesInfo)
	if err != nil {
		errorCh <- err

		return nil
	}

	vc.fuseControllers[mountPoint] = &fuseController{
		volumeController:       controller,
		sourcePath:             sourcePath,
		mountNamespaces:        []string{},
		allowedMountNamespaces: mountNamespacesInfo,
	}

	return controller
}

func (vc *VolumeController) fuseMount(
	mountPoint string, sourcePath string, disableUsageControl bool,
	mountNamespacesInfo map[string]*namespaceInfo, errorCh chan<- error) {
	controller := vc.createFuseServer(
		mountPoint, sourcePath, disableUsageControl, mountNamespacesInfo,
		errorCh)
	if controller == nil {
		return
	}

	controller.Wait()

	vc.fuseControllersCh <- fuseControllerFin{
		mountPoint: mountPoint,
	}
}

func (vc *VolumeController) Initialize(
	ctx context.Context, req *api.InitializeRequest) (
	*api.InitializeResponse, error) {
	if _, err := os.Stat(req.FuseSourcePath); os.IsNotExist(err) {
		return nil, fmt.Errorf(
			"The fuse source path %q does NOT exist", req.FuseSourcePath)
	}

	mountNamespacesInfo,
		err := getMountNamespacesInfoForExternalAllowedExecutables(
		req.ExternalAllowedExecutables)
	if err != nil {
		return nil, err
	}

	mountPoint, err := vc.volumePlugins.
		GenerateMountPointHostPathFromInitializeRequest(req)
	if err != nil {
		return nil, err
	}

	errorCh := make(chan error)

	go vc.fuseMount(
		mountPoint, req.FuseSourcePath, req.DisableUsageControl,
		mountNamespacesInfo, errorCh)

	err = <-errorCh
	if err != nil {
		return nil, err
	}

	klog.Infof(
		"Initialized the FUSE mount: mount point %q, source path %q",
		mountPoint, req.FuseSourcePath)

	return &api.InitializeResponse{
		FuseMountPointDir: mountPoint,
	}, nil
}

func deleteContainerRoots(
	volumeController *fuse.FuseVolumeController,
	containerRootNamespaces map[string]struct{}) {
	for namespace := range containerRootNamespaces {
		volumeController.DeleteAllowedExecutables(namespace, nil)
	}
}

func forcefulDeleteAllowedExecutablesAndContainerRoots(
	fuseController *fuseController,
	containerRootNamespaces map[string]struct{},
	numAllowedMountNamespaces int) {
	controller := fuseController.volumeController

	if len(containerRootNamespaces) > 0 {
		if len(containerRootNamespaces) != numAllowedMountNamespaces {
			klog.Warningf(
				"container root namespaces exist in the volume control root: "+
					"%v\n", containerRootNamespaces)
		}

		deleteContainerRoots(controller, containerRootNamespaces)
	}
}

func hasAllowedExecutablesAndContainerRoots(
	fuseController *fuse.FuseVolumeController,
	containerRootNamespaces map[string]struct{},
	numAllowedMountNamespaces int) bool {
	if len(containerRootNamespaces) > 0 {
		if len(containerRootNamespaces) != numAllowedMountNamespaces {
			klog.Errorf(
				"container root namespaces exist in the volume control root: "+
					"%v\n", containerRootNamespaces)
			return false
		}

		deleteContainerRoots(fuseController, containerRootNamespaces)
	}

	return true
}

func (vc *VolumeController) Finalize(
	ctx context.Context, req *api.FinalizeRequest) (
	*api.FinalizeResponse, error) {
	vc.fuseMutex.Lock()
	defer vc.fuseMutex.Unlock()

	fc, found := vc.fuseControllers[req.FuseMountPointDir]
	if !found {
		return nil, fmt.Errorf(
			"%q is not contained in the fuse controllers: %+v",
			req.FuseMountPointDir, vc.fuseControllers)
	}

	fuseController := fc.volumeController
	containerRootNamespaces := fuseController.GetContainerRootsNamespaces()

	numAllowedMountNamespaces := len(fc.allowedMountNamespaces)
	if req.Force {
		forcefulDeleteAllowedExecutablesAndContainerRoots(
			fc, containerRootNamespaces, numAllowedMountNamespaces)
	} else {
		areEmpty := hasAllowedExecutablesAndContainerRoots(
			fuseController, containerRootNamespaces, numAllowedMountNamespaces)
		if !areEmpty {
			return nil, fmt.Errorf(
				"The fuse controller has information on executable checksums " +
					"or/and container roots")
		}
	}

	err := fuseController.Unmount()
	if err != nil {
		return nil, err
	}

	delete(vc.fuseControllers, req.FuseMountPointDir)

	err = vc.volumePlugins.DeleteMountPoint(req.FuseMountPointDir)
	if err != nil {
		return nil, err
	}

	klog.Infof(
		"Finalized the FUSE mount: mount point %q", req.FuseMountPointDir)

	return &api.FinalizeResponse{}, nil
}

func (vc *VolumeController) getContainerInfo(
	pod *corev1.Pod, dataContainerName string) (map[string]string, error) {
	mountNamespaceToRootPath := map[string]string{}
	for _, container := range pod.Status.ContainerStatuses {
		rootFs, err := vc.runtimeClient.GetContainerInfo(container.ContainerID)
		if err != nil {
			return nil, err
		}

		if container.Name == dataContainerName {
			mountNamespaceToRootPath[rootFs.MountNamaspaceId] = ""
		} else {
			mountNamespaceToRootPath[rootFs.MountNamaspaceId] = rootFs.RootFs
		}
	}

	return mountNamespaceToRootPath, nil
}

func (vc *VolumeController) findMountPointForPersistentVolumeClaim(
	pod *corev1.Pod, pvcKey string) (string, error) {
	pvcNamespace, pvcName, err := util.SplitIntoNamespaceAndName(
		pvcKey)
	if err != nil {
		return "", err
	}

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil ||
			pod.Namespace != pvcNamespace ||
			volume.PersistentVolumeClaim.ClaimName != pvcName {
			continue
		}

		mountPoint, err := vc.volumePlugins.
			GetMountPointHostPathFromPvcVolumeSource(
				pvcName, vc.kubeClient, pod.Namespace)
		if err != nil {
			return "", err
		} else if mountPoint == "" {
			continue
		}

		_, ok := vc.fuseControllers[mountPoint]
		if !ok {
			continue
		}

		return mountPoint, nil
	}

	pvcNames := []string{}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		pvcNames = append(pvcNames, volume.PersistentVolumeClaim.ClaimName)
	}

	return "", fmt.Errorf(
		"[%s] The mount point for one of the persistent volume claims (%v) "+
			"does not include in the fuse controllers: %+v",
		pvcKey, pvcNames, vc.fuseControllers)
}

func (vc *VolumeController) addMountNamespacesToFuseControllers(
	mountPoint string, mountNamespaceToRootPath map[string]string) {
	for namespace := range mountNamespaceToRootPath {
		vc.fuseControllers[mountPoint].mountNamespaces = append(
			vc.fuseControllers[mountPoint].mountNamespaces, namespace)
	}
}

func (vc *VolumeController) startVolumeControl(
	mountPoint string, lifetimeExecutables map[string]*api.Executable,
	mountNamespaceToRootPath map[string]string) error {
	allowedExecutables := fuse.CreateAllowedExecutables(lifetimeExecutables)

	addedMountNamespaces := make(
		[]string, 0, len(mountNamespaceToRootPath))
	for mountNamespace, rootPath := range mountNamespaceToRootPath {
		err := vc.fuseControllers[mountPoint].volumeController.
			AddAllowedExecutables(
				mountNamespace, rootPath, allowedExecutables)
		if err != nil {
			for _, namespace := range addedMountNamespaces {
				vc.fuseControllers[mountPoint].volumeController.
					DeleteAllowedExecutables(namespace, nil)
			}

			return err
		} else {
			addedMountNamespaces = append(addedMountNamespaces, mountNamespace)
		}
	}

	return nil
}

func (vc *VolumeController) startMessageQueuePublishing(
	mountPoint string, messageQueue *api.MessageQueue,
	mountNamespaceToRootPath map[string]string) error {
	volumeRootPath := vc.fuseControllers[mountPoint].sourcePath
	addedMountNamespaces := make([]string, 0, len(mountNamespaceToRootPath))
	for namespace := range mountNamespaceToRootPath {
		addedMountNamespaces = append(addedMountNamespaces, namespace)
	}

	err := vc.fuseControllers[mountPoint].volumeController.
		AddMessageQueueUpdatePublisher(
			messageQueue, volumeRootPath, addedMountNamespaces)
	if err != nil {
		return fmt.Errorf(
			"Failed to add the update publisher for (a message queue "+
				"configuration: %+v and a volume root path :%s): %v",
			messageQueue, volumeRootPath, err.Error())
	}

	return nil
}

func (vc *VolumeController) Start(
	ctx context.Context, req *api.StartRequest) (*api.StartResponse, error) {
	podCtx, cancel := util.GetTimeoutContext(util.DefaultKubeClientTimeout)
	defer cancel()

	pod, err := vc.kubeClient.CoreV1().Pods(req.PodNamespace).Get(
		podCtx, req.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf(
			"Not get Pod[%s:%s]: %v",
			req.PodNamespace, req.PodName, err.Error())
	}

	mountNamespaceToRootPath, err := vc.getContainerInfo(
		pod, req.DataContainerName)
	if err != nil {
		return nil, err
	}

	vc.fuseMutex.Lock()
	defer vc.fuseMutex.Unlock()

	mountPoint, err := vc.findMountPointForPersistentVolumeClaim(
		pod, req.PvcKey)
	if err != nil {
		return nil, err
	}

	vc.addMountNamespacesToFuseControllers(
		mountPoint, mountNamespaceToRootPath)

	usageControlEnabled := false
	messageQueuePublisherEnabled := false

	if req.AllowedExecutables != nil {
		err := vc.startVolumeControl(
			mountPoint, req.AllowedExecutables, mountNamespaceToRootPath)
		if err != nil {
			return nil, err
		}

		usageControlEnabled = true

		klog.Info("Volume controller enabled usage control")
	}

	if req.MessageQueueUpdatePublisher != nil {
		err := vc.startMessageQueuePublishing(
			mountPoint, req.MessageQueueUpdatePublisher,
			mountNamespaceToRootPath)
		if err != nil {
			return nil, err
		}

		messageQueuePublisherEnabled = true

		klog.Info("Volume controller enabled message queue publishing")
	}

	klog.Infof("Started the volume control, req: %+v", req)

	return &api.StartResponse{
		MountPoint:                   mountPoint,
		UsageControlEnabled:          usageControlEnabled,
		MessageQueuePublisherEnabled: messageQueuePublisherEnabled,
	}, nil
}

func (vc *VolumeController) updateAllowedExecutablesForFuseControllers(
	pod *corev1.Pod, pvcKey string,
	executablesDiffs *fuse.AllowedExecutablesDiffs) (string, error) {
	pvcNamespace, pvcName, err := util.SplitIntoNamespaceAndName(pvcKey)
	if err != nil {
		return "", err
	}

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		if pod.Namespace != pvcNamespace {
			continue
		}

		if volume.PersistentVolumeClaim.ClaimName != pvcName {
			continue
		}

		mountPoint, err := vc.volumePlugins.
			GetMountPointHostPathFromPvcVolumeSource(
				pvcName, vc.kubeClient, pod.Namespace)
		if err != nil {
			return "", err
		} else if mountPoint == "" {
			continue
		}

		fc, ok := vc.fuseControllers[mountPoint]
		if !ok {
			continue
		}

		if len(executablesDiffs.AddedAndUpdated) != 0 {
			for _, mountNamespace := range fc.mountNamespaces {
				err := fc.volumeController.UpdateAllowedExecutables(
					mountNamespace, executablesDiffs.AddedAndUpdated)
				if err != nil {
					return "", err
				}
			}
		}

		if len(executablesDiffs.DeletedPaths) != 0 {
			for _, mountNamespace := range fc.mountNamespaces {
				err := fc.volumeController.DeleteAllowedExecutables(
					mountNamespace, executablesDiffs.DeletedPaths)
				if err != nil {
					return "", err
				}
			}
		}

		return mountPoint, nil
	}

	pvcNames := []string{}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		pvcNames = append(pvcNames, volume.PersistentVolumeClaim.ClaimName)
	}

	return "", fmt.Errorf(
		"[%s] The mount point for one of the persistent volume claims (%v) "+
			"does not include in the fuse controllers: %+v",
		pvcKey, pvcNames, vc.fuseControllers)
}

func (vc *VolumeController) UpdateAllowedExecutables(
	ctx context.Context, req *api.UpdateAllowedExecutablesRequest) (
	*api.UpdateAllowedExecutablesResponse, error) {
	podCtx, cancel := util.GetTimeoutContext(util.DefaultKubeClientTimeout)
	defer cancel()

	pod, err := vc.kubeClient.CoreV1().Pods(req.PodNamespace).Get(
		podCtx, req.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf(
			"Not get Pod[%s:%s]: %v",
			req.PodNamespace, req.PodName, err.Error())
	}

	allowedExecutablesDiffs := fuse.CreateAllowedExecutablesDiffs(
		req.ExecutablesDiffs)

	vc.fuseMutex.Lock()
	defer vc.fuseMutex.Unlock()

	pvcKeyToMountPoint := map[string]string{}
	for pvcKey, executablesDiffs := range allowedExecutablesDiffs {
		mountPoint, err := vc.updateAllowedExecutablesForFuseControllers(
			pod, pvcKey, executablesDiffs)
		if err != nil {
			return nil, err
		}

		pvcKeyToMountPoint[pvcKey] = mountPoint
	}

	return &api.UpdateAllowedExecutablesResponse{
		PvcKeyToMountPoint: pvcKeyToMountPoint,
	}, nil
}

func (vc *VolumeController) updateMessageQueueForFuseControllers(
	pod *corev1.Pod, pvcKey string, messageQueueConfig *api.MessageQueue) (
	string, error) {
	pvcNamespace, pvcName, err := util.SplitIntoNamespaceAndName(pvcKey)
	if err != nil {
		return "", err
	}

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		if pod.Namespace != pvcNamespace {
			continue
		}

		if volume.PersistentVolumeClaim.ClaimName != pvcName {
			continue
		}

		mountPoint, err := vc.volumePlugins.
			GetMountPointHostPathFromPvcVolumeSource(
				pvcName, vc.kubeClient, pod.Namespace)
		if err != nil {
			return "", err
		} else if mountPoint == "" {
			continue
		}

		fc, ok := vc.fuseControllers[mountPoint]
		if !ok {
			continue
		}

		if messageQueueConfig != nil {
			volumeRootPath := vc.fuseControllers[mountPoint].sourcePath

			err = fc.volumeController.AddMessageQueueUpdatePublisher(
				messageQueueConfig, volumeRootPath,
				vc.fuseControllers[mountPoint].mountNamespaces)
			if err != nil {
				return "", fmt.Errorf(
					"Failed to add the update publisher for the pvc key "+
						"%q: %v", pvcKey, err.Error())
			}
		} else {
			err = fc.volumeController.DeleteMessageQueueUpdatePublisher(
				vc.fuseControllers[mountPoint].mountNamespaces)
			if err != nil {
				return "", fmt.Errorf(
					"Failed to delete the update publisher for the pvc key "+
						"%q: %v", pvcKey, err.Error())
			}
		}

		return mountPoint, nil
	}

	pvcNames := []string{}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		pvcNames = append(pvcNames, volume.PersistentVolumeClaim.ClaimName)
	}

	return "", fmt.Errorf(
		"[%s] The mount point for one of the persistent volume claims (%v) "+
			"does not include in the fuse controllers: %+v",
		pvcKey, pvcNames, vc.fuseControllers)
}

func (vc *VolumeController) UpdateMessageQueueConfig(
	ctx context.Context, req *api.UpdateMessageQueueConfigRequest) (
	*api.UpdateMessageQueueConfigResponse, error) {
	podCtx, cancel := util.GetTimeoutContext(util.DefaultKubeClientTimeout)
	defer cancel()

	pod, err := vc.kubeClient.CoreV1().Pods(req.PodNamespace).Get(
		podCtx, req.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf(
			"Not get Pod[%s:%s]: %v",
			req.PodNamespace, req.PodName, err.Error())
	}

	vc.fuseMutex.Lock()
	defer vc.fuseMutex.Unlock()

	pvcKeyToMountPoint := map[string]string{}
	for pvcKey, messageQueueConfig := range req.MessageQueueDiffs {
		mountPoint, err := vc.updateMessageQueueForFuseControllers(
			pod, pvcKey, messageQueueConfig)
		if err != nil {
			return nil, err
		}

		pvcKeyToMountPoint[pvcKey] = mountPoint
	}

	return &api.UpdateMessageQueueConfigResponse{
		PvcKeyToMountPoint: pvcKeyToMountPoint,
	}, nil
}

func (vc *VolumeController) stopVolumeControl(
	mountPointToExecutablePaths map[string]*api.ExecutablePaths,
	mountNamespaces []string) {
	for mountPoint, executablePaths := range mountPointToExecutablePaths {
		fc, ok := vc.fuseControllers[mountPoint]
		if !ok {
			klog.Warningf(
				"Not find %q in the fuse controllers: %+v",
				mountPoint, vc.fuseControllers)
			continue
		}

		for _, mountNamespace := range mountNamespaces {
			err := fc.volumeController.DeleteAllowedExecutables(
				mountNamespace, executablePaths.Paths)
			if err != nil {
				klog.Errorf("DeleteAllowedExecutables(): %v", err.Error())
			}
		}
		fc.mountNamespaces = []string{}
	}

	if len(mountPointToExecutablePaths) != 0 {
		klog.Info("Volume controller disabled usage control")
	}
}

func (vc *VolumeController) stopMessageQueuePublishing(
	mountPoint string, mountNamespaces []string) error {
	if mountPoint == "" {
		return nil
	}

	err := vc.fuseControllers[mountPoint].volumeController.
		DeleteMessageQueueUpdatePublisher(mountNamespaces)
	if err != nil {
		return err
	}

	klog.Info("Volume controller disabled message queue publishing")

	return nil
}

func (vc *VolumeController) getMountNamespaces(
	pod *corev1.Pod) ([]string, error) {
	mountNamespaces := []string{}
	for _, container := range pod.Status.ContainerStatuses {
		rootFs, err := vc.runtimeClient.GetContainerInfo(container.ContainerID)
		if err != nil {
			return nil, err
		}

		mountNamespaces = append(mountNamespaces, rootFs.MountNamaspaceId)
	}

	return mountNamespaces, nil
}

func (vc *VolumeController) Stop(
	ctx context.Context, req *api.StopRequest) (*api.StopResponse, error) {
	podCtx, cancel := util.GetTimeoutContext(util.DefaultKubeClientTimeout)
	defer cancel()

	pod, err := vc.kubeClient.CoreV1().Pods(req.PodNamespace).Get(
		podCtx, req.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf(
			"Not get Pod[%s:%s]: %v",
			req.PodNamespace, req.PodName, err.Error())
	}

	mountNamespaces, err := vc.getMountNamespaces(pod)
	if err != nil {
		return nil, err
	}

	vc.fuseMutex.Lock()
	defer vc.fuseMutex.Unlock()

	vc.stopVolumeControl(req.MountPointToExecutablePaths, mountNamespaces)

	err = vc.stopMessageQueuePublishing(req.MountPoint, mountNamespaces)
	if err != nil {
		return nil, err
	}

	klog.Infof("Stopped the volume control, req: %+v", req)

	return &api.StopResponse{}, nil
}

func (vc *VolumeController) serveGRPC(
	endpoint string, grpcCh chan<- struct{}) *grpc.Server {
	grpcServer := grpc.NewServer()
	ep := strings.SplitN(endpoint, options.GrpcEndpointSeparator, 2)

	go func(server *grpc.Server) {
		defer close(grpcCh)

		ln, err := net.Listen(ep[0], ep[1])
		if err != nil {
			klog.Errorf("Not listen on %s: %v", endpoint, err.Error())
			return
		}
		defer ln.Close()

		api.RegisterVolumeControlServer(grpcServer, vc)
		err = server.Serve(ln)
		if err != nil {
			klog.Errorf("gRPC error: %v", err.Error())
		}
	}(grpcServer)

	return grpcServer
}

func (vc *VolumeController) deleteFuseServer(finMessage fuseControllerFin) {
	vc.fuseMutex.Lock()
	defer vc.fuseMutex.Unlock()

	delete(vc.fuseControllers, finMessage.mountPoint)
}

func (vc *VolumeController) Run(grpcServerEndpoint string) {
	defer utilruntime.HandleCrash()

	klog.Info("Starting the volume controller")

	fuseControllersCh := make(chan fuseControllerFin, fuseControllersChannelCapacity)
	grpcCh := make(chan struct{})
	vc.fuseControllersCh = fuseControllersCh

	grpcServer := vc.serveGRPC(grpcServerEndpoint, grpcCh)

	klog.Info("Started the volume controller")

outerLoop:
	for {
		select {
		case fuseFinMessage := <-fuseControllersCh:
			vc.deleteFuseServer(fuseFinMessage)

		case <-grpcCh:
			break outerLoop

		case <-vc.stopCh:
			err := vc.runtimeClient.Close()
			if err != nil {
				klog.Errorf(
					"Failed to close the runtime client: %v", err.Error())
			}
			grpcServer.GracefulStop()

			break outerLoop
		}
	}

	klog.Info("Shutting down the volume controller")
}
