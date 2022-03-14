// Copyright (c) 2022 Fujitsu Limited

package volume

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	api "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/grpc/volumecontrol/v1alpha1"
	mqpub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime"
	fakeruntime "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/runtime/testing"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/fuse"
	volumeplugins "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/plugins"

	"github.com/stretchr/testify/assert"
)

const (
	testRootDir         = "testing"
	mountPointName1     = "vol1"
	mountPointName2     = "vol2"
	containerRootPrefix = "/root"
	csiVolumeHandle     = "volume-handle-1"

	noInitReq = iota
	localInitReq
	csiInitReq

	pvSourceLocal
	pvSourceCsi

	kafkaBrokerAddressEnv = "KAFKA_BROKER_ADDRESS"

	// [REF] github.com/segmentio/kafka-go/docker-compose.yml
	saslUser     = "adminscram"
	saslPassword = "admin-secret-512"

	updateTopic = "update.topic"
)

var (
	mountPointsHostRootDir = filepath.Join(testRootDir, "fixtures/fuse")
	sourcesHostRootDir     = filepath.Join(testRootDir, "fixtures/volumes")
	fuseSourceDir1         = filepath.Join(
		sourcesHostRootDir, mountPointName1)
	fuseSourceDir2 = filepath.Join(
		sourcesHostRootDir, mountPointName2)
	localMountPointsRootDir = filepath.Join(mountPointsHostRootDir, "local")
	localMountPointDir      = filepath.Join(
		localMountPointsRootDir, mountPointName1)
	fuseMountPointHostDir = filepath.Join(
		localMountPointsRootDir, mountPointName1)
	csiMountPointsRootDir = filepath.Join(mountPointsHostRootDir, "csi")
	csiMountPointDir      = filepath.Join(
		csiMountPointsRootDir, csiVolumeHandle)
)

type initFuseMount struct {
	sourceDir string
}

type containerInfo struct {
	id             string
	mountNamespace string
}

type containerVolume struct {
	rootPath       string
	mountNamespace string
}

type executable struct {
	path     string
	checksum []byte
	writable bool
}

type persistentVolumeSource struct {
	kind            uint
	localPath       string
	csiVolumeHandle string
}

type podVolume struct {
	pvcName       string
	pvName        string
	podVolumeName string
	pvSource      *persistentVolumeSource
	fuseSourceDir string
}

type startFuseMount struct {
	pvcKey        string
	mountPointDir string
}

type podFuseMount struct {
	pod        *corev1.Pod
	containers []containerInfo
	fuseMount  startFuseMount
}

type mountPointInfo struct {
	mutex sync.Mutex
	infos map[*corev1.Pod]map[string]*api.StartResponse
}

func newMountPointInfo() *mountPointInfo {
	return &mountPointInfo{
		infos: map[*corev1.Pod]map[string]*api.StartResponse{},
	}
}

func (mpi *mountPointInfo) add(
	pod *corev1.Pod, pvcKey string, mountPointInfo *api.StartResponse) {
	mpi.mutex.Lock()
	defer mpi.mutex.Unlock()

	if mpInfos, ok := mpi.infos[pod]; ok {
		mpInfos[pvcKey] = mountPointInfo
	} else {
		mpi.infos[pod] = map[string]*api.StartResponse{
			pvcKey: mountPointInfo,
		}
	}
}

func (mpi *mountPointInfo) getWithoutMutex() map[*corev1.Pod]map[string]*api.StartResponse {
	return mpi.infos
}

func newFakeVolumeController(
	kubeClient kubernetes.Interface,
	runtimeClient runtime.ContainerRuntimeOperations) *VolumeController {
	volumeOptions := &volumeplugins.VolumeOptions{
		CsiPluginsDir:           sourcesHostRootDir,
		LocalFuseSourcesHostDir: sourcesHostRootDir,
	}

	volumeController := &VolumeController{
		kubeClient:    kubeClient,
		runtimeClient: runtimeClient,

		volumePlugins: volumeplugins.NewPlugins(
			mountPointsHostRootDir, volumeOptions),

		fuseControllers: map[string]*fuseController{},

		fuseConfig: &fuse.FuseOptions{
			MountPointsHostRootDirectory: mountPointsHostRootDir,
		},
	}

	return volumeController
}

func createFuseSourceDirectory(
	t *testing.T, fuseSourceDir string) {
	err := os.MkdirAll(fuseSourceDir, os.ModePerm)
	assert.NoError(t, err)
}

func deleteFuseSourceDirectory(
	t *testing.T, absFuseSourceDir string) {
	err := os.Remove(absFuseSourceDir)
	assert.NoError(t, err)
}

func deleteTestFuseDirectories(t *testing.T) {
	err := os.RemoveAll(testRootDir)
	assert.NoError(t, err)
}

func testInitialize(
	t *testing.T, volumeController *VolumeController, fuseSourceDir string,
	initRequestKind uint, allowedExecutables []*api.ExternalAllowedExecutable,
	expectedContainerRootsString map[string]fuse.ContainerRootString,
	expectError bool) (
	string, error) {
	ctx := context.Background()

	initReq := &api.InitializeRequest{
		FuseSourcePath: fuseSourceDir,
	}

	if allowedExecutables != nil {
		initReq.ExternalAllowedExecutables = allowedExecutables
	}

	switch initRequestKind {
	case noInitReq:
	case localInitReq:
		initReq.LocalFuseMountsHostRootDir = mountPointsHostRootDir
	case csiInitReq:
		initReq.CsiVolumeHandle = csiVolumeHandle
	default:
		assert.Fail(t, "Kind of the initialize() request must be given")
	}

	initRes, err := volumeController.Initialize(ctx, initReq)

	if expectError {
		assert.Error(t, err)
		return "", err
	} else {
		assert.NoError(t, err)
	}

	fuseController, ok := volumeController.
		fuseControllers[initRes.FuseMountPointDir]
	assert.True(t, ok)

	namespaces := fuseController.volumeController.GetContainerRootsNamespaces()
	expectedNamespaces := map[string]struct{}{}
	for namespace, expectedContainerRoot := range expectedContainerRootsString {
		containerRoot := fuseController.volumeController.GetContainerRootString(namespace)
		assert.Equal(t, expectedContainerRoot, containerRoot)

		expectedNamespaces[namespace] = struct{}{}
	}
	assert.Equal(t, expectedNamespaces, namespaces)

	return initRes.FuseMountPointDir, nil
}

func testFinalize(
	t *testing.T, volumeController *VolumeController, fuseMountPointDir string,
	force bool, expectError bool) error {
	ctx := context.Background()

	finalReq := &api.FinalizeRequest{
		FuseMountPointDir: fuseMountPointDir,
		Force:             force,
	}

	_, err := volumeController.Finalize(ctx, finalReq)
	if expectError {
		assert.Error(t, err)
		return err
	} else {
		assert.NoError(t, err)
	}

	_, fuseControllerFound := volumeController.fuseControllers[fuseMountPointDir]
	assert.False(t, fuseControllerFound)

	return nil
}

func testInitializeAndFinalize(
	t *testing.T, allowedExecutables []*api.ExternalAllowedExecutable,
	expectedContainerRootsString map[string]fuse.ContainerRootString) {

	volumeController := newFakeVolumeController(nil, nil)

	createFuseSourceDirectory(t, fuseSourceDir1)

	fuseMountPoint, err := testInitialize(
		t, volumeController, fuseSourceDir1, localInitReq, allowedExecutables,
		expectedContainerRootsString, false)
	assert.NoError(t, err)

	err = testFinalize(t, volumeController, fuseMountPoint, false, false)
	assert.NoError(t, err)

	deleteTestFuseDirectories(t)
}

func TestInitializeAndFinalize(t *testing.T) {
	testInitializeAndFinalize(t, nil, nil)
}

func createExternalAllowedExecutables(
	t *testing.T) (
	[]*api.ExternalAllowedExecutable, map[string]fuse.ContainerRootString) {
	procFs := util.NewProcFs()

	commandPid1 := os.Getpid()
	commandPath1, err := procFs.GetCommandPath(commandPid1)
	assert.NoError(t, err)
	commandPid2 := os.Getppid()
	commandPath2, err := procFs.GetCommandPath(commandPid2)
	assert.NoError(t, err)

	externalMountNamespace, err := procFs.GetMountNamespace(commandPid1)
	assert.NoError(t, err)

	allowedExecutables := []*api.ExternalAllowedExecutable{
		{
			CommandAbsolutePath: commandPath1,
			Writable:            true,
		},
		{
			CommandAbsolutePath: commandPath2,
		},
	}

	expectedContainerRootsString := map[string]fuse.ContainerRootString{
		externalMountNamespace: fuse.ContainerRootString{
			RootPath: dummyRootPath,
			AllowedExecutables: map[string]map[string]string{
				commandPath1: {
					fuse.AllowedExecutableChecksumKey: "",
					fuse.AllowedExecutableWritableKey: "true",
				},
				commandPath2: {
					fuse.AllowedExecutableChecksumKey: "",
					fuse.AllowedExecutableWritableKey: "false",
				},
			},
		},
	}

	return allowedExecutables, expectedContainerRootsString
}

func TestInitializeAndFinalizeWithAllowedExecutables(t *testing.T) {
	allowedExecutables,
		expectedContainerRootsString := createExternalAllowedExecutables(t)

	testInitializeAndFinalize(
		t, allowedExecutables, expectedContainerRootsString)
}

func TestInitializesAndFinalizes(t *testing.T) {
	volumeController := newFakeVolumeController(nil, nil)

	createFuseSourceDirectory(t, fuseSourceDir1)
	createFuseSourceDirectory(t, fuseSourceDir2)

	fuseMounts := []initFuseMount{
		{
			sourceDir: fuseSourceDir1,
		},
		{
			sourceDir: fuseSourceDir2,
		},
	}

	wg := new(sync.WaitGroup)

	var mpMutex sync.Mutex
	mountPoints := make([]string, 0, len(fuseMounts))

	for _, mount := range fuseMounts {
		wg.Add(1)
		go func(ifm initFuseMount) {
			defer wg.Done()

			fuseMountPoint, err := testInitialize(
				t, volumeController, ifm.sourceDir, localInitReq, nil, nil,
				false)
			assert.NoError(t, err)

			mpMutex.Lock()
			defer mpMutex.Unlock()

			mountPoints = append(mountPoints, fuseMountPoint)
		}(mount)
	}
	wg.Wait()

	for _, mountPoint := range mountPoints {
		wg.Add(1)
		go func(mp string) {
			err := testFinalize(t, volumeController, mp, false, false)
			wg.Done()
			assert.NoError(t, err)
		}(mountPoint)
	}
	wg.Wait()

	deleteTestFuseDirectories(t)
}

func TestInitializeForVolumePluginNotExist(t *testing.T) {
	volumeController := newFakeVolumeController(nil, nil)

	err := os.MkdirAll(fuseSourceDir1, os.ModePerm)
	assert.NoError(t, err)

	_, err = testInitialize(
		t, volumeController, fuseSourceDir1, noInitReq, nil, nil, true)

	initReq := &api.InitializeRequest{
		FuseSourcePath: fuseSourceDir1,
	}

	errMsg := fmt.Sprintf(
		"Could not generate the mount point path for "+
			"the request of the volume control initialization: %+v",
		initReq)
	assert.EqualError(t, err, errMsg)

	err = os.Remove(fuseSourceDir1)
	assert.NoError(t, err)
}

func TestInitializeForFuseMountPointExists(t *testing.T) {
	volumeController := newFakeVolumeController(nil, nil)

	createFuseSourceDirectory(t, fuseSourceDir1)
	err := os.MkdirAll(csiMountPointDir, os.ModePerm)
	assert.NoError(t, err)

	_, err = testInitialize(
		t, volumeController, fuseSourceDir1, csiInitReq, nil, nil, true)

	errMsg := fmt.Sprintf("The mount point %q exists", csiMountPointDir)
	assert.EqualError(t, err, errMsg)

	deleteTestFuseDirectories(t)
}

func TestInitializeForFuseControllerRegistered(t *testing.T) {
	volumeController := newFakeVolumeController(nil, nil)

	createFuseSourceDirectory(t, fuseSourceDir1)

	volumeController.
		fuseControllers[csiMountPointDir] = &fuseController{}

	_, err := testInitialize(
		t, volumeController, fuseSourceDir1, csiInitReq, nil, nil, true)

	errMsg := fmt.Sprintf(
		"%q has already registered in the fuse controllers: %+v",
		csiMountPointDir, volumeController.fuseControllers)
	assert.EqualError(t, err, errMsg)

	delete(volumeController.fuseControllers, csiMountPointDir)

	deleteTestFuseDirectories(t)
}

func TestFinalizeForMountPointNotExist(t *testing.T) {
	volumeController := newFakeVolumeController(nil, nil)

	dummyMountPoint := "dummy"
	err := testFinalize(t, volumeController, dummyMountPoint, false, true)
	errMsg := fmt.Sprintf(
		"%q is not contained in the fuse controllers: %+v",
		dummyMountPoint, volumeController.fuseControllers)
	assert.EqualError(t, err, errMsg)
}

func TestFinalizeHasAllowedExecutables(t *testing.T) {
	volumeController := newFakeVolumeController(nil, nil)

	createFuseSourceDirectory(t, fuseSourceDir1)

	fuseMountPoint, err := testInitialize(
		t, volumeController, fuseSourceDir1, localInitReq, nil, nil, false)
	assert.NoError(t, err)

	fc := volumeController.fuseControllers[fuseMountPoint]

	path := "path1"
	apiExecutables := map[string]*api.Executable{
		path: {
			Checksum: []byte{0x01, 0x23},
			Writable: true,
		},
	}

	mountNamespace := "mnt:[xxx]"
	rootPath := "/root"
	executables := fuse.CreateAllowedExecutables(apiExecutables)
	err = fc.volumeController.AddAllowedExecutables(
		mountNamespace, rootPath, executables)
	assert.NoError(t, err)

	err = testFinalize(t, volumeController, fuseMountPoint, false, true)
	errMsg := fmt.Sprintf(
		"The fuse controller has information on executable checksums " +
			"or/and container roots")
	assert.EqualError(t, err, errMsg)

	err = fc.volumeController.DeleteAllowedExecutables(
		mountNamespace, []string{path})
	assert.NoError(t, err)

	err = testFinalize(t, volumeController, fuseMountPoint, false, false)
	assert.NoError(t, err)

	deleteTestFuseDirectories(t)
}

func newPod(
	namespace string, name string, volumes []corev1.Volume,
	containerIds []string) *corev1.Pod {
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			Volumes: volumes,
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{},
		},
	}

	for _, containerId := range containerIds {
		pod.Status.ContainerStatuses = append(
			pod.Status.ContainerStatuses,
			corev1.ContainerStatus{
				ContainerID: containerId,
			})
	}

	return pod
}

func newLocalVolumeSource(path string) corev1.PersistentVolumeSource {
	return corev1.PersistentVolumeSource{
		Local: &corev1.LocalVolumeSource{
			Path: path,
		},
	}
}

func newCsiVolumeSource(volumeHandle string) corev1.PersistentVolumeSource {
	return corev1.PersistentVolumeSource{
		CSI: &corev1.CSIPersistentVolumeSource{
			VolumeHandle: volumeHandle,
		},
	}
}

func createPersistentVolumeSource(
	t *testing.T,
	pvSource *persistentVolumeSource) corev1.PersistentVolumeSource {
	switch pvSource.kind {
	case pvSourceLocal:
		return newLocalVolumeSource(pvSource.localPath)
	case pvSourceCsi:
		return newCsiVolumeSource(pvSource.csiVolumeHandle)
	default:
		assert.Fail(
			t, "Not support such a type of the persistent volume source: %v",
			pvSource.kind)
		return corev1.PersistentVolumeSource{}
	}
}

func newPersistentVolume(
	name string, pvSource corev1.PersistentVolumeSource, pvcNamespace string,
	pvcName string) *corev1.PersistentVolume {
	if pvcName == "" {
		return &corev1.PersistentVolume{
			TypeMeta: metav1.TypeMeta{
				APIVersion: corev1.SchemeGroupVersion.String(),
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					corev1.ResourceRequestsStorage: *resource.NewQuantity(
						5, resource.BinarySI),
				},
				PersistentVolumeSource: pvSource,
			},
		}
	} else {
		return &corev1.PersistentVolume{
			TypeMeta: metav1.TypeMeta{
				APIVersion: corev1.SchemeGroupVersion.String(),
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					corev1.ResourceRequestsStorage: *resource.NewQuantity(
						5, resource.BinarySI),
				},
				PersistentVolumeSource: pvSource,
				ClaimRef: &corev1.ObjectReference{
					APIVersion: corev1.SchemeGroupVersion.String(),
					Kind:       "PersistentVolumeClaim",
					Name:       pvcName,
					Namespace:  pvcNamespace,
				},
			},
		}
	}
}

func newPersistentVolumeClaim(
	namespace string, name string,
	pvName string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: pvName,
		},
	}
}

func newVolumeSource(pvcName string) *corev1.VolumeSource {
	if pvcName == "" {
		return &corev1.VolumeSource{}
	} else {
		return &corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
		}
	}
}

func createVolume(
	t *testing.T, podNamespace string, podVolumeName string, pvName string,
	pvcName string, pvSrc *persistentVolumeSource) (*corev1.PersistentVolume,
	*corev1.PersistentVolumeClaim, corev1.Volume) {
	pvSource := createPersistentVolumeSource(t, pvSrc)

	pv := newPersistentVolume(pvName, pvSource, podNamespace, pvcName)

	pvc := newPersistentVolumeClaim(podNamespace, pvcName, pvName)

	pvcVolumeSource := &corev1.PersistentVolumeClaimVolumeSource{
		ClaimName: pvcName,
	}
	podVolume := corev1.Volume{
		Name: podVolumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: pvcVolumeSource,
		},
	}

	return pv, pvc, podVolume
}

func createContainerRootFs(containerId string) string {
	return filepath.Join(containerRootPrefix, containerId)
}

func newFakeRuntimeClient(
	containers []containerInfo) runtime.ContainerRuntimeOperations {
	fakeRuntimeClient := new(fakeruntime.FakeRuntimeClient)

	for _, container := range containers {
		containerInfo := &runtime.ContainerInfo{
			RootFs:           createContainerRootFs(container.id),
			MountNamaspaceId: container.mountNamespace,
		}

		fakeRuntimeClient.On(
			"GetContainerInfo", container.id).Return(containerInfo, nil)
	}

	return fakeRuntimeClient
}

func getAllowedExecutablesFromLifetimeExecutables(
	lifetimeExecutables map[string]*api.Executable) map[string]map[string]string {
	allowedExecutables := map[string]map[string]string{}

	allowedExecutables = map[string]map[string]string{}
	for path, executable := range lifetimeExecutables {
		allowedExecutables[path] = map[string]string{
			fuse.AllowedExecutableChecksumKey: string(
				executable.Checksum),
			fuse.AllowedExecutableWritableKey: strconv.FormatBool(
				executable.Writable),
		}
	}

	return allowedExecutables
}

func testStart(
	t *testing.T, volumeController *VolumeController, pod *corev1.Pod,
	dataContainerName string, pvcKey string,
	lifetimeExecutables map[string]*api.Executable,
	messageQueueConfig *api.MessageQueue,
	pvcKeyToContainerVolumes map[string][]*containerVolume,
	expectedMountPointInfo *api.StartResponse,
	expectedExternalContainerRootsString map[string]fuse.ContainerRootString,
	expectError bool) (*api.StartResponse, error) {
	startReq := &api.StartRequest{
		PodNamespace:                pod.Namespace,
		PodName:                     pod.Name,
		DataContainerName:           dataContainerName,
		PvcKey:                      pvcKey,
		AllowedExecutables:          lifetimeExecutables,
		MessageQueueUpdatePublisher: messageQueueConfig,
	}

	ctx := context.Background()
	startRes, err := volumeController.Start(ctx, startReq)

	if expectError {
		assert.Error(t, err)
		return nil, err
	} else {
		assert.NoError(t, err)
	}

	assert.Equal(t, expectedMountPointInfo, startRes)

	expectedAllowedExecutables := getAllowedExecutablesFromLifetimeExecutables(
		lifetimeExecutables)

	expectedContainerRoots := map[string]fuse.ContainerRootString{}
	for mountNamespace, containerRoot := range expectedExternalContainerRootsString {
		expectedContainerRoots[mountNamespace] = containerRoot
	}

	assert.NotNil(
		t, volumeController.fuseControllers[expectedMountPointInfo.MountPoint])
	fc := volumeController.fuseControllers[expectedMountPointInfo.MountPoint]
	mountNamespaces := fc.volumeController.GetContainerRootsNamespaces()

	for _, containerVolume := range pvcKeyToContainerVolumes[pvcKey] {
		expectedContainerRoots[containerVolume.mountNamespace] =
			fuse.ContainerRootString{
				RootPath:           containerVolume.rootPath,
				AllowedExecutables: expectedAllowedExecutables,
			}
	}
	assert.True(t, len(mountNamespaces) > 0)

	for mountNamespace := range mountNamespaces {
		containerRoot := fc.volumeController.GetContainerRootString(
			mountNamespace)
		assert.Equal(
			t, expectedContainerRoots[mountNamespace].RootPath,
			containerRoot.RootPath)

		expectedContainerRoot, found := expectedContainerRoots[mountNamespace]
		assert.True(t, found)

		assert.Equal(
			t, expectedContainerRoot.AllowedExecutables,
			containerRoot.AllowedExecutables)
	}

	return startRes, nil
}

func postCheckStarts(
	t *testing.T, volumeController *VolumeController,
	expectedMountNamespaces map[string]map[string]struct{}) {
	assert.Equal(
		t, len(expectedMountNamespaces), len(volumeController.fuseControllers))
	for mountPoint, expectedNamespaces := range expectedMountNamespaces {
		namespaces := map[string]struct{}{}
		for _, namespace := range volumeController.fuseControllers[mountPoint].mountNamespaces {
			namespaces[namespace] = struct{}{}
		}
		assert.True(t, assert.ObjectsAreEqual(expectedNamespaces, namespaces))
	}
}

func testStop(
	t *testing.T, volumeController *VolumeController, pod *corev1.Pod,
	mountPointToExecutablePaths map[string]*api.ExecutablePaths,
	mountPoint string, expectError bool) {
	stopReq := &api.StopRequest{
		PodNamespace:                pod.Namespace,
		PodName:                     pod.Name,
		MountPointToExecutablePaths: mountPointToExecutablePaths,
		MountPoint:                  mountPoint,
	}
	ctx := context.Background()

	_, err := volumeController.Stop(ctx, stopReq)

	if expectError {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
	}
}

func postCheckStops(
	t *testing.T, volumeController *VolumeController,
	expectedContainerRoots map[string]fuse.ContainerRootString) {
	for _, fc := range volumeController.fuseControllers {
		fuseController := fc.volumeController
		mountNamespaces := fuseController.GetContainerRootsNamespaces()

		assert.Equal(t, 0, len(mountNamespaces)-len(fc.allowedMountNamespaces))
		assert.Equal(t, 0, len(fc.mountNamespaces))

		for namespace := range mountNamespaces {
			containerRoot := fuseController.GetContainerRootString(namespace)
			assert.Equal(t, expectedContainerRoots[namespace], containerRoot)
		}
	}
}

func createAllowedExecutables(
	executables []executable) (map[string]*api.Executable, []string) {
	lifetimeExecutables := map[string]*api.Executable{}
	executablePaths := make([]string, 0, len(executables))
	for _, test := range executables {
		lifetimeExecutables[test.path] = &api.Executable{
			Checksum: test.checksum,
			Writable: test.writable,
		}
		executablePaths = append(executablePaths, test.path)
	}

	return lifetimeExecutables, executablePaths
}

func createPodVolumes(
	t *testing.T, podNamespace string, podVolumes []podVolume) (
	[]k8sruntime.Object, []corev1.Volume) {
	kubeObjects := make([]k8sruntime.Object, 0, 2*len(podVolumes))
	volumes := make([]corev1.Volume, 0, len(podVolumes))

	for _, podVolume := range podVolumes {
		pv, pvc, podVolume := createVolume(
			t, podNamespace, podVolume.podVolumeName, podVolume.pvName,
			podVolume.pvcName, podVolume.pvSource)

		kubeObjects = append(kubeObjects, pv, pvc)
		volumes = append(volumes, podVolume)
	}

	return kubeObjects, volumes
}

func getInitializeRequestType(t *testing.T, pvSourceKind uint) uint {
	switch pvSourceKind {
	case pvSourceLocal:
		return localInitReq
	case pvSourceCsi:
		return csiInitReq
	default:
		assert.Fail(
			t, "Not support such a type of the persistent volume source: %v",
			pvSourceKind)
		return noInitReq
	}
}

func testStartAndStop(
	t *testing.T, allowedExecutables []*api.ExternalAllowedExecutable,
	expectedContainerRootsString map[string]fuse.ContainerRootString,
	messageQueuePublishEnabled bool) {
	podNamespace := metav1.NamespaceDefault
	podVolumes := []podVolume{
		{
			pvcName:       "pvc2",
			pvName:        "pv2",
			podVolumeName: "pod1vol2",
			pvSource: &persistentVolumeSource{
				kind:            pvSourceCsi,
				csiVolumeHandle: csiVolumeHandle,
			},
			fuseSourceDir: fuseSourceDir2,
		},
	}

	kubeObjects, volumes := createPodVolumes(t, podNamespace, podVolumes)

	podName := "pod1"
	containerIds := []string{
		"container1",
	}
	pod := newPod(podNamespace, podName, volumes, containerIds)
	kubeObjects = append(kubeObjects, pod)

	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	mountNamespaceId := "mnt:[xxx]"
	containers := []containerInfo{
		{
			id:             containerIds[0],
			mountNamespace: mountNamespaceId,
		},
	}
	fakeRuntimeClient := newFakeRuntimeClient(containers)

	volumeController := newFakeVolumeController(kubeClient, fakeRuntimeClient)

	mountPoints := []string{}
	dataContainerName := "data-c"
	testExecutables := []executable{
		{
			path:     "/usr/bin/x",
			checksum: []byte{0x01, 0x23},
			writable: true,
		},
		{
			path:     "/usr/local/bin/y",
			checksum: []byte{0x45, 0x67, 0x89},
			writable: false,
		},
	}
	executables, executablePaths := createAllowedExecutables(testExecutables)
	expectedMountNamespaces := map[string]map[string]struct{}{}
	pvcKeyToContainerVolumes := map[string][]*containerVolume{}
	messageQueueConfig := (*api.MessageQueue)(nil)

	if messageQueuePublishEnabled {
		brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
		messageQueueConfig = &api.MessageQueue{
			Brokers:                        []string{brokerAddress},
			User:                           saslUser,
			Password:                       saslPassword,
			Topic:                          updateTopic,
			CompressionCodec:               mqpub.CompressionCodecNone,
			MaxBatchBytes:                  "0",
			UpdatePublishChannelBufferSize: "10",
		}
	}

	createFuseSourceDirectory(t, podVolumes[0].fuseSourceDir)

	initReq := getInitializeRequestType(t, podVolumes[0].pvSource.kind)
	fuseMountPoint, err := testInitialize(
		t, volumeController, podVolumes[0].fuseSourceDir, initReq,
		allowedExecutables, expectedContainerRootsString, false)
	assert.NoError(t, err)
	mountPoints = append(mountPoints, fuseMountPoint)

	expectedMountNamespaces[fuseMountPoint] = map[string]struct{}{
		containers[0].mountNamespace: struct{}{},
	}

	pvcKey := util.ConcatenateNamespaceAndName(
		podNamespace, podVolumes[0].pvcName)

	expectedMountPointInfo := &api.StartResponse{
		MountPoint: fuseMountPoint,
	}
	if executables != nil {
		expectedMountPointInfo.UsageControlEnabled = true
	}
	if messageQueuePublishEnabled {
		expectedMountPointInfo.MessageQueuePublisherEnabled = true
	}

	pvcKeyToContainerVolumes[pvcKey] = []*containerVolume{
		{
			rootPath:       createContainerRootFs(containers[0].id),
			mountNamespace: containers[0].mountNamespace,
		},
	}

	mountPointInfo, err := testStart(
		t, volumeController, pod, dataContainerName, pvcKey, executables,
		messageQueueConfig, pvcKeyToContainerVolumes, expectedMountPointInfo,
		expectedContainerRootsString, false)
	assert.NoError(t, err)

	postCheckStarts(t, volumeController, expectedMountNamespaces)

	mountPointToExecutablePaths := map[string]*api.ExecutablePaths{}
	mountPoint := mountPointInfo.MountPoint
	mountPointToExecutablePaths[mountPoint] = &api.ExecutablePaths{
		Paths: executablePaths,
	}
	mqMountPoint := ""
	if messageQueuePublishEnabled {
		mqMountPoint = mountPoint
	}

	testStop(
		t, volumeController, pod, mountPointToExecutablePaths, mqMountPoint,
		false)
	postCheckStops(t, volumeController, expectedContainerRootsString)

	for _, mountPoint := range mountPoints {
		err := testFinalize(t, volumeController, mountPoint, false, false)
		assert.NoError(t, err)

		assert.Nil(t, volumeController.fuseControllers[mountPoint])
	}

	deleteTestFuseDirectories(t)
}

func TestStartAndStop(t *testing.T) {
	testStartAndStop(t, nil, nil, false)
}

func TestStartAndStopWithMessageQueuePublisher(t *testing.T) {
	testStartAndStop(t, nil, nil, true)
}

func TestStartAndStopWithAllowedExecutables(t *testing.T) {
	allowedExecutables,
		expectedContainerRootsString := createExternalAllowedExecutables(t)

	testStartAndStop(
		t, allowedExecutables, expectedContainerRootsString, false)
}

func TestForcefulDeleteAllowedExecutablesAndContainerRoots(t *testing.T) {
	podNamespace := metav1.NamespaceDefault

	pvcName := "pvc1"
	pvName := "pv1"
	podVolumeName := "pod1vol1"
	pvSource := &persistentVolumeSource{
		kind:      pvSourceLocal,
		localPath: fuseMountPointHostDir,
	}
	pv, pvc, podVolume := createVolume(
		t, podNamespace, podVolumeName, pvName, pvcName, pvSource)

	podName := "pod1"
	containerIds := []string{
		"container1",
	}
	volumes := []corev1.Volume{
		podVolume,
	}
	pod := newPod(podNamespace, podName, volumes, containerIds)

	kubeObjects := []k8sruntime.Object{
		pv, pvc, pod,
	}
	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	containers := []containerInfo{
		{
			id:             containerIds[0],
			mountNamespace: "mnt:[xxx]",
		},
	}
	fakeRuntimeClient := newFakeRuntimeClient(containers)

	volumeController := newFakeVolumeController(kubeClient, fakeRuntimeClient)

	createFuseSourceDirectory(t, fuseSourceDir1)

	fuseMountPoint, err := testInitialize(
		t, volumeController, fuseSourceDir1, localInitReq, nil, nil, false)
	assert.NoError(t, err)

	dataContainerName := "data-c"
	pvcKey := util.ConcatenateNamespaceAndName(
		podNamespace, pvcName)
	expectedMountPointInfo := &api.StartResponse{
		MountPoint:          fuseMountPoint,
		UsageControlEnabled: true,
	}
	testExecutables := []executable{
		{
			path:     "/usr/bin/x",
			checksum: []byte{0x01, 0x23},
			writable: true,
		},
		{
			path:     "/usr/local/bin/y",
			checksum: []byte{0x45, 0x67, 0x89},
			writable: false,
		},
	}
	executables, _ := createAllowedExecutables(testExecutables)

	pvcKeyToContainerVolumes := map[string][]*containerVolume{
		pvcKey: []*containerVolume{
			{
				rootPath:       createContainerRootFs(containers[0].id),
				mountNamespace: containers[0].mountNamespace,
			},
		},
	}

	_, _ = testStart(
		t, volumeController, pod, dataContainerName, pvcKey, executables,
		nil, pvcKeyToContainerVolumes, expectedMountPointInfo, nil, false)

	expectedMountNamespaces := map[string]map[string]struct{}{
		fuseMountPoint: map[string]struct{}{
			containers[0].mountNamespace: struct{}{},
		},
	}
	postCheckStarts(t, volumeController, expectedMountNamespaces)

	err = testFinalize(t, volumeController, fuseMountPoint, true, false)
	assert.NoError(t, err)

	postCheckStops(t, volumeController, nil)
	assert.Nil(
		t, volumeController.fuseControllers[fuseMountPoint])

	deleteTestFuseDirectories(t)
}

func TestStartForPersistentVolumeClaimKeyNotFound(t *testing.T) {
	podNamespace := metav1.NamespaceDefault

	pvcName := "pvc1"
	pvName := "pv1"
	podVolumeName := "pod1vol1"
	pvSource := &persistentVolumeSource{
		kind:      pvSourceLocal,
		localPath: fuseMountPointHostDir,
	}
	pv, pvc, podVolume := createVolume(
		t, podNamespace, podVolumeName, pvName, pvcName, pvSource)

	podName := "pod1"
	volumes := []corev1.Volume{
		podVolume,
	}
	pod := newPod(podNamespace, podName, volumes, nil)

	kubeObjects := []k8sruntime.Object{
		pv, pvc, pod,
	}
	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	fakeRuntimeClient := newFakeRuntimeClient(nil)

	volumeController := newFakeVolumeController(kubeClient, fakeRuntimeClient)

	dummyPvcName := "pvc1"
	pvcKeyNotFound := util.ConcatenateNamespaceAndName(
		podNamespace, dummyPvcName)
	dataContainerName := "data-c"

	testExecutables := []executable{
		{
			path:     "/usr/bin/x",
			checksum: []byte{0x01, 0x23},
			writable: true,
		},
		{
			path:     "/usr/local/bin/y",
			checksum: []byte{0x45, 0x67, 0x89},
			writable: false,
		},
	}
	executables, _ := createAllowedExecutables(testExecutables)

	_, err := testStart(
		t, volumeController, pod, dataContainerName, pvcKeyNotFound,
		executables, nil, nil, nil, nil, true)

	pvcKey := util.ConcatenateNamespaceAndName(podNamespace, pvcName)
	pvcNames := []string{
		dummyPvcName,
	}
	errMsg := fmt.Sprintf(
		"[%s] The mount point for one of the persistent volume claims (%+v) "+
			"does not include in the fuse controllers: %+v",
		pvcKey, pvcNames, volumeController.fuseControllers)
	assert.EqualError(t, err, errMsg)
}

func newFakeFailedRuntimeClient(
	containerId string) runtime.ContainerRuntimeOperations {
	fakeRuntimeClient := new(fakeruntime.FakeRuntimeClient)

	fakeRuntimeClient.On(
		"GetContainerInfo", containerId).Return(
		(*runtime.ContainerInfo)(nil),
		fmt.Errorf(
			"Not find the container information on %q", containerId))

	return fakeRuntimeClient
}

func TestStartForContainerInfoNotFound(t *testing.T) {
	podNamespace := metav1.NamespaceDefault

	pvcName := "pvc1"
	pvName := "pv1"
	podVolumeName := "pod1vol1"
	pvSource := &persistentVolumeSource{
		kind:      pvSourceLocal,
		localPath: fuseMountPointHostDir,
	}
	pv, pvc, podVolume := createVolume(
		t, podNamespace, podVolumeName, pvName, pvcName, pvSource)

	podName := "pod1"
	containerIds := []string{
		"container1",
	}
	volumes := []corev1.Volume{
		podVolume,
	}
	pod := newPod(podNamespace, podName, volumes, containerIds)

	kubeObjects := []k8sruntime.Object{
		pv, pvc, pod,
	}
	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	fakeRuntimeClient := newFakeFailedRuntimeClient(containerIds[0])

	volumeController := newFakeVolumeController(kubeClient, fakeRuntimeClient)

	createFuseSourceDirectory(t, fuseSourceDir1)

	fuseMountPoint, err := testInitialize(
		t, volumeController, fuseSourceDir1, localInitReq, nil, nil, false)
	assert.NoError(t, err)

	dataContainerName := "data-c"
	testExecutables := []executable{
		{
			path:     "/usr/bin/x",
			checksum: []byte{0x01, 0x23},
			writable: true,
		},
		{
			path:     "/usr/local/bin/y",
			checksum: []byte{0x45, 0x67, 0x89},
			writable: false,
		},
	}
	executables, _ := createAllowedExecutables(testExecutables)

	pvcKey := util.ConcatenateNamespaceAndName(
		podNamespace, pvcName)

	_, err = testStart(
		t, volumeController, pod, dataContainerName, pvcKey, executables, nil,
		nil, nil, nil, true)

	errMsg := fmt.Sprintf(
		"Not find the container information on %q", containerIds[0])
	assert.EqualError(t, err, errMsg)

	fuseController := volumeController.fuseControllers[fuseMountPoint].
		volumeController
	namespaces := fuseController.GetContainerRootsNamespaces()
	assert.Equal(t, 0, len(namespaces))

	err = testFinalize(t, volumeController, fuseMountPoint, true, false)
	assert.NoError(t, err)

	postCheckStops(t, volumeController, nil)
	assert.Nil(
		t, volumeController.fuseControllers[fuseMountPoint])

	deleteTestFuseDirectories(t)
}

func createAllowedExecutablesDiffs(
	executables []executable, executablePaths []string) *api.ExecutablesDiffs {
	addedAndUpdated := map[string]*api.Executable{}
	for _, executable := range executables {
		addedAndUpdated[executable.path] = &api.Executable{
			Checksum: executable.checksum,
			Writable: executable.writable,
		}
	}

	return &api.ExecutablesDiffs{
		AddedAndUpdated: addedAndUpdated,
		DeletedPaths:    executablePaths,
	}
}

func testUpdateAllowedExecutables(
	t *testing.T, volumeController *VolumeController, pod *corev1.Pod,
	executablesDiffs map[string]*api.ExecutablesDiffs) map[string]string {
	updateAEReq := &api.UpdateAllowedExecutablesRequest{
		PodNamespace:     pod.Namespace,
		PodName:          pod.Name,
		ExecutablesDiffs: executablesDiffs,
	}

	ctx := context.Background()
	updateAERes, err := volumeController.UpdateAllowedExecutables(
		ctx, updateAEReq)
	assert.NoError(t, err)

	return updateAERes.PvcKeyToMountPoint
}

func TestUpdateAllowedExecutables(t *testing.T) {
	podNamespace := metav1.NamespaceDefault

	pvcName := "pvc1"
	pvName := "pv1"
	podVolumeName := "pod1vol1"
	pvSource := &persistentVolumeSource{
		kind:      pvSourceLocal,
		localPath: fuseMountPointHostDir,
	}
	pv, pvc, podVolume := createVolume(
		t, podNamespace, podVolumeName, pvName, pvcName, pvSource)

	podName := "pod1"
	containerIds := []string{
		"container1",
	}
	volumes := []corev1.Volume{
		podVolume,
	}
	pod := newPod(podNamespace, podName, volumes, containerIds)

	kubeObjects := []k8sruntime.Object{
		pv, pvc, pod,
	}
	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	containers := []containerInfo{
		{
			id:             containerIds[0],
			mountNamespace: "mnt:[xxx]",
		},
	}
	fakeRuntimeClient := newFakeRuntimeClient(containers)

	volumeController := newFakeVolumeController(kubeClient, fakeRuntimeClient)

	createFuseSourceDirectory(t, fuseSourceDir1)

	fuseMountPoint, err := testInitialize(
		t, volumeController, fuseSourceDir1, localInitReq, nil, nil, false)
	assert.NoError(t, err)

	dataContainerName := "data-c"
	startExecutables := []executable{
		{
			path:     "/usr/bin/x",
			checksum: []byte{0x01, 0x23},
			writable: true,
		},
		{
			path:     "/usr/local/bin/y",
			checksum: []byte{0x45, 0x67, 0x89},
			writable: false,
		},
	}
	executables, _ := createAllowedExecutables(startExecutables)
	pvcKey := util.ConcatenateNamespaceAndName(
		podNamespace, pvcName)
	startMountPointInfo := &api.StartResponse{
		MountPoint:          fuseMountPoint,
		UsageControlEnabled: true,
	}

	pvcKeyToContainerVolumes := map[string][]*containerVolume{
		pvcKey: []*containerVolume{
			{
				rootPath:       createContainerRootFs(containers[0].id),
				mountNamespace: containers[0].mountNamespace,
			},
		},
	}

	_, _ = testStart(
		t, volumeController, pod, dataContainerName, pvcKey, executables, nil,
		pvcKeyToContainerVolumes, startMountPointInfo, nil, false)

	expectedMountNamespaces := map[string]map[string]struct{}{
		fuseMountPoint: map[string]struct{}{
			containers[0].mountNamespace: struct{}{},
		},
	}
	postCheckStarts(t, volumeController, expectedMountNamespaces)

	updateExecutables := []executable{
		{
			path:     "/usr/bin/x",
			checksum: []byte{0x01, 0x23},
			writable: false,
		},
		{
			path:     "/usr/sbin/z",
			checksum: []byte{0xab, 0xcd, 0xef},
			writable: false,
		},
	}
	updateExecutablePaths := []string{
		"/usr/local/bin/y",
	}

	executables, executablePaths := createAllowedExecutables(updateExecutables)
	execsDiffs := createAllowedExecutablesDiffs(
		updateExecutables, updateExecutablePaths)
	executablesDiffs := map[string]*api.ExecutablesDiffs{
		pvcKey: execsDiffs,
	}

	expectedUpdatePvcKeyToMountPoint := map[string]string{
		pvcKey: fuseMountPoint,
	}
	updatePvcKeyToMountPoint := testUpdateAllowedExecutables(
		t, volumeController, pod, executablesDiffs)
	assert.Equal(t, expectedUpdatePvcKeyToMountPoint, updatePvcKeyToMountPoint)

	mountPointToExecutablePaths := map[string]*api.ExecutablePaths{
		updatePvcKeyToMountPoint[pvcKey]: {
			Paths: executablePaths,
		},
	}
	testStop(t, volumeController, pod, mountPointToExecutablePaths, "", false)
	postCheckStops(t, volumeController, nil)

	err = testFinalize(t, volumeController, fuseMountPoint, false, false)
	assert.NoError(t, err)

	assert.Nil(t, volumeController.fuseControllers[fuseMountPoint])

	deleteTestFuseDirectories(t)
}

func testUpdateMessageQueueConfig(
	t *testing.T, volumeController *VolumeController, pod *corev1.Pod,
	messageQueueDiffs map[string]*api.MessageQueue) map[string]string {
	updateMQCReq := &api.UpdateMessageQueueConfigRequest{
		PodNamespace:      pod.Namespace,
		PodName:           pod.Name,
		MessageQueueDiffs: messageQueueDiffs,
	}

	ctx := context.Background()
	updateMQCRes, err := volumeController.UpdateMessageQueueConfig(
		ctx, updateMQCReq)
	assert.NoError(t, err)

	return updateMQCRes.PvcKeyToMountPoint
}

func TestUpdateMessageQueueConfig(t *testing.T) {
	podNamespace := metav1.NamespaceDefault

	pvcName := "pvc1"
	pvName := "pv1"
	podVolumeName := "pod1vol1"
	pvSource := &persistentVolumeSource{
		kind:      pvSourceLocal,
		localPath: fuseMountPointHostDir,
	}
	pv, pvc, podVolume := createVolume(
		t, podNamespace, podVolumeName, pvName, pvcName, pvSource)

	podName := "pod1"
	containerIds := []string{
		"container1",
	}
	volumes := []corev1.Volume{
		podVolume,
	}
	pod := newPod(podNamespace, podName, volumes, containerIds)

	kubeObjects := []k8sruntime.Object{
		pv, pvc, pod,
	}
	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	containers := []containerInfo{
		{
			id:             containerIds[0],
			mountNamespace: "mnt:[xxx]",
		},
	}
	fakeRuntimeClient := newFakeRuntimeClient(containers)

	volumeController := newFakeVolumeController(kubeClient, fakeRuntimeClient)

	createFuseSourceDirectory(t, fuseSourceDir1)

	fuseMountPoint, err := testInitialize(
		t, volumeController, fuseSourceDir1, localInitReq, nil, nil, false)
	assert.NoError(t, err)

	dataContainerName := "data-c"
	startExecutables := []executable{
		{
			path:     "/usr/bin/x",
			checksum: []byte{0x01, 0x23},
			writable: true,
		},
		{
			path:     "/usr/local/bin/y",
			checksum: []byte{0x45, 0x67, 0x89},
			writable: false,
		},
	}
	executables, executablePaths := createAllowedExecutables(startExecutables)
	pvcKey := util.ConcatenateNamespaceAndName(
		podNamespace, pvcName)
	startMountPointInfo := &api.StartResponse{
		MountPoint:          fuseMountPoint,
		UsageControlEnabled: true,
	}

	pvcKeyToContainerVolumes := map[string][]*containerVolume{
		pvcKey: []*containerVolume{
			{
				rootPath:       createContainerRootFs(containers[0].id),
				mountNamespace: containers[0].mountNamespace,
			},
		},
	}

	_, _ = testStart(
		t, volumeController, pod, dataContainerName, pvcKey, executables, nil,
		pvcKeyToContainerVolumes, startMountPointInfo, nil, false)

	expectedMountNamespaces := map[string]map[string]struct{}{
		fuseMountPoint: map[string]struct{}{
			containers[0].mountNamespace: struct{}{},
		},
	}
	postCheckStarts(t, volumeController, expectedMountNamespaces)

	expectedUpdatePvcKeyToMountPoint := map[string]string{
		pvcKey: fuseMountPoint,
	}

	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	messageQueueDiffs := map[string]*api.MessageQueue{
		pvcKey: {
			Brokers:                        []string{brokerAddress},
			User:                           saslUser,
			Password:                       saslPassword,
			Topic:                          updateTopic,
			CompressionCodec:               mqpub.CompressionCodecNone,
			MaxBatchBytes:                  "0",
			UpdatePublishChannelBufferSize: "10",
		},
	}
	updatePvcKeyToMountPoint := testUpdateMessageQueueConfig(
		t, volumeController, pod, messageQueueDiffs)
	assert.Equal(t, expectedUpdatePvcKeyToMountPoint, updatePvcKeyToMountPoint)

	mountPointToExecutablePaths := map[string]*api.ExecutablePaths{
		updatePvcKeyToMountPoint[pvcKey]: {
			Paths: executablePaths,
		},
	}
	testStop(
		t, volumeController, pod, mountPointToExecutablePaths, fuseMountPoint,
		false)
	postCheckStops(t, volumeController, nil)

	err = testFinalize(t, volumeController, fuseMountPoint, false, false)
	assert.NoError(t, err)

	assert.Nil(t, volumeController.fuseControllers[fuseMountPoint])

	deleteTestFuseDirectories(t)
}

func createPodsInfo(
	t *testing.T) ([]k8sruntime.Object, []containerInfo, []podFuseMount,
	map[string][]*containerVolume, map[string]map[string]struct{}) {
	podNamespace := metav1.NamespaceDefault
	pvcName := "pvc1"
	pvName := "pv1"
	podVolumeName := "pod1vol1"
	pvSource := &persistentVolumeSource{
		kind:            pvSourceCsi,
		csiVolumeHandle: csiVolumeHandle,
	}
	mountPointPath := csiMountPointDir
	pvcKey := util.ConcatenateNamespaceAndName(podNamespace, pvcName)

	pv, pvc, podVolume := createVolume(
		t, podNamespace, podVolumeName, pvName, pvcName, pvSource)

	testPods := []struct {
		podName          string
		containerIds     []string
		mountNamespaceId string
	}{
		{
			podName: "pod1-1",
			containerIds: []string{
				"container1-1",
			},
			mountNamespaceId: "mnt:[xxx]",
		},
		{
			podName: "pod1-2",
			containerIds: []string{
				"container1-2",
			},
			mountNamespaceId: "mnt:[yyy]",
		},
	}

	kubeObjects := make([]k8sruntime.Object, 0, 2+len(testPods))
	kubeObjects = append(kubeObjects, pv, pvc)

	containers := make([]containerInfo, 0, len(testPods))
	fuseMounts := make([]podFuseMount, 0, len(testPods))
	pvcKeyToContainerVolumes := map[string][]*containerVolume{}
	expectedMountNamespaces := map[string]map[string]struct{}{}

	for _, tPod := range testPods {
		volumes := []corev1.Volume{
			podVolume,
		}
		pod := newPod(podNamespace, tPod.podName, volumes, tPod.containerIds)

		kubeObjects = append(kubeObjects, pod)

		container := containerInfo{
			id:             tPod.containerIds[0],
			mountNamespace: tPod.mountNamespaceId,
		}
		containers = append(containers, container)

		fuseMounts = append(
			fuseMounts,
			podFuseMount{
				pod:        pod,
				containers: []containerInfo{container},
				fuseMount: startFuseMount{
					pvcKey:        pvcKey,
					mountPointDir: mountPointPath,
				},
			})

		containerVolumes, found := pvcKeyToContainerVolumes[pvcKey]
		if found {
			containerVolumes = append(
				containerVolumes,
				&containerVolume{
					rootPath:       createContainerRootFs(container.id),
					mountNamespace: container.mountNamespace,
				})

			pvcKeyToContainerVolumes[pvcKey] = containerVolumes
		} else {
			pvcKeyToContainerVolumes[pvcKey] = []*containerVolume{
				{
					rootPath:       createContainerRootFs(container.id),
					mountNamespace: container.mountNamespace,
				},
			}
		}

		mountNamespaces, found := expectedMountNamespaces[mountPointPath]
		if found {
			mountNamespaces[tPod.mountNamespaceId] = struct{}{}
		} else {
			expectedMountNamespaces[mountPointPath] = map[string]struct{}{
				tPod.mountNamespaceId: struct{}{},
			}
		}
	}

	return kubeObjects, containers, fuseMounts, pvcKeyToContainerVolumes,
		expectedMountNamespaces
}

func TestStartsAndStopsForPods(t *testing.T) {
	kubeObjects, containers, fuseMounts, pvcKeyToContainerVolumes,
		expectedMountNamespaces := createPodsInfo(t)

	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	fakeRuntimeClient := newFakeRuntimeClient(containers)

	volumeController := newFakeVolumeController(kubeClient, fakeRuntimeClient)

	createFuseSourceDirectory(t, fuseSourceDir1)
	createFuseSourceDirectory(t, fuseSourceDir2)

	fuseMountPoint1, err := testInitialize(
		t, volumeController, fuseSourceDir1, csiInitReq, nil, nil, false)
	assert.NoError(t, err)

	dataContainerName := "data-c"
	testExecutables := []executable{
		{
			path:     "/usr/bin/x",
			checksum: []byte{0x01, 0x23},
			writable: true,
		},
		{
			path:     "/usr/local/bin/y",
			checksum: []byte{0x45, 0x67, 0x89},
			writable: false,
		},
	}
	executables, executablePaths := createAllowedExecutables(testExecutables)

	wg := new(sync.WaitGroup)
	mountPointInfo := newMountPointInfo()

	for i, mount := range fuseMounts {
		wg.Add(1)
		go func(pfm podFuseMount, idx int) {
			defer wg.Done()

			expectedMountPointInfo := &api.StartResponse{
				MountPoint:                   pfm.fuseMount.mountPointDir,
				UsageControlEnabled:          true,
				MessageQueuePublisherEnabled: true,
			}

			brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
			messageQueueConfig := &api.MessageQueue{
				Brokers:                        []string{brokerAddress},
				User:                           saslUser,
				Password:                       saslPassword,
				Topic:                          updateTopic,
				CompressionCodec:               mqpub.CompressionCodecNone,
				MaxBatchBytes:                  "0",
				UpdatePublishChannelBufferSize: "10",
			}

			mpi, _ := testStart(
				t, volumeController, pfm.pod, dataContainerName,
				pfm.fuseMount.pvcKey, executables, messageQueueConfig,
				pvcKeyToContainerVolumes, expectedMountPointInfo, nil, false)

			mountPointInfo.add(pfm.pod, pfm.fuseMount.pvcKey, mpi)
		}(mount, i)
	}
	wg.Wait()

	postCheckStarts(t, volumeController, expectedMountNamespaces)

	for pod, mpi := range mountPointInfo.getWithoutMutex() {
		mountPointToExecutablePaths := map[string]*api.ExecutablePaths{}
		for _, mountPoint := range mpi {
			wg.Add(1)

			mPoint := mountPoint.MountPoint
			mountPointToExecutablePaths[mPoint] = &api.ExecutablePaths{
				Paths: executablePaths,
			}

			go func(p *corev1.Pod) {
				testStop(
					t, volumeController, p, mountPointToExecutablePaths,
					mPoint, false)
				wg.Done()
			}(pod)
		}
	}
	wg.Wait()

	postCheckStops(t, volumeController, nil)

	err = testFinalize(t, volumeController, fuseMountPoint1, false, false)
	assert.NoError(t, err)
	assert.Nil(t, volumeController.fuseControllers[fuseMountPoint1])

	deleteTestFuseDirectories(t)
}

func createVolumesInfo(
	t *testing.T, podNamespace string, container *containerInfo) (
	[]corev1.Volume, []k8sruntime.Object, []startFuseMount,
	map[string][]*containerVolume) {
	testVolumes := []struct {
		pvcName        string
		pvName         string
		podVolumeName  string
		mountPointPath string
		pvSource       *persistentVolumeSource
	}{
		{
			pvcName:        "pvc1",
			pvName:         "pv1",
			podVolumeName:  "pod1vol1",
			mountPointPath: localMountPointDir,
			pvSource: &persistentVolumeSource{
				kind:      pvSourceLocal,
				localPath: fuseMountPointHostDir,
			},
		},
		{
			pvcName:        "pvc2",
			pvName:         "pv2",
			podVolumeName:  "pod2vol1",
			mountPointPath: csiMountPointDir,
			pvSource: &persistentVolumeSource{
				kind:            pvSourceCsi,
				csiVolumeHandle: csiVolumeHandle,
			},
		},
	}

	volumes := make([]corev1.Volume, 0, len(testVolumes))
	kubeObjects := make([]k8sruntime.Object, 0, 2*len(testVolumes)+1)
	fuseMounts := make([]startFuseMount, 0, len(testVolumes))
	pvcKeyToContainerVolumes := map[string][]*containerVolume{}

	for _, tVolume := range testVolumes {
		pv, pvc, podVolume := createVolume(
			t, podNamespace, tVolume.podVolumeName, tVolume.pvName,
			tVolume.pvcName, tVolume.pvSource)

		volumes = append(volumes, podVolume)

		kubeObjects = append(kubeObjects, pv, pvc)

		pvcKey := util.ConcatenateNamespaceAndName(
			podNamespace, tVolume.pvcName)
		fuseMounts = append(
			fuseMounts,
			startFuseMount{
				pvcKey:        pvcKey,
				mountPointDir: tVolume.mountPointPath,
			})

		pvcKeyToContainerVolumes[pvcKey] = []*containerVolume{
			{
				rootPath:       createContainerRootFs(container.id),
				mountNamespace: container.mountNamespace,
			},
		}

	}

	return volumes, kubeObjects, fuseMounts, pvcKeyToContainerVolumes
}

func TestStartsAndStopsForPodVolumes(t *testing.T) {
	podNamespace := metav1.NamespaceDefault
	podName := "pod1"
	containerIds := []string{
		"container1",
	}
	mountNamespaceId := "mnt:[xxx]"
	containers := []containerInfo{
		{
			id:             containerIds[0],
			mountNamespace: mountNamespaceId,
		},
		{
			id:             containerIds[0],
			mountNamespace: mountNamespaceId,
		},
	}

	volumes, kubeObjects, fuseMounts, pvcKeyToContainerVolumes :=
		createVolumesInfo(t, podNamespace, &containers[0])

	pod := newPod(podNamespace, podName, volumes, containerIds)
	kubeObjects = append(kubeObjects, pod)

	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	fakeRuntimeClient := newFakeRuntimeClient(containers)

	volumeController := newFakeVolumeController(kubeClient, fakeRuntimeClient)

	createFuseSourceDirectory(t, fuseSourceDir1)
	createFuseSourceDirectory(t, fuseSourceDir2)

	fuseMountPoint1, err := testInitialize(
		t, volumeController, fuseSourceDir1, localInitReq, nil, nil, false)
	assert.NoError(t, err)
	fuseMountPoint2, err := testInitialize(
		t, volumeController, fuseSourceDir2, csiInitReq, nil, nil, false)
	assert.NoError(t, err)

	dataContainerName := "data-c"
	testExecutables := []executable{
		{
			path:     "/usr/bin/x",
			checksum: []byte{0x01, 0x23},
			writable: true,
		},
		{
			path:     "/usr/local/bin/y",
			checksum: []byte{0x45, 0x67, 0x89},
			writable: false,
		},
	}
	executables, executablePaths := createAllowedExecutables(testExecutables)

	wg := new(sync.WaitGroup)
	mountPointInfo := newMountPointInfo()

	for index, mount := range fuseMounts {
		wg.Add(1)
		go func(sfm startFuseMount, idx int) {
			defer wg.Done()

			expectedMountPointInfo := &api.StartResponse{
				MountPoint:                   sfm.mountPointDir,
				UsageControlEnabled:          true,
				MessageQueuePublisherEnabled: true,
			}

			brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
			messageQueueConfig := &api.MessageQueue{
				Brokers:  []string{brokerAddress},
				User:     saslUser,
				Password: saslPassword,
				Topic: fmt.Sprintf(
					"%s.%d", updateTopic, idx),
				CompressionCodec:               mqpub.CompressionCodecNone,
				MaxBatchBytes:                  "0",
				UpdatePublishChannelBufferSize: "10",
			}

			mpi, _ := testStart(
				t, volumeController, pod, dataContainerName, sfm.pvcKey,
				executables, messageQueueConfig,
				pvcKeyToContainerVolumes, expectedMountPointInfo, nil, false)

			mountPointInfo.add(pod, sfm.pvcKey, mpi)
		}(mount, index)
	}
	wg.Wait()

	expectedMountNamespaces := map[string]map[string]struct{}{
		localMountPointDir: map[string]struct{}{
			mountNamespaceId: struct{}{},
		},
		csiMountPointDir: map[string]struct{}{
			mountNamespaceId: struct{}{},
		},
	}
	postCheckStarts(t, volumeController, expectedMountNamespaces)

	for pod, mpi := range mountPointInfo.getWithoutMutex() {
		mountPointToExecutablePaths := map[string]*api.ExecutablePaths{}
		for _, mountPoint := range mpi {
			wg.Add(1)

			mPoint := mountPoint.MountPoint
			mountPointToExecutablePaths[mPoint] = &api.ExecutablePaths{
				Paths: executablePaths,
			}

			go func(p *corev1.Pod) {
				testStop(
					t, volumeController, p, mountPointToExecutablePaths,
					mPoint, false)
				wg.Done()
			}(pod)
		}
	}
	wg.Wait()

	postCheckStops(t, volumeController, nil)

	err = testFinalize(t, volumeController, fuseMountPoint1, false, false)
	assert.NoError(t, err)
	assert.Nil(t, volumeController.fuseControllers[fuseMountPoint1])

	err = testFinalize(t, volumeController, fuseMountPoint2, false, false)
	assert.NoError(t, err)
	assert.Nil(t, volumeController.fuseControllers[fuseMountPoint2])

	deleteTestFuseDirectories(t)
}

func createContainersInfo(
	pvcKey string) ([]containerInfo, []string, []string,
	map[string][]*containerVolume) {
	testContainers := []struct {
		containerId      string
		mountNamespaceId string
	}{
		{
			containerId:      "container1",
			mountNamespaceId: "mnt:[xxx]",
		},
		{
			containerId:      "container2",
			mountNamespaceId: "mnt:[yyy]",
		},
	}

	containers := make([]containerInfo, 0, len(testContainers))
	containerIds := make([]string, 0, len(testContainers))
	mountNamespaces := make([]string, 0, len(testContainers))
	pvcKeyToContainerVolumes := map[string][]*containerVolume{}

	for _, test := range testContainers {
		container := containerInfo{
			id:             test.containerId,
			mountNamespace: test.mountNamespaceId,
		}
		containers = append(containers, container)

		containerIds = append(containerIds, test.containerId)
		mountNamespaces = append(mountNamespaces, test.mountNamespaceId)

		containerVolumes, found := pvcKeyToContainerVolumes[pvcKey]
		if found {
			containerVolumes = append(
				containerVolumes,
				&containerVolume{
					rootPath:       createContainerRootFs(container.id),
					mountNamespace: container.mountNamespace,
				})

			pvcKeyToContainerVolumes[pvcKey] = containerVolumes
		} else {
			pvcKeyToContainerVolumes[pvcKey] = []*containerVolume{
				{
					rootPath:       createContainerRootFs(container.id),
					mountNamespace: container.mountNamespace,
				},
			}
		}
	}

	return containers, containerIds, mountNamespaces, pvcKeyToContainerVolumes
}

func TestStartsAndStopsForContainers(t *testing.T) {
	podNamespace := metav1.NamespaceDefault

	pvcName := "pvc1"
	pvName := "pv1"
	podVolumeName := "pod1vol1"
	pvSource := &persistentVolumeSource{
		kind:      pvSourceLocal,
		localPath: fuseMountPointHostDir,
	}
	pv1, pvc1, podVolume1 := createVolume(
		t, podNamespace, podVolumeName, pvName, pvcName, pvSource)

	podName := "pod1"

	pvcKey := util.ConcatenateNamespaceAndName(podNamespace, pvcName)
	containers, containerIds, mountNamespaces,
		pvcKeyToContainerVolumes := createContainersInfo(pvcKey)

	volumes := []corev1.Volume{
		podVolume1,
	}
	pod := newPod(podNamespace, podName, volumes, containerIds)

	kubeObjects := []k8sruntime.Object{
		pv1, pvc1, pod,
	}
	kubeClient := k8sfake.NewSimpleClientset(kubeObjects...)

	fakeRuntimeClient := newFakeRuntimeClient(containers)

	volumeController := newFakeVolumeController(kubeClient, fakeRuntimeClient)

	createFuseSourceDirectory(t, fuseSourceDir1)

	fuseMountPoint, err := testInitialize(
		t, volumeController, fuseSourceDir1, localInitReq, nil, nil, false)
	assert.NoError(t, err)

	dataContainerName := "data-c"
	testExecutables := []executable{
		{
			path:     "/usr/bin/x",
			checksum: []byte{0x01, 0x23},
			writable: true,
		},
		{
			path:     "/usr/local/bin/y",
			checksum: []byte{0x45, 0x67, 0x89},
			writable: false,
		},
	}
	executables, executablePaths := createAllowedExecutables(testExecutables)

	fuseMounts := []startFuseMount{
		{
			pvcKey:        pvcKey,
			mountPointDir: localMountPointDir,
		},
	}

	wg := new(sync.WaitGroup)
	mountPointInfo := newMountPointInfo()

	for _, mount := range fuseMounts {
		wg.Add(1)
		go func(sfm startFuseMount) {
			defer wg.Done()

			expectedMountPointInfo := &api.StartResponse{
				MountPoint:                   sfm.mountPointDir,
				UsageControlEnabled:          true,
				MessageQueuePublisherEnabled: true,
			}

			brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
			messageQueueConfig := &api.MessageQueue{
				Brokers:                        []string{brokerAddress},
				User:                           saslUser,
				Password:                       saslPassword,
				Topic:                          updateTopic,
				CompressionCodec:               mqpub.CompressionCodecNone,
				MaxBatchBytes:                  "0",
				UpdatePublishChannelBufferSize: "10",
			}

			mpi, _ := testStart(
				t, volumeController, pod, dataContainerName, sfm.pvcKey,
				executables, messageQueueConfig,
				pvcKeyToContainerVolumes, expectedMountPointInfo, nil, false)

			mountPointInfo.add(pod, sfm.pvcKey, mpi)
		}(mount)
	}
	wg.Wait()

	namespaces := map[string]struct{}{}
	for _, namespace := range mountNamespaces {
		namespaces[namespace] = struct{}{}
	}
	expectedMountNamespaces := map[string]map[string]struct{}{
		localMountPointDir: namespaces,
	}
	postCheckStarts(t, volumeController, expectedMountNamespaces)

	for pod, mpi := range mountPointInfo.getWithoutMutex() {
		mountPointToExecutablePaths := map[string]*api.ExecutablePaths{}
		for _, mountPoint := range mpi {
			wg.Add(1)

			mPoint := mountPoint.MountPoint
			mountPointToExecutablePaths[mPoint] = &api.ExecutablePaths{
				Paths: executablePaths,
			}

			go func(p *corev1.Pod) {
				testStop(
					t, volumeController, p, mountPointToExecutablePaths,
					mPoint, false)
				wg.Done()
			}(pod)
		}
	}
	wg.Wait()

	postCheckStops(t, volumeController, nil)

	err = testFinalize(t, volumeController, fuseMountPoint, false, false)
	assert.NoError(t, err)
	assert.Nil(t, volumeController.fuseControllers[fuseMountPoint])

	deleteTestFuseDirectories(t)
}
