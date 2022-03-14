// Copyright (c) 2022 Fujitsu Limited

package lifetimes

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/lifetimer/app/options"
	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	lifetimesclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/lifetimes/clientset/versioned"
	lifetimesscheme "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/lifetimes/clientset/versioned/scheme"
	lifetimesinformers "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/lifetimes/informers/externalversions"
	lifetimeslisters "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/lifetimes/listers/lifetimes/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/grpc/volumecontrol"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/storage"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/storage/filesystem"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/storage/rdb"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/trigger"
	lifetimesvolumectl "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/volumecontrol"
	mqpub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

const (
	controllerAgentName = "data-lifetimer"

	eventSuccessSynced = "Synced"

	messageResourceSynced = "%v synced successfully"

	// targets: [namespace1]/[name2],[namespace2]/[name2],...
	targetLifetimesNamesEnv = "K8S_LIFETIMES_NAMES"

	podNamespaceEnv      = "POD_NAMESPACE"
	podNameEnv           = "POD_NAME"
	dataContainerNameEnv = "DATA_CONTAINER_NAME"

	PodCheckMaxIteration   = 5
	waitReasonCntrCreating = "ContainerCreating"

	keyDelimiter = "#"

	stopAckChSize = 5
	statusChSize  = 5

	fileDataNamePrefix = "file"
	rdbDataNamePrefix  = "rdb"
)

type ProviderLifetimes struct {
	ApiServerUrl   string   `json:",apiServerUrl,omitempty"`
	KubeConfig     string   `json:"kubeConfig,omitempty"`
	LifetimesNames []string `json:"lifetimesNames"`
}

type providerSharedInformer struct {
	dataClient          coreclientset.Interface
	dataLifetimesClient lifetimesclientset.Interface
	lifetimesLister     lifetimeslisters.DataLifetimeLister
}

type lifetimeChannel struct {
	finCh        chan struct{}
	lifetimeSpec *lifetimesapi.LifetimeSpec
	numStopAcks  int
}

type triggerPolicyUpdate struct {
	iterUpdateTime time.Duration
	expiredTime    string
}

type lifetimePolicyUpdate struct {
	spec lifetimesapi.LifetimeSpec
}

type policyUpdateChannels struct {
	triggerChs []chan triggerPolicyUpdate
	lifetimeCh chan lifetimePolicyUpdate
}

type startChannel struct {
	time time.Time
	err  error
}

type LifetimeController struct {
	kubeClient kubernetes.Interface
	dataClient coreclientset.Interface

	workqueue workqueue.RateLimitingInterface
	recorder  record.EventRecorder

	providerSharedInformers map[string]*providerSharedInformer
	informerSynceds         []cache.InformerSynced

	volumeController      *options.VolumeControllerOptions
	messageQueuePublisher *mqpub.MessageQueuePublisherOptions

	policyValidator *policyValidator

	finChsMutex sync.Mutex
	finChs      map[string]*lifetimeChannel

	stopCh    <-chan struct{}
	stopAckCh chan string

	updateChs map[string]*policyUpdateChannels
}

type lifetimeControllerWithKubeConfig struct {
	controller *LifetimeController
	kubeConfig string
}

type sharedInformerFactory struct {
	lifetimes lifetimesinformers.SharedInformerFactory
}

type lifetimeInputOperator struct {
	ops storage.LifetimeInputOperations

	expireTicker *time.Ticker

	triggerUpdateCh <-chan triggerPolicyUpdate
	statusUpdateCh  chan<- updateStatus
	finCh           <-chan struct{}
	stopChFlag      *bool

	dataSpec     *lifetimesapi.InputDataSpec
	deletePolicy lifetimesapi.DeletePolicy
	status       *lifetimesapi.ConsumerStatus

	lcontroller *LifetimeController
}

type lifetimeOutputOperator struct {
	ops storage.LifetimeOutputOperations

	expireTicker *time.Ticker

	triggerUpdateCh <-chan triggerPolicyUpdate
	statusUpdateCh  chan<- updateStatus
	finCh           <-chan struct{}
	stopChFlag      *bool

	dataSpec     *lifetimesapi.OutputDataSpec
	deletePolicy lifetimesapi.DeletePolicy
	status       *lifetimesapi.ConsumerStatus

	lcontroller *LifetimeController
}

func getTargetLifetimes() ([]ProviderLifetimes, error) {
	targets := os.Getenv(targetLifetimesNamesEnv)
	if targets == "" {
		return nil, fmt.Errorf(
			"Could not get the target lifetimes (namespace/name) from "+
				"environment variable, %q",
			targetLifetimesNamesEnv)
	}

	var providerLifetimes []ProviderLifetimes
	err := json.Unmarshal([]byte(targets), &providerLifetimes)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not convert %q (the JSON format) into "+
				"'ProviderLifetimes' struct",
			targets)
	}

	return providerLifetimes, nil
}

func generateKey(kubeConfig string, lifetimesName string) string {
	return fmt.Sprintf(
		"%s%s%s", kubeConfig, keyDelimiter, lifetimesName)
}

func (lcwkc *lifetimeControllerWithKubeConfig) enqueueDataLifetime(
	obj interface{}) {
	lifetimesName, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	key := generateKey(lcwkc.kubeConfig, lifetimesName)

	providerSharedInformer, ok := lcwkc.controller.providerSharedInformers[key]
	if !ok {
		return
	} else {
		err = lcwkc.controller.policyValidator.
			addDataLifetimeToPolicyDataStore(
				lifetimesName, providerSharedInformer)
		if err != nil {
			utilruntime.HandleError(err)
			return
		}
	}

	lcwkc.controller.workqueue.Add(key)
}

func getLifetimesKey(old interface{}, new interface{}) (string, error) {
	oldKey, err := cache.DeletionHandlingMetaNamespaceKeyFunc(old)
	if err != nil {
		return "", fmt.Errorf("Not get the old key: %s", err.Error())
	}

	newKey, err := cache.MetaNamespaceKeyFunc(new)
	if err != nil {
		return "", fmt.Errorf("Not get the new key: %s", err.Error())
	}

	if oldKey != newKey {
		return "", fmt.Errorf(
			"[Unexpected] The old key (%s) does not match the new one (%s)",
			oldKey, newKey)
	}

	return newKey, nil
}

func (lc *LifetimeController) getLifetime(
	sharedInformer *providerSharedInformer, newKey string) (
	*lifetimesapi.DataLifetime, error) {
	namespace, name, err := cache.SplitMetaNamespaceKey(newKey)
	if err != nil {
		return nil, fmt.Errorf("Invalid resource key: %s", newKey)
	}

	lifetime, err := sharedInformer.lifetimesLister.DataLifetimes(
		namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf(
				"data lifetime %q in work queue no longer exists", newKey)
		}

		return nil, err
	}

	return lifetime, nil
}

func (lc *LifetimeController) updateTrigger(
	oldKey string, newLifetime *lifetimesapi.DataLifetime) (bool, error) {
	oldTriggerOpts, err := trigger.NewTrigger(
		lc.finChs[oldKey].lifetimeSpec.Trigger)
	if err != nil {
		return false, fmt.Errorf(
			"For old lifetime policy [%s]: %s", oldKey, err.Error())
	}

	newTriggerOpts, err := trigger.NewTrigger(newLifetime.Spec.Trigger)
	if err != nil {
		return false, fmt.Errorf(
			"For new lifetime policy [%s/%s]: %s",
			newLifetime.Namespace, newLifetime.Name, err.Error())
	}

	oldExpiredTime := oldTriggerOpts.GetEndTimeString()

	newExpiredTime := newTriggerOpts.GetEndTimeString()

	triggerUpdate := triggerPolicyUpdate{}
	updated := false

	if oldExpiredTime != newExpiredTime {
		triggerUpdate.expiredTime = newExpiredTime
		updated = true
	}

	if updated {
		for _, triggerCh := range lc.updateChs[oldKey].triggerChs {
			triggerCh <- triggerUpdate
		}
	}

	return updated, nil
}

func (lc *LifetimeController) updateAllowedExecutables(
	oldKey string, newLifetime *lifetimesapi.DataLifetime) (bool, error) {
	if lc.volumeController.DisableUsageControl {
		return false, nil
	}

	if len(newLifetime.Spec.InputData) == 0 &&
		len(newLifetime.Spec.OutputData) == 0 {
		return false, nil
	}

	executablesDiffs, err := lifetimesvolumectl.CreateGrpcExecutablesDiffs(
		&newLifetime.Spec, lc.finChs[oldKey].lifetimeSpec)
	if err != nil {
		return false, err
	} else if executablesDiffs == nil {
		return false, nil
	}

	ucClient := volumecontrol.NewClient(lc.volumeController.Endpoint)

	mountPoints, err := ucClient.UpdateAllowedExecutables(
		os.Getenv(podNamespaceEnv), os.Getenv(podNameEnv), executablesDiffs)
	if err != nil {
		return false, err
	}

	klog.Infof(
		"Updated allowed executables for the mount point %v", mountPoints)

	return true, nil
}

func (lc *LifetimeController) updateMessageQueueConfigurations(
	oldKey string, newLifetime *lifetimesapi.DataLifetime) (bool, error) {
	if len(newLifetime.Spec.OutputData) == 0 {
		return false, nil
	}

	messageQueueDiffs, err := lifetimesvolumectl.CreateGrpcMessageQueueDiffs(
		lc.dataClient, lc.kubeClient, lc.messageQueuePublisher,
		newLifetime.Spec.OutputData, lc.finChs[oldKey].lifetimeSpec.OutputData)
	if err != nil {
		return false, err
	} else if len(messageQueueDiffs) == 0 {
		return false, nil
	}

	ucClient := volumecontrol.NewClient(lc.volumeController.Endpoint)

	mountPoints, err := ucClient.UpdateMessageQueueConfigurations(
		os.Getenv(podNamespaceEnv), os.Getenv(podNameEnv), messageQueueDiffs)
	if err != nil {
		return false, err
	}

	klog.Infof(
		"Updated message queues for the mount point %v", mountPoints)

	return true, nil
}

func (lcwkc *lifetimeControllerWithKubeConfig) onUpdate(
	old interface{}, new interface{}) {
	lifetimesName, err := getLifetimesKey(old, new)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	key := generateKey(lcwkc.kubeConfig, lifetimesName)

	sharedInformer, ok := lcwkc.controller.providerSharedInformers[key]
	if !ok {
		return
	}

	lifetime, err := lcwkc.controller.getLifetime(
		sharedInformer, lifetimesName)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	lcwkc.controller.finChsMutex.Lock()
	defer lcwkc.controller.finChsMutex.Unlock()

	if _, ok := lcwkc.controller.finChs[key]; !ok {
		klog.Warningf(
			"The additional event for %q may be received, "+
				"but its additional handling may be finished", key)
		return
	}

	triggerUpdated, err := lcwkc.controller.updateTrigger(key, lifetime)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	allowedExecsUpdated, err := lcwkc.controller.updateAllowedExecutables(
		key, lifetime)
	if err != nil {
		utilruntime.HandleError(err)
	}

	messageQueuesUpdated, err := lcwkc.controller.
		updateMessageQueueConfigurations(key, lifetime)
	if err != nil {
		utilruntime.HandleError(err)
	}

	if triggerUpdated || allowedExecsUpdated || messageQueuesUpdated {
		lcwkc.controller.updateChs[key].lifetimeCh <- lifetimePolicyUpdate{
			spec: lifetime.DeepCopy().Spec,
		}
	}
}

func (lcwkc *lifetimeControllerWithKubeConfig) onDelete(obj interface{}) {
	lifetimesName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	key := generateKey(lcwkc.kubeConfig, lifetimesName)

	providerSharedInformer, ok := lcwkc.controller.providerSharedInformers[key]
	if !ok {
		return
	} else {
		klog.Info("onDelete()")
		err = lcwkc.controller.policyValidator.
			deleteDataLifetimeFromPolicyDataStore(
				lifetimesName, providerSharedInformer)
		if err != nil {
			utilruntime.HandleError(err)
			return
		}
	}

	lcwkc.controller.finChsMutex.Lock()
	defer lcwkc.controller.finChsMutex.Unlock()

	if _, ok := lcwkc.controller.finChs[key]; !ok {
		klog.Warningf(
			"The additional event for %q may not be received", key)
		return
	}

	close(lcwkc.controller.finChs[key].finCh)
	delete(lcwkc.controller.finChs, key)

	klog.Infof("Deleted the 'DataLifetime': %q", key)
}

func createProviderSharedInformers(
	controller *LifetimeController, syncFrequency time.Duration) (
	[]sharedInformerFactory, error) {
	targetLifetimes, err := getTargetLifetimes()
	if err != nil {
		return nil, err
	}

	sharedInformerFactories := make(
		[]sharedInformerFactory, 0, len(targetLifetimes))
	sharedInformers := map[string]*providerSharedInformer{}
	informerSynceds := make([]cache.InformerSynced, 0, len(targetLifetimes)*2)

	for _, providerLifetimes := range targetLifetimes {
		providerConfig, err := clientcmd.BuildConfigFromFlags(
			providerLifetimes.ApiServerUrl, providerLifetimes.KubeConfig)
		if err != nil {
			return nil, fmt.Errorf(
				"Error building kubeconfig for a data provider: %s", err.Error())
		}

		dataClient, err := coreclientset.NewForConfig(providerConfig)
		if err != nil {
			return nil, fmt.Errorf(
				"Error building data-core clientset: %s", err.Error())
		}

		dataLifetimesClient, err := lifetimesclientset.NewForConfig(
			providerConfig)
		if err != nil {
			return nil, fmt.Errorf(
				"Error building data-lifetime clientset: %s", err.Error())
		}

		dataLifetimeInformerFactory := lifetimesinformers.NewSharedInformerFactory(
			dataLifetimesClient, syncFrequency)

		lifetimeInformer := dataLifetimeInformerFactory.Lifetimes().V1alpha1().
			DataLifetimes()
		controllerWithUrl := &lifetimeControllerWithKubeConfig{
			controller: controller,
			kubeConfig: providerLifetimes.KubeConfig,
		}
		lifetimeInformer.Informer().AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc:    controllerWithUrl.enqueueDataLifetime,
				UpdateFunc: controllerWithUrl.onUpdate,
				DeleteFunc: controllerWithUrl.onDelete,
			})

		informerSynceds = append(
			informerSynceds,
			lifetimeInformer.Informer().HasSynced)

		for _, lifetimesName := range providerLifetimes.LifetimesNames {
			key := generateKey(providerLifetimes.KubeConfig, lifetimesName)
			sharedInformers[key] = &providerSharedInformer{
				dataClient:          dataClient,
				dataLifetimesClient: dataLifetimesClient,
				lifetimesLister:     lifetimeInformer.Lister(),
			}
		}

		sharedInformerFactories = append(
			sharedInformerFactories,
			sharedInformerFactory{
				lifetimes: dataLifetimeInformerFactory,
			})
	}

	controller.providerSharedInformers = sharedInformers
	controller.informerSynceds = informerSynceds

	return sharedInformerFactories, nil
}

func newLifetimeController(
	kubeClient kubernetes.Interface, dataClient coreclientset.Interface,
	volumeController *options.VolumeControllerOptions,
	messageQueuePublisher *mqpub.MessageQueuePublisherOptions,
	dataStoreClient datastore.ValidationDataStoreOperations,
	stopCh <-chan struct{}) *LifetimeController {
	controller := &LifetimeController{
		kubeClient: kubeClient,
		dataClient: dataClient,

		workqueue: workqueue.NewNamedRateLimitingQueue(
			workqueue.DefaultControllerRateLimiter(), "dataLifetime"),

		volumeController:      volumeController,
		messageQueuePublisher: messageQueuePublisher,

		policyValidator: newPolicyValidator(dataStoreClient),

		finChs: map[string]*lifetimeChannel{},

		stopCh:    stopCh,
		stopAckCh: make(chan string, stopAckChSize),

		updateChs: map[string]*policyUpdateChannels{},
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})

	controller.recorder = eventBroadcaster.NewRecorder(
		lifetimesscheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	return controller
}

func NewLifetimeController(
	kubeClient kubernetes.Interface,
	dataClient coreclientset.Interface, syncFrequency time.Duration,
	volumeController *options.VolumeControllerOptions,
	messageQueuePublisher *mqpub.MessageQueuePublisherOptions,
	dataStoreClient datastore.ValidationDataStoreOperations,
	stopCh <-chan struct{}) (*LifetimeController, error) {
	controller := newLifetimeController(
		kubeClient, dataClient, volumeController, messageQueuePublisher,
		dataStoreClient, stopCh)

	providerSharedInformerFactories, err := createProviderSharedInformers(
		controller, syncFrequency)
	if err != nil {
		return nil, err
	}

	for _, factory := range providerSharedInformerFactories {
		factory.lifetimes.Start(stopCh)
	}

	return controller, nil
}

func (lc *LifetimeController) getLifetimeOperationsForInputData(
	inputDataSpec *lifetimesapi.InputDataSpec, pod *corev1.Pod,
	providerSharedInformer *providerSharedInformer,
	status *lifetimesapi.ConsumerStatus) (
	storage.LifetimeInputOperations, error) {
	switch {
	case inputDataSpec.FileSystemSpec != nil:
		return filesystem.NewFileSystemLifetimerForInputData(
			inputDataSpec.FileSystemSpec, pod,
			providerSharedInformer.dataClient, lc.kubeClient,
			inputDataSpec.MessageQueueSubscriber, status,
			lc.volumeController.DisableUsageControl,
			lc.volumeController.Endpoint, os.Getenv(podNamespaceEnv),
			os.Getenv(podNameEnv), os.Getenv(dataContainerNameEnv))
	case inputDataSpec.RdbSpec != nil:
		return rdb.NewRdbLifetimerForInputData(
			inputDataSpec.RdbSpec, providerSharedInformer.dataClient,
			lc.kubeClient, inputDataSpec.MessageQueueSubscriber), nil
	default:
		return nil, fmt.Errorf("Input data type must be specified")
	}
}

func (lc *LifetimeController) getPod(
	podNamespace string, podName string) (*corev1.Pod, error) {
	ctx, cancel := util.GetTimeoutContext(util.DefaultKubeClientTimeout)
	defer cancel()

	pod, err := lc.kubeClient.CoreV1().Pods(podNamespace).Get(
		ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf(
			"Not get Pod[%s:%s]: %v", podNamespace, podName, err.Error())
	}

	return pod, nil
}

func (lc *LifetimeController) isPodRunning(
	podNamespace string, podName string) (*corev1.Pod, error) {
outerLoop:
	for i := 0; i < PodCheckMaxIteration; i++ {
		pod, err := lc.getPod(podNamespace, podName)
		if err != nil {
			return nil, err
		}

		for _, container := range pod.Status.ContainerStatuses {
			if container.State.Running == nil {
				if container.State.Waiting != nil &&
					container.State.Waiting.Reason == waitReasonCntrCreating {
					time.Sleep(time.Second * 1)
					continue outerLoop
				} else {
					return nil, fmt.Errorf(
						"Invalid container state %q: %+v",
						container.Name, container.State)
				}
			}
		}

		klog.V(2).Infof("Got the pod-running at the %d-th loop", i)

		return pod, nil
	}

	return nil, fmt.Errorf(
		"[Timeout] The Pod[%s:%s] state is not running", podNamespace, podName)
}

func (lc *LifetimeController) setupFinChannel(
	key string, lifetime *lifetimesapi.DataLifetime) <-chan struct{} {
	lc.finChsMutex.Lock()
	defer lc.finChsMutex.Unlock()

	if finCh, ok := lc.finChs[key]; ok {
		close(finCh.finCh)
	}

	finCh := make(chan struct{})
	lc.finChs[key] = &lifetimeChannel{
		finCh:        finCh,
		lifetimeSpec: lifetime.Spec.DeepCopy(),

		//+1: lifetimeCacheCleaner()
		numStopAcks: len(lifetime.Spec.InputData) +
			len(lifetime.Spec.OutputData) + 1,
	}

	return finCh
}

func (lc *LifetimeController) setupPolicyUpdateChannels(
	key string, numChs int) (
	[]<-chan triggerPolicyUpdate, <-chan lifetimePolicyUpdate) {
	triggerChs := []<-chan triggerPolicyUpdate{}
	lifetimeCh := make(chan lifetimePolicyUpdate)

	lc.updateChs[key] = &policyUpdateChannels{
		lifetimeCh: lifetimeCh,
	}
	for i := 0; i < numChs; i++ {
		triggerCh := make(chan triggerPolicyUpdate)

		lc.updateChs[key].triggerChs = append(
			lc.updateChs[key].triggerChs, triggerCh)

		triggerChs = append(triggerChs, triggerCh)
	}

	return triggerChs, lifetimeCh
}

func startStatusUpdater(
	dataLifetimesClient lifetimesclientset.Interface,
	lifetime *lifetimesapi.DataLifetime, namespace string,
	stopCh <-chan struct{}, lifetimeSpecUpdateCh <-chan lifetimePolicyUpdate,
	statusUpdateCh <-chan updateStatus) {
	updater := newStatusUpdater(
		dataLifetimesClient, lifetime.DeepCopy(), namespace, statusUpdateCh,
		stopCh, lifetimeSpecUpdateCh)

	go updater.start()
}

func (lio *lifetimeInputOperator) createDataName() string {
	switch {
	case lio.dataSpec.FileSystemSpec != nil:
		toPvcRef := lio.dataSpec.FileSystemSpec.ToPersistentVolumeClaimRef
		return fmt.Sprintf(
			"%s:%s/%s", fileDataNamePrefix, toPvcRef.Namespace, toPvcRef.Name)

	case lio.dataSpec.RdbSpec != nil:
		return fmt.Sprintf(
			"%s:%s", rdbDataNamePrefix, lio.dataSpec.RdbSpec.LocalDb)

	default:
		klog.Error("Target Data is neither a file nor RDB data")
		return "Unknown"
	}
}

func (lio *lifetimeInputOperator) updateLifetimeStatus(isDeleted bool) {
	if isDeleted {
		lio.status.ExpiredAt = metav1.NewTime(time.Now().UTC())
	} else {
		lio.status.LastUpdatedAt = metav1.NewTime(time.Now().UTC())
	}

	lio.statusUpdateCh <- updateStatus{
		dataName:   lio.createDataName(),
		timestamps: *lio.status,
	}
}

func (loo *lifetimeOutputOperator) createDataName() string {
	switch {
	case loo.dataSpec.FileSystemSpec != nil:
		pvcRef := loo.dataSpec.FileSystemSpec.PersistentVolumeClaimRef
		return fmt.Sprintf(
			"%s:%s/%s", fileDataNamePrefix, pvcRef.Namespace, pvcRef.Name)

	case loo.dataSpec.RdbSpec != nil:
		return fmt.Sprintf(
			"%s:%s", rdbDataNamePrefix, loo.dataSpec.RdbSpec.DestinationDb)

	default:
		klog.Error("Target Data is neither a file nor RDB data")
		return "Unknown"
	}
}

func (loo *lifetimeOutputOperator) updateLifetimeStatus(isDeleted bool) {
	if isDeleted {
		loo.status.ExpiredAt = metav1.NewTime(time.Now().UTC())
	} else {
		loo.status.LastUpdatedAt = metav1.NewTime(time.Now().UTC())
	}

	loo.statusUpdateCh <- updateStatus{
		dataName:   loo.createDataName(),
		timestamps: *loo.status,
	}
}

func (lio *lifetimeInputOperator) processInitialLifetimeOperation(
	startCh <-chan startChannel) (time.Time, error) {
	select {
	case <-lio.finCh:
		klog.Info("Received a message from the 'fin' channel")
		return time.Time{}, nil

	case <-lio.lcontroller.stopCh:
		klog.Info("Received a message from the 'stop' channel")
		*lio.stopChFlag = true
		return time.Time{}, nil

	case start := <-startCh:
		return start.time, start.err
	}
}

func (lio *lifetimeInputOperator) initializeDataFromMessageQueue(
	ctx context.Context, startCh chan<- startChannel) {
	startTime, err := lio.ops.Start(ctx)

	start := startChannel{
		time: startTime,
		err:  err,
	}
	startCh <- start

	if err == nil {
		lio.updateLifetimeStatus(false)
	}
}

func (lio *lifetimeInputOperator) processInitialLifetimeOperations() (
	time.Time, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startCh := make(chan startChannel)
	defer close(startCh)

	go lio.initializeDataFromMessageQueue(ctx, startCh)

	return lio.processInitialLifetimeOperation(startCh)
}

func (lio *lifetimeInputOperator) processLifetimeOperation() bool {
	select {
	case <-lio.expireTicker.C:
		err := lio.ops.Delete(lio.deletePolicy)
		if err != nil {
			klog.Error(err.Error())
			return true
		}

		lio.updateLifetimeStatus(true)

		klog.Infof("Input data %q are expired", lio.createDataName())
		return true

	case triggerUpdate := <-lio.triggerUpdateCh:
		if triggerUpdate.expiredTime != "" {
			lio.expireTicker.Stop()

			expiryEpoch, err := trigger.GetExpiryEpochFromEndTime(
				triggerUpdate.expiredTime)
			if err != nil {
				klog.Error(err.Error())
				return true
			}

			lio.expireTicker = time.NewTicker(expiryEpoch)

			klog.Infof(
				"Update the end time for the input consumer data (%s/%s) "+
					"to %v\n", lio.createDataName(), triggerUpdate.expiredTime)
		}

		return false

	case <-lio.finCh:
		klog.Info("Received a message from the 'fin' channel")
		return true

	case <-lio.lcontroller.stopCh:
		klog.Info("Received a message from the 'stop' channel")
		*lio.stopChFlag = true
		return true
	}
}

func (lio *lifetimeInputOperator) updateDataFromMessageQueue(
	ctx context.Context, startTime time.Time) {
	stopped, err := lio.ops.FirstUpdate(ctx, startTime)
	if err != nil {
		klog.Error(err.Error())
		return
	} else if stopped {
		return
	}

	lio.updateLifetimeStatus(false)

	for {
		stopped, err := lio.ops.Update(ctx)
		if err != nil {
			klog.Error(err.Error())
			break
		} else if stopped {
			break
		}

		lio.updateLifetimeStatus(false)
	}
}

func (lio *lifetimeInputOperator) processLifetimeOperations(
	startTime time.Time) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go lio.updateDataFromMessageQueue(ctx, startTime)

	for {
		if lio.processLifetimeOperation() {
			break
		}
	}
}

func (lc *LifetimeController) processLifetimeOperationsForInputData(
	expiredTime time.Duration, finCh <-chan struct{},
	triggerUpdateCh <-chan triggerPolicyUpdate,
	statusUpdateCh chan<- updateStatus, pod *corev1.Pod, key string,
	dataSpec *lifetimesapi.InputDataSpec,
	deletePolicy lifetimesapi.DeletePolicy,
	providerSharedInformer *providerSharedInformer) {
	stopChFlag := false
	defer func(caughtStopCh *bool) {
		if *caughtStopCh {
			klog.Info(
				"Send stopAck from processLifetimeOperationsForInputData()")
			lc.stopAckCh <- key
		}
	}(&stopChFlag)

	expireTicker := time.NewTicker(expiredTime)
	defer expireTicker.Stop()

	status := &lifetimesapi.ConsumerStatus{}
	lifetimeOps, err := lc.getLifetimeOperationsForInputData(
		dataSpec, pod, providerSharedInformer, status)
	if err != nil {
		klog.Errorf("Failed to get lifetime operations: %s", err.Error())
		return
	}

	operator := &lifetimeInputOperator{
		ops:             lifetimeOps,
		expireTicker:    expireTicker,
		triggerUpdateCh: triggerUpdateCh,
		statusUpdateCh:  statusUpdateCh,
		finCh:           finCh,
		stopChFlag:      &stopChFlag,
		dataSpec:        dataSpec,
		deletePolicy:    deletePolicy,
		status:          status,
		lcontroller:     lc,
	}

	startTime, err := operator.processInitialLifetimeOperations()
	if err != nil {
		klog.Errorf(
			"Failed to start the lifetime operations, %+v: %v",
			lifetimeOps, err)
		return
	}
	defer func() {
		err = lifetimeOps.Stop()
		if err != nil {
			klog.Errorf(
				"Failed to sttop the lifetime operations, %+v: %v",
				lifetimeOps, err)
		}

	}()

	operator.processLifetimeOperations(startTime)
}

func (lc *LifetimeController) getLifetimeOperationsForOutputData(
	outputDataSpec *lifetimesapi.OutputDataSpec, pod *corev1.Pod,
	status *lifetimesapi.ConsumerStatus) (
	storage.LifetimeOutputOperations, error) {
	switch {
	case outputDataSpec.FileSystemSpec != nil:
		return filesystem.NewFileSystemLifetimerForOutputData(
			outputDataSpec.FileSystemSpec, pod, lc.dataClient, lc.kubeClient,
			outputDataSpec.MessageQueuePublisher, lc.messageQueuePublisher,
			lc.volumeController.DisableUsageControl,
			lc.volumeController.Endpoint, os.Getenv(podNamespaceEnv),
			os.Getenv(podNameEnv), os.Getenv(dataContainerNameEnv))
	case outputDataSpec.RdbSpec != nil:
		return rdb.NewRdbLifetimerForOutputData(outputDataSpec.RdbSpec), nil
	default:
		return nil, fmt.Errorf("Output data type must be specified")
	}
}

func (loo *lifetimeOutputOperator) processLifetimeOperation() bool {
	select {
	case <-loo.expireTicker.C:
		err := loo.ops.Delete(loo.deletePolicy)
		if err != nil {
			klog.Error(err.Error())
			return true
		}

		loo.updateLifetimeStatus(true)

		klog.Infof("Output data %q are expired", loo.createDataName())

		return true

	case triggerUpdate := <-loo.triggerUpdateCh:
		if triggerUpdate.expiredTime != "" {
			loo.expireTicker.Stop()

			expiryEpoch, err := trigger.GetExpiryEpochFromEndTime(
				triggerUpdate.expiredTime)
			if err != nil {
				klog.Error(err.Error())
				return true
			}

			loo.expireTicker = time.NewTicker(expiryEpoch)

			klog.Infof(
				"Update the end time for the output provider data (%s/%s) "+
					"to %v\n", loo.createDataName(), triggerUpdate.expiredTime)
		}

		return false

	case <-loo.finCh:
		klog.Info("Received a message from the 'fin' channel")
		return true

	case <-loo.lcontroller.stopCh:
		klog.Info("Received a message from the 'stop' channel")
		*loo.stopChFlag = true
		return true
	}
}

func (lc *LifetimeController) processLifetimeOperationsForOutputData(
	expiredTime time.Duration, finCh <-chan struct{},
	triggerUpdateCh <-chan triggerPolicyUpdate,
	statusUpdateCh chan<- updateStatus, pod *corev1.Pod, key string,
	dataSpec *lifetimesapi.OutputDataSpec,
	deletePolicy lifetimesapi.DeletePolicy) {
	stopChFlag := false
	defer func(caughtStopCh *bool) {
		if *caughtStopCh {
			klog.Info(
				"Send stopAck from processLifetimeOperationsForOutputData()")
			lc.stopAckCh <- key
		}
	}(&stopChFlag)

	expireTicker := time.NewTicker(expiredTime)
	defer expireTicker.Stop()

	status := &lifetimesapi.ConsumerStatus{}
	lifetimeOps, err := lc.getLifetimeOperationsForOutputData(
		dataSpec, pod, status)
	if err != nil {
		klog.Errorf("Error getting lifetime operations: %s", err.Error())
		return
	}

	operator := &lifetimeOutputOperator{
		ops:             lifetimeOps,
		expireTicker:    expireTicker,
		triggerUpdateCh: triggerUpdateCh,
		statusUpdateCh:  statusUpdateCh,
		finCh:           finCh,
		stopChFlag:      &stopChFlag,
		dataSpec:        dataSpec,
		deletePolicy:    deletePolicy,
		status:          status,
		lcontroller:     lc,
	}

	err = lifetimeOps.Start(context.Background())
	if err != nil {
		klog.Errorf(
			"Failed to start the lifetime operations, %+v: %v",
			lifetimeOps, err)
		return
	}
	defer func() {
		err = lifetimeOps.Stop()
		if err != nil {
			klog.Errorf(
				"Failed to sttop the lifetime operations, %+v: %v",
				lifetimeOps, err)
		}

	}()

	for {
		if operator.processLifetimeOperation() {
			break
		}
	}
}

func (lc *LifetimeController) lifetimeCacheCleaner(
	providerSharedInformer *providerSharedInformer, lifetimesName string,
	key string) {
	defer func() {
		lc.stopAckCh <- key
	}()

	<-lc.stopCh

	err := lc.policyValidator.deleteDataLifetimeFromPolicyDataStore(
		lifetimesName, providerSharedInformer)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
}

func (lc *LifetimeController) startLifetimeOperations(
	lifetime *lifetimesapi.DataLifetime, namespace string, key string,
	providerSharedInformer *providerSharedInformer,
	lifetimesName string) error {
	triggerOpts, err := trigger.NewTrigger(lifetime.Spec.Trigger)
	if err != nil {
		return err
	}

	expiredTime, err := triggerOpts.GetExpiryEpoch()
	if err != nil {
		return err
	}

	pod, err := lc.isPodRunning(
		os.Getenv(podNamespaceEnv), os.Getenv(podNameEnv))
	if err != nil {
		return err
	}

	finCh := lc.setupFinChannel(key, lifetime)

	triggerChs, lifetimeCh := lc.setupPolicyUpdateChannels(
		key, len(lifetime.Spec.InputData)+len(lifetime.Spec.OutputData))

	statusUpdateCh := make(chan updateStatus, statusChSize)
	startStatusUpdater(
		providerSharedInformer.dataLifetimesClient, lifetime, namespace,
		lc.stopCh, lifetimeCh, statusUpdateCh)

	for i, dataSpec := range lifetime.Spec.InputData {
		dataSpec := dataSpec
		go lc.processLifetimeOperationsForInputData(
			expiredTime, finCh, triggerChs[i], statusUpdateCh, pod, key,
			&dataSpec, lifetime.Spec.DeletePolicy, providerSharedInformer)
	}

	triggerBaseIndex := len(lifetime.Spec.InputData)
	for i, dataSpec := range lifetime.Spec.OutputData {
		dataSpec := dataSpec
		go lc.processLifetimeOperationsForOutputData(
			expiredTime, finCh, triggerChs[triggerBaseIndex+i], statusUpdateCh,
			pod, key, &dataSpec, lifetime.Spec.DeletePolicy)
	}

	go lc.lifetimeCacheCleaner(providerSharedInformer, lifetimesName, key)

	return nil
}

func getLifetimesName(key string) (string, error) {
	keys := strings.Split(key, keyDelimiter)
	if len(keys) != 2 {
		return "", fmt.Errorf("Could not correctly split a key %q", key)
	}

	return keys[1], nil
}

func (lc *LifetimeController) syncHandler(key string) error {
	lifetimesName, err := getLifetimesName(key)
	if err != nil {
		utilruntime.HandleError(err)
		return err
	}

	namespace, name, err := cache.SplitMetaNamespaceKey(lifetimesName)
	if err != nil {
		utilruntime.HandleError(
			fmt.Errorf("Invalid resource key: %s", lifetimesName))
		return nil
	}

	providerSharedInformer := lc.providerSharedInformers[key]

	lifetime, err := providerSharedInformer.lifetimesLister.DataLifetimes(
		namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			utilruntime.HandleError(
				fmt.Errorf(
					"data lifetime %q in work queue no longer exists", key))
			return nil
		}

		return err
	}

	err = lc.startLifetimeOperations(
		lifetime.DeepCopy(), namespace, key, providerSharedInformer,
		lifetimesName)
	if err != nil {
		return err
	}

	msg := fmt.Sprintf(messageResourceSynced, "lifetime")
	lc.recorder.Event(
		lifetime, corev1.EventTypeNormal, eventSuccessSynced, msg)

	return nil
}

func (lc *LifetimeController) processNextWorkItem() bool {
	obj, shutdown := lc.workqueue.Get()

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer lc.workqueue.Done(obj)

		key, ok := obj.(string)
		if !ok {
			lc.workqueue.Forget(obj)
			utilruntime.HandleError(
				fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		if err := lc.syncHandler(key); err != nil {
			lc.workqueue.AddRateLimited(key)
			return fmt.Errorf("error synching %q: %s", key, err.Error())
		}

		lc.workqueue.Forget(obj)
		klog.Infof("Successfully synced %q", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return false
	}

	return true
}

func (lc *LifetimeController) runWorker() {
	for lc.processNextWorkItem() {
	}
}

func (lc *LifetimeController) closePolicyUpdateChannels(key string) {
	for _, triggerCh := range lc.updateChs[key].triggerChs {
		close(triggerCh)
	}
	close(lc.updateChs[key].lifetimeCh)
	delete(lc.updateChs, key)
}

func (lc *LifetimeController) closeFinAndUpdateChannels(key string) {
	lc.finChs[key].numStopAcks -= 1
	if lc.finChs[key].numStopAcks > 0 {
		return
	}

	close(lc.finChs[key].finCh)
	delete(lc.finChs, key)

	lc.closePolicyUpdateChannels(key)
}

func (lc *LifetimeController) finalize() {
	<-lc.stopCh

	klog.Info("Shutting down workers")
	waitTicker := time.NewTicker(time.Second * 5)
	defer waitTicker.Stop()

outerLoop:
	for {
		select {
		case key := <-lc.stopAckCh:
			lc.finChsMutex.Lock()
			lc.closeFinAndUpdateChannels(key)
			lc.finChsMutex.Unlock()

			if len(lc.finChs) == 0 {
				break outerLoop
			}

		case <-waitTicker.C:
			lc.finChsMutex.Lock()
			workerKeys := make([]string, 0, len(lc.finChs))
			for key := range lc.finChs {
				workerKeys = append(workerKeys, key)
			}
			lc.finChsMutex.Unlock()
			klog.Errorf(
				"[Timeout] Could not wait for shutdowns for %+v", workerKeys)
			return
		}
	}

	err := lc.policyValidator.finalize()
	if err != nil {
		klog.Errorf("%v", err)
	}

	klog.Info("Shut down all the workers")
}

func (lc *LifetimeController) Run(threadiness int) error {
	defer utilruntime.HandleCrash()
	defer lc.workqueue.ShutDown()

	klog.Info("Starting a data lifetime controller")

	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(
		lc.stopCh, lc.informerSynceds...); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	for i := 0; i < threadiness; i++ {
		go wait.Until(lc.runWorker, time.Second, lc.stopCh)
	}

	klog.Info("Started workers")

	lc.finalize()

	return nil
}
