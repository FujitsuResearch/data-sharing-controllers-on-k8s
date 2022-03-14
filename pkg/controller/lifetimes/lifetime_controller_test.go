// Copyright (c) 2022 Fujitsu Limited

package lifetimes

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8scorev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/lifetimer/app/options"
	coreapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/core/v1alpha1"
	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	coreclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned"
	corefake "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned/fake"
	lifetimesclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/lifetimes/clientset/versioned"
	lifetimesfake "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/lifetimes/clientset/versioned/fake"
	lifetimesinformers "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/lifetimes/informers/externalversions"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
	mqpub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue/publish"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
	volumemq "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue"
	volumepub "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/volume/messagequeue/publish"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

const (
	kubeConfigContent = `
apiVersion: v1
clusters:
- cluster:
    server: https://localhost:8080
  name: foo-cluster
contexts:
- context:
    cluster: foo-cluster
    user: foo-user
    namespace: bar
  name: foo-context
current-context: foo-context
kind: Config
users:
- name: foo-user
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1alpha1
      args:
      - arg-1
      - arg-2
      command: foo-command
`

	dataNamespace    = metav1.NamespaceDefault
	dataName1        = "consumer-data-1"
	dataName2        = "consumer-data-2"
	dataName3        = "consumer-data-3"
	dataName4        = "consumer-data-4"
	consumerPvcName1 = "consumer-pvc-1"
	consumerPvcName2 = "consumer-pvc-2"
	consumerPvcName3 = "consumer-pvc-3"
	consumerPvcName4 = "consumer-pvc-4"
	mqNamespace      = metav1.NamespaceDefault
	mqName           = "mq-1"
	brokerNamespace  = metav1.NamespaceDefault
	brokerName       = "broker-1"
	saslNamespace    = metav1.NamespaceDefault

	kafkaBrokerAddressEnv = "KAFKA_BROKER_ADDRESS"

	// [REF] github.com/segmentio/kafka-go/docker-compose.yml
	saslUser     = "adminscram"
	saslPassword = "admin-secret-512"
)

var (
	alwaysReady        = func() bool { return true }
	noResyncPeriodFunc = func() time.Duration { return 0 }

	podSourceVolume1     = "pod_source_volume_1"
	podSourceVolume2     = "pod_source_volume_2"
	podLocalVolume1      = "pod_local_volume_1"
	podLocalVolume2      = "pod_local_volume_2"
	podOutputVolume1     = "pod_output_volume_1"
	podOutputVolume2     = "pod_output_volume_2"
	sourceRootDirectory1 = "testing/fixtures/src_1"
	sourceRootDirectory2 = "testing/fixtures/src_2"
	sourceRootDirectory3 = "testing/fixtures/src_3"
	sourceFileName1      = "source_1.txt"
	sourceFileName2      = "source_2.txt"
	sourceFileName3      = "source_3.txt"
	sourceFilePath1      = filepath.Join(
		sourceRootDirectory1, sourceFileName1)
	sourceFilePath2 = filepath.Join(
		sourceRootDirectory1, sourceFileName2)
	sourceFilePath3 = filepath.Join(
		sourceRootDirectory2, sourceFileName3)
	destinationRootDirectory1 = "testing/fixtures/dst_1"
	destinationRootDirectory2 = "testing/fixtures/dst_2"
	destinationFilePath1      = filepath.Join(
		destinationRootDirectory1, sourceFileName1)
	destinationFilePath2 = filepath.Join(
		destinationRootDirectory1, sourceFileName2)
	destinationFilePath3 = filepath.Join(
		destinationRootDirectory2, sourceFileName3)
	outputRootDirectory1 = "testing/fixtures/output_1"
	outputRootDirectory2 = "testing/fixtures/output_2"
	outputFilePath1      = filepath.Join(
		outputRootDirectory1, sourceFileName1)
	outputFilePath2 = filepath.Join(
		outputRootDirectory2, sourceFileName2)
)

type intervals struct {
	old string
	new string
}

type endTimes struct {
	old string
	new string
}

type updateActionInfo struct {
	statusKeys        []string
	counts            int
	isUpdated         bool
	isProviderUpdated bool
	isDeleted         bool
}

type actionInfo struct {
	action     clienttesting.Action
	updateInfo *updateActionInfo
}

type lifetimesFixture struct {
	dataObjects []runtime.Object
	dataClient  *corefake.Clientset

	lifetimeObjects []runtime.Object
	lifetimeClient  *lifetimesfake.Clientset

	lifetimeLister []*lifetimesapi.DataLifetime

	lifetimeActions []actionInfo
}

type fixture struct {
	t *testing.T

	kubeObjects         []runtime.Object
	messageQueueObjects []runtime.Object

	kubeClient             *k8sfake.Clientset
	messageQueueDataClient *corefake.Clientset

	lifetimes map[string]*lifetimesFixture

	miniRedis *miniredis.Miniredis
}

type fakeSharedInformer struct {
	dataClient     coreclientset.Interface
	lifetimeClient lifetimesclientset.Interface
	factory        *sharedInformerFactory
}

type publishedDataSpec struct {
	volumeRootPath string
	outputDataSpec *lifetimesapi.OutputFileSystemSpec
	updateTopic    string
}

type publisherFixture struct {
	dataObjects               []runtime.Object
	kubeObjects               []runtime.Object
	messageQueueSpec          *lifetimesapi.MessageQueueSpec
	messageQueuePublisherOpts *mqpub.MessageQueuePublisherOptions
	messageQueueConfig        *messagequeue.MessageQueueConfig
	outputDataSpec            publishedDataSpec
	updateTopic               string
}

func createUpdateActionInfo(
	statusKeys []string, counts int, isUpdate bool,
	isProviderUpdated bool) *updateActionInfo {
	actionInfo := &updateActionInfo{
		statusKeys:        statusKeys,
		counts:            counts,
		isUpdated:         isUpdate,
		isProviderUpdated: isProviderUpdated,
		isDeleted:         !isUpdate,
	}

	return actionInfo
}

func TestGetTargetLifetimes(t *testing.T) {
	lifetimeNamespace1 := metav1.NamespaceDefault
	lifetimeNamespace2 := metav1.NamespaceSystem
	lifetimeName1 := "lifetime1"
	lifetimeName2 := "lifetime2"
	apiServerUrl := "http://127.0.0.1"
	kubeConfig := "/tmp/provider_kubeconfig1"
	targetLifetime1 := util.ConcatenateNamespaceAndName(
		lifetimeNamespace1, lifetimeName1)
	targetLifetime2 := util.ConcatenateNamespaceAndName(
		lifetimeNamespace1, lifetimeName2)
	targetLifetime3 := util.ConcatenateNamespaceAndName(
		lifetimeNamespace2, lifetimeName1)
	lifetimesJson := fmt.Sprintf(
		"[{\"apiServerUrl\": %q, \"kubeConfig\": %q, "+
			"\"lifetimesNames\": [%q, %q]}, "+
			"{\"lifetimesNames\": [%q]}]",
		apiServerUrl, kubeConfig, targetLifetime1, targetLifetime2,
		targetLifetime3)
	os.Setenv("K8S_LIFETIMES_NAMES", lifetimesJson)

	providerLifetimes, err := getTargetLifetimes()
	assert.NoError(t, err)

	expectedProviderLifetimes := []ProviderLifetimes{
		{
			ApiServerUrl: apiServerUrl,
			KubeConfig:   kubeConfig,
			LifetimesNames: []string{
				targetLifetime1,
				targetLifetime2,
			},
		},
		{
			LifetimesNames: []string{
				targetLifetime3,
			},
		},
	}
	assert.Equal(t, expectedProviderLifetimes, providerLifetimes)
}

func newDataLifetimeWithTrigger(
	namespace string, name string, endTime string) *lifetimesapi.DataLifetime {
	return &lifetimesapi.DataLifetime{
		TypeMeta: metav1.TypeMeta{
			APIVersion: lifetimesapi.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: lifetimesapi.LifetimeSpec{
			Trigger: &lifetimesapi.Trigger{
				EndTime: endTime,
			},
		},
	}
}

func validateTrigger(
	t *testing.T, triggerUpdateCh <-chan triggerPolicyUpdate,
	stopCh chan struct{}, endTimes endTimes) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	triggerUpdate := triggerPolicyUpdate{}
	select {
	case <-ticker.C:
	case triggerUpdate = <-triggerUpdateCh:
	}

	expectedTriggerUpdate := triggerPolicyUpdate{}

	if endTimes.old != endTimes.new {
		expectedTriggerUpdate.expiredTime = endTimes.new
	}

	assert.Equal(t, expectedTriggerUpdate, triggerUpdate)

	close(stopCh)
}

func testUpdateTrigger(
	t *testing.T, endTimes endTimes, expectedupdated bool) {
	lifetimeNamespace := metav1.NamespaceSystem
	lifetimeName := "lifetime1"
	targetLifetime := util.ConcatenateNamespaceAndName(
		lifetimeNamespace, lifetimeName)

	kubeConfigName := "kubeconfig1"
	key := generateKey(kubeConfigName, targetLifetime)

	oldLifetime := newDataLifetimeWithTrigger(
		lifetimeNamespace, lifetimeName, endTimes.old)

	controller := LifetimeController{
		finChs: map[string]*lifetimeChannel{
			key: &lifetimeChannel{
				lifetimeSpec: &oldLifetime.Spec,
			},
		},
		updateChs: map[string]*policyUpdateChannels{},
	}

	triggerChs, _ := controller.setupPolicyUpdateChannels(key, 1)
	assert.Equal(t, 1, len(triggerChs))

	finCh := make(chan struct{})
	go validateTrigger(t, triggerChs[0], finCh, endTimes)

	newLifetime := newDataLifetimeWithTrigger(
		lifetimeNamespace, lifetimeName, endTimes.new)

	updated, err := controller.updateTrigger(key, newLifetime)
	assert.NoError(t, err)
	assert.Equal(t, expectedupdated, updated)

	<-finCh
}

func TestUpdateTriggerForEndTimeUpdate(t *testing.T) {
	now := time.Now()
	oldEndTime := now.Add(time.Duration(1 * time.Second))
	oldEndTimeRFC3339 := oldEndTime.Format(time.RFC3339)
	newEndTime := now.Add(time.Duration(2 * time.Second))
	newEndTimeRFC3339 := newEndTime.Format(time.RFC3339)
	endTimes := endTimes{
		old: oldEndTimeRFC3339,
		new: newEndTimeRFC3339,
	}

	testUpdateTrigger(t, endTimes, true)
}

func TestUpdateTriggerForNonUpdate(t *testing.T) {
	now := time.Now()
	endTime := now.Add(time.Duration(1 * time.Second))
	endTimeRFC3339 := endTime.Format(time.RFC3339)
	endTimes := endTimes{
		old: endTimeRFC3339,
		new: endTimeRFC3339,
	}

	testUpdateTrigger(t, endTimes, false)
}

func newLifetimesFixture(
	messageQueueConfigs []*coreapi.MessageQueue,
	lifetimes []*lifetimesapi.DataLifetime) *lifetimesFixture {
	dataObjects := make([]runtime.Object, 0, len(messageQueueConfigs))
	lifetimeObjects := make([]runtime.Object, 0, len(lifetimes))

	for _, messageQueueConfig := range messageQueueConfigs {
		dataObjects = append(dataObjects, messageQueueConfig)
	}

	for _, lifetime := range lifetimes {
		lifetimeObjects = append(lifetimeObjects, lifetime)
	}

	return &lifetimesFixture{
		dataObjects:     dataObjects,
		lifetimeObjects: lifetimeObjects,
		lifetimeLister:  lifetimes,
	}
}

func newFixture(
	t *testing.T, lifetimes map[string]*lifetimesFixture,
	miniRedis *miniredis.Miniredis) *fixture {
	return &fixture{
		t:                   t,
		kubeObjects:         []runtime.Object{},
		messageQueueObjects: []runtime.Object{},
		lifetimes:           lifetimes,
		miniRedis:           miniRedis,
	}
}

func (f *fixture) createFakeSharedInformers(
	key string) map[string]*fakeSharedInformer {
	sharedInformers := map[string]*fakeSharedInformer{}

	for kubeConfigName, lf := range f.lifetimes {
		if !strings.HasPrefix(key, kubeConfigName) {
			continue
		}

		dataClient := corefake.NewSimpleClientset(lf.dataObjects...)
		lf.dataClient = dataClient

		lifetimeClient := lifetimesfake.NewSimpleClientset(lf.lifetimeObjects...)
		lf.lifetimeClient = lifetimeClient

		sharedInformers[key] = &fakeSharedInformer{
			dataClient:     dataClient,
			lifetimeClient: lifetimeClient,
		}
		dataLifetimeInformerFactory := lifetimesinformers.NewSharedInformerFactory(
			lifetimeClient, noResyncPeriodFunc())

		sharedInformers[key].factory = &sharedInformerFactory{
			lifetimes: dataLifetimeInformerFactory,
		}
	}

	return sharedInformers
}

func (f *fixture) overwriteSharedInformers(
	controller *LifetimeController,
	sharedInformers map[string]*fakeSharedInformer) {
outerLoop:
	for key, sharedInformer := range controller.providerSharedInformers {
		for k, informer := range sharedInformers {
			if k == key {
				sharedInformer.dataClient = informer.dataClient
				sharedInformer.dataLifetimesClient = informer.lifetimeClient
				sharedInformer.lifetimesLister = informer.factory.lifetimes.
					Lifetimes().V1alpha1().DataLifetimes().Lister()
				continue outerLoop
			}
		}

		assert.FailNow(f.t, fmt.Sprintf("Not find the API server for %q", key))
	}
}

func getKubeConfigName(t *testing.T, key string) string {
	keys := strings.Split(key, keyDelimiter)
	if len(keys) != 2 {
		t.Errorf("Not get kubeconfig name: %s\n", key)
	}

	return keys[0]
}

func (f *fixture) startSharedInformers(
	sharedInformers map[string]*fakeSharedInformer, stopCh <-chan struct{}) {
	for key, informer := range sharedInformers {
		kubeConfigName := getKubeConfigName(f.t, key)

		for _, lifetime := range f.lifetimes[kubeConfigName].lifetimeLister {
			informer.factory.lifetimes.Lifetimes().V1alpha1().DataLifetimes().
				Informer().GetIndexer().Add(lifetime)
		}

		informer.factory.lifetimes.Start(stopCh)
	}
}

func (f *fixture) newFakeController(
	key string, stopCh <-chan struct{}) *LifetimeController {
	kubeClient := k8sfake.NewSimpleClientset(f.kubeObjects...)
	f.kubeClient = kubeClient

	messageQueueDataClient := corefake.NewSimpleClientset(
		f.messageQueueObjects...)
	f.messageQueueDataClient = messageQueueDataClient

	volumeController := &options.VolumeControllerOptions{
		DisableUsageControl: true,
	}
	messageQueuePublisherOpts := &mqpub.MessageQueuePublisherOptions{
		CompressionCodec: mqpub.CompressionCodecNone,
	}

	dataStoreOpts := newFakeRedisValidationClient(f.t, f.miniRedis)

	controller := newLifetimeController(
		kubeClient, messageQueueDataClient, volumeController,
		messageQueuePublisherOpts, dataStoreOpts, stopCh)

	sharedInformerFactories, err := createProviderSharedInformers(
		controller, 60*time.Second)
	assert.NoError(f.t, err)

	sharedInformers := f.createFakeSharedInformers(key)
	assert.Equal(f.t, len(sharedInformers), len(sharedInformerFactories))

	f.overwriteSharedInformers(controller, sharedInformers)

	controller.recorder = &record.FakeRecorder{}

	f.startSharedInformers(sharedInformers, stopCh)

	return controller
}

func filterInformerActions(
	actions []clienttesting.Action) []clienttesting.Action {
	filteredActions := make([]clienttesting.Action, 0, len(actions))

	for _, action := range actions {
		if len(action.GetNamespace()) == 0 &&
			action.Matches("watch", "datalifetimes") {
			continue
		}

		filteredActions = append(filteredActions, action)
	}

	return filteredActions
}

func checkObject(
	t *testing.T, expectedObject runtime.Object, actualObject runtime.Object,
	updateInfo *updateActionInfo) {
	switch objectType := expectedObject.(type) {
	case *coreapi.MessageQueue:
		assert.True(t, assert.ObjectsAreEqual(expectedObject, actualObject))

	case *lifetimesapi.DataLifetime:
		expectedLifetime := expectedObject.(*lifetimesapi.DataLifetime)
		actualLifetime, ok := actualObject.(*lifetimesapi.DataLifetime)
		assert.True(t, ok)

		if !updateInfo.isUpdated && !updateInfo.isDeleted {
			assert.True(
				t, assert.ObjectsAreEqual(expectedLifetime, actualLifetime))
			return
		}

		if updateInfo.isUpdated {
			if updateInfo.isProviderUpdated {
				if actualLifetime.Status.Provider.LastUpdatedAt.IsZero() {
					t.Errorf("Provider 'LastUpdatedAt' is ZERO.")
				}
			} else {
				counter := 0
				for _, key := range updateInfo.statusKeys {
					status, ok := actualLifetime.Status.Consumer[key]
					if ok {
						if !status.LastUpdatedAt.IsZero() {
							counter += 1
						}
					}
				}
				assert.Equal(t, updateInfo.counts, counter)
			}
		}

		if updateInfo.isDeleted {
			counter := 0
			for _, key := range updateInfo.statusKeys {
				status, ok := actualLifetime.Status.Consumer[key]
				if ok {
					if !status.ExpiredAt.IsZero() {
						counter += 1
					}
				}
			}
			assert.Equal(t, updateInfo.counts, counter)
		}

	default:
		t.Errorf("Not support such an action type: %v\n", objectType)
	}
}

func checkAction(
	t *testing.T, expected clienttesting.Action, actual clienttesting.Action,
	updateInfo *updateActionInfo) {
	assert.True(
		t, expected.Matches(actual.GetVerb(), actual.GetResource().Resource))

	assert.Equal(t, expected.GetSubresource(), actual.GetSubresource())

	assert.Equal(t, reflect.TypeOf(expected), reflect.TypeOf(actual))

	switch actualType := actual.(type) {
	// 'CreationAction' and 'UpdateAction' are the same interface
	case clienttesting.UpdateAction:
		e, ok := expected.(clienttesting.UpdateAction)
		assert.True(t, ok)

		expectedObject := e.GetObject()
		actualObject := actualType.GetObject()
		checkObject(t, expectedObject, actualObject, updateInfo)

	case clienttesting.DeleteAction:
		e, ok := expected.(clienttesting.DeleteAction)
		assert.True(t, ok)

		expectedNamespace := e.GetNamespace()
		actualNamespace := actualType.GetNamespace()
		assert.True(t, expectedNamespace == actualNamespace)

		expectedName := e.GetName()
		actualName := actualType.GetName()
		assert.True(t, expectedName == actualName)

	case clienttesting.ListAction:
		e, ok := expected.(clienttesting.ListAction)
		assert.True(t, ok)

		expectedList, ok := e.DeepCopy().(clienttesting.ListActionImpl)
		assert.True(t, ok)
		actualList, ok := actualType.DeepCopy().(clienttesting.ListActionImpl)
		assert.True(t, ok)

		assert.True(
			t, assert.ObjectsAreEqual(expectedList.Kind, actualList.Kind))

	default:
		t.Errorf("Not support such an action type: %v\n", actualType)
	}
}

func checkActions(
	t *testing.T, expected []actionInfo, actual []clienttesting.Action) {
	filteredActions := filterInformerActions(actual)
	assert.Equal(t, len(expected), len(filteredActions))

	for index, action := range filteredActions {
		expectedAction := expected[index]
		checkAction(
			t, expectedAction.action, action, expectedAction.updateInfo)
	}
}

func (f *fixture) runController(
	key string, expectError bool) *LifetimeController {
	stopCh := util.SetupSignalHandler()

	controller := f.newFakeController(key, stopCh)

	err := controller.syncHandler(key)
	if expectError {
		assert.Error(f.t, err)
	} else {
		assert.NoError(f.t, err)
	}

	return controller
}

func createKubeConfig(t *testing.T) *os.File {
	kubeConfig, err := ioutil.TempFile("", "kubeconfig")
	if err != nil {
		t.Error(err)
	}

	err = ioutil.WriteFile(kubeConfig.Name(), []byte(kubeConfigContent), 0666)
	assert.NoError(t, err)

	return kubeConfig
}

func newPod(
	namespace string, name string, dataContainerName string,
	volumeMounts []k8scorev1.VolumeMount,
	volumes []k8scorev1.Volume) *k8scorev1.Pod {
	pod := &k8scorev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: k8scorev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: k8scorev1.PodSpec{
			Containers: []k8scorev1.Container{
				{
					Name:         dataContainerName,
					VolumeMounts: volumeMounts,
				},
			},
			Volumes: volumes,
		},
		Status: k8scorev1.PodStatus{
			ContainerStatuses: []k8scorev1.ContainerStatus{
				{
					Name: dataContainerName,
					State: k8scorev1.ContainerState{
						Running: &k8scorev1.ContainerStateRunning{
							StartedAt: metav1.Time{
								Time: time.Now(),
							},
						},
					},
				},
			},
		},
	}

	return pod
}

func newSecret() *k8scorev1.Secret {
	saslData := map[string][]byte{
		"password": []byte(saslPassword),
	}

	return &k8scorev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: k8scorev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      saslUser,
			Namespace: saslNamespace,
		},
		Data: saslData,
	}
}

func newMessageQueue() *coreapi.MessageQueue {
	return &coreapi.MessageQueue{
		TypeMeta: metav1.TypeMeta{
			APIVersion: coreapi.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      mqName,
			Namespace: mqNamespace,
		},
		Spec: coreapi.MessageQueueSpec{
			Brokers: []string{
				os.Getenv(kafkaBrokerAddressEnv),
			},
			SaslRef: &corev1.ObjectReference{
				Name:      saslUser,
				Namespace: saslNamespace,
			},
		},
	}
}

func newDataLifetime(
	lifetimeNamespace string, lifetimeName string,
	inputData []lifetimesapi.InputDataSpec,
	outputData []lifetimesapi.OutputDataSpec, startOffset time.Duration,
	endOffset time.Duration) *lifetimesapi.DataLifetime {
	now := time.Now()
	startTime := now.Add(startOffset)
	startTimeRFC3339 := startTime.Format(time.RFC3339)
	endTime := now.Add(endOffset)
	endTimeRFC3339 := endTime.Format(time.RFC3339)

	return &lifetimesapi.DataLifetime{
		TypeMeta: metav1.TypeMeta{
			APIVersion: lifetimesapi.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      lifetimeName,
			Namespace: lifetimeNamespace,
		},
		Spec: lifetimesapi.LifetimeSpec{
			Trigger: &lifetimesapi.Trigger{
				StartTime: startTimeRFC3339,
				EndTime:   endTimeRFC3339,
			},
			InputData:  inputData,
			OutputData: outputData,
		},
	}
}

func newPublisherFixture(
	messageQueueConfigs []*coreapi.MessageQueue, kubeObjects []runtime.Object,
	messageQueueSpec *lifetimesapi.MessageQueueSpec,
	publishedData publishedDataSpec) *publisherFixture {
	brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
	dataObjects := make([]runtime.Object, 0, len(messageQueueConfigs))

	for _, messageQueueConfig := range messageQueueConfigs {
		dataObjects = append(dataObjects, messageQueueConfig)
	}

	messageQueueConfig := messagequeue.NewMessageQueueConfig(
		[]string{brokerAddress}, saslUser, saslPassword)

	messageQueuePublisherOpts := &mqpub.MessageQueuePublisherOptions{
		CompressionCodec: mqpub.CompressionCodecNone,
	}

	return &publisherFixture{
		dataObjects:               dataObjects,
		kubeObjects:               kubeObjects,
		messageQueueSpec:          messageQueueSpec,
		messageQueuePublisherOpts: messageQueuePublisherOpts,
		messageQueueConfig:        messageQueueConfig,
		outputDataSpec:            publishedData,
	}
}

func (pf *publisherFixture) initPublisherRuns(
	t *testing.T, ctx context.Context, wg *sync.WaitGroup,
	initWg *sync.WaitGroup) {
	defer wg.Done()

	dataClient := corefake.NewSimpleClientset(pf.dataObjects...)
	kubeClient := k8sfake.NewSimpleClientset(pf.kubeObjects...)

	publisher, err := volumepub.NewVolumeMessageQueueInitPublisher(
		pf.outputDataSpec.outputDataSpec, pf.messageQueuePublisherOpts,
		dataClient, kubeClient, pf.messageQueueSpec,
		pf.outputDataSpec.volumeRootPath)
	assert.NoError(t, err)
	initWg.Done()

	defer publisher.CloseMessageQueues()

	err = publisher.HandleInitQueueMessages(ctx)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func (pf *publisherFixture) updatePublisherRuns(
	t *testing.T, ctx context.Context, wg *sync.WaitGroup,
	initWg *sync.WaitGroup) {
	defer wg.Done()

	publisher, err := volumepub.NewVolumeMessageQueueUpdatePublisher(
		pf.messageQueueConfig, pf.outputDataSpec.updateTopic,
		pf.messageQueuePublisherOpts.CompressionCodec,
		pf.outputDataSpec.volumeRootPath, 0, 10)
	assert.NoError(t, err)
	initWg.Done()

	defer publisher.CloseMessageQueue()

	<-ctx.Done()
}

func createInputDirectoriesAndOutputFiles(t *testing.T) {
	err := os.Mkdir(destinationRootDirectory1, 0755)
	assert.NoError(t, err)
	err = os.Mkdir(destinationRootDirectory2, 0755)
	assert.NoError(t, err)

	err = os.Mkdir(outputRootDirectory1, 0755)
	assert.NoError(t, err)
	err = os.Mkdir(outputRootDirectory2, 0755)
	assert.NoError(t, err)

	file1, err := os.Create(outputFilePath1)
	assert.NoError(t, err)
	err = file1.Close()
	assert.NoError(t, err)

	file2, err := os.Create(outputFilePath2)
	assert.NoError(t, err)
	err = file2.Close()
	assert.NoError(t, err)
}

func removeInputAndOutputDirectories(t *testing.T) {
	os.RemoveAll(destinationRootDirectory1)
	os.RemoveAll(destinationRootDirectory2)

	os.RemoveAll(outputRootDirectory1)
	os.RemoveAll(outputRootDirectory2)
}

func validateDeletedFiles(t *testing.T) {
	dst, err := os.Stat(destinationFilePath1)
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0000), dst.Mode())
	dst, err = os.Stat(destinationFilePath2)
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0000), dst.Mode())
	dst, err = os.Stat(destinationFilePath3)
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0000), dst.Mode())

	output, err := os.Stat(outputFilePath1)
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0000), output.Mode())
	output, err = os.Stat(outputFilePath2)
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0000), output.Mode())
}

func createStatusKeys(lifetimeSpec *lifetimesapi.LifetimeSpec) []string {
	statusKeys := make(
		[]string, 0, len(lifetimeSpec.InputData)+len(lifetimeSpec.OutputData))

	for _, dataSpec := range lifetimeSpec.InputData {
		toPvcRef := dataSpec.FileSystemSpec.ToPersistentVolumeClaimRef
		dataName := fmt.Sprintf(
			"%s:%s/%s", fileDataNamePrefix, toPvcRef.Namespace, toPvcRef.Name)
		statusKeys = append(statusKeys, dataName)
	}

	for _, dataSpec := range lifetimeSpec.OutputData {
		pvcRef := dataSpec.FileSystemSpec.PersistentVolumeClaimRef
		dataName := fmt.Sprintf(
			"%s:%s/%s", fileDataNamePrefix, pvcRef.Namespace, pvcRef.Name)
		statusKeys = append(statusKeys, dataName)
	}

	return statusKeys
}

func createLifetimeNewListAction(namespace string) actionInfo {
	return actionInfo{
		action: clienttesting.NewListAction(
			schema.GroupVersionResource{Resource: "datalifetimes"},
			schema.GroupVersionKind{
				Group:   lifetimesapi.SchemeGroupVersion.Group,
				Version: lifetimesapi.SchemeGroupVersion.Version,
				Kind:    "DataLifetime",
			}, namespace, metav1.ListOptions{}),
	}
}

func createLifetimeUpdateAction(
	namespace string, lifetime *lifetimesapi.DataLifetime,
	updateInfo *updateActionInfo) actionInfo {
	return actionInfo{
		action: clienttesting.NewUpdateAction(
			schema.GroupVersionResource{Resource: "datalifetimes"},
			namespace, lifetime),
		updateInfo: updateInfo,
	}
}

func TestUpdateAndDeleteDatas(t *testing.T) {
	providerPvcName1 := "provider-pvc-1-1"
	providerPvcName2 := "provider-pvc-2-1"

	lifetimeNamespace := metav1.NamespaceSystem
	lifetimeName := "lifetime1"
	startOffset := time.Duration(-1 * time.Second)
	endOffset := time.Duration(33 * time.Second)

	fromPvcRef1 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      providerPvcName1,
	}
	toPvcRef1 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      consumerPvcName1,
	}
	fromPvcRef2 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      providerPvcName2,
	}
	toPvcRef2 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      consumerPvcName2,
	}
	messageQueueSpec := &lifetimesapi.MessageQueueSpec{
		MessageQueueRef: &k8scorev1.ObjectReference{
			Namespace: mqNamespace,
			Name:      mqName,
		},
	}
	inputData := []lifetimesapi.InputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
				FromPersistentVolumeClaimRef: fromPvcRef1,
				ToPersistentVolumeClaimRef:   toPvcRef1,
			},
			MessageQueueSubscriber: messageQueueSpec,
		},
		{
			FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
				FromPersistentVolumeClaimRef: fromPvcRef2,
				ToPersistentVolumeClaimRef:   toPvcRef2,
			},
			MessageQueueSubscriber: messageQueueSpec,
		},
	}
	pvcRef1 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      consumerPvcName3,
	}
	pvcRef2 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      consumerPvcName4,
	}
	outputData := []lifetimesapi.OutputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: pvcRef1,
			},
		},
		{
			FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: pvcRef2,
			},
		},
	}
	lifetime := newDataLifetime(
		lifetimeNamespace, lifetimeName, inputData, outputData, startOffset,
		endOffset)

	messageQueueConfigs := []*coreapi.MessageQueue{
		newMessageQueue(),
	}
	lifetimes := []*lifetimesapi.DataLifetime{
		lifetime,
	}

	kubeConfig := createKubeConfig(t)
	defer os.Remove(kubeConfig.Name())

	kubeConfigName := kubeConfig.Name()
	targetLifetime := util.ConcatenateNamespaceAndName(
		lifetimeNamespace, lifetimeName)

	lifetimesJson := fmt.Sprintf(
		"[{\"kubeConfig\": %q, \"lifetimesNames\": [%q]}]",
		kubeConfigName, targetLifetime)
	os.Setenv("K8S_LIFETIMES_NAMES", lifetimesJson)

	ltFixture := newLifetimesFixture(messageQueueConfigs, lifetimes)
	lf := map[string]*lifetimesFixture{
		kubeConfigName: ltFixture,
	}

	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	f := newFixture(t, lf, miniRedis)

	podNamespace := metav1.NamespaceDefault
	podName := "pod1"
	dataContainerName := "data-c"
	volumeMounts := []k8scorev1.VolumeMount{
		{
			Name:      podSourceVolume1,
			MountPath: sourceRootDirectory1,
		},
		{
			Name:      podLocalVolume1,
			MountPath: destinationRootDirectory1,
		},
		{
			Name:      podSourceVolume2,
			MountPath: sourceRootDirectory2,
		},
		{
			Name:      podLocalVolume2,
			MountPath: destinationRootDirectory2,
		},
		{
			Name:      podOutputVolume1,
			MountPath: outputRootDirectory1,
		},
		{
			Name:      podOutputVolume2,
			MountPath: outputRootDirectory2,
		},
	}
	volumes := []k8scorev1.Volume{
		{
			Name: podSourceVolume1,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: providerPvcName1,
				},
			},
		},
		{
			Name: podLocalVolume1,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName1,
				},
			},
		},
		{
			Name: podSourceVolume2,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: providerPvcName2,
				},
			},
		},
		{
			Name: podLocalVolume2,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName2,
				},
			},
		},
		{
			Name: podOutputVolume1,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName3,
				},
			},
		},
		{
			Name: podOutputVolume2,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName4,
				},
			},
		},
	}
	pod := newPod(
		podNamespace, podName, dataContainerName, volumeMounts, volumes)

	secret := newSecret()

	f.kubeObjects = append(f.kubeObjects, pod, secret)
	for _, messageQueue := range messageQueueConfigs {
		f.messageQueueObjects = append(f.messageQueueObjects, messageQueue)
	}
	os.Setenv(podNamespaceEnv, podNamespace)
	os.Setenv(podNameEnv, podName)
	os.Setenv(dataContainerNameEnv, dataContainerName)

	lifetimeKey, err := cache.DeletionHandlingMetaNamespaceKeyFunc(lifetime)
	assert.NoError(t, err)
	assert.Equal(t, targetLifetime, lifetimeKey)

	createInputDirectoriesAndOutputFiles(t)
	defer removeInputAndOutputDirectories(t)

	topic1 := volumemq.GetMessageQueueTopic(dataNamespace, providerPvcName1)
	initTopics1, err := topic1.CreateInitTopics()
	assert.NoError(t, err)
	updateTopicName1, err := topic1.CreateUpdateTopic()
	assert.NoError(t, err)
	topic2 := volumemq.GetMessageQueueTopic(dataNamespace, providerPvcName2)
	initTopics2, err := topic2.CreateInitTopics()
	assert.NoError(t, err)
	updateTopicName2, err := topic2.CreateUpdateTopic()
	assert.NoError(t, err)

	kubeObjects := []runtime.Object{
		secret,
	}
	publishedDataSpecs := []publishedDataSpec{
		{
			volumeRootPath: sourceRootDirectory1,
			outputDataSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: dataNamespace,
					Name:      providerPvcName1,
				},
			},
			updateTopic: updateTopicName1,
		},
		{
			volumeRootPath: sourceRootDirectory2,
			outputDataSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: dataNamespace,
					Name:      providerPvcName2,
				},
			},
			updateTopic: updateTopicName2,
		},
	}
	publishersWg := new(sync.WaitGroup)
	publishersInitWg := new(sync.WaitGroup)
	ctx, cancel := context.WithCancel(context.Background())
	for _, publishedDataSpec := range publishedDataSpecs {
		pf := newPublisherFixture(
			messageQueueConfigs, kubeObjects, messageQueueSpec,
			publishedDataSpec)

		publishersWg.Add(1)
		publishersInitWg.Add(1)
		go pf.initPublisherRuns(t, ctx, publishersWg, publishersInitWg)
		publishersWg.Add(1)
		publishersInitWg.Add(1)
		go pf.updatePublisherRuns(t, ctx, publishersWg, publishersInitWg)
	}

	publishersInitWg.Wait()

	defer func() {
		brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
		config := messagequeue.NewMessageQueueConfig(
			[]string{brokerAddress}, saslUser, saslPassword)
		dialer, err := config.CreateSaslDialer()
		assert.NoError(t, err)
		topicNames := []string{
			initTopics1.Request,
			initTopics1.Response,
			updateTopicName1,
			initTopics2.Request,
			initTopics2.Response,
			updateTopicName2,
		}
		err = messagequeue.DeleteTopics(brokerAddress, dialer, topicNames)
		assert.NoError(t, err)
	}()

	key := generateKey(kubeConfigName, targetLifetime)
	f.runController(key, false)

	time.Sleep(35 * time.Second)

	cancel()
	publishersWg.Wait()

	validateDeletedFiles(t)

	statusKeys := createStatusKeys(&lifetime.Spec)
	updateInfo1 := createUpdateActionInfo(statusKeys, 1, true, false)
	updateInfo2 := createUpdateActionInfo(statusKeys, 2, true, false)
	deleteInfo1 := createUpdateActionInfo(statusKeys, 1, false, false)
	deleteInfo2 := createUpdateActionInfo(statusKeys, 2, false, false)
	deleteInfo3 := createUpdateActionInfo(statusKeys, 3, false, false)
	deleteInfo4 := createUpdateActionInfo(statusKeys, 4, false, false)

	lf[kubeConfigName].lifetimeActions = append(
		lf[kubeConfigName].lifetimeActions,
		createLifetimeNewListAction(dataNamespace),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, updateInfo1),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, updateInfo2),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, deleteInfo1),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, deleteInfo2),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, deleteInfo3),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, deleteInfo4),
	)
	checkActions(
		f.t, lf[kubeConfigName].lifetimeActions,
		lf[kubeConfigName].lifetimeClient.Actions())
}

func TestUpdateDatas(t *testing.T) {
	providerPvcName1 := "provider-pvc-1-2"
	providerPvcName2 := "provider-pvc-2-2"

	lifetimeNamespace := metav1.NamespaceSystem
	lifetimeName := "lifetime1"
	startOffset := time.Duration(-1 * time.Second)
	endOffset := time.Duration(35 * time.Second)

	fromPvcRef1 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      providerPvcName1,
	}
	toPvcRef1 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      consumerPvcName1,
	}
	fromPvcRef2 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      providerPvcName2,
	}
	toPvcRef2 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      consumerPvcName2,
	}
	messageQueueSpec := &lifetimesapi.MessageQueueSpec{
		MessageQueueRef: &k8scorev1.ObjectReference{
			Namespace: mqNamespace,
			Name:      mqName,
		},
	}
	inputData := []lifetimesapi.InputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
				FromPersistentVolumeClaimRef: fromPvcRef1,
				ToPersistentVolumeClaimRef:   toPvcRef1,
			},
			MessageQueueSubscriber: messageQueueSpec,
		},
		{
			FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
				FromPersistentVolumeClaimRef: fromPvcRef2,
				ToPersistentVolumeClaimRef:   toPvcRef2,
			},
			MessageQueueSubscriber: messageQueueSpec,
		},
	}
	outputData := []lifetimesapi.OutputDataSpec{}
	lifetime := newDataLifetime(
		lifetimeNamespace, lifetimeName, inputData, outputData, startOffset,
		endOffset)

	messageQueueConfigs := []*coreapi.MessageQueue{
		newMessageQueue(),
	}
	lifetimes := []*lifetimesapi.DataLifetime{
		lifetime,
	}

	kubeConfig := createKubeConfig(t)
	defer os.Remove(kubeConfig.Name())

	kubeConfigName := kubeConfig.Name()
	targetLifetime := util.ConcatenateNamespaceAndName(
		lifetimeNamespace, lifetimeName)

	lifetimesJson := fmt.Sprintf(
		"[{\"kubeConfig\": %q, \"lifetimesNames\": [%q]}]",
		kubeConfigName, targetLifetime)
	os.Setenv("K8S_LIFETIMES_NAMES", lifetimesJson)

	ltFixture := newLifetimesFixture(messageQueueConfigs, lifetimes)
	lf := map[string]*lifetimesFixture{
		kubeConfigName: ltFixture,
	}

	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	f := newFixture(t, lf, miniRedis)

	podNamespace := metav1.NamespaceDefault
	podName := "pod1"
	dataContainerName := "data-c"
	volumeMounts := []k8scorev1.VolumeMount{
		{
			Name:      podSourceVolume1,
			MountPath: sourceRootDirectory1,
		},
		{
			Name:      podLocalVolume1,
			MountPath: destinationRootDirectory1,
		},
		{
			Name:      podSourceVolume2,
			MountPath: sourceRootDirectory2,
		},
		{
			Name:      podLocalVolume2,
			MountPath: destinationRootDirectory2,
		},
	}
	volumes := []k8scorev1.Volume{
		{
			Name: podSourceVolume1,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: providerPvcName1,
				},
			},
		},
		{
			Name: podLocalVolume1,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName1,
				},
			},
		},
		{
			Name: podSourceVolume2,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: providerPvcName2,
				},
			},
		},
		{
			Name: podLocalVolume2,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName2,
				},
			},
		},
	}
	pod := newPod(
		podNamespace, podName, dataContainerName, volumeMounts, volumes)

	secret := newSecret()

	f.kubeObjects = append(f.kubeObjects, pod, secret)
	for _, messageQueue := range messageQueueConfigs {
		f.messageQueueObjects = append(f.messageQueueObjects, messageQueue)
	}
	os.Setenv(podNamespaceEnv, podNamespace)
	os.Setenv(podNameEnv, podName)
	os.Setenv(dataContainerNameEnv, dataContainerName)

	lifetimeKey, err := cache.DeletionHandlingMetaNamespaceKeyFunc(lifetime)
	assert.NoError(t, err)
	assert.Equal(t, targetLifetime, lifetimeKey)

	err = os.Mkdir(destinationRootDirectory1, 0755)
	assert.NoError(t, err)
	err = os.Mkdir(destinationRootDirectory2, 0755)
	assert.NoError(t, err)
	defer os.Remove(destinationRootDirectory1)
	defer os.Remove(destinationRootDirectory2)

	topic1 := volumemq.GetMessageQueueTopic(dataNamespace, providerPvcName1)
	initTopics1, err := topic1.CreateInitTopics()
	assert.NoError(t, err)
	updateTopicName1, err := topic1.CreateUpdateTopic()
	assert.NoError(t, err)
	topic2 := volumemq.GetMessageQueueTopic(dataNamespace, providerPvcName2)
	initTopics2, err := topic2.CreateInitTopics()
	assert.NoError(t, err)
	updateTopicName2, err := topic2.CreateUpdateTopic()
	assert.NoError(t, err)

	kubeObjects := []runtime.Object{
		secret,
	}
	publishedDataSpecs := []publishedDataSpec{
		{
			volumeRootPath: sourceRootDirectory1,
			outputDataSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: dataNamespace,
					Name:      providerPvcName1,
				},
			},
			updateTopic: updateTopicName1,
		},
		{
			volumeRootPath: sourceRootDirectory2,
			outputDataSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: dataNamespace,
					Name:      providerPvcName2,
				},
			},
			updateTopic: updateTopicName2,
		},
	}
	publishersWg := new(sync.WaitGroup)
	publishersInitWg := new(sync.WaitGroup)
	ctx, cancel := context.WithCancel(context.Background())
	for _, publishedDataSpec := range publishedDataSpecs {
		pf := newPublisherFixture(
			messageQueueConfigs, kubeObjects, messageQueueSpec,
			publishedDataSpec)

		publishersWg.Add(1)
		publishersInitWg.Add(1)
		go pf.initPublisherRuns(t, ctx, publishersWg, publishersInitWg)
		publishersWg.Add(1)
		publishersInitWg.Add(1)
		go pf.updatePublisherRuns(t, ctx, publishersWg, publishersInitWg)
	}

	publishersInitWg.Wait()

	defer func() {
		brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
		config := messagequeue.NewMessageQueueConfig(
			[]string{brokerAddress}, saslUser, saslPassword)
		dialer, err := config.CreateSaslDialer()
		assert.NoError(t, err)
		topicNames := []string{
			initTopics1.Request,
			initTopics1.Response,
			updateTopicName1,
			initTopics2.Request,
			initTopics2.Response,
			updateTopicName2,
		}
		err = messagequeue.DeleteTopics(brokerAddress, dialer, topicNames)
		assert.NoError(t, err)
	}()

	key := generateKey(kubeConfigName, targetLifetime)
	controller := f.runController(key, false)

	time.Sleep(30 * time.Second)

	close(controller.finChs[key].finCh)

	cancel()
	publishersWg.Wait()

	dst, err := os.Stat(destinationFilePath1)
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0644), dst.Mode())
	dst, err = os.Stat(destinationFilePath2)
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0644), dst.Mode())
	dst, err = os.Stat(destinationFilePath3)
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0644), dst.Mode())

	assert.NoError(t, os.Remove(destinationFilePath1))
	assert.NoError(t, os.Remove(destinationFilePath2))
	assert.NoError(t, os.Remove(destinationFilePath3))

	statusKeys := createStatusKeys(&lifetime.Spec)
	updateInfo1 := createUpdateActionInfo(statusKeys, 1, true, false)
	updateInfo2 := createUpdateActionInfo(statusKeys, 2, true, false)

	lf[kubeConfigName].lifetimeActions = append(
		lf[kubeConfigName].lifetimeActions,
		createLifetimeNewListAction(dataNamespace),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, updateInfo1),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, updateInfo2),
	)
	checkActions(
		f.t, lf[kubeConfigName].lifetimeActions,
		lf[kubeConfigName].lifetimeClient.Actions())
}

func TestUpdateAndDeleteDatasWithTriggerUpdate(t *testing.T) {
	providerPvcName1 := "provider-pvc-1-3"
	providerPvcName2 := "provider-pvc-2-3"

	lifetimeNamespace := metav1.NamespaceSystem
	lifetimeName := "lifetime1"
	startOffset := time.Duration(-2 * time.Second)
	endOffset := time.Duration(40 * time.Second)

	fromPvcRef1 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      providerPvcName1,
	}
	toPvcRef1 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      consumerPvcName1,
	}
	fromPvcRef2 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      providerPvcName2,
	}
	toPvcRef2 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      consumerPvcName2,
	}
	messageQueueSpec := &lifetimesapi.MessageQueueSpec{
		MessageQueueRef: &k8scorev1.ObjectReference{
			Namespace: mqNamespace,
			Name:      mqName,
		},
	}
	inputData := []lifetimesapi.InputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
				FromPersistentVolumeClaimRef: fromPvcRef1,
				ToPersistentVolumeClaimRef:   toPvcRef1,
			},
			MessageQueueSubscriber: messageQueueSpec,
		},
		{
			FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
				FromPersistentVolumeClaimRef: fromPvcRef2,
				ToPersistentVolumeClaimRef:   toPvcRef2,
			},
			MessageQueueSubscriber: messageQueueSpec,
		},
	}
	pvcRef1 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      consumerPvcName3,
	}
	pvcRef2 := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      consumerPvcName4,
	}
	outputData := []lifetimesapi.OutputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: pvcRef1,
			},
		},
		{
			FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: pvcRef2,
			},
		},
	}
	lifetime := newDataLifetime(
		lifetimeNamespace, lifetimeName, inputData, outputData, startOffset,
		endOffset)

	messageQueueConfigs := []*coreapi.MessageQueue{
		newMessageQueue(),
	}
	lifetimes := []*lifetimesapi.DataLifetime{
		lifetime,
	}

	kubeConfig := createKubeConfig(t)
	defer os.Remove(kubeConfig.Name())

	kubeConfigName := kubeConfig.Name()
	targetLifetime := util.ConcatenateNamespaceAndName(
		lifetimeNamespace, lifetimeName)

	lifetimesJson := fmt.Sprintf(
		"[{\"kubeConfig\": %q, \"lifetimesNames\": [%q]}]",
		kubeConfigName, targetLifetime)
	os.Setenv("K8S_LIFETIMES_NAMES", lifetimesJson)

	ltFixture := newLifetimesFixture(messageQueueConfigs, lifetimes)
	lf := map[string]*lifetimesFixture{
		kubeConfigName: ltFixture,
	}

	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	f := newFixture(t, lf, miniRedis)

	podNamespace := metav1.NamespaceDefault
	podName := "pod1"
	dataContainerName := "data-c"
	volumeMounts := []k8scorev1.VolumeMount{
		{
			Name:      podSourceVolume1,
			MountPath: sourceRootDirectory1,
		},
		{
			Name:      podLocalVolume1,
			MountPath: destinationRootDirectory1,
		},
		{
			Name:      podSourceVolume2,
			MountPath: sourceRootDirectory2,
		},
		{
			Name:      podLocalVolume2,
			MountPath: destinationRootDirectory2,
		},
		{
			Name:      podOutputVolume1,
			MountPath: outputRootDirectory1,
		},
		{
			Name:      podOutputVolume2,
			MountPath: outputRootDirectory2,
		},
	}
	volumes := []k8scorev1.Volume{
		{
			Name: podSourceVolume1,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: providerPvcName1,
				},
			},
		},
		{
			Name: podLocalVolume1,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName1,
				},
			},
		},
		{
			Name: podSourceVolume2,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: providerPvcName2,
				},
			},
		},
		{
			Name: podLocalVolume2,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName2,
				},
			},
		},
		{
			Name: podOutputVolume1,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName3,
				},
			},
		},
		{
			Name: podOutputVolume2,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName4,
				},
			},
		},
	}
	pod := newPod(
		podNamespace, podName, dataContainerName, volumeMounts, volumes)

	secret := newSecret()

	f.kubeObjects = append(f.kubeObjects, pod, secret)
	for _, messageQueue := range messageQueueConfigs {
		f.messageQueueObjects = append(f.messageQueueObjects, messageQueue)
	}
	os.Setenv(podNamespaceEnv, podNamespace)
	os.Setenv(podNameEnv, podName)
	os.Setenv(dataContainerNameEnv, dataContainerName)

	lifetimeKey, err := cache.DeletionHandlingMetaNamespaceKeyFunc(lifetime)
	assert.NoError(t, err)
	assert.Equal(t, targetLifetime, lifetimeKey)

	createInputDirectoriesAndOutputFiles(t)
	defer removeInputAndOutputDirectories(t)

	topic1 := volumemq.GetMessageQueueTopic(dataNamespace, providerPvcName1)
	initTopics1, err := topic1.CreateInitTopics()
	assert.NoError(t, err)
	updateTopicName1, err := topic1.CreateUpdateTopic()
	assert.NoError(t, err)
	topic2 := volumemq.GetMessageQueueTopic(dataNamespace, providerPvcName2)
	initTopics2, err := topic2.CreateInitTopics()
	assert.NoError(t, err)
	updateTopicName2, err := topic2.CreateUpdateTopic()
	assert.NoError(t, err)

	kubeObjects := []runtime.Object{
		secret,
	}
	publishedDataSpecs := []publishedDataSpec{
		{
			volumeRootPath: sourceRootDirectory1,
			outputDataSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: dataNamespace,
					Name:      providerPvcName1,
				},
			},
			updateTopic: updateTopicName1,
		},
		{
			volumeRootPath: sourceRootDirectory2,
			outputDataSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: dataNamespace,
					Name:      providerPvcName2,
				},
			},
			updateTopic: updateTopicName2,
		},
	}
	publishersWg := new(sync.WaitGroup)
	publishersInitWg := new(sync.WaitGroup)
	ctx, cancel := context.WithCancel(context.Background())
	for _, publishedDataSpec := range publishedDataSpecs {
		pf := newPublisherFixture(
			messageQueueConfigs, kubeObjects, messageQueueSpec,
			publishedDataSpec)

		publishersWg.Add(1)
		publishersInitWg.Add(1)
		go pf.initPublisherRuns(t, ctx, publishersWg, publishersInitWg)
		publishersWg.Add(1)
		publishersInitWg.Add(1)
		go pf.updatePublisherRuns(t, ctx, publishersWg, publishersInitWg)
	}

	publishersInitWg.Wait()

	defer func() {
		brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
		config := messagequeue.NewMessageQueueConfig(
			[]string{brokerAddress}, saslUser, saslPassword)
		dialer, err := config.CreateSaslDialer()
		assert.NoError(t, err)
		topicNames := []string{
			initTopics1.Request,
			initTopics1.Response,
			updateTopicName1,
			initTopics2.Request,
			initTopics2.Response,
			updateTopicName2,
		}
		err = messagequeue.DeleteTopics(brokerAddress, dialer, topicNames)
		assert.NoError(t, err)
	}()

	key := generateKey(kubeConfigName, targetLifetime)
	controller := f.runController(key, false)

	time.Sleep(1 * time.Second)

	now := time.Now()
	endTime := now.Add(35 * time.Second)
	endTimeRFC3339 := endTime.Format(time.RFC3339)
	for _, triggerCh := range controller.updateChs[key].triggerChs {
		triggerCh <- triggerPolicyUpdate{
			expiredTime: endTimeRFC3339,
		}
	}

	time.Sleep(3 * time.Second)

	cancel()
	publishersWg.Wait()

	validateDeletedFiles(t)

	statusKeys := createStatusKeys(&lifetime.Spec)
	updateInfo1 := createUpdateActionInfo(statusKeys, 1, true, false)
	updateInfo2 := createUpdateActionInfo(statusKeys, 2, true, false)
	deleteInfo1 := createUpdateActionInfo(statusKeys, 1, false, false)
	deleteInfo2 := createUpdateActionInfo(statusKeys, 2, false, false)
	deleteInfo3 := createUpdateActionInfo(statusKeys, 3, false, false)
	deleteInfo4 := createUpdateActionInfo(statusKeys, 4, false, false)

	lf[kubeConfigName].lifetimeActions = append(
		lf[kubeConfigName].lifetimeActions,
		createLifetimeNewListAction(dataNamespace),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, updateInfo1),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, updateInfo2),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, deleteInfo1),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, deleteInfo2),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, deleteInfo3),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, deleteInfo4),
	)
	checkActions(
		f.t, lf[kubeConfigName].lifetimeActions,
		lf[kubeConfigName].lifetimeClient.Actions())
}

func TestDoNothing(t *testing.T) {
	dataNamespace := metav1.NamespaceDefault
	providerPvcName := "provider-pvc-1-4"
	consumerPvcName := "consumer-pvc-1"

	lifetimeNamespace := metav1.NamespaceSystem
	lifetimeName := "lifetime1"
	startOffset := time.Duration(2 * time.Second)
	endOffset := time.Duration(20 * time.Second)

	fromPvcRef := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      providerPvcName,
	}
	toPvcRef := &corev1.ObjectReference{
		Namespace: dataNamespace,
		Name:      consumerPvcName,
	}
	messageQueueSpec := &lifetimesapi.MessageQueueSpec{
		MessageQueueRef: &k8scorev1.ObjectReference{
			Namespace: mqNamespace,
			Name:      mqName,
		},
	}
	inputData := []lifetimesapi.InputDataSpec{
		{
			FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
				FromPersistentVolumeClaimRef: fromPvcRef,
				ToPersistentVolumeClaimRef:   toPvcRef,
			},
			MessageQueueSubscriber: messageQueueSpec,
		},
	}
	outputData := []lifetimesapi.OutputDataSpec{}
	lifetime := newDataLifetime(
		lifetimeNamespace, lifetimeName, inputData, outputData, startOffset,
		endOffset)

	messageQueueConfigs := []*coreapi.MessageQueue{
		newMessageQueue(),
	}
	lifetimes := []*lifetimesapi.DataLifetime{
		lifetime,
	}

	kubeConfig := createKubeConfig(t)
	defer os.Remove(kubeConfig.Name())

	kubeConfigName := kubeConfig.Name()
	targetLifetime := util.ConcatenateNamespaceAndName(
		lifetimeNamespace, lifetimeName)

	lifetimesJson := fmt.Sprintf(
		"[{\"kubeConfig\": %q, \"lifetimesNames\": [%q]}]",
		kubeConfigName, targetLifetime)
	os.Setenv("K8S_LIFETIMES_NAMES", lifetimesJson)

	ltFixture := newLifetimesFixture(messageQueueConfigs, lifetimes)
	lf := map[string]*lifetimesFixture{
		kubeConfigName: ltFixture,
	}

	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	f := newFixture(t, lf, miniRedis)

	podNamespace := metav1.NamespaceDefault
	podName := "pod1"
	dataContainerName := "data-c"

	err = os.Mkdir(sourceRootDirectory3, 0755)
	assert.NoError(t, err)
	defer os.Remove(sourceRootDirectory3)

	volumeMounts := []k8scorev1.VolumeMount{
		{
			Name:      podSourceVolume1,
			MountPath: sourceRootDirectory3,
		},
		{
			Name:      podLocalVolume1,
			MountPath: destinationRootDirectory1,
		},
	}
	volumes := []k8scorev1.Volume{
		{
			Name: podSourceVolume1,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: providerPvcName,
				},
			},
		},
		{
			Name: podLocalVolume1,
			VolumeSource: k8scorev1.VolumeSource{
				PersistentVolumeClaim: &k8scorev1.
					PersistentVolumeClaimVolumeSource{
					ClaimName: consumerPvcName,
				},
			},
		},
	}
	pod := newPod(
		podNamespace, podName, dataContainerName, volumeMounts, volumes)

	secret := newSecret()

	f.kubeObjects = append(f.kubeObjects, pod, secret)
	for _, messageQueue := range messageQueueConfigs {
		f.messageQueueObjects = append(f.messageQueueObjects, messageQueue)
	}

	os.Setenv(podNamespaceEnv, podNamespace)
	os.Setenv(podNameEnv, podName)
	os.Setenv(dataContainerNameEnv, dataContainerName)

	lifetimeKey, err := cache.DeletionHandlingMetaNamespaceKeyFunc(lifetime)
	assert.NoError(t, err)
	assert.Equal(t, targetLifetime, lifetimeKey)

	topic := volumemq.GetMessageQueueTopic(dataNamespace, providerPvcName)
	initTopics, err := topic.CreateInitTopics()
	assert.NoError(t, err)
	updateTopic, err := topic.CreateUpdateTopic()
	assert.NoError(t, err)

	kubeObjects := []runtime.Object{
		secret,
	}
	publishedDataSpecs := []publishedDataSpec{
		{
			volumeRootPath: sourceRootDirectory3,
			outputDataSpec: &lifetimesapi.OutputFileSystemSpec{
				PersistentVolumeClaimRef: &corev1.ObjectReference{
					Namespace: dataNamespace,
					Name:      providerPvcName,
				},
			},
			updateTopic: updateTopic,
		},
	}
	publishersWg := new(sync.WaitGroup)
	publishersInitWg := new(sync.WaitGroup)
	ctx, cancel := context.WithCancel(context.Background())
	for _, publishedDataSpec := range publishedDataSpecs {
		pf := newPublisherFixture(
			messageQueueConfigs, kubeObjects, messageQueueSpec,
			publishedDataSpec)

		publishersWg.Add(1)
		publishersInitWg.Add(1)
		go pf.initPublisherRuns(t, ctx, publishersWg, publishersInitWg)
		publishersWg.Add(1)
		publishersInitWg.Add(1)
		go pf.updatePublisherRuns(t, ctx, publishersWg, publishersInitWg)
	}

	publishersInitWg.Wait()

	defer func() {
		brokerAddress := os.Getenv(kafkaBrokerAddressEnv)
		config := messagequeue.NewMessageQueueConfig(
			[]string{brokerAddress}, saslUser, saslPassword)
		dialer, err := config.CreateSaslDialer()
		assert.NoError(t, err)
		topicNames := []string{
			initTopics.Request,
			initTopics.Response,
			updateTopic,
		}
		err = messagequeue.DeleteTopics(brokerAddress, dialer, topicNames)
		assert.NoError(t, err)
	}()

	key := generateKey(kubeConfigName, targetLifetime)
	controller := f.runController(key, false)

	time.Sleep(15 * time.Second)

	close(controller.finChs[key].finCh)

	cancel()
	publishersWg.Wait()

	_, err = os.Stat(destinationFilePath1)
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(destinationFilePath2)
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))

	statusKeys := createStatusKeys(&lifetime.Spec)
	updateInfo := createUpdateActionInfo(statusKeys, 1, true, false)
	lf[kubeConfigName].lifetimeActions = append(
		lf[kubeConfigName].lifetimeActions,
		createLifetimeNewListAction(dataNamespace),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime, updateInfo),
	)
	checkActions(
		f.t, lf[kubeConfigName].lifetimeActions,
		lf[kubeConfigName].lifetimeClient.Actions())
}
