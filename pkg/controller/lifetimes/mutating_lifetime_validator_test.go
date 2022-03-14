// Copyright (c) 2022 Fujitsu Limited

package lifetimes

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	goredis "github.com/go-redis/redis/v8"
	k8scorev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	lifetimesfake "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/lifetimes/clientset/versioned/fake"
	lifetimesinformers "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/lifetimes/informers/externalversions"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore/redis"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

var (
	inputSourcePersistentVolumeClaim1  = "input_source_pvc_1"
	inputSourcePersistentVolumeClaim2  = "input_source_pvc_2"
	inputLocalPersistentVolumeClaim1   = "input_local_pvc_1"
	inputLocalPersistentVolumeClaim2   = "input_local_pvc_2"
	outputSourcePersistentVolumeClaim1 = "output_source_pvc_1"
	outputSourcePersistentVolumeClaim2 = "output_source_pvc_2"
	outputLocalPersistentVolumeClaim1  = "output_local_pvc_1"
	outputLocalPersistentVolumeClaim2  = "output_local_pvc_2"
)

type inputSpec struct {
	namespace string
	name      string
	sourcePvc string
	localPvc  string
}

type outputSpec struct {
	namespace string
	name      string
	sourcePvc string
}

func newFakeRedisValidationClient(
	t *testing.T,
	miniRedis *miniredis.Miniredis) datastore.ValidationDataStoreOperations {
	client := goredis.NewClient(
		&goredis.Options{
			Addr: miniRedis.Addr(),
		})

	redisClient := &redis.RedisClient{}

	err := redis.SetRedisClient(client, redisClient)
	if err != nil {
		assert.FailNow(t, "Could NOT set the redis client: %v", client)
	}

	return redisClient
}

func newFakeProviderSharedInformer(
	lifetimesFixture *lifetimesFixture,
	stopCh <-chan struct{}) *providerSharedInformer {
	client := lifetimesfake.NewSimpleClientset(
		lifetimesFixture.lifetimeObjects...)
	informerFactory := lifetimesinformers.NewSharedInformerFactory(
		client, noResyncPeriodFunc())
	for _, lifetime := range lifetimesFixture.lifetimeLister {
		informerFactory.Lifetimes().V1alpha1().DataLifetimes().Informer().
			GetIndexer().Add(lifetime)
	}

	informerFactory.Start(stopCh)

	lifetimesFixture.lifetimeClient = client

	return &providerSharedInformer{
		dataLifetimesClient: client,
		lifetimesLister: informerFactory.Lifetimes().V1alpha1().
			DataLifetimes().Lister(),
	}
}

func makeInputData(inputSpecs []inputSpec) []lifetimesapi.InputDataSpec {
	inputDataSpecs := make([]lifetimesapi.InputDataSpec, 0, len(inputSpecs))
	for _, input := range inputSpecs {
		inputDataSpecs = append(
			inputDataSpecs,
			lifetimesapi.InputDataSpec{
				FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
					FromPersistentVolumeClaimRef: &k8scorev1.ObjectReference{
						Namespace: input.namespace,
						Name:      input.sourcePvc,
					},
					ToPersistentVolumeClaimRef: &k8scorev1.ObjectReference{
						Namespace: input.namespace,
						Name:      input.localPvc,
					},
				},
			})
	}

	return inputDataSpecs
}

func makeOutputData(outputSpecs []outputSpec) []lifetimesapi.OutputDataSpec {
	outputDataSpecs := make([]lifetimesapi.OutputDataSpec, 0, len(outputSpecs))
	for _, output := range outputSpecs {
		outputDataSpecs = append(
			outputDataSpecs,
			lifetimesapi.OutputDataSpec{
				FileSystemSpec: &lifetimesapi.OutputFileSystemSpec{
					PersistentVolumeClaimRef: &k8scorev1.ObjectReference{
						Namespace: output.namespace,
						Name:      output.sourcePvc,
					},
				},
			})
	}

	return outputDataSpecs
}

func generateLifetimeCacheKey(lifetimeKey string) string {
	return fmt.Sprintf(
		"%s%s", datastore.LifetimeCachePrefix, lifetimeKey)
}

func TestAddAndDeleteDataLifetimeToAndFromPolicyDataStore(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	dataStoreOpts := newFakeRedisValidationClient(t, miniRedis)
	policyValidator := newPolicyValidator(dataStoreOpts)

	lifetimeNamespace := metav1.NamespaceDefault
	lifetimeName := "lifetime1"
	startOffset := time.Duration(-1 * time.Second)
	endOffset := time.Duration(1 * time.Second)

	inputSpecs := []inputSpec{
		{
			namespace: dataNamespace,
			name:      dataName1,
			sourcePvc: inputSourcePersistentVolumeClaim1,
			localPvc:  inputLocalPersistentVolumeClaim1,
		},
		{
			namespace: dataNamespace,
			name:      dataName2,
			sourcePvc: inputSourcePersistentVolumeClaim2,
			localPvc:  inputLocalPersistentVolumeClaim2,
		},
	}
	inputData := makeInputData(inputSpecs)

	outputSpecs := []outputSpec{
		{
			namespace: dataNamespace,
			name:      dataName1,
			sourcePvc: outputSourcePersistentVolumeClaim1,
		},
		{
			namespace: dataNamespace,
			name:      dataName2,
			sourcePvc: outputSourcePersistentVolumeClaim2,
		},
	}
	outputData := makeOutputData(outputSpecs)

	lifetime := newDataLifetime(
		lifetimeNamespace, lifetimeName, inputData, outputData, startOffset,
		endOffset)
	lifetimes := []*lifetimesapi.DataLifetime{
		lifetime,
	}

	ltFixture := newLifetimesFixture(nil, lifetimes)
	stopCh := util.SetupSignalHandler()
	providerSharedInformer := newFakeProviderSharedInformer(
		ltFixture, stopCh)
	lifetimesName := fmt.Sprintf("%s/%s", lifetimeNamespace, lifetimeName)
	err = policyValidator.addDataLifetimeToPolicyDataStore(
		lifetimesName, providerSharedInformer)
	assert.NoError(t, err)

	ltFixture.lifetimeActions = append(
		ltFixture.lifetimeActions,
		createLifetimeNewListAction(dataNamespace))
	checkActions(
		t, ltFixture.lifetimeActions, ltFixture.lifetimeClient.Actions())

	endTime := lifetime.Spec.Trigger.EndTime
	for _, outputData := range lifetime.Spec.OutputData {
		key, err := datastore.GetDataLifetimeKeyFromOutputData(outputData)
		assert.NoError(t, err)

		lifetimeCacheKey := generateLifetimeCacheKey(key)
		lifetimeCacheString, err := miniRedis.Get(lifetimeCacheKey)
		assert.NoError(t, err)

		var lifetimeCache datastore.LifetimeCache
		err = json.Unmarshal([]byte(lifetimeCacheString), &lifetimeCache)
		assert.NoError(t, err)

		expectedLifetimeCache := datastore.LifetimeCache{
			EndTime: endTime,
		}

		assert.Equal(t, expectedLifetimeCache, lifetimeCache)
	}

	err = policyValidator.deleteDataLifetimeFromPolicyDataStore(
		lifetimesName, providerSharedInformer)
	assert.NoError(t, err)
	checkActions(
		t, ltFixture.lifetimeActions, ltFixture.lifetimeClient.Actions())

	for _, outputData := range lifetime.Spec.OutputData {
		key, err := datastore.GetDataLifetimeKeyFromOutputData(outputData)
		assert.NoError(t, err)

		lifetimeCacheKey := generateLifetimeCacheKey(key)
		_, err = miniRedis.Get(lifetimeCacheKey)
		assert.Error(t, err)
		assert.EqualError(t, err, "ERR no such key")
	}
}

func TestAddAndDeleteDataLifetimeToAndFromPolicyDataStoreWithSuccessors(
	t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	dataStoreOpts := newFakeRedisValidationClient(t, miniRedis)
	policyValidator := newPolicyValidator(dataStoreOpts)

	lifetimeNamespace := metav1.NamespaceDefault
	lifetimeName1 := "lifetime1"
	successorLifetimeName1 := "lifetime2"
	successorLifetimeName2 := "lifetime3"
	startOffset := time.Duration(-1 * time.Second)
	endOffset := time.Duration(1 * time.Minute)
	endOffsetInCache := time.Duration(2 * time.Minute)

	inputSpecs := []inputSpec{
		{
			namespace: dataNamespace,
			name:      dataName1,
			sourcePvc: inputSourcePersistentVolumeClaim1,
			localPvc:  inputLocalPersistentVolumeClaim1,
		},
		{
			namespace: dataNamespace,
			name:      dataName2,
			sourcePvc: inputSourcePersistentVolumeClaim2,
			localPvc:  inputLocalPersistentVolumeClaim2,
		},
	}
	inputData1 := makeInputData(inputSpecs)

	outputSpecs := []outputSpec{
		{
			namespace: dataNamespace,
			name:      dataName1,
			sourcePvc: outputSourcePersistentVolumeClaim1,
		},
		{
			namespace: dataNamespace,
			name:      dataName2,
			sourcePvc: outputSourcePersistentVolumeClaim2,
		},
	}
	outputData := makeOutputData(outputSpecs)

	lifetime1 := newDataLifetime(
		lifetimeNamespace, lifetimeName1, inputData1, outputData, startOffset,
		endOffset)

	inputSpecs = []inputSpec{
		{
			namespace: dataNamespace,
			name:      dataName1,
			sourcePvc: inputSourcePersistentVolumeClaim1,
			localPvc:  inputLocalPersistentVolumeClaim1,
		},
	}
	inputData2 := makeInputData(inputSpecs)

	lifetime2 := newDataLifetime(
		lifetimeNamespace, successorLifetimeName1, inputData2, nil,
		startOffset, endOffsetInCache)

	inputSpecs = []inputSpec{
		{
			namespace: dataNamespace,
			name:      dataName2,
			sourcePvc: inputSourcePersistentVolumeClaim2,
			localPvc:  inputLocalPersistentVolumeClaim2,
		},
	}
	inputData3 := makeInputData(inputSpecs)

	lifetime3 := newDataLifetime(
		lifetimeNamespace, successorLifetimeName2, inputData3, nil,
		startOffset, endOffsetInCache)

	lifetimes := []*lifetimesapi.DataLifetime{
		lifetime1,
		lifetime2,
		lifetime3,
	}

	ltFixture := newLifetimesFixture(nil, lifetimes)
	stopCh := util.SetupSignalHandler()
	providerSharedInformer := newFakeProviderSharedInformer(
		ltFixture, stopCh)

	for index, outputData := range lifetime1.Spec.OutputData {
		now := time.Now()
		endTime := now.Add(endOffsetInCache)
		endTimeRFC3339 := endTime.Format(time.RFC3339)

		key, err := datastore.GetDataLifetimeKeyFromOutputData(outputData)
		assert.NoError(t, err)

		lifetimesName := fmt.Sprintf(
			"%s/%s",
			lifetimes[index+1].Namespace, lifetimes[index+1].Name)
		lifetimeCache := datastore.LifetimeCache{
			EndTime: endTimeRFC3339,
			SuccessorLifetimes: map[string]struct{}{
				lifetimesName: struct{}{},
			},
		}
		lifetimeCacheBytes, err := json.Marshal(lifetimeCache)
		assert.NoError(t, err)

		lifetimeCacheKey := generateLifetimeCacheKey(key)
		err = miniRedis.Set(lifetimeCacheKey, string(lifetimeCacheBytes))
		assert.NoError(t, err)
	}

	lifetimesName := fmt.Sprintf("%s/%s", lifetimeNamespace, lifetimeName1)
	err = policyValidator.addDataLifetimeToPolicyDataStore(
		lifetimesName, providerSharedInformer)
	assert.NoError(t, err)

	statusKeys2 := createStatusKeys(&lifetime2.Spec)
	updateInfo2 := createUpdateActionInfo(statusKeys2, 1, true, true)
	statusKeys3 := createStatusKeys(&lifetime2.Spec)
	updateInfo3 := createUpdateActionInfo(statusKeys3, 2, true, true)

	ltFixture.lifetimeActions = append(
		ltFixture.lifetimeActions,
		createLifetimeNewListAction(dataNamespace),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime2, updateInfo2),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime3, updateInfo3),
	)
	checkActions(
		t, ltFixture.lifetimeActions, ltFixture.lifetimeClient.Actions())

	endTime := lifetime1.Spec.Trigger.EndTime
	for index, outputData := range lifetime1.Spec.OutputData {
		key, err := datastore.GetDataLifetimeKeyFromOutputData(outputData)
		assert.NoError(t, err)

		lifetimeCacheKey := generateLifetimeCacheKey(key)
		lifetimeCacheString, err := miniRedis.Get(lifetimeCacheKey)
		assert.NoError(t, err)

		var lifetimeCache datastore.LifetimeCache
		err = json.Unmarshal([]byte(lifetimeCacheString), &lifetimeCache)
		assert.NoError(t, err)

		lifetimesName := fmt.Sprintf(
			"%s/%s",
			lifetimes[index+1].Namespace, lifetimes[index+1].Name)
		expectedLifetimeCache := datastore.LifetimeCache{
			EndTime: endTime,
			SuccessorLifetimes: map[string]struct{}{
				lifetimesName: struct{}{},
			},
		}

		assert.Equal(t, expectedLifetimeCache, lifetimeCache)
	}

	err = policyValidator.deleteDataLifetimeFromPolicyDataStore(
		lifetimesName, providerSharedInformer)
	assert.NoError(t, err)
	checkActions(
		t, ltFixture.lifetimeActions, ltFixture.lifetimeClient.Actions())

	residualKeys := make([]string, 0, len(lifetime1.Spec.OutputData))
	for index, outputData := range lifetime1.Spec.OutputData {
		key, err := datastore.GetDataLifetimeKeyFromOutputData(outputData)
		assert.NoError(t, err)

		lifetimeCacheKey := generateLifetimeCacheKey(key)
		lifetimeCacheString, err := miniRedis.Get(lifetimeCacheKey)
		assert.NoError(t, err)

		var lifetimeCache datastore.LifetimeCache
		err = json.Unmarshal([]byte(lifetimeCacheString), &lifetimeCache)
		assert.NoError(t, err)

		lifetimesName := fmt.Sprintf(
			"%s/%s",
			lifetimes[index+1].Namespace, lifetimes[index+1].Name)
		expectedLifetimeCache := datastore.LifetimeCache{
			EndTime: endTime,
			SuccessorLifetimes: map[string]struct{}{
				lifetimesName: struct{}{},
			},
			SuccessorPodTerminated: true,
		}

		assert.Equal(t, expectedLifetimeCache, lifetimeCache)

		residualKeys = append(residualKeys, key)
	}

	for _, key := range residualKeys {
		lifetimeCacheKey := generateLifetimeCacheKey(key)
		keyExists := miniRedis.Del(lifetimeCacheKey)
		assert.True(t, keyExists)
	}
}

func TestAddAndDeleteDataLifetimeToAndFromPolicyDataStoreWithSuccessor(
	t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	dataStoreOpts := newFakeRedisValidationClient(t, miniRedis)
	policyValidator := newPolicyValidator(dataStoreOpts)

	lifetimeNamespace := metav1.NamespaceDefault
	lifetimeName1 := "lifetime1"
	successorLifetimeName1 := "lifetime2"
	successorLifetimeName2 := "lifetime3"
	startOffset := time.Duration(-1 * time.Second)
	endOffset := time.Duration(1 * time.Minute)
	endOffsetInCache := time.Duration(2 * time.Minute)

	inputSpecs := []inputSpec{
		{
			namespace: dataNamespace,
			name:      dataName1,
			sourcePvc: inputSourcePersistentVolumeClaim1,
			localPvc:  inputLocalPersistentVolumeClaim1,
		},
		{
			namespace: dataNamespace,
			name:      dataName2,
			sourcePvc: inputSourcePersistentVolumeClaim2,
			localPvc:  inputLocalPersistentVolumeClaim2,
		},
	}
	inputData1 := makeInputData(inputSpecs)

	outputSpecs := []outputSpec{
		{
			namespace: dataNamespace,
			name:      dataName1,
			sourcePvc: outputSourcePersistentVolumeClaim1,
		},
		{
			namespace: dataNamespace,
			name:      dataName2,
			sourcePvc: outputSourcePersistentVolumeClaim2,
		},
	}
	outputData := makeOutputData(outputSpecs)

	lifetime1 := newDataLifetime(
		lifetimeNamespace, lifetimeName1, inputData1, outputData, startOffset,
		endOffset)

	inputSpecs = []inputSpec{
		{
			namespace: dataNamespace,
			name:      dataName1,
			sourcePvc: inputSourcePersistentVolumeClaim1,
			localPvc:  inputLocalPersistentVolumeClaim1,
		},
	}
	inputData2 := makeInputData(inputSpecs)

	lifetime2 := newDataLifetime(
		lifetimeNamespace, successorLifetimeName1, inputData2, nil,
		startOffset, endOffset)

	inputSpecs = []inputSpec{
		{
			namespace: dataNamespace,
			name:      dataName2,
			sourcePvc: inputSourcePersistentVolumeClaim2,
			localPvc:  inputLocalPersistentVolumeClaim2,
		},
	}
	inputData3 := makeInputData(inputSpecs)

	lifetime3 := newDataLifetime(
		lifetimeNamespace, successorLifetimeName2, inputData3, nil,
		startOffset, endOffsetInCache)

	lifetimes := []*lifetimesapi.DataLifetime{
		lifetime1,
		lifetime2,
		lifetime3,
	}

	ltFixture := newLifetimesFixture(nil, lifetimes)
	stopCh := util.SetupSignalHandler()
	providerSharedInformer := newFakeProviderSharedInformer(
		ltFixture, stopCh)

	for index, outputData := range lifetime1.Spec.OutputData {
		now := time.Now()
		endTime := now.Add(endOffsetInCache)
		endTimeRFC3339 := endTime.Format(time.RFC3339)

		key, err := datastore.GetDataLifetimeKeyFromOutputData(outputData)
		assert.NoError(t, err)

		lifetimesName := fmt.Sprintf(
			"%s/%s",
			lifetimes[index+1].Namespace, lifetimes[index+1].Name)
		lifetimeCache := datastore.LifetimeCache{
			EndTime: endTimeRFC3339,
			SuccessorLifetimes: map[string]struct{}{
				lifetimesName: struct{}{},
			},
		}
		lifetimeCacheBytes, err := json.Marshal(lifetimeCache)
		assert.NoError(t, err)

		lifetimeCacheKey := generateLifetimeCacheKey(key)
		err = miniRedis.Set(lifetimeCacheKey, string(lifetimeCacheBytes))
		assert.NoError(t, err)
	}

	lifetimesName := fmt.Sprintf("%s/%s", lifetimeNamespace, lifetimeName1)
	err = policyValidator.addDataLifetimeToPolicyDataStore(
		lifetimesName, providerSharedInformer)
	assert.NoError(t, err)

	statusKeys3 := createStatusKeys(&lifetime2.Spec)
	updateInfo3 := createUpdateActionInfo(statusKeys3, 1, true, true)

	ltFixture.lifetimeActions = append(
		ltFixture.lifetimeActions,
		createLifetimeNewListAction(dataNamespace),
		createLifetimeUpdateAction(lifetimeNamespace, lifetime3, updateInfo3),
	)
	checkActions(
		t, ltFixture.lifetimeActions, ltFixture.lifetimeClient.Actions())

	endTime := lifetime1.Spec.Trigger.EndTime
	key, err := datastore.GetDataLifetimeKeyFromOutputData(
		lifetime1.Spec.OutputData[1])
	assert.NoError(t, err)

	lifetimeCacheKey := generateLifetimeCacheKey(key)
	lifetimeCacheString, err := miniRedis.Get(lifetimeCacheKey)
	assert.NoError(t, err)

	var lifetimeCache datastore.LifetimeCache
	err = json.Unmarshal([]byte(lifetimeCacheString), &lifetimeCache)
	assert.NoError(t, err)

	lifetimesName = fmt.Sprintf("%s/%s", lifetime3.Namespace, lifetime3.Name)
	expectedLifetimeCache := datastore.LifetimeCache{
		EndTime: endTime,
		SuccessorLifetimes: map[string]struct{}{
			lifetimesName: struct{}{},
		},
	}

	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	lifetimesName = fmt.Sprintf("%s/%s", lifetimeNamespace, lifetimeName1)
	err = policyValidator.deleteDataLifetimeFromPolicyDataStore(
		lifetimesName, providerSharedInformer)
	assert.NoError(t, err)
	checkActions(
		t, ltFixture.lifetimeActions, ltFixture.lifetimeClient.Actions())

	key, err = datastore.GetDataLifetimeKeyFromOutputData(
		lifetime1.Spec.OutputData[1])
	assert.NoError(t, err)

	lifetimeCacheKey = generateLifetimeCacheKey(key)
	lifetimeCacheString, err = miniRedis.Get(lifetimeCacheKey)
	assert.NoError(t, err)

	err = json.Unmarshal([]byte(lifetimeCacheString), &lifetimeCache)
	assert.NoError(t, err)

	lifetimesName = fmt.Sprintf("%s/%s", lifetime3.Namespace, lifetime3.Name)
	expectedLifetimeCache = datastore.LifetimeCache{
		EndTime: endTime,
		SuccessorLifetimes: map[string]struct{}{
			lifetimesName: struct{}{},
		},
		SuccessorPodTerminated: true,
	}

	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	keyExists := miniRedis.Del(lifetimeCacheKey)
	assert.True(t, keyExists)
}
