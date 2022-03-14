// Copyright (c) 2022 Fujitsu Limited

package validation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	goredis "github.com/go-redis/redis/v8"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore/redis"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

var (
	lifetimesBytes = []byte(`{
"apiVersion": "lifetimes.dsc/v1alpha1",
"kind": "DataLifetime",
"metadata": {
  "namespace": "default",
  "name": "lifetime"
},
"spec": {
  "trigger": {
    "endTime": "2021-01-02T03:04:00+09:00"
  },
  "inputData": [
    {
      "fileSystemSpec": {
        "fromPersistentVolumeClaimRef": {
          "namespace": "default",
          "name": "input-from-pvc-1"
        },
        "toPersistentVolumeClaimRef": {
          "namespace": "default",
          "name": "input-to-pvc-1"
        },
        "allowedExecutables": [
          {
            "cmdPath": "/bin/cat"
          },
          {
            "cmdPath": "/bin/bash",
            "writable": true
          }
        ]
      }
    },
    {
      "fileSystemSpec": {
        "fromPersistentVolumeClaimRef": {
          "namespace": "default",
          "name": "input-from-pvc-2"
        },
        "toPersistentVolumeClaimRef": {
          "namespace": "default",
          "name": "input-to-pvc-2"
        }
      }
    }
  ],
  "outputData": [
    {
      "fileSystemSpec": {
        "persistentVolumeClaimRef": {
          "namespace": "default",
          "name": "output-pvc"
        },
        "allowedExecutables": [
          {
            "cmdPath": "/bin/bash",
            "writable": true
          }
        ]
      }
    }
  ]
}
}`)

	inputPvName        = "input-pv-1"
	inputVolumeHandle  = "input-volume-handle-1"
	pvcNamespace       = metav1.NamespaceDefault
	inputPvcName       = "input-to-pvc-1"
	outputPvName       = "output-pv"
	outputVolumeHandle = "output-volume-handle"
	outputPvcName      = "output-pvc"
)

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

func fakePolicyValidator(
	t *testing.T, miniRedis *miniredis.Miniredis,
	localLifetimeCaches map[string]*localCache,
	kubeClient kubernetes.Interface) *PolicyValidator {
	dataStore := newFakeRedisValidationClient(t, miniRedis)

	return &PolicyValidator{
		dataStoreOpts:       dataStore,
		localLifetimeCaches: localLifetimeCaches,
		kubeClient:          kubeClient,
	}
}

func newPersistentVolumeForCsiVolume(
	name string, volumeHandle string, pvcNamespace string,
	pvcName string) *corev1.PersistentVolume {
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
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					VolumeHandle: volumeHandle,
				},
			},
			ClaimRef: &corev1.ObjectReference{
				APIVersion: corev1.SchemeGroupVersion.String(),
				Kind:       "PersistentVolumeClaim",
				Name:       pvcName,
				Namespace:  pvcNamespace,
			},
		},
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

func newKubeClient() kubernetes.Interface {
	inputPv := newPersistentVolumeForCsiVolume(
		inputPvName, inputVolumeHandle, pvcNamespace, inputPvcName)
	inputPvc := newPersistentVolumeClaim(
		pvcNamespace, inputPvcName, inputPvName)
	outputPv := newPersistentVolumeForCsiVolume(
		outputPvName, outputVolumeHandle, pvcNamespace, outputPvcName)
	outputPvc := newPersistentVolumeClaim(
		pvcNamespace, outputPvcName, outputPvName)

	kubeObjects := []runtime.Object{}
	kubeObjects = append(kubeObjects, inputPv, inputPvc, outputPv, outputPvc)

	return k8sfake.NewSimpleClientset(kubeObjects...)
}

func makeAdmissionReview(
	operation admissionv1.Operation,
	isDelete bool) admissionv1.AdmissionReview {
	request := &admissionv1.AdmissionRequest{
		Resource: metav1.GroupVersionResource{
			Group:    "lifetimes.dsc",
			Version:  "v1alpha1",
			Resource: "datalifetimes",
		},
	}

	if isDelete {
		request.OldObject = runtime.RawExtension{
			Raw: lifetimesBytes,
		}
	} else {
		request.Object = runtime.RawExtension{
			Raw: lifetimesBytes,
		}
	}

	request.Operation = operation

	return admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			Kind:       "AdmissionReview",
			APIVersion: "admission.k8s.io/v1",
		},
		Request: request,
	}
}

func makeHttpRequest(
	t *testing.T, operation admissionv1.Operation) *http.Request {
	target := "http://example.com/dummy"

	isDelete := false
	if operation == admissionv1.Delete {
		isDelete = true
	}

	admissionReviewOperation := makeAdmissionReview(operation, isDelete)
	admissionReviewBytes, err := json.Marshal(admissionReviewOperation)
	assert.NoError(t, err)
	requestBody := bytes.NewBuffer(admissionReviewBytes)
	request := httptest.NewRequest(http.MethodPost, target, requestBody)
	request.Header.Set("Content-Type", jsonContentType)

	return request
}

func TestInitializeLocalLifetimeCaches(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	localLifetimeCaches := map[string]string{
		"key1": "2021-01-02T03:04:00+09:00",
		"key2": "2022-02-03T04:05:00+06:00",
	}

	kubeClient := newKubeClient()

	policyValidator := fakePolicyValidator(t, miniRedis, nil, kubeClient)

	for key, endTime := range localLifetimeCaches {
		_, err := policyValidator.dataStoreOpts.AddDataLifetime(key, endTime)
		assert.NoError(t, err)
	}

	err = policyValidator.initializeLocalLifetimeCaches()
	assert.NoError(t, err)

	assert.Equal(
		t, len(localLifetimeCaches), len(policyValidator.localLifetimeCaches))

	for key, lifetimeCache := range policyValidator.localLifetimeCaches {
		endTime, ok := localLifetimeCaches[key]
		assert.True(t, ok)
		expectedCache := &localCache{
			endTime: endTime,
		}
		assert.Equal(t, expectedCache, lifetimeCache)
	}
}

func TestServeHttp(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	localLifetimeCaches := map[string]*localCache{}
	kubeClient := newKubeClient()

	policyValidator := fakePolicyValidator(
		t, miniRedis, localLifetimeCaches, kubeClient)

	request := makeHttpRequest(t, admissionv1.Create)
	recorder := httptest.NewRecorder()

	policyValidator.ServeHTTP(recorder, request)
	assert.Equal(t, http.StatusOK, recorder.Code)

	var admissionReview admissionv1.AdmissionReview
	err = json.Unmarshal(recorder.Body.Bytes(), &admissionReview)
	assert.NoError(t, err)
	assert.True(t, admissionReview.Response.Allowed)

	var lifetimes lifetimesapi.DataLifetime
	err = json.Unmarshal(lifetimesBytes, &lifetimes)
	assert.NoError(t, err)

	for _, inputData := range lifetimes.Spec.InputData {
		key, err := datastore.GetDataLifetimeKeyFromInputData(inputData)
		assert.NoError(t, err)

		_, err = miniRedis.Get(key)
		assert.Error(t, err)
		assert.EqualError(t, err, "ERR no such key")
	}

	expectedLocalLifetimeCaches := map[string]*localCache{
		"default/lifetime": &localCache{
			endTime: "2021-01-02T03:04:00+09:00",
		},
	}
	assert.Equal(
		t, expectedLocalLifetimeCaches, policyValidator.localLifetimeCaches)

	request = makeHttpRequest(t, admissionv1.Delete)
	recorder = httptest.NewRecorder()

	policyValidator.ServeHTTP(recorder, request)
	assert.Equal(t, http.StatusOK, recorder.Code)

	err = json.Unmarshal(recorder.Body.Bytes(), &admissionReview)
	assert.NoError(t, err)
	assert.True(t, admissionReview.Response.Allowed)

	expectedLocalLifetimeCaches = map[string]*localCache{}
	assert.Equal(
		t, expectedLocalLifetimeCaches, policyValidator.localLifetimeCaches)
}

func generateLifetimeCacheKey(lifetimeKey string) string {
	return fmt.Sprintf(
		"%s%s", datastore.LifetimeCachePrefix, lifetimeKey)
}

func testServeHttpWithLifetimeCache(
	t *testing.T, successorPodTerminated bool) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	localLifetimeCaches := map[string]*localCache{
		"default/lifetime": &localCache{
			endTime: "2021-01-02T03:05:00+09:00",
		},
	}
	kubeClient := newKubeClient()

	policyValidator := fakePolicyValidator(
		t, miniRedis, localLifetimeCaches, kubeClient)

	var lifetimes lifetimesapi.DataLifetime
	err = json.Unmarshal(lifetimesBytes, &lifetimes)
	assert.NoError(t, err)

	for _, inputData := range lifetimes.Spec.InputData {
		key, err := datastore.GetDataLifetimeKeyFromInputData(inputData)
		assert.NoError(t, err)

		lifetimeCache := datastore.LifetimeCache{
			EndTime: "2021-01-02T03:05:00+09:00",
		}
		lifetimeCache.SuccessorPodTerminated = successorPodTerminated
		lifetimeCacheBytes, err := json.Marshal(lifetimeCache)
		assert.NoError(t, err)

		lifetimeCacheKey := generateLifetimeCacheKey(key)
		err = miniRedis.Set(lifetimeCacheKey, string(lifetimeCacheBytes))
		assert.NoError(t, err)

		_, err = miniRedis.Get(lifetimeCacheKey)
		assert.NoError(t, err)
	}

	request := makeHttpRequest(t, admissionv1.Update)
	recorder := httptest.NewRecorder()

	policyValidator.ServeHTTP(recorder, request)
	assert.Equal(t, http.StatusOK, recorder.Code)

	var admissionReview admissionv1.AdmissionReview
	err = json.Unmarshal(recorder.Body.Bytes(), &admissionReview)
	assert.NoError(t, err)
	assert.True(t, admissionReview.Response.Allowed)

	for _, inputData := range lifetimes.Spec.InputData {
		if inputData.FileSystemSpec.ToPersistentVolumeClaimRef.
			Name != inputPvcName {
			continue
		}

		key, err := datastore.GetDataLifetimeKeyFromInputData(inputData)
		assert.NoError(t, err)

		lifetimeCacheKey := generateLifetimeCacheKey(key)
		lifetimeCacheString, err := miniRedis.Get(lifetimeCacheKey)
		assert.NoError(t, err)
		var lifetimeCache datastore.LifetimeCache
		err = json.Unmarshal([]byte(lifetimeCacheString), &lifetimeCache)
		assert.NoError(t, err)
		expectSuccessorLifetimes := map[string]struct{}{
			"default/lifetime": struct{}{},
		}
		assert.Equal(
			t, expectSuccessorLifetimes, lifetimeCache.SuccessorLifetimes)
	}

	expectedLocalLifetimeCaches := map[string]*localCache{
		"default/lifetime": &localCache{
			endTime: "2021-01-02T03:04:00+09:00",
		},
	}
	assert.Equal(
		t, expectedLocalLifetimeCaches, policyValidator.localLifetimeCaches)

	request = makeHttpRequest(t, admissionv1.Delete)
	recorder = httptest.NewRecorder()

	policyValidator.ServeHTTP(recorder, request)
	assert.Equal(t, http.StatusOK, recorder.Code)

	err = json.Unmarshal(recorder.Body.Bytes(), &admissionReview)
	assert.NoError(t, err)
	assert.True(t, admissionReview.Response.Allowed)

	for _, inputData := range lifetimes.Spec.InputData {
		key, err := datastore.GetDataLifetimeKeyFromInputData(inputData)
		assert.NoError(t, err)

		lifetimeCacheKey := generateLifetimeCacheKey(key)
		keyExists := miniRedis.Del(lifetimeCacheKey)
		if successorPodTerminated {
			assert.False(t, keyExists)
		} else {
			assert.True(t, keyExists)
		}
	}

	expectedLocalLifetimeCaches = map[string]*localCache{}
	assert.Equal(
		t, expectedLocalLifetimeCaches, policyValidator.localLifetimeCaches)
}

func TestServeHttpWithLifetimeCache(t *testing.T) {
	testServeHttpWithLifetimeCache(t, false)
}

func TestServeHttpWithLifetimeCacheForSuccessorPodTerminated(t *testing.T) {
	testServeHttpWithLifetimeCache(t, true)
}

func TestServeHttpWithLifetimeCacheInEndTimeIsAfterSuccessor(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	localLifetimeCaches := map[string]*localCache{
		"default/lifetime": &localCache{
			endTime: "2021-01-02T03:02:00+09:00",
		},
	}
	kubeClient := newKubeClient()

	policyValidator := fakePolicyValidator(
		t, miniRedis, localLifetimeCaches, kubeClient)

	var lifetimes lifetimesapi.DataLifetime
	err = json.Unmarshal(lifetimesBytes, &lifetimes)
	assert.NoError(t, err)

	for _, inputData := range lifetimes.Spec.InputData {
		key, err := datastore.GetDataLifetimeKeyFromInputData(inputData)
		assert.NoError(t, err)

		lifetimeCache := datastore.LifetimeCache{
			EndTime: "2021-01-02T03:03:00+09:00",
		}
		lifetimeCacheBytes, err := json.Marshal(lifetimeCache)
		assert.NoError(t, err)

		lifetimeCacheKey := generateLifetimeCacheKey(key)
		err = miniRedis.Set(lifetimeCacheKey, string(lifetimeCacheBytes))
		assert.NoError(t, err)
	}

	request := makeHttpRequest(t, admissionv1.Create)
	recorder := httptest.NewRecorder()

	policyValidator.ServeHTTP(recorder, request)
	assert.Equal(t, http.StatusOK, recorder.Code)

	var admissionReview admissionv1.AdmissionReview
	err = json.Unmarshal(recorder.Body.Bytes(), &admissionReview)
	assert.NoError(t, err)
	assert.False(t, admissionReview.Response.Allowed)
	endTime := "2021-01-02 03:04:00 +0900 +0900"
	predecessorEndTime := "2021-01-02 03:03:00 +0900 +0900"
	expectedErrorMessage := fmt.Sprintf(
		"End time %q must be equal to or before one for its successors %q",
		endTime, predecessorEndTime)
	assert.Equal(
		t, expectedErrorMessage, admissionReview.Response.Result.Message)

	for _, inputData := range lifetimes.Spec.InputData {
		key, err := datastore.GetDataLifetimeKeyFromInputData(inputData)
		assert.NoError(t, err)

		lifetimeCacheKey := generateLifetimeCacheKey(key)
		keyExists := miniRedis.Del(lifetimeCacheKey)
		assert.True(t, keyExists)
	}
}

func createLifetimeWithPredecessors(
	lifetimeName string) *lifetimesapi.DataLifetime {
	inputPvcName1 := "input-to-pvc-1"
	inputPvcName2 := "input-to-pvc-2"
	inputPvcName3 := "input-to-pvc-3"

	return &lifetimesapi.DataLifetime{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: metav1.NamespaceDefault,
			Name:      lifetimeName,
		},
		Spec: lifetimesapi.LifetimeSpec{
			Trigger: &lifetimesapi.Trigger{
				EndTime: "2021-01-02T03:04:00+09:00",
			},
			InputData: []lifetimesapi.InputDataSpec{
				lifetimesapi.InputDataSpec{
					FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
						FromPersistentVolumeClaimRef: &corev1.ObjectReference{
							Namespace: metav1.NamespaceDefault,
							Name:      inputPvcName1,
						},
					},
				},
				lifetimesapi.InputDataSpec{
					FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
						FromPersistentVolumeClaimRef: &corev1.ObjectReference{
							Namespace: metav1.NamespaceDefault,
							Name:      inputPvcName2,
						},
					},
				},
				lifetimesapi.InputDataSpec{
					FileSystemSpec: &lifetimesapi.InputFileSystemSpec{
						FromPersistentVolumeClaimRef: &corev1.ObjectReference{
							Namespace: metav1.NamespaceDefault,
							Name:      inputPvcName3,
						},
					},
				},
			},
		},
	}
}

func addPredecessorEndTimeInDataStore(
	t *testing.T, lifetime *lifetimesapi.DataLifetime, endTimes []string,
	policyValidator *PolicyValidator) {
	assert.Equal(t, len(endTimes), len(lifetime.Spec.InputData))

	for index, input := range lifetime.Spec.InputData {
		key, err := datastore.GetDataLifetimeKeyFromInputData(input)
		assert.NoError(t, err)

		_, err = policyValidator.dataStoreOpts.AddDataLifetime(
			key, endTimes[index])
		assert.NoError(t, err)
	}
}

func validateLifetimeCaches(
	t *testing.T, lifetime *lifetimesapi.DataLifetime, endTimes []string,
	policyValidator *PolicyValidator) {
	lifetimeKey := util.ConcatenateNamespaceAndName(
		metav1.NamespaceDefault, lifetime.Name)

	successorLifetimes := map[string]struct{}{
		lifetimeKey: struct{}{},
	}
	for index, input := range lifetime.Spec.InputData {
		key, err := datastore.GetDataLifetimeKeyFromInputData(input)
		assert.NoError(t, err)

		lifetimeCache, err := policyValidator.dataStoreOpts.GetDataLifetime(
			key)
		assert.NoError(t, err)

		assert.Equal(t, endTimes[index], lifetimeCache.EndTime)
		assert.Equal(t, successorLifetimes, lifetimeCache.SuccessorLifetimes)
	}

	assert.Equal(
		t, "2021-01-02T03:04:00+09:00",
		policyValidator.localLifetimeCaches[lifetimeKey].endTime)

}

func TestUpdateEndTimeInPolicy(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	localLifetimeCaches := map[string]*localCache{
		"default/lifetime1": &localCache{
			endTime: "2021-01-02T03:05:00+09:00",
		},
	}
	kubeClient := newKubeClient()

	policyValidator := fakePolicyValidator(
		t, miniRedis, localLifetimeCaches, kubeClient)

	lifetimeName := "lifetime1"

	lifetime := createLifetimeWithPredecessors(lifetimeName)
	endTimes := []string{
		"2021-01-02T03:05:00+09:00",
		"2021-01-02T03:06:00+09:00",
		"2021-01-02T03:07:00+09:00",
	}

	addPredecessorEndTimeInDataStore(t, lifetime, endTimes, policyValidator)

	err = policyValidator.updateEndTimeInPolicy(lifetime)
	assert.NoError(t, err)

	validateLifetimeCaches(t, lifetime, endTimes, policyValidator)
}

func TestUpdateEndTimeInPolicyWithoutLocalLifetimeCaches(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	localLifetimeCaches := map[string]*localCache{}
	kubeClient := newKubeClient()

	policyValidator := fakePolicyValidator(
		t, miniRedis, localLifetimeCaches, kubeClient)

	lifetimeName := "lifetime1"

	lifetime := createLifetimeWithPredecessors(lifetimeName)
	endTimes := []string{
		"2021-01-02T03:05:00+09:00",
		"2021-01-02T03:06:00+09:00",
		"2021-01-02T03:07:00+09:00",
	}

	addPredecessorEndTimeInDataStore(t, lifetime, endTimes, policyValidator)

	err = policyValidator.updateEndTimeInPolicy(lifetime)
	assert.NoError(t, err)

	validateLifetimeCaches(t, lifetime, endTimes, policyValidator)
}

func TestUpdateEndTimeInPolicyForInvalidEndtime(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)
	defer miniRedis.Close()

	localLifetimeCaches := map[string]*localCache{
		"default/lifetime1": &localCache{
			endTime: "2021-01-02T03:05:00+09:00",
		},
	}
	kubeClient := newKubeClient()

	policyValidator := fakePolicyValidator(
		t, miniRedis, localLifetimeCaches, kubeClient)

	lifetimeName := "lifetime1"

	lifetime := createLifetimeWithPredecessors(lifetimeName)
	endTimes := []string{
		"2021-01-02T03:03:00+09:00",
		"2021-01-02T03:04:00+09:00",
		"2021-01-02T03:05:00+09:00",
	}

	addPredecessorEndTimeInDataStore(t, lifetime, endTimes, policyValidator)

	err = policyValidator.updateEndTimeInPolicy(lifetime)
	assert.Error(t, err)
	assert.EqualError(
		t, err,
		fmt.Sprintf(
			"End time %q must be equal to or before one for its successors %q",

			"2021-01-02 03:04:00 +0900 +0900",
			"2021-01-02 03:03:00 +0900 +0900"))
}
