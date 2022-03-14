// Copyright (c) 2022 Fujitsu Limited

package validation

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/cmd/policy-validator/app/options"
	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore"
	datastoreclient "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore/client"
	datastoreconfig "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore/config"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/trigger"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

const (
	jsonContentType = "application/json"

	admissionReviewApiVersion = "admission.k8s.io/v1"
	admissionReviewKind       = "AdmissionReview"
)

var (
	lifetimesResource = metav1.GroupVersionResource{
		Group:    "lifetimes.dsc",
		Version:  "v1alpha1",
		Resource: "datalifetimes",
	}

	universalDeserializer = serializer.NewCodecFactory(runtime.NewScheme()).
				UniversalDecoder()
)

type localCache struct {
	endTime string
}

type PolicyValidator struct {
	dataStoreOpts       datastore.ValidationDataStoreOperations
	localLifetimeCaches map[string]*localCache

	kubeClient kubernetes.Interface
}

func (pv *PolicyValidator) initializeLocalLifetimeCaches() error {
	keys, err := pv.dataStoreOpts.GetDataLifetimeKeys()
	if err != nil {
		return fmt.Errorf(
			"Could not get keys of the data lifetime caches: %v", err.Error())
	}

	localLifetimeCaches := map[string]*localCache{}
	for _, key := range keys {
		lifetimeCache, err := pv.dataStoreOpts.GetDataLifetime(key)
		if err != nil {
			return fmt.Errorf("Could not get the lifetme cache for %q", key)
		}

		localLifetimeCaches[key] = &localCache{
			endTime: lifetimeCache.EndTime,
		}
	}

	pv.localLifetimeCaches = localLifetimeCaches

	return nil
}

func NewPolicyValidator(
	dataStoreConfig *datastoreconfig.DataStoreOptions,
	kubeClient kubernetes.Interface) (
	*PolicyValidator, error) {
	client, err := datastoreclient.NewValidationDataStoreClient(
		dataStoreConfig)
	if err != nil {
		return nil, err
	}

	validator := &PolicyValidator{
		dataStoreOpts: client,
		kubeClient:    kubeClient,
	}

	err = validator.initializeLocalLifetimeCaches()
	if err != nil {
		return nil, err
	}

	return validator, nil
}

func validateAdmissionRequest(
	responseWriter http.ResponseWriter, request *http.Request) error {
	if request.Method != http.MethodPost {
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
		return fmt.Errorf(
			"Invalid method: %s, only POST requests are allowed",
			request.Method)
	}

	contentType := request.Header.Get("Content-Type")
	if contentType != jsonContentType {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return fmt.Errorf(
			"Not support such a content type: %s, only %s is supported",
			contentType, jsonContentType)
	}

	return nil
}

func getAdmissionReviewRequest(
	responseWriter http.ResponseWriter, request *http.Request) (
	admissionv1.AdmissionReview, error) {
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return admissionv1.AdmissionReview{},
			fmt.Errorf("Could read the request body: %v", err)
	}

	var admissionReviewRequest admissionv1.AdmissionReview
	err = json.Unmarshal(body, &admissionReviewRequest)
	if err != nil {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return admissionv1.AdmissionReview{},
			fmt.Errorf("Could not deserialize the admission request: %v", err)
	}

	if admissionReviewRequest.Request == nil {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return admissionv1.AdmissionReview{},
			fmt.Errorf(
				"Malformed admission review: the request is nil")
	}

	return admissionReviewRequest, nil
}

func (pv *PolicyValidator) getPredecessorEndTime(
	predecessor lifetimesapi.InputDataSpec) (time.Time, error) {
	key, err := datastore.GetDataLifetimeKeyFromInputData(predecessor)
	if err != nil {
		return time.Time{}, err
	}

	lifetimeCache, err := pv.dataStoreOpts.GetDataLifetime(key)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"Not find the predecessor %q in the daastore: %v", key, err)
	} else if lifetimeCache == nil {
		return time.Time{}, nil
	}

	return time.Parse(time.RFC3339, lifetimeCache.EndTime)
}

func (pv *PolicyValidator) getPredecessorsEarliestEndTime(
	predecessors []lifetimesapi.InputDataSpec) (time.Time, error) {
	if len(predecessors) == 0 {
		return time.Time{}, nil
	}

	earliestEndTime := time.Time{}
	for _, predecessor := range predecessors {
		endTime, err := pv.getPredecessorEndTime(predecessor)
		if err != nil {
			return time.Time{}, err
		} else if endTime.IsZero() {
			continue
		}

		if earliestEndTime.IsZero() ||
			endTime.UTC().Before(earliestEndTime.UTC()) {
			earliestEndTime = endTime
		}
	}

	return earliestEndTime, nil
}

func (pv *PolicyValidator) handleSuccessorLifetimeInDataStore(
	successorLifetime string,
	predecessors []lifetimesapi.InputDataSpec, isAddition bool) error {
	keys := make([]string, 0, len(predecessors))
	errorHandler := func(
		keys []string, successor string) {
		for _, key := range keys {
			if isAddition {
				pv.dataStoreOpts.DeleteSuccessorDataLifetimeCustomResource(
					key, successor)
			} else {
				pv.dataStoreOpts.AddSuccessorDataLifetimeCustomResource(
					key, successorLifetime)
			}
		}
	}

	for _, predecessor := range predecessors {
		key, err := datastore.GetDataLifetimeKeyFromInputData(predecessor)
		if err != nil {
			errorHandler(keys, successorLifetime)
			return err
		}

		if isAddition {
			err = pv.dataStoreOpts.AddSuccessorDataLifetimeCustomResource(
				key, successorLifetime)
			if err != nil {
				errorHandler(keys, successorLifetime)
				return err
			}

			klog.Infof(
				"Add the successor %q to the lifetime cache %q",
				successorLifetime, key)

			keys = append(keys, key)
		} else {
			err = pv.dataStoreOpts.DeleteSuccessorDataLifetimeCustomResource(
				key, successorLifetime)
			if err != nil {
				errorHandler(keys, successorLifetime)
				return err
			}

			klog.Infof(
				"Delete the successor %q from the lifetime cache %q",
				successorLifetime, key)

			keys = append(keys, key)
		}
	}

	return nil
}

func (pv *PolicyValidator) validateAddLifetimesPolicy(
	lifetime *lifetimesapi.DataLifetime) error {
	predecessorEndTime, err := pv.getPredecessorsEarliestEndTime(
		lifetime.Spec.InputData)
	if err != nil {
		return err
	} else if predecessorEndTime.IsZero() {
		return pv.addToLocalLifetimeCaches(lifetime)
	}

	triggerOpts, err := trigger.NewTrigger(lifetime.Spec.Trigger)
	if err != nil {
		return err
	}
	endTime := triggerOpts.GetEndTime()

	if endTime.UTC().After(predecessorEndTime.UTC()) {
		return fmt.Errorf(
			"End time %q must be equal to or before one for its successors %q",
			endTime, predecessorEndTime)
	}

	successorLifetime := util.ConcatenateNamespaceAndName(
		lifetime.Namespace, lifetime.Name)

	err = pv.handleSuccessorLifetimeInDataStore(
		successorLifetime, lifetime.Spec.InputData, true)
	if err != nil {
		return err
	}

	return pv.addToLocalLifetimeCaches(lifetime)
}

func (pv *PolicyValidator) isEndTimeChanged(
	lifetime *lifetimesapi.DataLifetime) (bool, error) {
	triggerOpts, err := trigger.NewTrigger(lifetime.Spec.Trigger)
	if err != nil {
		return false, err
	}

	newEndTimeString := triggerOpts.GetEndTimeString()

	lifetimeKey := util.ConcatenateNamespaceAndName(
		lifetime.Namespace, lifetime.Name)
	oldLocalCache, ok := pv.localLifetimeCaches[lifetimeKey]
	if !ok {
		klog.Warningf(
			"Lifetime key %q does NOT exist in the local lifetime caches",
			lifetimeKey)
		return true, nil
	}

	return oldLocalCache.endTime != newEndTimeString, nil
}

func (pv *PolicyValidator) updateEndTimeInPolicy(
	lifetime *lifetimesapi.DataLifetime) error {
	changed, err := pv.isEndTimeChanged(lifetime)
	if err != nil {
		return err
	} else if !changed {
		return nil
	}

	predecessorEndTime, err := pv.getPredecessorsEarliestEndTime(
		lifetime.Spec.InputData)
	if err != nil {
		return err
	} else if predecessorEndTime.IsZero() {
		return pv.updateEndTimeInLocalLifetimeCaches(lifetime)
	}

	triggerOpts, err := trigger.NewTrigger(lifetime.Spec.Trigger)
	if err != nil {
		return err
	}
	endTime := triggerOpts.GetEndTime()

	if endTime.UTC().After(predecessorEndTime.UTC()) {
		return fmt.Errorf(
			"End time %q must be equal to or before one for its successors %q",
			endTime, predecessorEndTime)
	}

	successorLifetime := util.ConcatenateNamespaceAndName(
		lifetime.Namespace, lifetime.Name)

	err = pv.handleSuccessorLifetimeInDataStore(
		successorLifetime, lifetime.Spec.InputData, true)
	if err != nil {
		return err
	}

	return pv.updateEndTimeInLocalLifetimeCaches(lifetime)
}

func (pv *PolicyValidator) validateDeleteLifetimesPolicy(
	lifetime *lifetimesapi.DataLifetime) error {
	successorLifetime := util.ConcatenateNamespaceAndName(
		lifetime.Namespace, lifetime.Name)

	err := pv.handleSuccessorLifetimeInDataStore(
		successorLifetime, lifetime.Spec.InputData, false)
	if err != nil {
		return err
	}

	pv.deleteFromLocalLifetimeCaches(lifetime)

	return nil
}

func (pv *PolicyValidator) addToLocalLifetimeCaches(
	lifetime *lifetimesapi.DataLifetime) error {
	triggerOpts, err := trigger.NewTrigger(lifetime.Spec.Trigger)
	if err != nil {
		return err
	}

	endTimeString := triggerOpts.GetEndTimeString()
	lifetimeKey := util.ConcatenateNamespaceAndName(
		lifetime.Namespace, lifetime.Name)
	pv.localLifetimeCaches[lifetimeKey] = &localCache{
		endTime: endTimeString,
	}

	klog.Infof(
		"Add a local cache for the lifetime policy %q: %+v",
		lifetimeKey, pv.localLifetimeCaches[lifetimeKey])

	return nil
}

func (pv *PolicyValidator) updateEndTimeInLocalLifetimeCaches(
	lifetime *lifetimesapi.DataLifetime) error {
	triggerOpts, err := trigger.NewTrigger(lifetime.Spec.Trigger)
	if err != nil {
		return err
	}

	endTimeString := triggerOpts.GetEndTimeString()
	lifetimeKey := util.ConcatenateNamespaceAndName(
		lifetime.Namespace, lifetime.Name)

	localLifetimeCache, ok := pv.localLifetimeCaches[lifetimeKey]
	if ok {
		localLifetimeCache.endTime = endTimeString
	} else {
		pv.localLifetimeCaches[lifetimeKey] = &localCache{
			endTime: endTimeString,
		}
	}

	klog.Infof(
		"Update the end time %q in the lifetime policy %q",
		endTimeString, lifetimeKey)

	return nil
}

func (pv *PolicyValidator) deleteFromLocalLifetimeCaches(
	lifetime *lifetimesapi.DataLifetime) {
	lifetimeKey := util.ConcatenateNamespaceAndName(
		lifetime.Namespace, lifetime.Name)

	delete(pv.localLifetimeCaches, lifetimeKey)

	klog.Infof("Delete the the lifetime policy %q", lifetimeKey)
}

func (pv *PolicyValidator) validateLifetimesPolicy(
	responseWriter http.ResponseWriter,
	request *admissionv1.AdmissionRequest) error {
	if request.Resource != lifetimesResource {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return fmt.Errorf(
			"Expect a resource to be %v, but %v",
			lifetimesResource, request.Resource)
	}

	switch request.Operation {
	case admissionv1.Create:
		var lifetime lifetimesapi.DataLifetime
		_, _, err := universalDeserializer.Decode(
			request.Object.Raw, nil, &lifetime)
		if err != nil {
			responseWriter.WriteHeader(http.StatusBadRequest)
			return fmt.Errorf(
				"Could not deserialize the 'DataLifetime' object: %v", err)
		}

		return pv.validateAddLifetimesPolicy(&lifetime)
	case admissionv1.Update:
		var lifetime lifetimesapi.DataLifetime
		_, _, err := universalDeserializer.Decode(
			request.Object.Raw, nil, &lifetime)
		if err != nil {
			responseWriter.WriteHeader(http.StatusBadRequest)
			return fmt.Errorf(
				"Could not deserialize the 'DataLifetime' object: %v", err)
		}

		return pv.updateEndTimeInPolicy(&lifetime)
	case admissionv1.Delete:
		var lifetime lifetimesapi.DataLifetime
		_, _, err := universalDeserializer.Decode(
			request.OldObject.Raw, nil, &lifetime)
		if err != nil {
			responseWriter.WriteHeader(http.StatusBadRequest)
			return fmt.Errorf(
				"Could not deserialize the 'DataLifetime' object: %v", err)
		}

		return pv.validateDeleteLifetimesPolicy(&lifetime)
	default:
		responseWriter.WriteHeader(http.StatusInternalServerError)
		return fmt.Errorf(
			"Not support such an admission operation: %v", request.Operation)
	}
}

func createAdmissionReviewResponse(uid types.UID, validationErr error) (
	[]byte, error) {
	admissionResponse := admissionv1.AdmissionResponse{
		UID: uid,
	}

	if validationErr != nil {
		admissionResponse.Allowed = false
		admissionResponse.Result = &metav1.Status{
			Code:    http.StatusBadRequest,
			Message: validationErr.Error(),
		}
	} else {
		admissionResponse.Allowed = true
	}

	admissionReviewResponse := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: admissionReviewApiVersion,
			Kind:       admissionReviewKind,
		},
		Response: &admissionResponse,
	}

	bytes, err := json.Marshal(&admissionReviewResponse)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not serialize the admission response: %v", err)
	}

	return bytes, nil
}

func (pv *PolicyValidator) doServeAdmissionFunc(
	responseWriter http.ResponseWriter, request *http.Request) (
	[]byte, error) {
	err := validateAdmissionRequest(responseWriter, request)
	if err != nil {
		return createAdmissionReviewResponse("", err)
	}

	admissionReviewRequest, err := getAdmissionReviewRequest(
		responseWriter, request)
	if err != nil {
		return createAdmissionReviewResponse("", err)
	}

	validationErr := pv.validateLifetimesPolicy(
		responseWriter, admissionReviewRequest.Request)

	return createAdmissionReviewResponse(
		admissionReviewRequest.Request.UID, validationErr)
}

func (pv *PolicyValidator) ServeHTTP(
	responseWriter http.ResponseWriter, request *http.Request) {
	var writeErr error
	bytes, err := pv.doServeAdmissionFunc(responseWriter, request)
	if err != nil {
		klog.Errorf("Error handling the webhook request: %v", err)
		responseWriter.WriteHeader(http.StatusInternalServerError)
		_, writeErr = responseWriter.Write([]byte(err.Error()))
	} else {
		_, writeErr = responseWriter.Write(bytes)
	}

	if writeErr != nil {
		klog.Errorf("Could not write the response: %v", writeErr.Error())
	}
}

func (pv *PolicyValidator) Run(
	endpoint string, path string, tlsConfig *options.WebhookTlsOptions) error {
	defer pv.dataStoreOpts.Close()

	multiplexer := http.NewServeMux()
	multiplexer.Handle(path, pv)

	server := http.Server{
		Handler: multiplexer,
		Addr:    endpoint,
	}

	certificateFile := filepath.Join(
		tlsConfig.TlsDirectory, tlsConfig.CertificateFileName)
	keyFile := filepath.Join(tlsConfig.TlsDirectory, tlsConfig.KeyFileName)

	klog.Info("Starting a policy validator")
	return server.ListenAndServeTLS(certificateFile, keyFile)
}
