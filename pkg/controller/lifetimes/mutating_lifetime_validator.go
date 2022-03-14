// Copyright (c) 2022 Fujitsu Limited

package lifetimes

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/lifetimes/trigger"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

type policyValidator struct {
	dataStoreOpts datastore.ValidationDataStoreOperations
}

type successorError struct {
	name string
	err  error
}

func newPolicyValidator(
	dataStoreOpts datastore.ValidationDataStoreOperations) *policyValidator {
	return &policyValidator{
		dataStoreOpts: dataStoreOpts,
	}
}

func (pv *policyValidator) finalize() error {
	err := pv.dataStoreOpts.Close()
	if err != nil {
		return fmt.Errorf("Could not close the data store client: %v", err)
	}

	return nil
}

func isNewEndTimeBeforeOldEndTime(
	newEndTimeStr string, oldEndTimeStr string) (bool, error) {
	newEndTime, err := time.Parse(time.RFC3339, newEndTimeStr)
	if err != nil {
		return false, err
	}

	oldEndTime, err := time.Parse(time.RFC3339, oldEndTimeStr)
	if err != nil {
		return false, err
	}

	if newEndTime.UTC().Before(oldEndTime.UTC()) {
		return true, nil
	}

	return false, nil
}

func updateSuccessorLifetime(
	lifetimesName string, newEndTime string,
	providerSharedInformer *providerSharedInformer,
	errorCh chan<- successorError) {
	namespace, name, err := cache.SplitMetaNamespaceKey(lifetimesName)
	if err != nil {
		errorCh <- successorError{
			name: lifetimesName,
			err: fmt.Errorf(
				"Invalid resource key for the successor lifetime %q",
				lifetimesName),
		}
		return
	}

	lifetime, err := providerSharedInformer.lifetimesLister.DataLifetimes(
		namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			errorCh <- successorError{
				name: lifetimesName,
				err: fmt.Errorf(
					"successor data lifetime %q in work queue no longer "+
						"exists",
					lifetimesName),
			}
			return
		}
	}

	triggerOpts, err := trigger.NewTrigger(lifetime.Spec.Trigger)
	if err != nil {
		errorCh <- successorError{
			name: lifetimesName,
			err:  err,
		}
		return
	}
	successorEndTime := triggerOpts.GetEndTimeString()

	before, err := isNewEndTimeBeforeOldEndTime(newEndTime, successorEndTime)
	if err != nil {
		errorCh <- successorError{
			name: lifetimesName,
			err:  err,
		}
		return
	} else if !before {
		errorCh <- successorError{
			name: lifetimesName,
			err:  nil,
		}
		return
	}

	triggerOpts.UpdateEndTime(newEndTime)

	lifetime.Status.Provider.LastUpdatedAt = metav1.NewTime(time.Now().UTC())

	ctx, cancel := util.GetTimeoutContext(util.DefaultKubeClientTimeout)
	defer cancel()

	_, err = providerSharedInformer.dataLifetimesClient.
		LifetimesV1alpha1().DataLifetimes(lifetime.Namespace).Update(
		ctx, lifetime, metav1.UpdateOptions{})
	if err != nil {
		errorCh <- successorError{
			name: lifetimesName,
			err: fmt.Errorf(
				"Failed to update the successor lifetime %q: %v",
				lifetimesName, err),
		}
		return
	}

	errorCh <- successorError{
		name: lifetimesName,
		err:  nil,
	}

	klog.Infof(
		"Updated the end time %q for the successor lifetime %q",
		newEndTime, lifetimesName)
}

func updateSuccessorLifetimes(
	successors map[string]struct{}, newEndTime string,
	providerSharedInformer *providerSharedInformer) error {
	errorCh := make(chan successorError)
	numSuccessors := len(successors)
	errors := make([]successorError, 0, numSuccessors)
	for successorLifetimesName, _ := range successors {
		lifetimesName := successorLifetimesName
		go updateSuccessorLifetime(
			lifetimesName, newEndTime, providerSharedInformer, errorCh)

	}

	for i := 0; i < numSuccessors; i++ {
		select {
		case err := <-errorCh:
			if err.err != nil {
				errors = append(errors, err)
			}
		}
	}

	if len(errors) != 0 {
		errMessage := fmt.Sprintf(
			"Failed to update the successor lifetime:\n")
		for _, err := range errors {
			errMessage += fmt.Sprintf("\t%q: %v\n", err.name, err.err)
		}

		return fmt.Errorf("%s", errMessage)
	}

	return nil
}

func (pv *policyValidator) addDataLifetimeToPolicyDataStore(
	lifetimesName string,
	providerSharedInformer *providerSharedInformer) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(lifetimesName)
	if err != nil {
		return fmt.Errorf("Invalid resource key: %s", lifetimesName)
	}

	lifetime, err := providerSharedInformer.lifetimesLister.DataLifetimes(
		namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf(
				"data lifetime %q in work queue no longer exists",
				lifetimesName)
		}
	}

	for _, output := range lifetime.Spec.OutputData {
		key, err := datastore.GetDataLifetimeKeyFromOutputData(output)
		if err != nil {
			return err
		}

		triggerOpts, err := trigger.NewTrigger(lifetime.Spec.Trigger)
		if err != nil {
			return err
		}
		newEndTime := triggerOpts.GetEndTimeString()

		oldLifetimeCache, err := pv.dataStoreOpts.GetDataLifetime(key)
		if err != nil {
			return err
		}

		lifetimeCache, err := pv.dataStoreOpts.AddDataLifetime(key, newEndTime)
		if err != nil {
			return err
		}

		klog.Infof(
			"Add the lifetime cache, key %q, end time %q", key, newEndTime)

		if oldLifetimeCache == nil {
			continue
		} else if oldLifetimeCache.EndTime != "" {
			before, err := isNewEndTimeBeforeOldEndTime(
				newEndTime, oldLifetimeCache.EndTime)
			if err != nil {
				return err
			} else if !before {
				continue
			}
		}

		err = updateSuccessorLifetimes(
			lifetimeCache.SuccessorLifetimes, newEndTime,
			providerSharedInformer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pv *policyValidator) deleteDataLifetimeFromPolicyDataStore(
	lifetimesName string,
	providerSharedInformer *providerSharedInformer) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(lifetimesName)
	if err != nil {
		return fmt.Errorf("Invalid resource key: %s", lifetimesName)
	}

	lifetime, err := providerSharedInformer.lifetimesLister.DataLifetimes(
		namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			return fmt.Errorf(
				"data lifetime %q in work queue no longer exists",
				lifetimesName)
		}
	}

	for _, output := range lifetime.Spec.OutputData {
		key, err := datastore.GetDataLifetimeKeyFromOutputData(output)
		if err != nil {
			return err
		}

		err = pv.dataStoreOpts.DeleteDataLifetime(key)
		if err != nil {
			return err
		}

		klog.Infof("Delete the lifetime cache, key %q", key)
	}

	return nil
}
